// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//

// realmetrics.go implements a Linux /proc collector for *real* host
// utilisation (as opposed to the quota-allocation view). The cubelet
// heartbeat invokes Sample() once per report interval; deltas are computed
// against the previous sample so no hot polling loop is introduced.
package resourcesource

import (
	"bufio"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// monotonicNow returns seconds from a monotonic clock. Declared as a
// package-level variable so tests can stub it to control the measured
// interval between samples.
var monotonicNow = func() float64 {
	return time.Since(processStartMono).Seconds()
}

// processStartMono anchors the monotonic clock for interval measurement.
var processStartMono = time.Now()

// defaultRealMetricsCollector is the process-wide sampler used by the
// cubelet heartbeat. It is created lazily on first SampleRealMetrics() so
// the baseline is anchored at first use rather than at package init. Tests
// can swap it via SetRealMetricsCollector.
var (
	realMetricsMu        sync.Mutex
	realMetricsCollector *RealMetricsCollector
)

// SetRealMetricsCollector overrides the process-wide sampler (tests).
// Passing nil restores the default lazy behaviour.
func SetRealMetricsCollector(c *RealMetricsCollector) {
	realMetricsMu.Lock()
	defer realMetricsMu.Unlock()
	realMetricsCollector = c
}

// SampleRealMetrics samples the process-wide collector, creating it on
// first use. Returns nil only if sampling itself is impossible; callers
// treat a nil return as "no real metrics this tick".
func SampleRealMetrics() *RealMetrics {
	realMetricsMu.Lock()
	c := realMetricsCollector
	if c == nil {
		c = NewRealMetricsCollector()
		realMetricsCollector = c
	}
	realMetricsMu.Unlock()
	return c.Sample()
}

// RealMetrics is the pure-data twin of masterclient.RealMetrics. It carries
// observed host CPU / load / disk-IO rates for one heartbeat tick. All
// fields are best-effort: a subsystem that cannot be read leaves its value
// zero-valued rather than failing the whole sample.
type RealMetrics struct {
	// CpuUtilPct is aggregate CPU busy percentage (0-100) across all cores,
	// computed from the /proc/stat delta since the previous sample.
	CpuUtilPct float64
	// PerCoreUtils holds per-core busy percentages (0-100), ordered by cpuN.
	PerCoreUtils []float64
	// Load1 / Load5 / Load15 are the host load averages from /proc/loadavg.
	Load1  float64
	Load5  float64
	Load15 float64
	// DiskIOPS is reads+writes per second, aggregated over physical block
	// devices (loop/ram excluded), from the /proc/diskstats delta.
	DiskIOPS float64
	// DiskReadBps / DiskWriteBps are read/write throughput in bytes/sec.
	DiskReadBps  float64
	DiskWriteBps float64
}

// RealMetricsCollector samples /proc on demand and rates the values against
// the previous sample. It is safe for concurrent use; the cubelet heartbeat
// is the only caller but the mutex keeps tests and future callers honest.
type RealMetricsCollector struct {
	mu sync.Mutex

	prevCPU   map[string]cpuTimes
	prevDisk  map[string]diskTimes
	hasCPU    bool
	hasDisk   bool

	// lastSampleMono is the monotonic timestamp (seconds) of the previous
	// sample, used to rate disk deltas per elapsed second.
	lastSampleMono float64
}

type cpuTimes struct {
	user, nice, system, idle, iowait, irq, softirq, steal uint64
}

func (c cpuTimes) total() uint64 {
	return c.user + c.nice + c.system + c.idle + c.iowait + c.irq + c.softirq + c.steal
}

func (c cpuTimes) busy() uint64 {
	// Busy = everything except idle and iowait (matches top/mpstat %busy).
	return c.total() - c.idle - c.iowait
}

type diskTimes struct {
	readIOs, writeIOs     uint64
	readSectors, writeSectors uint64
}

// NewRealMetricsCollector returns a collector. The first Sample() call
// establishes the baseline and returns a zero-valued RealMetrics (rates are
// undefined over a zero interval), so callers should treat the first tick
// after boot as empty.
func NewRealMetricsCollector() *RealMetricsCollector {
	return &RealMetricsCollector{}
}

// Sample reads /proc/stat, /proc/loadavg and /proc/diskstats once and
// returns rates relative to the previous Sample(). It never blocks on a
// timer; callers are expected to invoke it on their existing report
// interval so no additional hot loop is added.
func (rc *RealMetricsCollector) Sample() *RealMetrics {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	out := &RealMetrics{}

	curCPU, agg, cores := readProcStat()
	if rc.hasCPU {
		out.CpuUtilPct = cpuUtilPct(rc.prevCPU["cpu"], curCPU["cpu"])
		if len(cores) > 0 {
			out.PerCoreUtils = make([]float64, len(cores))
			for i, name := range cores {
				out.PerCoreUtils[i] = cpuUtilPct(rc.prevCPU[name], curCPU[name])
			}
		}
	}
	rc.prevCPU = curCPU
	rc.hasCPU = rc.hasCPU || agg

	out.Load1, out.Load5, out.Load15 = readProcLoadavg()

	curDisk := readProcDiskstats()
	if rc.hasDisk {
		var readIOs, writeIOs, readSec, writeSec uint64
		for dev, cur := range curDisk {
			prev, ok := rc.prevDisk[dev]
			if !ok {
				continue
			}
			readIOs += subDelta(cur.readIOs, prev.readIOs)
			writeIOs += subDelta(cur.writeIOs, prev.writeIOs)
			readSec += subDelta(cur.readSectors, prev.readSectors)
			writeSec += subDelta(cur.writeSectors, prev.writeSectors)
		}
		// Rates are per elapsed wall second. The collector does not track
		// time itself; the caller's fixed interval is the natural unit, so
		// we store raw deltas and let the reporter divide by its interval.
		// However, to keep the payload self-contained we compute per-second
		// rates using the measured interval between samples.
		// intervalSeconds is supplied via Sample()'s wall clock; see
		// sampleIntervalSeconds below.
		secs := rc.sampleIntervalSeconds()
		if secs <= 0 {
			secs = 1
		}
		out.DiskIOPS = float64(readIOs+writeIOs) / secs
		// Sectors are always 512 bytes on Linux.
		out.DiskReadBps = float64(readSec*512) / secs
		out.DiskWriteBps = float64(writeSec*512) / secs
	}
	rc.prevDisk = curDisk
	rc.hasDisk = true

	return out
}

// sampleIntervalSeconds returns elapsed seconds since the previous sample.
// It uses a monotonic wall clock read; declared here so tests can stub it.
func (rc *RealMetricsCollector) sampleIntervalSeconds() float64 {
	now := monotonicNow()
	if rc.lastSampleMono == 0 {
		rc.lastSampleMono = now
		return 0
	}
	d := now - rc.lastSampleMono
	rc.lastSampleMono = now
	return d
}

// --- /proc readers (pure functions, easily unit-tested) ---

// readProcStat parses /proc/stat. It returns a map of cpu-name -> times
// (including the aggregate "cpu" row and each "cpuN" row), whether the
// aggregate row was present, and the ordered list of per-core row names.
func readProcStat() (map[string]cpuTimes, bool, []string) {
	out := map[string]cpuTimes{}
	var cores []string
	hasAgg := false
	f, err := os.Open("/proc/stat")
	if err != nil {
		return out, false, nil
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "cpu") {
			// cpu rows are first; stop early once past them.
			if len(out) > 0 {
				break
			}
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 5 {
			continue
		}
		name := fields[0]
		// Only accept "cpu" and "cpuN".
		if name != "cpu" && !strings.HasPrefix(name, "cpu") {
			continue
		}
		if name != "cpu" {
			// verify suffix is numeric
			if _, err := strconv.Atoi(name[3:]); err != nil {
				continue
			}
		}
		ct := cpuTimes{}
		vals := make([]uint64, 0, 8)
		for _, s := range fields[1:] {
			v, err := strconv.ParseUint(s, 10, 64)
			if err != nil {
				break
			}
			vals = append(vals, v)
		}
		if len(vals) < 4 {
			continue
		}
		// Pad to 8 fields (user nice system idle iowait irq softirq steal).
		for len(vals) < 8 {
			vals = append(vals, 0)
		}
		ct.user, ct.nice, ct.system, ct.idle = vals[0], vals[1], vals[2], vals[3]
		ct.iowait, ct.irq, ct.softirq, ct.steal = vals[4], vals[5], vals[6], vals[7]
		out[name] = ct
		if name == "cpu" {
			hasAgg = true
		} else {
			cores = append(cores, name)
		}
	}
	sort.Slice(cores, func(i, j int) bool {
		ai, _ := strconv.Atoi(cores[i][3:])
		aj, _ := strconv.Atoi(cores[j][3:])
		return ai < aj
	})
	return out, hasAgg, cores
}

// cpuUtilPct computes busy percentage between two samples of the same cpu.
// Returns 0 when the total delta is zero (no time elapsed).
func cpuUtilPct(prev, cur cpuTimes) float64 {
	totalDelta := cur.total() - prev.total()
	if totalDelta == 0 {
		return 0
	}
	// Guard against counter wrap (extremely rare on 64-bit).
	if cur.total() < prev.total() || cur.busy() < prev.busy() {
		return 0
	}
	busyDelta := cur.busy() - prev.busy()
	pct := float64(busyDelta) * 100.0 / float64(totalDelta)
	if pct > 100 {
		pct = 100
	}
	if pct < 0 {
		pct = 0
	}
	return pct
}

// readProcLoadavg reads /proc/loadavg and returns load1, load5, load15.
func readProcLoadavg() (float64, float64, float64) {
	f, err := os.Open("/proc/loadavg")
	if err != nil {
		return 0, 0, 0
	}
	defer f.Close()
	var l1, l5, l15 float64
	sc := bufio.NewScanner(f)
	if sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) >= 3 {
			l1, _ = strconv.ParseFloat(fields[0], 64)
			l5, _ = strconv.ParseFloat(fields[1], 64)
			l15, _ = strconv.ParseFloat(fields[2], 64)
		}
	}
	return l1, l5, l15
}

// readProcDiskstats parses /proc/diskstats and returns per-device counters
// for physical block devices only (loop/ram/dm excluded). Only whole-disk
// devices (not partitions) are aggregated to avoid double counting.
func readProcDiskstats() map[string]diskTimes {
	out := map[string]diskTimes{}
	f, err := os.Open("/proc/diskstats")
	if err != nil {
		return out
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) < 14 {
			continue
		}
		dev := fields[2]
		if !isPhysicalDisk(dev) {
			continue
		}
		dt := diskTimes{}
		dt.readIOs, _ = strconv.ParseUint(fields[3], 10, 64)
		dt.readSectors, _ = strconv.ParseUint(fields[5], 10, 64)
		dt.writeIOs, _ = strconv.ParseUint(fields[7], 10, 64)
		dt.writeSectors, _ = strconv.ParseUint(fields[9], 10, 64)
		out[dev] = dt
	}
	return out
}

// isPhysicalDisk reports whether a block device name is a whole physical
// disk we should count. Excludes loop, ram, device-mapper, and partitions.
func isPhysicalDisk(dev string) bool {
	if dev == "" {
		return false
	}
	prefixes := []string{"loop", "ram", "dm-", "md", "sr", "zram", "nbd", "rbd"}
	for _, p := range prefixes {
		if strings.HasPrefix(dev, p) {
			return false
		}
	}
	// nvme whole-disk: nvme0n1 (partition nvme0n1p1 -> reject)
	if strings.HasPrefix(dev, "nvme") {
		return !strings.Contains(dev, "p")
	}
	// mmcblk whole-disk: mmcblk0 (partition mmcblk0p1 -> reject)
	if strings.HasPrefix(dev, "mmcblk") {
		return !strings.Contains(dev, "p")
	}
	// vd/sd/hd/xvd whole-disk: sda, vda, xvda (partition sda1 -> reject if trailing digit)
	last := dev[len(dev)-1]
	if last >= '0' && last <= '9' {
		// trailing digit means partition for sd/vd/xvd/hd naming
		return false
	}
	return true
}

// subDelta returns cur-prev guarding against counter wrap.
func subDelta(cur, prev uint64) uint64 {
	if cur < prev {
		return 0
	}
	return cur - prev
}
