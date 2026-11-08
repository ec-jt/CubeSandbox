// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//

package resourcesource

import (
	"testing"
)

func TestCpuUtilPctComputesBusyRatio(t *testing.T) {
	prev := cpuTimes{user: 100, nice: 0, system: 50, idle: 800, iowait: 50, irq: 0, softirq: 0, steal: 0}
	// Elapse: +100 user, +50 system, +300 idle, +50 iowait. busy delta=150, total delta=500.
	cur := cpuTimes{user: 200, nice: 0, system: 100, idle: 1100, iowait: 100, irq: 0, softirq: 0, steal: 0}
	got := cpuUtilPct(prev, cur)
	want := 150.0 * 100.0 / 500.0
	if got != want {
		t.Fatalf("cpuUtilPct = %v, want %v", got, want)
	}
}

func TestCpuUtilPctZeroDelta(t *testing.T) {
	a := cpuTimes{user: 1, system: 1, idle: 1}
	if got := cpuUtilPct(a, a); got != 0 {
		t.Fatalf("zero delta should give 0, got %v", got)
	}
}

func TestCpuUtilPctCounterWrap(t *testing.T) {
	prev := cpuTimes{user: 1000, system: 1000, idle: 1000}
	cur := cpuTimes{user: 10, system: 10, idle: 10}
	if got := cpuUtilPct(prev, cur); got != 0 {
		t.Fatalf("wrapped counters should give 0, got %v", got)
	}
}

func TestIsPhysicalDisk(t *testing.T) {
	cases := map[string]bool{
		"sda":        true,
		"sda1":       false,
		"vda":        true,
		"vdb2":       false,
		"xvda":       true,
		"nvme0n1":    true,
		"nvme0n1p1":  false,
		"nvme1n1":    true,
		"mmcblk0":    true,
		"mmcblk0p2":  false,
		"loop0":      false,
		"ram0":       false,
		"dm-0":       false,
		"md0":        false,
		"sr0":        false,
		"zram0":      false,
		"hdb":        true,
		"hdb3":       false,
	}
	for dev, want := range cases {
		if got := isPhysicalDisk(dev); got != want {
			t.Errorf("isPhysicalDisk(%q) = %v, want %v", dev, got, want)
		}
	}
}

func TestSubDelta(t *testing.T) {
	if got := subDelta(10, 4); got != 6 {
		t.Fatalf("subDelta(10,4)=%v want 6", got)
	}
	if got := subDelta(4, 10); got != 0 {
		t.Fatalf("subDelta(4,10)=%v want 0 (wrap guard)", got)
	}
}

func TestSampleFirstCallReturnsBaseline(t *testing.T) {
	rc := NewRealMetricsCollector()
	m := rc.Sample()
	if m == nil {
		t.Fatal("Sample returned nil")
	}
	// First sample establishes the baseline: no rates yet.
	if m.CpuUtilPct != 0 || m.DiskIOPS != 0 || m.DiskReadBps != 0 || m.DiskWriteBps != 0 {
		t.Fatalf("first sample should have zero rates, got %+v", m)
	}
	// Load average is absolute (not a rate) and may be non-zero on a live
	// host; just assert it is non-negative.
	if m.Load1 < 0 || m.Load5 < 0 || m.Load15 < 0 {
		t.Fatalf("load averages must be non-negative, got %+v", m)
	}
}
