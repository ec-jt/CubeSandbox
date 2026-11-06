// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package sourcefence

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

func Identify(pid int) (Process, error) {
	p := Process{PID: pid}
	if pid <= 1 || pid == os.Getpid() {
		return p, fmt.Errorf("unsafe runtime pid")
	}
	boot, e := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if e != nil {
		return p, e
	}
	p.BootID = strings.TrimSpace(string(boot))
	b, e := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if e != nil {
		return p, e
	}
	i := strings.LastIndexByte(string(b), ')')
	if i < 0 {
		return p, fmt.Errorf("invalid proc stat")
	}
	f := strings.Fields(string(b[i+1:]))
	if len(f) < 20 {
		return p, fmt.Errorf("short proc stat")
	}
	p.StartTime = f[19]
	if _, e = strconv.ParseUint(p.StartTime, 10, 64); e != nil {
		return p, e
	}
	p.Executable, e = os.Readlink(fmt.Sprintf("/proc/%d/exe", pid))
	return p, e
}

// StopProcesses kills only pinned process incarnations and waits for kernel
// exit notification. It does not invoke destroy hooks, unlink snapshots, or
// claim that the separate native storage target has drained pending I/O.
func StopProcesses(ctx context.Context, ps []Process) error {
	fds := make([]int, 0, len(ps))
	defer func() {
		for _, fd := range fds {
			unix.Close(fd)
		}
	}()
	for _, p := range ps {
		fd, e := unix.PidfdOpen(p.PID, 0)
		if e != nil {
			return fmt.Errorf("pin runtime %d: %w", p.PID, e)
		}
		fds = append(fds, fd)
		current, e := Identify(p.PID)
		if e != nil {
			return e
		}
		if current != p {
			return ErrBinding
		}
	}
	if e := ctx.Err(); e != nil {
		return e
	}
	for _, fd := range fds {
		if e := unix.PidfdSendSignal(fd, unix.SIGKILL, nil, 0); e != nil {
			return e
		}
	}
	for _, fd := range fds {
		for {
			if e := ctx.Err(); e != nil {
				return e
			}
			poll := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}
			_, e := unix.Poll(poll, 20)
			if e == unix.EINTR {
				continue
			}
			if e != nil {
				return e
			}
			if poll[0].Revents&unix.POLLIN != 0 {
				break
			}
		}
	}
	return nil
}

func RuntimeExecutable(p Process, kind string) bool {
	b := filepath.Base(p.Executable)
	if kind == "shim" {
		return b == "containerd-shim-cube-rs"
	}
	return kind == "vmm" && (b == "cube-hypervisor" || b == "cloud-hypervisor")
}
