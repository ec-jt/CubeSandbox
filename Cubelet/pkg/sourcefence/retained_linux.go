// Copyright (c) 2026 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package sourcefence

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	task "github.com/containerd/containerd/api/runtime/task/v2"
	"github.com/containerd/ttrpc"
	"golang.org/x/sys/unix"
)

// Capability is qualified by exact reviewed build bytes, not a version string,
// caller annotation, or a binary's own claim. Adding a build requires review of
// its embedded native stop, immutable arm protocol and fresh delete handler.
const RetainedEmbeddedSHA256 = "196894089efdb1134bec7fa5c18d78bf832fa8ed2076c6a051a6aec4474b9c4d"
const RetainedEmbeddedForm = "embedded-v28-retained-v1"

// RetainedConfig is node configuration, never populated from request labels.
// The runtime installation must remain read-only until all retained sources
// are explicitly reconciled. Privileged remount/namespace changes are outside
// this local protocol and must not race it.
type RetainedConfig struct {
	RuntimePath string `toml:"runtime_path"`
	SHA256      string `toml:"sha256"`
	Form        string `toml:"form"`
}

func (c RetainedConfig) validate() error {
	if c.Form != RetainedEmbeddedForm || c.SHA256 != RetainedEmbeddedSHA256 || !filepath.IsAbs(c.RuntimePath) || filepath.Clean(c.RuntimePath) != c.RuntimePath {
		return fmt.Errorf("retained runtime capability not qualified by node configuration")
	}
	return nil
}

// RetainedRuntime owns the pidfd, executable fd and authenticated connection
// from capture through durable admission, arm, Update and observed exit.
type RetainedRuntime struct {
	process Process
	pidfd   int
	binary  *os.File
	client  *ttrpc.Client
	bundle  string
	config  RetainedConfig
}

func (r *RetainedRuntime) Close() {
	if r.client != nil {
		r.client.Close()
	}
	if r.binary != nil {
		r.binary.Close()
	}
	if r.pidfd >= 0 {
		unix.Close(r.pidfd)
		r.pidfd = -1
	}
}

func trustedPath(path string, directory bool) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return ErrBinding
	}
	for p := path; ; p = filepath.Dir(p) {
		fi, err := os.Lstat(p)
		if err != nil {
			return err
		}
		st, ok := fi.Sys().(*syscall.Stat_t)
		if !ok || st.Uid != 0 || fi.Mode().Perm()&0022 != 0 || fi.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("untrusted retained path %s", p)
		}
		if (p != path || directory) && !fi.IsDir() {
			return ErrBinding
		}
		if p == "/" {
			break
		}
	}
	return nil
}

func readTrusted(path string) ([]byte, error) {
	if err := trustedPath(path, false); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_RDONLY|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !fi.Mode().IsRegular() || fi.Size() > 65536 {
		return nil, ErrBinding
	}
	return io.ReadAll(io.LimitReader(f, 65537))
}

func readPID(path string) (int, error) {
	b, err := readTrusted(path)
	if err != nil {
		return 0, err
	}
	p, err := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil || p <= 1 {
		return 0, ErrBinding
	}
	return p, nil
}

func (r *RetainedRuntime) recheck() error {
	p, err := Identify(r.process.PID)
	if err != nil {
		return err
	}
	if p != r.process {
		return ErrBinding
	}
	// This build embeds VMM threads. A distinct child runtime is not qualified.
	children, err := filepath.Glob(fmt.Sprintf("/proc/%d/task/*/children", p.PID))
	if err != nil || len(children) == 0 {
		return fmt.Errorf("runtime membership unavailable")
	}
	for _, path := range children {
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if len(bytes.TrimSpace(b)) != 0 {
			return fmt.Errorf("separate runtime/child process unsupported")
		}
	}
	for _, name := range []string{"shim.pid", "vmm.pid"} {
		pid, err := readPID(filepath.Join(r.bundle, name))
		if err != nil {
			return err
		}
		if pid != p.PID {
			return fmt.Errorf("embedded runtime membership mismatch")
		}
	}
	b, err := readTrusted(filepath.Join(r.bundle, "shim-binary-path"))
	if err != nil {
		return err
	}
	if string(b) != r.config.RuntimePath {
		return fmt.Errorf("containerd reaper path mismatch")
	}
	fi, err := os.Stat(r.config.RuntimePath)
	if err != nil {
		return err
	}
	bi, err := r.binary.Stat()
	if err != nil {
		return err
	}
	if !os.SameFile(fi, bi) {
		return fmt.Errorf("reaper binary changed")
	}
	ei, err := os.Stat(fmt.Sprintf("/proc/%d/exe", p.PID))
	if err != nil {
		return err
	}
	if !os.SameFile(ei, bi) {
		return fmt.Errorf("running shim differs from pinned reaper")
	}
	var fs unix.Statfs_t
	if err = unix.Fstatfs(int(r.binary.Fd()), &fs); err != nil {
		return err
	}
	if fs.Flags&unix.ST_RDONLY == 0 {
		return fmt.Errorf("retained reaper installation must be read-only")
	}
	return nil
}

func CaptureRetained(ctx context.Context, cfg RetainedConfig, bundle string) (_ *RetainedRuntime, err error) {
	if err = cfg.validate(); err != nil {
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	if err = trustedPath(cfg.RuntimePath, false); err != nil {
		return nil, err
	}
	r := &RetainedRuntime{pidfd: -1, bundle: bundle, config: cfg}
	defer func() {
		if err != nil {
			r.Close()
		}
	}()
	r.binary, err = os.OpenFile(cfg.RuntimePath, os.O_RDONLY|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	fi, err := r.binary.Stat()
	if err != nil {
		return nil, err
	}
	if !fi.Mode().IsRegular() || fi.Mode().Perm()&0111 == 0 {
		return nil, ErrBinding
	}
	h := sha256.New()
	if _, err = io.Copy(h, r.binary); err != nil {
		return nil, err
	}
	if hex.EncodeToString(h.Sum(nil)) != cfg.SHA256 {
		return nil, fmt.Errorf("retained runtime binary digest mismatch")
	}
	pid, err := readPID(filepath.Join(bundle, "shim.pid"))
	if err != nil {
		return nil, err
	}
	r.pidfd, err = unix.PidfdOpen(pid, 0)
	if err != nil {
		return nil, err
	}
	r.process, err = Identify(pid)
	if err != nil {
		return nil, err
	}
	if r.process.Executable != cfg.RuntimePath {
		return nil, fmt.Errorf("running shim path mismatch")
	}
	if err = r.recheck(); err != nil {
		return nil, err
	}
	b, err := readTrusted(filepath.Join(bundle, "address"))
	if err != nil {
		return nil, err
	}
	address := strings.TrimPrefix(string(bytes.TrimSpace(b)), "unix://")
	if !strings.HasPrefix(address, "/") && !strings.HasPrefix(address, "@") {
		return nil, fmt.Errorf("unsupported retained TTRPC address")
	}
	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", address)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			conn.Close()
		}
	}()
	raw, err := conn.(*net.UnixConn).SyscallConn()
	if err != nil {
		return nil, err
	}
	var cred *unix.Ucred
	var peerErr error
	if err = raw.Control(func(fd uintptr) { cred, peerErr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED) }); err != nil {
		return nil, err
	}
	if peerErr != nil {
		return nil, peerErr
	}
	if cred.Uid != 0 || int(cred.Pid) != pid {
		return nil, fmt.Errorf("retained TTRPC peer mismatch")
	}
	r.client = ttrpc.NewClient(conn)
	if err = r.recheck(); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *RetainedRuntime) Processes() []Process { return []Process{r.process} }

// Stop never signals a PID and never falls back to ordinary Delete. A timeout
// retains both deny stores and cannot authorize destination recovery.
func (r *RetainedRuntime) Stop(ctx context.Context, record Record) error {
	if record.Version != 1 || record.Stage != Admitted || record.Binding.Validate() != nil || len(record.Processes) != 1 || record.Processes[0] != r.process {
		return ErrBinding
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := r.recheck(); err != nil {
		return err
	}
	b, err := json.Marshal(record)
	if err != nil {
		return err
	}
	// Exec the verified open inode, not a second path lookup. The exact record
	// is fsynced by the qualified shim arm CLI before it exits successfully.
	cmd := exec.CommandContext(ctx, "/proc/self/fd/3", "--arm-retained-runtime")
	cmd.ExtraFiles = []*os.File{r.binary}
	cmd.Stdin = bytes.NewReader(b)
	cmd.Env = []string{"PATH=/usr/bin:/bin"}
	cmd.WaitDelay = time.Second
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("retained durable handoff failed: %w: %s", err, out)
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	if err = r.recheck(); err != nil {
		return err
	}
	b, err = json.Marshal(record.Binding)
	if err != nil {
		return err
	}
	_, updateErr := task.NewTaskClient(r.client).Update(ctx, &task.UpdateTaskRequest{ID: record.Binding.SandboxID, Annotations: map[string]string{
		"cube.shimapi.update.action":         "StopRetainedRuntime",
		"cube.recovery.source_fence.binding": string(b),
	}})
	// EOF/lost ACK is harmless only when this exact pinned process has exited.
	if err = waitRetainedExit(ctx, r.pidfd); err != nil {
		return fmt.Errorf("retained exit not observed (Update: %v): %w", updateErr, err)
	}
	return nil
}

func waitRetainedExit(ctx context.Context, fd int) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		poll := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}
		_, err := unix.Poll(poll, 20)
		if err == unix.EINTR {
			continue
		}
		if err != nil {
			return err
		}
		if poll[0].Revents&unix.POLLIN != 0 {
			return nil
		}
		if poll[0].Revents&(unix.POLLERR|unix.POLLNVAL) != 0 {
			return fmt.Errorf("invalid retained pidfd")
		}
	}
}
