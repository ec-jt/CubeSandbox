package sourcefence

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestRetainedExactCapabilityConfig(t *testing.T) {
	good := RetainedConfig{"/qualified/containerd-shim-cube-rs", RetainedEmbeddedSHA256, RetainedEmbeddedForm}
	if err := good.validate(); err != nil {
		t.Fatal(err)
	}
	for _, cfg := range []RetainedConfig{{}, {good.RuntimePath, strings.Repeat("a", 64), good.Form}, {good.RuntimePath, good.SHA256, "separate-cube-runtime"}, {"containerd-shim-cube-rs", good.SHA256, good.Form}} {
		if _, err := CaptureRetained(context.Background(), cfg, "/missing"); err == nil {
			t.Fatal("unqualified config accepted")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := CaptureRetained(ctx, good, "/missing"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestRetainedWrongRecordBeforeArm(t *testing.T) {
	r := &RetainedRuntime{pidfd: -1, process: Process{123, "1", "boot", "/shim"}}
	for _, record := range []Record{{}, {Version: 1, Binding: binding(), Stage: Admitted, Processes: []Process{{124, "1", "boot", "/shim"}}}, {Version: 1, Binding: binding(), Stage: RuntimeStopped, Processes: r.Processes()}} {
		if err := r.Stop(context.Background(), record); !errors.Is(err, ErrBinding) {
			t.Fatal(err)
		}
	}
}

func TestRetainedMissingAndWrongBinary(t *testing.T) {
	good := RetainedConfig{"/qualified/missing-runtime", RetainedEmbeddedSHA256, RetainedEmbeddedForm}
	if _, err := CaptureRetained(context.Background(), good, "/missing"); err == nil {
		t.Fatal("missing binary accepted")
	}
	// A real root-owned executable is not qualified merely because its path
	// is supplied in node configuration. No process or arm helper is started.
	p, err := exec.LookPath("sleep")
	if err != nil {
		t.Fatal(err)
	}
	p, err = filepath.EvalSymlinks(p)
	if err != nil {
		t.Fatal(err)
	}
	good.RuntimePath = p
	if _, err = CaptureRetained(context.Background(), good, "/missing"); err == nil || !strings.Contains(err.Error(), "digest mismatch") {
		t.Fatal("foreign binary did not fail digest qualification", err)
	}
}

func TestRetainedOldProcessRejectedWithoutSignal(t *testing.T) {
	c := exec.Command("sleep", "30")
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { c.Process.Kill(); c.Wait() }()
	p, err := Identify(c.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	p.StartTime = "0"
	r := &RetainedRuntime{pidfd: -1, process: p}
	record := Record{Version: 1, Binding: binding(), Stage: Admitted, Processes: []Process{p}}
	if err := r.Stop(context.Background(), record); !errors.Is(err, ErrBinding) {
		t.Fatal(err)
	}
	if err := c.Process.Signal(unix.Signal(0)); err != nil {
		t.Fatal("unverified process was signalled", err)
	}
}

func TestRetainedUntrustedPath(t *testing.T) {
	p := filepath.Join(t.TempDir(), "shim")
	if err := os.Symlink("/usr/bin/sleep", p); err != nil {
		t.Fatal(err)
	}
	if err := trustedPath(p, false); err == nil {
		t.Fatal("symlink accepted")
	}
}

func TestRetainedPidfdTimeoutCancelAndObservedExit(t *testing.T) {
	c := exec.Command("sleep", "30")
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	defer c.Process.Kill()
	fd, err := unix.PidfdOpen(c.Process.Pid, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(fd)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	if err := waitRetainedExit(ctx, fd); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	cancel()
	ctx, cancel = context.WithCancel(context.Background())
	cancel()
	if err := waitRetainedExit(ctx, fd); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := c.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	c.Wait()
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := waitRetainedExit(ctx, fd); err != nil {
		t.Fatal(err)
	}
}

func TestRetainedEmbeddedAdmissionOrderingAndLostResult(t *testing.T) {
	s := openTest(t)
	b := binding()
	p := Process{20, "1", "boot", "/shim"}
	capture := func() ([]Process, error) { return []Process{p}, nil }
	calls := 0
	stop := func(ctx context.Context, ps []Process) error {
		calls++
		r, err := s.load(b.SandboxID)
		if err != nil || r.Stage != Admitted || r.Binding != b || len(r.Processes) != 1 || r.Processes[0] != p {
			t.Fatal("callback before exact durable record", r, err)
		}
		if calls == 1 {
			return errors.New("arm result lost")
		}
		return nil
	}
	r, err := s.Stop(context.Background(), true, b, capture, stop)
	if err == nil || r.Stage != Admitted {
		t.Fatal(r, err)
	}
	r, err = s.Stop(context.Background(), false, b, capture, stop)
	if !errors.Is(err, ErrDrainUnsupported) || r.Stage != RuntimeStopped {
		t.Fatal(r, err)
	}
	r, err = s.Stop(context.Background(), false, b, func() ([]Process, error) { t.Fatal("recaptured stopped runtime"); return nil, nil }, stop)
	if !errors.Is(err, ErrDrainUnsupported) || calls != 2 {
		t.Fatal(r, err, calls)
	}
	if !errors.Is(s.Check(b.SandboxID), ErrFenced) {
		t.Fatal("lost permanent deny")
	}
}
