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
)

func binding() Binding {
	return Binding{"operation", "source", "node", "incarnation", "selected-v1", strings.Repeat("a", 64)}
}
func capture() ([]Process, error) {
	return []Process{{20, "1", "boot", "shim"}, {21, "2", "boot", "vmm"}}, nil
}
func openTest(t *testing.T) *Store {
	t.Helper()
	s, e := Open(filepath.Join(t.TempDir(), "fences"))
	if e != nil {
		t.Fatal(e)
	}
	return s
}

func TestSourceFencePersistenceRestartConfigOff(t *testing.T) {
	s := openTest(t)
	b := binding()
	calls := 0
	stop := func(context.Context, []Process) error {
		calls++
		if !errors.Is(s.Check(b.SandboxID), ErrFenced) {
			t.Fatal("stop before durable deny")
		}
		return errors.New("failed stop")
	}
	r, e := s.Stop(context.Background(), true, b, capture, stop)
	if e == nil || r.Stage != Admitted {
		t.Fatal(r, e)
	}
	s, e = Open(s.root)
	if e != nil {
		t.Fatal(e)
	}
	if !errors.Is(s.Check(b.SandboxID), ErrFenced) || !errors.Is(s.Any(), ErrFenced) {
		t.Fatal("restart lost gate")
	}
	r, e = s.Stop(context.Background(), false, b, func() ([]Process, error) { t.Fatal("recapture retry"); return nil, nil }, stop)
	if e == nil || r.Stage != Admitted || calls != 2 {
		t.Fatal(r, e, calls)
	}
	other := b
	other.SandboxID = "unfenced"
	_, e = s.Stop(context.Background(), false, other, capture, stop)
	if !errors.Is(e, ErrDisabled) {
		t.Fatal(e)
	}
}

func TestSourceFenceImmutableBinding(t *testing.T) {
	s := openTest(t)
	b := binding()
	s.Stop(context.Background(), true, b, capture, func(context.Context, []Process) error { return errors.New("stop failure") })
	for _, field := range []string{"operation", "node", "incarnation", "checkpoint", "digest"} {
		t.Run(field, func(t *testing.T) {
			v := b
			switch field {
			case "operation":
				v.OperationID = "foreign"
			case "node":
				v.NodeID = "foreign"
			case "incarnation":
				v.Incarnation = "stale"
			case "checkpoint":
				v.CheckpointID = "v2"
			case "digest":
				v.CheckpointDigest = strings.Repeat("b", 64)
			}
			_, e := s.Stop(context.Background(), true, v, capture, func(context.Context, []Process) error { t.Fatal("foreign stop"); return nil })
			if !errors.Is(e, ErrBinding) {
				t.Fatal(e)
			}
		})
	}
}

func TestSourceFenceCancelAndDeadlineRetainAdmission(t *testing.T) {
	for _, expired := range []bool{false, true} {
		s := openTest(t)
		ctx, cancel := context.WithCancel(context.Background())
		if expired {
			cancel()
		}
		r, e := s.Stop(ctx, true, binding(), capture, func(ctx context.Context, _ []Process) error { cancel(); return ctx.Err() })
		if !errors.Is(e, context.Canceled) {
			t.Fatal(e)
		}
		if expired {
			if r != nil || s.Check("source") != nil {
				t.Fatal("pre-cancel admitted")
			}
		} else {
			if r.Stage != Admitted || !errors.Is(s.Check("source"), ErrFenced) {
				t.Fatal("cancel unlocked")
			}
		}
		cancel()
	}
	s := openTest(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	r, e := s.Stop(ctx, true, binding(), capture, func(ctx context.Context, _ []Process) error { <-ctx.Done(); return ctx.Err() })
	if !errors.Is(e, context.DeadlineExceeded) || r.Stage != Admitted || s.Check("source") == nil {
		t.Fatal(r, e)
	}
}

func TestSourceFenceMissingUnreachableNeverFenced(t *testing.T) {
	for _, why := range []string{"missing", "unreachable", "timeout", "stale pid"} {
		t.Run(why, func(t *testing.T) {
			s := openTest(t)
			r, e := s.Stop(context.Background(), true, binding(), func() ([]Process, error) { return nil, errors.New(why) }, func(context.Context, []Process) error { t.Fatal("stop without identity"); return nil })
			if e == nil || r != nil || s.Check("source") != nil {
				t.Fatal(r, e)
			}
		})
	}
}

func TestSourceFenceCorruptionFailClosed(t *testing.T) {
	for _, raw := range []string{"", "{", `{ "version": 999 }`, `null`, `{ "stage": "completed" }`} {
		s := openTest(t)
		if e := os.WriteFile(s.filename("source"), []byte(raw), 0600); e != nil {
			t.Fatal(e)
		}
		if s.Check("source") == nil || s.Any() == nil {
			t.Fatal("corrupt record unlocked")
		}
		_, e := s.Stop(context.Background(), true, binding(), capture, func(context.Context, []Process) error { t.Fatal("corrupt stop"); return nil })
		if e == nil {
			t.Fatal("corrupt accepted")
		}
	}
}

func TestSourceFenceRuntimeStoppedIsNeverComplete(t *testing.T) {
	s := openTest(t)
	calls := 0
	stop := func(context.Context, []Process) error { calls++; return nil }
	for i := 0; i < 2; i++ {
		r, e := s.Stop(context.Background(), true, binding(), capture, stop)
		if !errors.Is(e, ErrDrainUnsupported) || r.Stage != RuntimeStopped {
			t.Fatal(r, e)
		}
	}
	if calls != 1 {
		t.Fatal("identical retry stopped twice")
	}
}

func TestSourceFenceRealProcessExitAndIdentity(t *testing.T) {
	start := func() (*exec.Cmd, Process) {
		cmd := exec.Command("sleep", "60")
		if e := cmd.Start(); e != nil {
			t.Fatal(e)
		}
		t.Cleanup(func() { cmd.Process.Kill(); cmd.Wait() })
		p, e := Identify(cmd.Process.Pid)
		if e != nil {
			t.Fatal(e)
		}
		return cmd, p
	}
	a, p := start()
	b, q := start()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	stale := p
	stale.StartTime = "0"
	if !errors.Is(StopProcesses(ctx, []Process{stale, q}), ErrBinding) {
		t.Fatal("stale process accepted")
	}
	if _, e := Identify(a.Process.Pid); e != nil {
		t.Fatal("stale request killed source")
	}
	if e := StopProcesses(ctx, []Process{p, q}); e != nil {
		t.Fatal(e)
	}
	a.Wait()
	b.Wait()
}

func TestSourceFencePreexistingPackageMutation(t *testing.T) {
	s := openTest(t)
	Barrier.RLock()
	entered := make(chan struct{})
	done := make(chan struct{})
	go func() {
		close(entered)
		Barrier.Lock()
		defer Barrier.Unlock()
		s.Stop(context.Background(), true, binding(), capture, func(context.Context, []Process) error { return errors.New("blocked stop") })
		close(done)
	}()
	<-entered
	select {
	case <-done:
		t.Fatal("admitted over inflight writer")
	case <-time.After(20 * time.Millisecond):
	}
	if s.Check("source") != nil {
		t.Fatal("fence persisted before writer drained")
	}
	Barrier.RUnlock()
	<-done
	if s.Check("source") == nil {
		t.Fatal("gate absent")
	}
}

func TestSourceFenceSelectedReferencesUnchanged(t *testing.T) {
	d := t.TempDir()
	ref := filepath.Join(d, "selected-v1")
	os.WriteFile(ref, []byte("immutable selected checkpoint"), 0600)
	s, e := Open(filepath.Join(d, "fences"))
	if e != nil {
		t.Fatal(e)
	}
	s.Stop(context.Background(), true, binding(), capture, func(context.Context, []Process) error { return errors.New("retained teardown unsupported") })
	b, e := os.ReadFile(ref)
	if e != nil || string(b) != "immutable selected checkpoint" {
		t.Fatal("reference modified", e)
	}
}

func TestSourceFenceFreshProcessPersistence(t *testing.T) {
	if root := os.Getenv("SOURCE_FENCE_TEST_ROOT"); root != "" {
		s, e := Open(root)
		if e != nil {
			t.Fatal(e)
		}
		if !errors.Is(s.Check("source"), ErrFenced) {
			t.Fatal("fresh process lost fence")
		}
		_, e = s.Stop(context.Background(), false, binding(), capture, func(context.Context, []Process) error { return errors.New("retained unsupported") })
		if e == nil || errors.Is(e, ErrDisabled) {
			t.Fatal("config off lost identical retry", e)
		}
		return
	}
	s := openTest(t)
	s.Stop(context.Background(), true, binding(), capture, func(context.Context, []Process) error { return errors.New("stop failed") })
	c := exec.Command(os.Args[0], "-test.run=^TestSourceFenceFreshProcessPersistence$")
	c.Env = append(os.Environ(), "SOURCE_FENCE_TEST_ROOT="+s.root)
	if out, e := c.CombinedOutput(); e != nil {
		t.Fatalf("fresh process: %v %s", e, out)
	}
}
