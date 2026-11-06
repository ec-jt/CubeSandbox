//go:build linux

package cubecow

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// This test is deliberately opt-in: run only inside the disposable outer VM on
// a new root-owned XFS fixture. Never set this to a production volume directory.
func TestXFSDrainNative(t *testing.T) {
	dir := os.Getenv("CUBE_XFS_DRAIN_TEST_DIR")
	if dir == "" {
		t.Skip("requires isolated native XFS fixture")
	}
	path := filepath.Join(dir, "new-source-volume")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.Write([]byte("retained-native-data")); err != nil {
		t.Fatal(err)
	}
	h, err := OpenXFSDrainFile(path)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	identity := h.Identity()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	err = h.Drain(ctx)
	cancel()
	if err == nil {
		t.Fatal("existing writer accepted as drained")
	}
	if _, err = f.WriteAt([]byte("still-writable"), 128); err != nil {
		t.Fatal(err)
	}
	// Changing size invalidates the captured identity, rather than silently
	// rebinding the selected generation after stop.
	f.Close()
	if err = h.Drain(context.Background()); err == nil {
		t.Fatal("size substitution accepted")
	}
	h.Close()
	h, err = OpenXFSDrainFile(path)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	identity = h.Identity()
	// A live shared writable mapping must also prevent lease acquisition, even
	// after the userspace fd is closed. This is not an idle-counter test.
	mapped, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	data, err := unix.Mmap(int(mapped.Fd()), 0, 142, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		t.Fatal(err)
	}
	mapped.Close()
	ctx, cancel = context.WithTimeout(context.Background(), 50*time.Millisecond)
	err = h.Drain(ctx)
	cancel()
	if err == nil {
		t.Fatal("mapped writer accepted as drained")
	}
	data[0] = 'M'
	if err = unix.Munmap(data); err != nil {
		t.Fatal(err)
	}
	writer, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		done <- h.Drain(ctx)
	}()
	select {
	case err := <-done:
		t.Fatalf("returned with writer held: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	if _, err = writer.WriteAt([]byte("inflight-finished"), 0); err != nil {
		t.Fatal(err)
	}
	writer.Close()
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	if h.Identity() != identity {
		t.Fatal("identity changed")
	}
	if w, e := os.OpenFile(path, os.O_WRONLY, 0); e == nil {
		w.Close()
		t.Fatal("new writer admitted after drain")
	}
	if err = os.Remove(path); err == nil {
		t.Fatal("drained source deleted")
	}
	b, err := os.ReadFile(path)
	if err != nil || string(b[:17]) != "inflight-finished" {
		t.Fatalf("retained data: %q %v", b, err)
	}
	h.Close()
	h, err = OpenXFSDrainFile(path)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	if h.Identity() != identity {
		t.Fatal("reopened identity changed")
	}
	if err = h.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Remain immutable and retained after the test, no automatic flag clearing.
	flags, err := unix.IoctlGetInt(int(h.file.Fd()), unix.FS_IOC_GETFLAGS)
	if err != nil || flags&xfsImmutable == 0 {
		t.Fatal("immutable lost", err)
	}
	t.Logf("retained inode drained and reopened: %+v", identity)
}

func TestXFSDrainNativeSubstitution(t *testing.T) {
	dir := os.Getenv("CUBE_XFS_DRAIN_TEST_DIR")
	if dir == "" {
		t.Skip("requires isolated native XFS fixture")
	}
	p := filepath.Join(dir, "substitution")
	if err := os.WriteFile(p, []byte("original"), 0600); err != nil {
		t.Fatal(err)
	}
	h, err := OpenXFSDrainFile(p)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	if err = os.Rename(p, p+"-retained"); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(p, []byte("replacement"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = h.Drain(context.Background()); err == nil {
		t.Fatal("substitution accepted")
	}
	if b, e := os.ReadFile(p + "-retained"); e != nil || string(b) != "original" {
		t.Fatal("original not retained", e)
	}
}

func TestXFSDrainRejectsNonXFSAndCancel(t *testing.T) {
	p := filepath.Join(t.TempDir(), "volume")
	if err := os.WriteFile(p, []byte("data"), 0600); err != nil {
		t.Fatal(err)
	}
	if h, err := OpenXFSDrainFile(p); err == nil {
		h.Close()
		t.Fatal("untrusted temporary path accepted")
	}
	var h *XFSDrainFile
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if h.Drain(ctx) == nil {
		t.Fatal("closed handle accepted")
	}
}
