//go:build linux

package cubecow

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"golang.org/x/sys/unix"
)

// XFSFileIdentity identifies one retained native inode, not a whole source or a
// checkpoint. Native XFS currently has no VolumeUUID. Do not substitute this
// identity for an S3 UUID or advertise a whole-source fencing receipt.
type XFSFileIdentity struct {
	Device     uint64   `json:"device"`
	Inode      uint64   `json:"inode"`
	Generation int      `json:"inode_generation"`
	FSID       [2]int32 `json:"fsid"`
	Size       int64    `json:"size"`
}

type XFSDrainFile struct {
	file     *os.File
	dir      *os.File
	identity XFSFileIdentity
}

const xfsImmutable = 0x10

// OpenXFSDrainFile pins an existing inode through trusted root-owned directories.
// It creates, deletes and activates nothing. Call before stopping its owner.
// The caller must enumerate ALL source resources and persist this identity before
// invoking Drain; this low-level primitive intentionally makes no such claim.
func OpenXFSDrainFile(path string) (*XFSDrainFile, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, fmt.Errorf("XFS drain requires a clean absolute native path")
	}
	dir, err := os.Open("/")
	if err != nil {
		return nil, err
	}
	parts := splitNativePath(path)
	for _, part := range parts[:len(parts)-1] {
		fd, e := unix.Openat(int(dir.Fd()), part, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		dir.Close()
		if e != nil {
			return nil, e
		}
		dir = os.NewFile(uintptr(fd), part)
		var st unix.Stat_t
		if e = unix.Fstat(fd, &st); e != nil || st.Uid != 0 || st.Mode&0022 != 0 {
			dir.Close()
			return nil, fmt.Errorf("untrusted XFS drain directory")
		}
	}
	fd, err := unix.Openat(int(dir.Fd()), parts[len(parts)-1], unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		dir.Close()
		return nil, err
	}
	h := &XFSDrainFile{file: os.NewFile(uintptr(fd), path), dir: dir}
	h.identity, err = xfsIdentity(fd)
	if err != nil {
		h.Close()
		return nil, err
	}
	return h, nil
}

func splitNativePath(path string) []string {
	var parts []string
	for path != "/" {
		parts = append([]string{filepath.Base(path)}, parts...)
		path = filepath.Dir(path)
	}
	// Root itself is not a regular native volume.
	if len(parts) == 0 {
		return []string{"."}
	}
	return parts
}

func xfsIdentity(fd int) (XFSFileIdentity, error) {
	var st unix.Stat_t
	var fs unix.Statfs_t
	if err := unix.Fstat(fd, &st); err != nil {
		return XFSFileIdentity{}, err
	}
	if err := unix.Fstatfs(fd, &fs); err != nil {
		return XFSFileIdentity{}, err
	}
	if fs.Type != unix.XFS_SUPER_MAGIC || st.Mode&unix.S_IFMT != unix.S_IFREG || st.Uid != 0 || st.Mode&0022 != 0 || st.Nlink != 1 || st.Size <= 0 {
		return XFSFileIdentity{}, fmt.Errorf("drain supports only single-link root-owned XFS regular files")
	}
	// Linux _IOR('v', 1, long), missing from the pinned x/sys constants.
	gen, err := unix.IoctlGetInt(fd, uint(0x80007601|(strconv.IntSize/8)<<16))
	if err != nil {
		return XFSFileIdentity{}, err
	}
	return XFSFileIdentity{Device: uint64(st.Dev), Inode: st.Ino, Generation: gen, FSID: fs.Fsid.Val, Size: st.Size}, nil
}

func (h *XFSDrainFile) Identity() XFSFileIdentity { return h.identity }

func (h *XFSDrainFile) checkName() error {
	var st unix.Stat_t
	err := unix.Fstatat(int(h.dir.Fd()), filepath.Base(h.file.Name()), &st, unix.AT_SYMLINK_NOFOLLOW)
	if err != nil {
		return err
	}
	if uint64(st.Dev) != h.identity.Device || st.Ino != h.identity.Inode || st.Mode&unix.S_IFMT != unix.S_IFREG {
		return fmt.Errorf("native XFS pathname substituted")
	}
	return nil
}
func (h *XFSDrainFile) Close() {
	if h != nil {
		if h.file != nil {
			h.file.Close()
			h.file = nil
		}
		if h.dir != nil {
			h.dir.Close()
			h.dir = nil
		}
	}
}

// Drain excludes existing opens/mappings with a kernel WRITE lease, sets the
// persistent immutable bit while the lease excludes new opens, and fsyncs the
// exact retained inode and its directory. The lease is NOT an idle counter.
// EAGAIN means a real conflicting reference exists; cancellation is failure.
// No failure path clears immutable: partial progress remains fail-closed.
// Privileged flag clearing, raw-device writes, other source files, and distributed
// ownership are outside this guarantee. No automatic source-fence integration
// is permitted until complete native resource membership is established.
func (h *XFSDrainFile) Drain(ctx context.Context) error {
	if h == nil || h.file == nil {
		return fmt.Errorf("closed XFS drain handle")
	}
	fd := int(h.file.Fd())
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := h.checkName(); err != nil {
		return err
	}
	current, err := xfsIdentity(fd)
	if err != nil {
		return err
	}
	if current != h.identity {
		return fmt.Errorf("XFS inode identity changed")
	}
	// Linux lease breaks otherwise send SIGIO to the process. Route notification
	// to no owner; correctness rests on immutable and GETLEASE, not a handler.
	if _, err = unix.FcntlInt(h.file.Fd(), unix.F_SETOWN, 0); err != nil {
		return err
	}
	for {
		if err = ctx.Err(); err != nil {
			return err
		}
		_, err = unix.FcntlInt(h.file.Fd(), unix.F_SETLEASE, unix.F_WRLCK)
		if err == nil {
			break
		}
		if err != unix.EAGAIN {
			return fmt.Errorf("XFS writer exclusion: %w", err)
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	defer unix.FcntlInt(h.file.Fd(), unix.F_SETLEASE, unix.F_UNLCK)
	if err = ctx.Err(); err != nil {
		return err
	}
	flags, err := unix.IoctlGetInt(fd, unix.FS_IOC_GETFLAGS)
	if err != nil {
		return err
	}
	if flags&xfsImmutable == 0 {
		if err = unix.IoctlSetPointerInt(fd, unix.FS_IOC_SETFLAGS, flags|xfsImmutable); err != nil {
			return err
		}
	}
	// A lease may be broken by a competing opener. Do not count a downgraded or
	// timed-out lease as exclusion, even if the immutable operation succeeded.
	lease, err := unix.FcntlInt(h.file.Fd(), unix.F_GETLEASE, 0)
	if err != nil || lease != unix.F_WRLCK {
		return fmt.Errorf("XFS lease changed during drain")
	}
	if err = h.file.Sync(); err != nil {
		return err
	}
	if err = h.dir.Sync(); err != nil {
		return err
	}
	flags, err = unix.IoctlGetInt(fd, unix.FS_IOC_GETFLAGS)
	if err != nil || flags&xfsImmutable == 0 {
		return fmt.Errorf("XFS immutable state lost")
	}
	current, err = xfsIdentity(fd)
	if err != nil {
		return err
	}
	if current != h.identity {
		return fmt.Errorf("XFS inode identity changed during drain")
	}
	if err := h.checkName(); err != nil {
		return err
	}
	return ctx.Err()
}
