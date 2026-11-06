package ocifilestorage

import (
	"errors"
	"os"
	"path"
	"strings"
	"time"

	"github.com/go-git/go-billy/v5"
)

const (
	snapshotDir = ".snapshot"
	createPerm  = 0o666
)

var errNoChroot = errors.New("ocifilestorage: chroot is not supported")

// vfs is the billy file system an export presents over NFS: the live tree plus a hidden, read-only .snapshot
// directory at the root that lists the snapshots by name.
type vfs struct {
	live  billy.Filesystem
	snaps billy.Filesystem
	root  *os.Root
	fsID  string
	stat  ExportMount
	ro    bool
}

func (v *vfs) close() { _ = v.root.Close() }

// resolve maps a path to the file system that holds it; snap reports a path inside .snapshot.
func (v *vfs) resolve(name string) (billy.Filesystem, string, bool) {
	p := path.Clean("/" + name)
	if p == "/"+snapshotDir || strings.HasPrefix(p, "/"+snapshotDir+"/") {
		return v.snaps, relPath(strings.TrimPrefix(p, "/"+snapshotDir)), true
	}

	return v.live, relPath(p), false
}

// relPath makes a cleaned absolute path relative to the file system root, as the bound file systems require.
func relPath(p string) string {
	if r := strings.TrimPrefix(path.Clean("/"+p), "/"); r != "" {
		return r
	}

	return "."
}

func (v *vfs) Capabilities() billy.Capability {
	if v.ro {
		return billy.ReadCapability | billy.SeekCapability
	}

	return billy.DefaultCapabilities
}

func denied(op, name string) error { return &os.PathError{Op: op, Path: name, Err: os.ErrPermission} }

func (v *vfs) OpenFile(name string, flag int, perm os.FileMode) (billy.File, error) {
	fs, rel, snap := v.resolve(name)
	if snap && flag&(os.O_WRONLY|os.O_RDWR|os.O_CREATE|os.O_TRUNC|os.O_APPEND) != 0 {
		return nil, denied("open", name)
	}

	f, err := fs.OpenFile(rel, flag, perm)
	if err != nil {
		return nil, err
	}

	return &namedFile{File: f, name: name}, nil
}

// namedFile reports the export-relative name go-nfs expects instead of the host path of the backing file.
type namedFile struct {
	billy.File
	name string
}

func (f *namedFile) Name() string { return f.name }

func (v *vfs) Create(name string) (billy.File, error) {
	return v.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, createPerm)
}

func (v *vfs) Open(name string) (billy.File, error) { return v.OpenFile(name, os.O_RDONLY, 0) }

func (v *vfs) Stat(name string) (os.FileInfo, error) {
	fs, rel, _ := v.resolve(name)

	return fs.Stat(rel)
}

func (v *vfs) Lstat(name string) (os.FileInfo, error) {
	fs, rel, _ := v.resolve(name)

	return fs.Lstat(rel)
}

func (v *vfs) ReadDir(name string) ([]os.FileInfo, error) {
	fs, rel, _ := v.resolve(name)

	return fs.ReadDir(rel)
}

func (v *vfs) Readlink(name string) (string, error) {
	fs, rel, _ := v.resolve(name)

	return fs.Readlink(rel)
}

func (v *vfs) writable(op string, names ...string) error {
	for _, n := range names {
		if _, _, snap := v.resolve(n); snap {
			return denied(op, n)
		}
	}

	return nil
}

func (v *vfs) Rename(from, to string) error {
	if err := v.writable("rename", from, to); err != nil {
		return err
	}

	return v.live.Rename(relPath(from), relPath(to))
}

func (v *vfs) Remove(name string) error {
	if err := v.writable("remove", name); err != nil {
		return err
	}

	return v.live.Remove(relPath(name))
}

func (v *vfs) MkdirAll(name string, perm os.FileMode) error {
	if err := v.writable("mkdir", name); err != nil {
		return err
	}

	return v.live.MkdirAll(relPath(name), perm)
}

func (v *vfs) Symlink(target, link string) error {
	if err := v.writable("symlink", link); err != nil {
		return err
	}

	return v.live.Symlink(target, relPath(link))
}

func (v *vfs) TempFile(dir, prefix string) (billy.File, error) {
	if err := v.writable("tempfile", dir); err != nil {
		return nil, err
	}

	return v.live.TempFile(relPath(dir), prefix)
}

func (v *vfs) Join(elem ...string) string { return path.Join(elem...) }

func (v *vfs) Chroot(string) (billy.Filesystem, error) { return nil, errNoChroot }

func (v *vfs) Root() string { return "/" }

func (v *vfs) rel(name string) (string, error) {
	if err := v.writable("change", name); err != nil {
		return "", err
	}

	return relPath(name), nil
}

func (v *vfs) Chmod(name string, mode os.FileMode) error {
	rel, err := v.rel(name)
	if err != nil {
		return err
	}

	return v.root.Chmod(rel, mode)
}

func (v *vfs) Lchown(name string, uid, gid int) error {
	rel, err := v.rel(name)
	if err != nil {
		return err
	}

	return v.root.Lchown(rel, uid, gid)
}

func (v *vfs) Chown(name string, uid, gid int) error {
	rel, err := v.rel(name)
	if err != nil {
		return err
	}

	return v.root.Chown(rel, uid, gid)
}

func (v *vfs) Chtimes(name string, atime, mtime time.Time) error {
	rel, err := v.rel(name)
	if err != nil {
		return err
	}

	return v.root.Chtimes(rel, atime, mtime)
}
