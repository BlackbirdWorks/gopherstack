package ocifilestorage

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
)

const dirPerm = 0o755

// copyTree copies the regular files, directories and symlinks of src into dst, preserving modes and times. Both
// sides are opened as roots so that a symlink inside the tree can never lead the copy outside it.
func copyTree(src, dst string) error {
	if err := os.MkdirAll(dst, dirPerm); err != nil {
		return err
	}

	from, err := os.OpenRoot(src)
	if err != nil {
		return err
	}

	defer from.Close()

	to, err := os.OpenRoot(dst)
	if err != nil {
		return err
	}

	defer to.Close()

	return fs.WalkDir(from.FS(), ".", func(p string, d fs.DirEntry, werr error) error {
		if werr != nil || p == "." {
			return werr
		}

		switch {
		case d.IsDir():
			return copyDir(to, p, d)
		case d.Type()&fs.ModeSymlink != 0:
			link, lerr := fs.ReadLink(from.FS(), p)
			if lerr != nil {
				return lerr
			}

			return to.Symlink(link, p)
		case d.Type().IsRegular():
			return copyFile(from, to, p)
		}

		return nil
	})
}

func copyDir(to *os.Root, p string, d fs.DirEntry) error {
	info, err := d.Info()
	if err != nil {
		return err
	}

	if err = to.Mkdir(p, dirPerm); err != nil && !errors.Is(err, fs.ErrExist) {
		return err
	}

	return to.Chmod(p, info.Mode().Perm())
}

func copyFile(from, to *os.Root, p string) (err error) {
	in, err := from.Open(p)
	if err != nil {
		return err
	}

	defer in.Close()

	info, err := in.Stat()
	if err != nil {
		return err
	}

	out, err := to.OpenFile(p, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, info.Mode().Perm())
	if err != nil {
		return err
	}

	defer func() {
		if cerr := out.Close(); err == nil && cerr != nil {
			err = fmt.Errorf("close %s: %w", p, cerr)
		}
	}()

	if _, err = io.Copy(out, in); err != nil {
		return err
	}

	return to.Chtimes(p, info.ModTime(), info.ModTime())
}

// treeSize sums the sizes of the regular files below dir.
func treeSize(dir string) int64 {
	var total int64

	_ = filepath.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error {
		if err != nil || !d.Type().IsRegular() {
			return nil //nolint:nilerr // best-effort metering
		}

		if info, ierr := d.Info(); ierr == nil {
			total += info.Size()
		}

		return nil
	})

	return total
}

// emptyDir removes everything below dir but keeps dir.
func emptyDir(dir string) error {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return err
	}

	for _, e := range ents {
		if err = os.RemoveAll(filepath.Join(dir, e.Name())); err != nil {
			return err
		}
	}

	return nil
}
