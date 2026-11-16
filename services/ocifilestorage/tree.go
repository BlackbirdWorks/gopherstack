package ocifilestorage

import (
	"context"
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
func copyTree(ctx context.Context, src, dst string) error {
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
		if cerr := ctx.Err(); cerr != nil {
			return cerr
		}

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
			return copyFile(ctx, from, to, p)
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

func copyFile(ctx context.Context, from, to *os.Root, p string) (err error) {
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

	if _, err = io.Copy(out, cancellable(ctx, in)); err != nil {
		return err
	}

	return to.Chtimes(p, info.ModTime(), info.ModTime())
}

type readerFunc func([]byte) (int, error)

func (f readerFunc) Read(p []byte) (int, error) { return f(p) }

func cancellable(ctx context.Context, r io.Reader) io.Reader {
	return readerFunc(func(p []byte) (int, error) {
		if err := ctx.Err(); err != nil {
			return 0, err
		}

		return r.Read(p)
	})
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

// syncTree makes dst match src, copying only the files whose size or modification time differ and removing
// what src no longer holds. It returns the number of bytes copied.
func syncTree(ctx context.Context, src, dst string) (int64, error) {
	if err := os.MkdirAll(dst, dirPerm); err != nil {
		return 0, err
	}

	from, err := os.OpenRoot(src)
	if err != nil {
		return 0, err
	}

	defer from.Close()

	to, err := os.OpenRoot(dst)
	if err != nil {
		return 0, err
	}

	defer to.Close()

	keep := map[string]struct{}{}

	var copied int64

	err = fs.WalkDir(from.FS(), ".", func(p string, d fs.DirEntry, werr error) error {
		if cerr := ctx.Err(); cerr != nil {
			return cerr
		}

		if werr != nil || p == "." {
			return werr
		}

		keep[p] = struct{}{}

		n, serr := syncEntry(ctx, from, to, p, d)
		copied += n

		return serr
	})
	if err != nil {
		return copied, err
	}

	return copied, prune(to, keep)
}

func syncEntry(ctx context.Context, from, to *os.Root, p string, d fs.DirEntry) (int64, error) {
	cur, lerr := to.Lstat(p)
	exists := lerr == nil

	switch {
	case d.IsDir():
		return 0, syncDir(to, p, d, cur, exists)
	case d.Type()&fs.ModeSymlink != 0:
		return 0, syncLink(from, to, p, exists)
	case d.Type().IsRegular():
		return syncFile(ctx, from, to, p, d, cur, exists)
	}

	return 0, nil
}

func syncDir(to *os.Root, p string, d fs.DirEntry, cur fs.FileInfo, exists bool) error {
	if exists && !cur.IsDir() {
		if err := to.RemoveAll(p); err != nil {
			return err
		}
	}

	return copyDir(to, p, d)
}

func syncLink(from, to *os.Root, p string, exists bool) error {
	link, err := fs.ReadLink(from.FS(), p)
	if err != nil {
		return err
	}

	if exists {
		if err = to.RemoveAll(p); err != nil {
			return err
		}
	}

	return to.Symlink(link, p)
}

func syncFile(
	ctx context.Context, from, to *os.Root, p string, d fs.DirEntry, cur fs.FileInfo, exists bool,
) (int64, error) {
	info, err := d.Info()
	if err != nil {
		return 0, err
	}

	if exists && cur.Mode().IsRegular() && cur.Size() == info.Size() && cur.ModTime().Equal(info.ModTime()) {
		return 0, nil
	}

	if exists && !cur.Mode().IsRegular() {
		if err = to.RemoveAll(p); err != nil {
			return 0, err
		}
	}

	return info.Size(), copyFile(ctx, from, to, p)
}

// prune removes everything below root that keep does not list.
func prune(root *os.Root, keep map[string]struct{}) error {
	var drop []string

	err := fs.WalkDir(root.FS(), ".", func(p string, d fs.DirEntry, werr error) error {
		if werr != nil || p == "." {
			return werr
		}

		if _, ok := keep[p]; !ok {
			drop = append(drop, p)

			if d.IsDir() {
				return fs.SkipDir
			}
		}

		return nil
	})
	if err != nil {
		return err
	}

	for _, p := range drop {
		if err = root.RemoveAll(p); err != nil {
			return err
		}
	}

	return nil
}
