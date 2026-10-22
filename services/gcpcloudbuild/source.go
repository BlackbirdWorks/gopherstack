package gcpcloudbuild

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"cloud.google.com/go/cloudbuild/apiv1/v2/cloudbuildpb"
)

const (
	maxArchiveBytes   = 1 << 30
	maxArchiveEntries = 100_000
	dirPerm           = 0o777
	filePerm          = 0o666
	execBits          = 0o111
	permMask          = 0o777
)

var (
	errNoStore        = errors.New("cloud storage is not available to this build")
	errUnsupportedSrc = errors.New(
		"only storageSource, gitSource and repoSource are supported; storageSourceManifest " +
			"and connectedRepository are not",
	)
	errArchiveFormat   = errors.New("source archive must be a .tar.gz or .zip file")
	errArchiveTooLarge = errors.New("source archive is too large")
	errUnsafePath      = errors.New("source archive entry escapes the workspace")
)

// fetchSource places the build's source in the workspace; a build without a source starts empty.
func (r *runner) fetchSource(ctx context.Context) error {
	src := r.build.GetSource()

	switch s := src.GetSource().(type) {
	case nil:
		return nil
	case *cloudbuildpb.Source_StorageSource:
		return r.fetchStorage(ctx, s.StorageSource)
	case *cloudbuildpb.Source_GitSource, *cloudbuildpb.Source_RepoSource:
		return r.fetchGit(ctx, src)
	default:
		return errUnsupportedSrc
	}
}

func (r *runner) fetchStorage(ctx context.Context, ss *cloudbuildpb.StorageSource) error {
	if r.svc.Store == nil {
		return errNoStore
	}

	r.log.system(fmt.Sprintf("Fetching storage object: gs://%s/%s", ss.GetBucket(), ss.GetObject()))

	data, err := r.svc.Store.ReadObject(ss.GetBucket(), ss.GetObject())
	if err != nil {
		return fmt.Errorf("read gs://%s/%s: %w", ss.GetBucket(), ss.GetObject(), err)
	}

	if err = ctx.Err(); err != nil {
		return fmt.Errorf("fetch source: %w", err)
	}

	return extractArchive(data, r.work)
}

func extractArchive(data []byte, dest string) error {
	switch {
	case bytes.HasPrefix(data, []byte{0x1f, 0x8b}):
		return extractTarGz(data, dest)
	case bytes.HasPrefix(data, []byte("PK")):
		return extractZip(data, dest)
	}

	return errArchiveFormat
}

// target maps an archive entry name to a path inside dest, rejecting anything that would leave it.
func target(dest, name string) (string, error) {
	clean := filepath.Clean(filepath.FromSlash(name))
	if clean == "." {
		return dest, nil
	}

	if filepath.IsAbs(clean) || clean == ".." ||
		strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("%w: %q", errUnsafePath, name)
	}

	return filepath.Join(dest, clean), nil
}

type extractor struct {
	dest    string
	total   int64
	entries int
}

func (e *extractor) account(size int64) error {
	e.entries++
	e.total += size

	if e.entries > maxArchiveEntries || e.total > maxArchiveBytes {
		return errArchiveTooLarge
	}

	return nil
}

func (e *extractor) file(name string, mode os.FileMode, r io.Reader) error {
	path, err := target(e.dest, name)
	if err != nil {
		return err
	}

	if err = os.MkdirAll(filepath.Dir(path), dirPerm); err != nil {
		return fmt.Errorf("create %q: %w", filepath.Dir(name), err)
	}

	perm := os.FileMode(filePerm)
	if mode&execBits != 0 {
		perm |= execBits
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, perm)
	if err != nil {
		return fmt.Errorf("create %q: %w", name, err)
	}

	n, err := io.Copy(f, io.LimitReader(r, maxArchiveBytes-e.total+1))
	if cErr := f.Close(); err == nil {
		err = cErr
	}

	if err != nil {
		return fmt.Errorf("write %q: %w", name, err)
	}

	return e.account(n)
}

func (e *extractor) dir(name string) error {
	path, err := target(e.dest, name)
	if err != nil {
		return err
	}

	if err = os.MkdirAll(path, dirPerm); err != nil {
		return fmt.Errorf("create %q: %w", name, err)
	}

	return e.account(0)
}

func extractTarGz(data []byte, dest string) error {
	gz, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("%w: %w", errArchiveFormat, err)
	}

	defer func() { _ = gz.Close() }()

	e := &extractor{dest: dest}
	tr := tar.NewReader(gz)

	for {
		hdr, nErr := tr.Next()
		if errors.Is(nErr, io.EOF) {
			return nil
		}

		if nErr != nil {
			return fmt.Errorf("read source archive: %w", nErr)
		}

		switch hdr.Typeflag {
		case tar.TypeDir:
			err = e.dir(hdr.Name)
		case tar.TypeReg:
			err = e.file(hdr.Name, os.FileMode(hdr.Mode&permMask), tr)
		}

		if err != nil {
			return err
		}
	}
}

func extractZip(data []byte, dest string) error {
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return fmt.Errorf("%w: %w", errArchiveFormat, err)
	}

	e := &extractor{dest: dest}

	for _, f := range zr.File {
		if err = e.zipEntry(f); err != nil {
			return err
		}
	}

	return nil
}

func (e *extractor) zipEntry(f *zip.File) error {
	switch {
	case f.FileInfo().IsDir():
		return e.dir(f.Name)
	case !f.Mode().IsRegular():
		return nil
	}

	rc, err := f.Open()
	if err != nil {
		return fmt.Errorf("read %q: %w", f.Name, err)
	}

	defer func() { _ = rc.Close() }()

	return e.file(f.Name, f.Mode(), rc)
}
