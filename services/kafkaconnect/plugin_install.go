package kafkaconnect

import (
	"archive/zip"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const maxUnzipBytes = 1 << 30

var errZipEscape = errors.New("plugin archive entry escapes the plugin directory")

// unzipPlugin extracts a custom plugin ZIP into dst, rejecting entries that would escape it.
func unzipPlugin(data []byte, dst string) error {
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return fmt.Errorf("%w: %w", errWorkerBadArchive, err)
	}

	var total int64

	for _, f := range zr.File {
		n, werr := extractZipEntry(f, dst, maxUnzipBytes-total)
		if werr != nil {
			return werr
		}

		total += n
	}

	return nil
}

func extractZipEntry(f *zip.File, dst string, budget int64) (int64, error) {
	root := filepath.Clean(dst)
	target := filepath.Join(root, filepath.FromSlash(f.Name))

	if target != root && !strings.HasPrefix(target, root+string(os.PathSeparator)) {
		return 0, fmt.Errorf("%w: %s", errZipEscape, f.Name)
	}

	if f.FileInfo().IsDir() {
		if err := os.MkdirAll(target, workerDirMode); err != nil {
			return 0, fmt.Errorf("plugin dir: %w", err)
		}

		return 0, nil
	}

	if err := os.MkdirAll(filepath.Dir(target), workerDirMode); err != nil {
		return 0, fmt.Errorf("plugin dir: %w", err)
	}

	rc, err := f.Open()
	if err != nil {
		return 0, fmt.Errorf("%w: %w", errWorkerBadArchive, err)
	}

	defer rc.Close()

	out, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, workerFileMode)
	if err != nil {
		return 0, fmt.Errorf("plugin file: %w", err)
	}

	defer out.Close()

	n, err := io.Copy(out, io.LimitReader(rc, budget))
	if err != nil {
		return n, fmt.Errorf("extract plugin file: %w", err)
	}

	return n, nil
}
