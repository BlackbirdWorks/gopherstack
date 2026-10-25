package azurefunctions

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
)

const (
	maxPackageBytes = 256 << 20
	maxUnpacked     = 1 << 30
	dirMode         = 0o755
	exeBits         = 0o111
	readBits        = 0o444
)

func validatePackage(data []byte) error {
	if _, err := zip.NewReader(bytes.NewReader(data), int64(len(data))); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidPackage, err)
	}

	return nil
}

// extractArchive unpacks data into dir, refusing entries that escape it and skipping links.
func extractArchive(data []byte, dir string) error {
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return fmt.Errorf("open archive: %w", err)
	}

	var total uint64

	for _, f := range zr.File {
		total += f.UncompressedSize64
		if total > maxUnpacked {
			return errTooLarge
		}

		if err = extractEntry(f, dir); err != nil {
			return err
		}
	}

	return nil
}

func extractEntry(f *zip.File, dir string) error {
	if strings.Contains(f.Name, "..") {
		return fmt.Errorf("%w: %s", errUnsafePath, f.Name)
	}

	root := filepath.Clean(dir)
	target := filepath.Join(root, filepath.FromSlash(f.Name))

	if target != root && !strings.HasPrefix(target, root+string(os.PathSeparator)) {
		return fmt.Errorf("%w: %s", errUnsafePath, f.Name)
	}

	if f.FileInfo().IsDir() {
		return os.MkdirAll(target, dirMode)
	}

	if !f.Mode().IsRegular() {
		return nil
	}

	if err := os.MkdirAll(filepath.Dir(target), dirMode); err != nil {
		return fmt.Errorf("create directory: %w", err)
	}

	rc, err := f.Open()
	if err != nil {
		return fmt.Errorf("read %s: %w", f.Name, err)
	}
	defer rc.Close()

	mode := os.FileMode(readBits) | f.Mode().Perm()&exeBits | 0o200

	out, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode) //nolint:gosec // target is confined to dir above
	if err != nil {
		return fmt.Errorf("create %s: %w", f.Name, err)
	}

	_, err = io.Copy(out, io.LimitReader(rc, maxUnpacked))
	if cerr := out.Close(); err == nil {
		err = cerr
	}

	if err == nil {
		err = os.Chmod(target, mode) //nolint:gosec // see above
	}

	return err
}

// FunctionDef is one function discovered from a deployed package's function.json files.
type FunctionDef struct {
	Config map[string]any
	Name   string
}

// scanFunctions lists the functions a classic (function.json) package declares, in name order. Packages that
// use a code-first model declare their functions only to the running host.
func scanFunctions(data []byte) []FunctionDef {
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil
	}

	var out []FunctionDef

	for _, f := range zr.File {
		dir, base := path.Split(f.Name)
		if base != "function.json" || strings.Count(dir, "/") != 1 || strings.HasPrefix(dir, ".") {
			continue
		}

		rc, openErr := f.Open()
		if openErr != nil {
			continue
		}

		raw, readErr := io.ReadAll(io.LimitReader(rc, maxUnpacked))
		_ = rc.Close()

		var cfg map[string]any
		if readErr != nil || json.Unmarshal(bytes.TrimPrefix(raw, []byte("\xef\xbb\xbf")), &cfg) != nil {
			continue
		}

		out = append(out, FunctionDef{Name: strings.TrimSuffix(dir, "/"), Config: cfg})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out
}
