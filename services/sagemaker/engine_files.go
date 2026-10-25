package sagemaker

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
)

const (
	maxStagedBytes  = 1 << 30
	gzipMagic0      = 0x1f
	gzipMagic1      = 0x8b
	s3Scheme        = "s3"
	manifestPrefixK = "prefix"
)

var (
	errBadS3URI      = errors.New("not a valid s3 uri")
	errUnsafeArchive = errors.New("archive entry escapes the target directory")
	errNoStore       = errors.New("S3 is not available to the container engine")
	errNoObjects     = errors.New("no objects found")
	errManifestEntry = errors.New("manifest entry is not an object key")
)

// parseS3URI splits s3://bucket/key into bucket and key.
func parseS3URI(raw string) (string, string, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != s3Scheme || u.Host == "" {
		return "", "", fmt.Errorf("%w: %q", errBadS3URI, raw)
	}

	return u.Host, strings.TrimPrefix(u.Path, "/"), nil
}

func joinS3(base string, parts ...string) string {
	var sb strings.Builder

	sb.WriteString(strings.TrimSuffix(base, "/"))

	for _, p := range parts {
		if p = strings.Trim(p, "/"); p != "" {
			sb.WriteString("/" + p)
		}
	}

	return sb.String()
}

func isGzip(data []byte) bool {
	return len(data) > 2 && data[0] == gzipMagic0 && data[1] == gzipMagic1
}

// extractTarGz unpacks a gzip-compressed tar archive under dest, rejecting entries that escape it.
func extractTarGz(data []byte, dest string) error {
	zr, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("read gzip: %w", err)
	}

	defer zr.Close()

	tr := tar.NewReader(io.LimitReader(zr, maxStagedBytes))

	for {
		hdr, nerr := tr.Next()
		if errors.Is(nerr, io.EOF) {
			return nil
		}

		if nerr != nil {
			return fmt.Errorf("read tar: %w", nerr)
		}

		if err = extractEntry(tr, hdr, dest); err != nil {
			return err
		}
	}
}

func extractEntry(tr *tar.Reader, hdr *tar.Header, dest string) error {
	if strings.Contains(hdr.Name, "..") {
		return fmt.Errorf("%w: %s", errUnsafeArchive, hdr.Name)
	}

	target := filepath.Join(dest, filepath.FromSlash(hdr.Name))
	if target != filepath.Clean(dest) && !strings.HasPrefix(target, filepath.Clean(dest)+string(os.PathSeparator)) {
		return fmt.Errorf("%w: %s", errUnsafeArchive, hdr.Name)
	}

	switch hdr.Typeflag {
	case tar.TypeDir:
		return os.MkdirAll(target, dirMode)
	case tar.TypeReg:
		if err := os.MkdirAll(filepath.Dir(target), dirMode); err != nil {
			return fmt.Errorf("mkdir: %w", err)
		}

		f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, hdr.FileInfo().Mode().Perm()|fileMode)
		if err != nil {
			return fmt.Errorf("create %s: %w", hdr.Name, err)
		}

		_, cerr := io.Copy(f, tr)
		if closeErr := f.Close(); cerr == nil {
			cerr = closeErr
		}

		return cerr
	default:
		return nil
	}
}

// tarGzDir packs every regular file under dir into a gzip-compressed tar archive.
func tarGzDir(dir string) ([]byte, error) {
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", dir, err)
	}

	defer root.Close()

	var buf bytes.Buffer

	zw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(zw)
	fsys := root.FS()

	err = fs.WalkDir(fsys, ".", func(rel string, d fs.DirEntry, werr error) error {
		if werr != nil || !d.Type().IsRegular() {
			return werr
		}

		info, ierr := d.Info()
		if ierr != nil {
			return ierr
		}

		hdr, herr := tar.FileInfoHeader(info, "")
		if herr != nil {
			return herr
		}

		hdr.Name = rel
		if werr = tw.WriteHeader(hdr); werr != nil {
			return werr
		}

		f, oerr := fsys.Open(rel)
		if oerr != nil {
			return oerr
		}

		defer f.Close()

		_, cerr := io.Copy(tw, f)

		return cerr
	})
	if err != nil {
		return nil, fmt.Errorf("pack %s: %w", dir, err)
	}

	if err = tw.Close(); err != nil {
		return nil, fmt.Errorf("close tar: %w", err)
	}

	if err = zw.Close(); err != nil {
		return nil, fmt.Errorf("close gzip: %w", err)
	}

	return buf.Bytes(), nil
}

// stageS3 downloads the objects an S3 URI addresses into root/sub and returns that directory.
// dataType is S3Prefix (default), ManifestFile or AugmentedManifestFile. Objects keep their
// path relative to the prefix. sub must not escape root.
func stageS3(ctx context.Context, store ObjectStore, uri, dataType, root, sub string) (string, error) {
	if store == nil {
		return "", errNoStore
	}

	bucket, key, err := parseS3URI(uri)
	if err != nil {
		return "", err
	}

	dir := filepath.Join(root, filepath.FromSlash(sub))
	if !strings.HasPrefix(dir, filepath.Clean(root)+string(os.PathSeparator)) {
		return "", fmt.Errorf("%w: %s", errUnsafeArchive, sub)
	}

	if err = os.MkdirAll(dir, dirMode); err != nil {
		return "", fmt.Errorf("mkdir %s: %w", dir, err)
	}

	switch dataType {
	case "ManifestFile":
		err = stageManifest(ctx, store, bucket, key, dir)
	case "AugmentedManifestFile":
		err = stageObject(ctx, store, bucket, key, dir, path.Base(key))
	default:
		err = stagePrefix(ctx, store, bucket, key, dir)
	}

	return dir, err
}

func stagePrefix(ctx context.Context, store ObjectStore, bucket, prefix, dir string) error {
	keys, err := store.ListObjectKeys(ctx, bucket, prefix)
	if err != nil {
		return fmt.Errorf("list s3://%s/%s: %w", bucket, prefix, err)
	}

	sort.Strings(keys)

	if len(keys) == 0 {
		return fmt.Errorf("%w: s3://%s/%s", errNoObjects, bucket, prefix)
	}

	for _, k := range keys {
		rel := strings.TrimPrefix(strings.TrimPrefix(k, prefix), "/")
		if strings.HasSuffix(k, "/") {
			continue
		}

		if rel == "" || (k == prefix && !strings.HasSuffix(prefix, "/")) {
			rel = path.Base(k)
		}

		if err = stageObject(ctx, store, bucket, k, dir, rel); err != nil {
			return err
		}
	}

	return nil
}

func stageManifest(ctx context.Context, store ObjectStore, bucket, key, dir string) error {
	raw, err := store.GetObjectBytes(ctx, bucket, key)
	if err != nil {
		return fmt.Errorf("read manifest s3://%s/%s: %w", bucket, key, err)
	}

	var entries []json.RawMessage
	if err = json.Unmarshal(raw, &entries); err != nil {
		return fmt.Errorf("parse manifest s3://%s/%s: %w", bucket, key, err)
	}

	prefix := ""

	for i, ent := range entries {
		var obj map[string]string
		if json.Unmarshal(ent, &obj) == nil {
			prefix = obj[manifestPrefixK]

			continue
		}

		var name string
		if err = json.Unmarshal(ent, &name); err != nil || prefix == "" {
			return fmt.Errorf("manifest s3://%s/%s: entry %d: %w", bucket, key, i, errManifestEntry)
		}

		b, k, perr := parseS3URI(joinS3(prefix, name))
		if perr != nil {
			return perr
		}

		if err = stageObject(ctx, store, b, k, dir, path.Base(k)); err != nil {
			return err
		}
	}

	return nil
}

func stageObject(ctx context.Context, store ObjectStore, bucket, key, dir, rel string) error {
	data, err := store.GetObjectBytes(ctx, bucket, key)
	if err != nil {
		return fmt.Errorf("get s3://%s/%s: %w", bucket, key, err)
	}

	target := filepath.Join(dir, filepath.FromSlash(rel))
	if !strings.HasPrefix(target, filepath.Clean(dir)+string(os.PathSeparator)) {
		return fmt.Errorf("%w: %s", errUnsafeArchive, rel)
	}

	if err = os.MkdirAll(filepath.Dir(target), dirMode); err != nil {
		return fmt.Errorf("mkdir: %w", err)
	}

	return os.WriteFile(target, data, fileMode)
}

// uploadDir stores every regular file under dir below the S3 prefix uri.
func uploadDir(ctx context.Context, store ObjectStore, dir, uri string) error {
	if store == nil {
		return errNoStore
	}

	bucket, prefix, err := parseS3URI(uri)
	if err != nil {
		return err
	}

	root, err := os.OpenRoot(dir)
	if err != nil {
		return fmt.Errorf("open %s: %w", dir, err)
	}

	defer root.Close()

	fsys := root.FS()

	return fs.WalkDir(fsys, ".", func(rel string, d fs.DirEntry, werr error) error {
		if werr != nil || !d.Type().IsRegular() {
			return werr
		}

		data, rerr := fs.ReadFile(fsys, rel)
		if rerr != nil {
			return rerr
		}

		return store.PutObjectBytes(ctx, bucket, path.Join(prefix, rel), data)
	})
}

// readTree returns every regular file under dir keyed by its slash-separated relative path.
func readTree(dir string) (map[string][]byte, error) {
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", dir, err)
	}

	defer root.Close()

	fsys := root.FS()
	files := make(map[string][]byte)

	err = fs.WalkDir(fsys, ".", func(rel string, d fs.DirEntry, werr error) error {
		if werr != nil || !d.Type().IsRegular() {
			return werr
		}

		data, rerr := fs.ReadFile(fsys, rel)
		if rerr != nil {
			return rerr
		}

		files[rel] = data

		return nil
	})

	return files, err
}

func putS3URI(ctx context.Context, store ObjectStore, uri string, data []byte) error {
	if store == nil {
		return errNoStore
	}

	bucket, key, err := parseS3URI(uri)
	if err != nil {
		return err
	}

	return store.PutObjectBytes(ctx, bucket, key, data)
}

func getS3URI(ctx context.Context, store ObjectStore, uri string) ([]byte, error) {
	if store == nil {
		return nil, errNoStore
	}

	bucket, key, err := parseS3URI(uri)
	if err != nil {
		return nil, err
	}

	return store.GetObjectBytes(ctx, bucket, key)
}

// writeJSONFile marshals v to dir/name.
func writeJSONFile(dir, name string, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal %s: %w", name, err)
	}

	if err = os.MkdirAll(dir, dirMode); err != nil {
		return fmt.Errorf("mkdir %s: %w", dir, err)
	}

	return os.WriteFile(filepath.Join(dir, name), data, fileMode)
}

// newStageDir creates a fresh world-accessible staging directory below the engine work dir.
func (e *engineState) newStageDir(prefix string) (string, error) {
	if err := os.MkdirAll(e.cfg.WorkDir, dirMode); err != nil {
		return "", fmt.Errorf("create work dir: %w", err)
	}

	dir, err := os.MkdirTemp(e.cfg.WorkDir, prefix+"-")
	if err != nil {
		return "", fmt.Errorf("create stage dir: %w", err)
	}

	return dir, os.Chmod(dir, 0o777) //nolint:gosec // containers run as arbitrary users and must write here
}

// mkSharedDir creates dir writable by any container user.
func mkSharedDir(dir string) error {
	if err := os.MkdirAll(dir, dirMode); err != nil {
		return fmt.Errorf("mkdir %s: %w", dir, err)
	}

	return os.Chmod(dir, 0o777) //nolint:gosec // containers run as arbitrary users and must write here
}
