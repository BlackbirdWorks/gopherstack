package mwaa

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
)

const airflowMaxObject = 256 << 20

var errAirflowUnsafePath = errors.New("object key escapes the target directory")

// siblingServices is the slice of *CLI used to read DAG sources from emulated S3.
type siblingServices interface {
	GetS3Handler() service.Registerable
}

// SetAppConfig records the service.AppContext.Config for lazy sibling lookup.
func (b *InMemoryBackend) SetAppConfig(cfg any) {
	b.mu.Lock("SetAppConfig")
	defer b.mu.Unlock()

	b.appConfig = cfg
}

func (b *InMemoryBackend) s3Backend() (s3backend.StorageBackend, bool) {
	b.mu.RLock("s3Backend")
	cfg := b.appConfig
	b.mu.RUnlock()

	s, ok := cfg.(siblingServices)
	if !ok {
		return nil, false
	}

	h, ok := s.GetS3Handler().(*s3backend.S3Handler)
	if !ok || h == nil || h.Backend == nil {
		return nil, false
	}

	return h.Backend, true
}

// s3Objects reads DAG sources from the emulator's own S3 backend.
type s3Objects struct{ b *InMemoryBackend }

func (o s3Objects) List(ctx context.Context, bucket, prefix string) ([]string, error) {
	backend, ok := o.b.s3Backend()
	if !ok {
		return nil, errAirflowS3Missing
	}

	var keys []string

	in := &awss3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String(prefix)}

	for {
		out, err := backend.ListObjectsV2(ctx, in)
		if err != nil {
			return nil, fmt.Errorf("list %s/%s: %w", bucket, prefix, err)
		}

		for _, obj := range out.Contents {
			keys = append(keys, aws.ToString(obj.Key))
		}

		if !aws.ToBool(out.IsTruncated) || out.NextContinuationToken == nil {
			return keys, nil
		}

		in.ContinuationToken = out.NextContinuationToken
	}
}

func (o s3Objects) Get(ctx context.Context, bucket, key, version string) ([]byte, error) {
	backend, ok := o.b.s3Backend()
	if !ok {
		return nil, errAirflowS3Missing
	}

	in := &awss3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}
	if version != "" {
		in.VersionId = aws.String(version)
	}

	out, err := backend.GetObject(ctx, in)
	if err != nil {
		return nil, fmt.Errorf("get %s/%s: %w", bucket, key, err)
	}

	defer out.Body.Close()

	return io.ReadAll(io.LimitReader(out.Body, airflowMaxObject)) //nolint:wrapcheck // read errors are self-describing
}

func bucketFromARN(bucketARN string) (string, error) {
	bucket := bucketARN[strings.LastIndex(bucketARN, ":")+1:]
	if !arn.HasService(bucketARN, "s3") || bucket == "" {
		return "", errAirflowBucketARN
	}

	return bucket, nil
}

// syncAirflow mirrors the environment's S3 sources into dir; full also writes requirements, plugins and auth config.
func (b *InMemoryBackend) syncAirflow(
	ctx context.Context, e *airflowEngine, dir string, in airflowLaunch, full bool,
) error {
	dags, err := e.dagFiles(ctx, in)
	if err != nil {
		return err
	}

	if err = mirrorDir(filepath.Join(dir, "dags"), dags); err != nil {
		return err
	}

	if !full {
		return nil
	}

	return e.stageStatic(ctx, dir, in)
}

func (e *airflowEngine) dagFiles(ctx context.Context, in airflowLaunch) (map[string][]byte, error) {
	if in.bucketARN == "" {
		return map[string][]byte{}, nil
	}

	bucket, err := bucketFromARN(in.bucketARN)
	if err != nil {
		return nil, err
	}

	prefix := strings.Trim(in.dagPath, "/")
	if prefix != "" {
		prefix += "/"
	}

	keys, err := e.cfg.Objects.List(ctx, bucket, prefix)
	if err != nil {
		return nil, fmt.Errorf("list dags: %w", err)
	}

	files := make(map[string][]byte, len(keys))

	for _, key := range keys {
		rel := strings.TrimPrefix(key, prefix)
		if rel == "" || strings.HasSuffix(key, "/") {
			continue
		}

		if files[rel], err = e.cfg.Objects.Get(ctx, bucket, key, ""); err != nil {
			return nil, fmt.Errorf("read dag %s: %w", key, err)
		}
	}

	return files, nil
}

func (e *airflowEngine) stageStatic(ctx context.Context, dir string, in airflowLaunch) error {
	reqs, err := e.singleObject(ctx, in, in.reqPath, in.reqVersion)
	if err != nil {
		return fmt.Errorf("requirements: %w", err)
	}

	plugins, err := e.pluginFiles(ctx, in)
	if err != nil {
		return fmt.Errorf("plugins: %w", err)
	}

	if err = mirrorDir(filepath.Join(dir, "plugins"), plugins); err != nil {
		return err
	}

	startup, err := e.singleObject(ctx, in, in.startupPath, in.startupVersion)
	if err != nil {
		return fmt.Errorf("startup script: %w", err)
	}

	if err = os.MkdirAll(filepath.Join(dir, "state"), airflowDirMode); err != nil {
		return fmt.Errorf("create state dir: %w", err)
	}

	_ = os.Chmod(filepath.Join(dir, "state"), airflowDirMode)

	passwords, _ := json.Marshal(map[string]string{airflowAdminUser: in.adminPass})

	for name, data := range map[string][]byte{
		"requirements.txt":           reqs,
		"startup.sh":                 startup,
		"webserver_config.py":        []byte(webserverConfig),
		"simple_auth_passwords.json": passwords,
	} {
		if err = writeFileAtomic(filepath.Join(dir, name), data); err != nil {
			return err
		}
	}

	return nil
}

func (e *airflowEngine) singleObject(ctx context.Context, in airflowLaunch, key, version string) ([]byte, error) {
	if in.bucketARN == "" || key == "" {
		return nil, nil
	}

	bucket, err := bucketFromARN(in.bucketARN)
	if err != nil {
		return nil, err
	}

	return e.cfg.Objects.Get(ctx, bucket, strings.TrimLeft(key, "/"), version)
}

func (e *airflowEngine) pluginFiles(ctx context.Context, in airflowLaunch) (map[string][]byte, error) {
	raw, err := e.singleObject(ctx, in, in.pluginsPath, in.pluginsVersion)
	if err != nil || len(raw) == 0 {
		return map[string][]byte{}, err
	}

	return unzipFiles(raw)
}

func unzipFiles(raw []byte) (map[string][]byte, error) {
	zr, err := zip.NewReader(bytes.NewReader(raw), int64(len(raw)))
	if err != nil {
		return nil, fmt.Errorf("open plugins zip: %w", err)
	}

	files := make(map[string][]byte, len(zr.File))

	for _, f := range zr.File {
		if f.FileInfo().IsDir() {
			continue
		}

		if path.IsAbs(f.Name) || strings.Contains(f.Name, "..") || strings.Contains(f.Name, `\`) {
			return nil, fmt.Errorf("%w: %q", errAirflowUnsafePath, f.Name)
		}

		data, rerr := readZipEntry(f)
		if rerr != nil {
			return nil, rerr
		}

		files[f.Name] = data
	}

	return files, nil
}

func readZipEntry(f *zip.File) ([]byte, error) {
	rc, err := f.Open()
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", f.Name, err)
	}

	defer rc.Close()

	return io.ReadAll(io.LimitReader(rc, airflowMaxObject)) //nolint:wrapcheck // read errors are self-describing
}

// mirrorDir makes dir contain exactly files: changed files are rewritten atomically, extras removed.
func mirrorDir(dir string, files map[string][]byte) error {
	if err := os.MkdirAll(dir, airflowDirMode); err != nil {
		return fmt.Errorf("create %s: %w", dir, err)
	}

	_ = os.Chmod(dir, airflowDirMode)

	want := make(map[string]struct{}, len(files))

	for rel, data := range files {
		target, err := safeJoin(dir, rel)
		if err != nil {
			return err
		}

		want[target] = struct{}{}

		if old, rerr := os.ReadFile(target); rerr == nil && bytes.Equal(old, data) {
			continue
		}

		if err = writeFileAtomic(target, data); err != nil {
			return err
		}
	}

	return pruneDir(dir, want)
}

func pruneDir(dir string, want map[string]struct{}) error {
	var stale []string

	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if _, keep := want[p]; err == nil && !d.IsDir() && !keep {
			stale = append(stale, p)
		}

		return err
	})
	if err != nil {
		return fmt.Errorf("prune %s: %w", dir, err)
	}

	for _, p := range stale {
		if err = os.Remove(p); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("prune %s: %w", p, err)
		}
	}

	return nil
}

func safeJoin(dir, rel string) (string, error) {
	clean := path.Clean("/" + rel)
	if clean == "/" || strings.Contains(rel, "..") {
		return "", fmt.Errorf("%w: %q", errAirflowUnsafePath, rel)
	}

	return filepath.Join(dir, filepath.FromSlash(clean)), nil
}

func writeFileAtomic(target string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(target), airflowDirMode); err != nil {
		return fmt.Errorf("create dir for %s: %w", target, err)
	}

	tmp := target + ".tmp"

	if err := os.WriteFile(tmp, data, airflowFileMode); err != nil {
		return fmt.Errorf("write %s: %w", target, err)
	}

	if err := os.Rename(tmp, target); err != nil {
		return fmt.Errorf("rename %s: %w", target, err)
	}

	return nil
}
