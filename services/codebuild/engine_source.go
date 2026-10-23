package codebuild

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
)

const (
	sourceTypeS3         = "S3"
	sourceTypeCodeCommit = "CODECOMMIT"
	sourceTypeNone       = "NO_SOURCE"
	sourceTypePipeline   = "CODEPIPELINE"

	s3ARNPrefix       = "arn:aws:s3:::"
	defaultSpecName   = "buildspec.yml"
	dirPerm           = 0o750
	filePerm          = 0o640
	maxExtractedBytes = 1 << 30
)

var (
	errSourceUnsupported = errors.New("source type is not supported by the docker engine")
	errSourceDependency  = errors.New("required emulator backend is not wired")
	errUnsafePath        = errors.New("path escapes the workspace")
)

// splitS3Location parses "bucket/key" or "arn:aws:s3:::bucket/key".
func splitS3Location(loc string) (string, string, bool) {
	loc = strings.TrimPrefix(loc, s3ARNPrefix)
	bucket, key, ok := strings.Cut(loc, "/")

	return bucket, key, ok && bucket != "" && key != ""
}

// repoNameFromLocation extracts the repository name from a CodeCommit clone URL or bare name.
func repoNameFromLocation(loc string) string {
	loc = strings.TrimSuffix(strings.TrimSpace(loc), "/")

	return loc[strings.LastIndex(loc, "/")+1:]
}

// fetchSource materialises the build's primary source into dir.
func (r *buildRunner) fetchSource(ctx context.Context, dir string) error {
	src := r.build.Source
	if src == nil {
		return nil
	}

	switch src.Type {
	case "", sourceTypeNone, sourceTypePipeline:
		return nil
	case sourceTypeS3:
		return r.fetchS3Source(ctx, src.Location, dir)
	case sourceTypeCodeCommit:
		return r.fetchCodeCommitSource(ctx, src.Location, dir)
	default:
		if isGitSourceType(src.Type) {
			return r.fetchGitSource(ctx, dir)
		}

		return fmt.Errorf("%w: %s", errSourceUnsupported, src.Type)
	}
}

func (r *buildRunner) fetchS3Source(ctx context.Context, loc, dir string) error {
	bucket, key, ok := splitS3Location(loc)
	if !ok || strings.HasSuffix(key, "/") {
		return fmt.Errorf("%w: S3 source location %q must be bucket/object-key", errSourceUnsupported, loc)
	}

	if r.svc.Store == nil {
		return fmt.Errorf("%w: S3", errSourceDependency)
	}

	data, err := r.svc.Store.GetObjectBytes(ctx, bucket, key)
	if err != nil {
		return fmt.Errorf("download s3://%s/%s: %w", bucket, key, err)
	}

	if bytes.HasPrefix(data, []byte("PK")) {
		return unzipInto(data, dir)
	}

	return writeSafeFile(dir, path.Base(key), data)
}

func (r *buildRunner) fetchCodeCommitSource(ctx context.Context, loc, dir string) error {
	if r.svc.Repos == nil {
		return fmt.Errorf("%w: CodeCommit", errSourceDependency)
	}

	files, err := r.svc.Repos.RepoFiles(ctx, r.region, repoNameFromLocation(loc), r.build.SourceVersion)
	if err != nil {
		return fmt.Errorf("clone %s: %w", loc, err)
	}

	for name, data := range files {
		if werr := writeSafeFile(dir, name, data); werr != nil {
			return werr
		}
	}

	return nil
}

// safeJoin joins rel under dir, rejecting paths that escape it.
func safeJoin(dir, rel string) (string, error) {
	full := filepath.Join(dir, filepath.FromSlash(rel))

	if full != filepath.Clean(dir) && !strings.HasPrefix(full, filepath.Clean(dir)+string(filepath.Separator)) {
		return "", fmt.Errorf("%w: %s", errUnsafePath, rel)
	}

	return full, nil
}

func writeSafeFile(dir, rel string, data []byte) error {
	full, err := safeJoin(dir, rel)
	if err != nil {
		return err
	}

	if err = os.MkdirAll(filepath.Dir(full), dirPerm); err != nil {
		return fmt.Errorf("mkdir %s: %w", filepath.Dir(full), err)
	}

	if err = os.WriteFile(full, data, filePerm); err != nil {
		return fmt.Errorf("write %s: %w", full, err)
	}

	return nil
}

func unzipInto(data []byte, dir string) error {
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return fmt.Errorf("open source zip: %w", err)
	}

	for _, f := range zr.File {
		if err = extractEntry(f, dir); err != nil {
			return err
		}
	}

	return nil
}

func extractEntry(f *zip.File, dir string) error {
	root := filepath.Clean(dir)
	full := filepath.Join(root, filepath.FromSlash(f.Name))

	// Guard inline: CodeQL go/zipslip only recognises a same-function prefix check.
	if !strings.HasPrefix(full, root+string(filepath.Separator)) {
		return fmt.Errorf("%w: %s", errUnsafePath, f.Name)
	}

	if f.FileInfo().IsDir() {
		if merr := os.MkdirAll(full, dirPerm); merr != nil {
			return fmt.Errorf("mkdir %s: %w", full, merr)
		}

		return nil
	}

	rc, err := f.Open()
	if err != nil {
		return fmt.Errorf("open %s: %w", f.Name, err)
	}

	body, err := io.ReadAll(io.LimitReader(rc, maxExtractedBytes))
	_ = rc.Close()

	if err != nil {
		return fmt.Errorf("read %s: %w", f.Name, err)
	}

	if err = os.MkdirAll(filepath.Dir(full), dirPerm); err != nil {
		return fmt.Errorf("mkdir %s: %w", filepath.Dir(full), err)
	}

	if err = os.WriteFile(full, body, filePerm); err != nil {
		return fmt.Errorf("write %s: %w", full, err)
	}

	if f.Mode()&0o111 != 0 {
		_ = os.Chmod(full, dirPerm)
	}

	return nil
}

// isInlineBuildspec reports whether the buildspec value is YAML text rather than a path or ARN.
func isInlineBuildspec(v string) bool {
	t := strings.TrimSpace(v)

	return strings.Contains(t, "\n") || strings.HasPrefix(t, "version:")
}

// loadBuildspec resolves the buildspec text: inline, S3 ARN, a path in the source, or buildspec.yml.
func (r *buildRunner) loadBuildspec(ctx context.Context, dir string) (string, error) {
	spec := ""
	if r.build.Source != nil {
		spec = strings.TrimSpace(r.build.Source.Buildspec)
	}

	switch {
	case spec == "":
		spec = defaultSpecName
	case isInlineBuildspec(spec):
		return spec, nil
	case strings.HasPrefix(spec, s3ARNPrefix):
		bucket, key, ok := splitS3Location(spec)
		if !ok || r.svc.Store == nil {
			return "", fmt.Errorf("%w: buildspec %s", errSourceDependency, spec)
		}

		data, err := r.svc.Store.GetObjectBytes(ctx, bucket, key)
		if err != nil {
			return "", fmt.Errorf("download buildspec %s: %w", spec, err)
		}

		return string(data), nil
	}

	full, err := safeJoin(dir, spec)
	if err != nil {
		return "", err
	}

	data, err := os.ReadFile(full)
	if err != nil {
		return "", fmt.Errorf("stat %s: %w", spec, err)
	}

	return string(data), nil
}
