package codebuild

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	cacheTypeLocal = "LOCAL"
	cacheTypeS3    = "S3"

	cacheModeSource = "LOCAL_SOURCE_CACHE"
	cacheModeDocker = "LOCAL_DOCKER_LAYER_CACHE"
	cacheModeCustom = "LOCAL_CUSTOM_CACHE"

	containerCacheDir = "/codebuild/cache"
	containerDockerLB = "/var/lib/docker"
	cacheSrcDir       = "src"
	cacheAbsTar       = "abs.tar"
	cacheCustomDir    = "custom"
	cacheSourceDir    = "source"
	cacheDockerDir    = "docker"
	cacheS3Object     = "cache.tar.gz"
)

var errCacheCommand = errors.New("cache archive command failed")

type specCache struct {
	Key          string   `yaml:"key"`
	Action       string   `yaml:"action"`
	FallbackKeys []string `yaml:"fallback-keys"`
	Paths        []string `yaml:"paths"`
}

func (r *buildRunner) cacheCfg() *ProjectCache {
	c := r.build.Cache
	if c == nil || (c.Type != cacheTypeLocal && c.Type != cacheTypeS3) {
		return nil
	}

	return c
}

func (r *buildRunner) localMode(mode string) bool {
	c := r.cacheCfg()

	return c != nil && c.Type == cacheTypeLocal && slices.Contains(c.Modes, mode)
}

// cacheRoot is the persistent host directory holding this project's local caches.
func (r *buildRunner) cacheRoot() string {
	key := filepath.Base(r.b.accountID + "-" + r.region + "-" + r.build.ProjectName)

	return filepath.Join(r.cfg.CacheDir, key)
}

// cacheMounts prepares the per-build cache stage and returns the container bind mounts it needs.
func (r *buildRunner) cacheMounts() ([]string, error) {
	c := r.cacheCfg()
	if c == nil {
		return nil, nil
	}

	var mounts []string

	if r.localMode(cacheModeDocker) && r.build.Environment != nil && r.build.Environment.PrivilegedMode {
		dir := filepath.Join(r.cacheRoot(), cacheDockerDir)
		if err := os.MkdirAll(dir, dirPerm); err != nil {
			return nil, fmt.Errorf("create docker layer cache: %w", err)
		}

		mounts = append(mounts, dir+":"+containerDockerLB)
	}

	if !r.localMode(cacheModeCustom) && c.Type != cacheTypeS3 {
		return mounts, nil
	}

	stage, err := os.MkdirTemp(r.cfg.WorkDir, "codebuild-cache-")
	if err != nil {
		return nil, fmt.Errorf("create cache stage: %w", err)
	}

	r.cacheStage = stage

	return append(mounts, stage+":"+containerCacheDir), nil
}

func (r *buildRunner) warnCache(what string, err error) {
	logger.Load(r.ctx).WarnContext(r.ctx, "CodeBuild: cache "+what+" failed", "error", err)
	r.log.write(r.ctx, marker("Cache %s failed: %v", what, err))
}

func s3CacheLocation(loc, project string) (string, string, bool) {
	bucket, prefix, _ := strings.Cut(strings.TrimPrefix(loc, s3ARNPrefix), "/")
	if bucket == "" {
		return "", "", false
	}

	return bucket, strings.Trim(prefix+"/"+project+"/"+cacheS3Object, "/"), true
}

// splitCachePaths separates source-relative patterns from absolute container directories.
func splitCachePaths(paths []string, srcDir string) ([]string, []string) {
	var rel, abs []string

	for _, p := range paths {
		p = strings.TrimSpace(p)

		switch {
		case p == "":
		case strings.HasPrefix(p, srcDir+"/"):
			rel = append(rel, strings.TrimPrefix(p, srcDir+"/"))
		case strings.HasPrefix(p, "/"):
			abs = append(abs, staticPrefix(p))
		default:
			rel = append(rel, p)
		}
	}

	return rel, abs
}

// staticPrefix is the longest directory prefix of a glob without wildcards.
func staticPrefix(p string) string {
	if i := strings.IndexAny(p, "*?["); i >= 0 {
		p = p[:i]
		if j := strings.LastIndex(p, "/"); j >= 0 {
			p = p[:j]
		}
	}

	return strings.TrimSuffix(p, "/")
}

func (r *buildRunner) cachePaths() []string {
	if r.spec == nil || r.spec.Cache == nil {
		return nil
	}

	return r.spec.Cache.Paths
}

func (r *buildRunner) restoreCache() {
	if r.cacheStage == "" {
		return
	}

	rel, abs := splitCachePaths(r.cachePaths(), r.srcDir)
	if len(rel)+len(abs) == 0 {
		return
	}

	r.resolveCacheKey()

	if r.cacheAction() == cacheActionSave {
		return
	}

	r.log.write(r.ctx, marker("Restoring cache"))
	r.populateStage()

	if err := copyTree(filepath.Join(r.cacheStage, cacheSrcDir), r.workDir, true); err != nil {
		r.warnCache("restore", err)
	}

	tarPath := filepath.Join(r.cacheStage, cacheAbsTar)
	if _, err := os.Stat(tarPath); err != nil || len(abs) == 0 {
		return
	}

	if err := r.execCache("tar -xf " + containerCacheDir + "/" + cacheAbsTar + " -C /"); err != nil {
		r.warnCache("restore", err)
	}
}

func absTarScript(abs []string) string {
	var sb strings.Builder

	sb.WriteString("set --; for p in")

	for _, p := range abs {
		sb.WriteString(" " + shQuote(strings.TrimPrefix(p, "/")))
	}

	sb.WriteString("; do [ -e \"/$p\" ] && set -- \"$@\" \"$p\"; done\n")
	sb.WriteString("[ $# -eq 0 ] || tar -cf " + containerCacheDir + "/" + cacheAbsTar + " -C / \"$@\"\n")

	return sb.String()
}

// saveCache archives the buildspec's cache paths and persists them locally or in S3.
func (r *buildRunner) saveCache() error {
	if r.cacheStage == "" || !r.succeeding {
		return nil
	}

	rel, abs := splitCachePaths(r.cachePaths(), r.srcDir)
	if len(rel)+len(abs) == 0 {
		return nil
	}

	if r.cacheAction() == cacheActionRestore {
		return nil
	}

	if r.cacheKey != "" && r.keyedExists(r.cacheKey) {
		r.log.write(r.ctx, marker("Cache for key %q already exists, not updating", r.cacheKey))

		return nil
	}

	r.log.write(r.ctx, marker("Saving cache"))

	if err := r.stageCache(rel, abs); err != nil {
		return err
	}

	return r.persistStage()
}

func (r *buildRunner) stageCache(rel, abs []string) error {
	_ = os.RemoveAll(filepath.Join(r.cacheStage, cacheSrcDir))
	_ = os.Remove(filepath.Join(r.cacheStage, cacheAbsTar))

	files, err := matchFiles(r.workDir, rel)
	if err != nil {
		return err
	}

	for _, f := range files {
		if err = copyFile(filepath.Join(r.workDir, filepath.FromSlash(f)),
			filepath.Join(r.cacheStage, cacheSrcDir, filepath.FromSlash(f))); err != nil {
			return err
		}
	}

	if len(abs) == 0 {
		return nil
	}

	return r.execCache(absTarScript(abs))
}

func (r *buildRunner) execCache(script string) error {
	res, err := r.rt.Exec(r.ctx, r.containerID, shellInvocation(script))
	if err != nil {
		return fmt.Errorf("cache exec: %w", err)
	}

	if res.ExitCode != 0 {
		return fmt.Errorf("%w: %s", errCacheCommand, strings.TrimSpace(res.Stdout+res.Stderr))
	}

	return nil
}

func (r *buildRunner) uploadS3Cache(bucket, key string) error {
	if r.svc.Store == nil {
		return fmt.Errorf("%w: S3 cache", errSourceDependency)
	}

	data, err := tarGz(r.cacheStage)
	if err != nil {
		return err
	}

	return r.svc.Store.PutObjectBytes(r.ctx, bucket, key, data)
}

func (r *buildRunner) restoreSourceCache(dir string) {
	if !r.localMode(cacheModeSource) {
		return
	}

	_ = copyTree(filepath.Join(r.cacheRoot(), cacheSourceDir), dir, false)
}

func (r *buildRunner) saveSourceCache(dir string) {
	if !r.localMode(cacheModeSource) {
		return
	}

	root := r.cacheRoot()
	if err := os.MkdirAll(root, dirPerm); err != nil {
		r.warnCache("save source cache", err)

		return
	}

	tmp, err := os.MkdirTemp(root, cacheSourceDir+".tmp-")
	if err != nil {
		r.warnCache("save source cache", err)

		return
	}

	defer os.RemoveAll(tmp)

	if err = copyTree(filepath.Join(dir, ".git"), filepath.Join(tmp, ".git"), false); err != nil {
		r.warnCache("save source cache", err)

		return
	}

	_ = os.RemoveAll(filepath.Join(root, cacheSourceDir))
	_ = os.Rename(tmp, filepath.Join(root, cacheSourceDir))
}

func copyFile(src, dst string) error {
	data, err := os.ReadFile(src)
	if err != nil {
		return fmt.Errorf("read %s: %w", src, err)
	}

	mode := fs.FileMode(filePerm)
	if st, serr := os.Stat(src); serr == nil {
		mode = st.Mode().Perm() | filePerm
	}

	if err = os.MkdirAll(filepath.Dir(dst), dirPerm); err != nil {
		return fmt.Errorf("mkdir %s: %w", filepath.Dir(dst), err)
	}

	if err = os.WriteFile(dst, data, mode); err != nil { //nolint:gosec // dst is under the build's own cache dirs
		return fmt.Errorf("write %s: %w", dst, err)
	}

	return nil
}

// copyTree copies regular files from src into dst; a missing src is not an error.
func copyTree(src, dst string, skipExisting bool) error {
	if _, err := os.Stat(src); err != nil {
		return nil //nolint:nilerr // missing source tree means nothing to copy
	}

	return filepath.WalkDir(src, func(p string, d fs.DirEntry, werr error) error {
		if werr != nil || !d.Type().IsRegular() {
			return werr
		}

		rel, err := filepath.Rel(src, p)
		if err != nil {
			return fmt.Errorf("rel %s: %w", p, err)
		}

		target := filepath.Join(dst, rel)
		if _, serr := os.Stat(target); skipExisting && serr == nil {
			return nil
		}

		return copyFile(p, target)
	})
}

// replaceTree swaps dst for a copy of src.
func replaceTree(src, dst string) error {
	if err := os.MkdirAll(filepath.Dir(dst), dirPerm); err != nil {
		return fmt.Errorf("mkdir %s: %w", filepath.Dir(dst), err)
	}

	tmp, err := os.MkdirTemp(filepath.Dir(dst), filepath.Base(dst)+".tmp-")
	if err != nil {
		return fmt.Errorf("mkdir temp for %s: %w", dst, err)
	}

	defer os.RemoveAll(tmp)

	if err = copyTree(src, tmp, false); err != nil {
		return err
	}

	_ = os.RemoveAll(dst)

	if err = os.Rename(tmp, dst); err != nil {
		if _, serr := os.Stat(dst); serr == nil {
			return nil // a concurrent build already persisted a complete tree
		}

		return fmt.Errorf("persist cache: %w", err)
	}

	return nil
}

func tarGz(dir string) ([]byte, error) {
	var buf bytes.Buffer

	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)

	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, werr error) error {
		if werr != nil || !d.Type().IsRegular() {
			return werr
		}

		rel, rerr := filepath.Rel(dir, p)
		if rerr != nil {
			return fmt.Errorf("rel %s: %w", p, rerr)
		}

		data, rerr := os.ReadFile(p) //nolint:gosec // p is walked under the build's own cache stage
		if rerr != nil {
			return fmt.Errorf("read %s: %w", p, rerr)
		}

		mode := int64(filePerm)
		if st, serr := os.Stat(p); serr == nil {
			mode = int64(st.Mode().Perm())
		}

		hdr := &tar.Header{Name: filepath.ToSlash(rel), Mode: mode, Size: int64(len(data)), Typeflag: tar.TypeReg}
		if rerr = tw.WriteHeader(hdr); rerr != nil {
			return fmt.Errorf("tar header: %w", rerr)
		}

		_, rerr = tw.Write(data)

		return rerr
	})
	if err != nil {
		return nil, fmt.Errorf("archive cache: %w", err)
	}

	if err = tw.Close(); err != nil {
		return nil, fmt.Errorf("close tar: %w", err)
	}

	if err = gz.Close(); err != nil {
		return nil, fmt.Errorf("close gzip: %w", err)
	}

	return buf.Bytes(), nil
}

func untarGz(data []byte, dir string) error {
	gz, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("open cache archive: %w", err)
	}

	tr := tar.NewReader(gz)

	for {
		hdr, nerr := tr.Next()
		if errors.Is(nerr, io.EOF) {
			return nil
		}

		if nerr != nil {
			return fmt.Errorf("read cache archive: %w", nerr)
		}

		if hdr.Typeflag != tar.TypeReg {
			continue
		}

		if nerr = extractCacheEntry(dir, hdr, tr); nerr != nil {
			return nerr
		}
	}
}

func extractCacheEntry(dir string, hdr *tar.Header, r io.Reader) error {
	if strings.Contains(hdr.Name, "..") {
		return fmt.Errorf("%w: %s", errUnsafePath, hdr.Name)
	}

	root := filepath.Clean(dir)
	full := filepath.Join(root, filepath.FromSlash(hdr.Name))

	if !strings.HasPrefix(full, root+string(filepath.Separator)) {
		return fmt.Errorf("%w: %s", errUnsafePath, hdr.Name)
	}

	body, err := io.ReadAll(io.LimitReader(r, maxExtractedBytes))
	if err != nil {
		return fmt.Errorf("read %s: %w", hdr.Name, err)
	}

	if err = os.MkdirAll(filepath.Dir(full), dirPerm); err != nil {
		return fmt.Errorf("mkdir %s: %w", filepath.Dir(full), err)
	}

	mode := fs.FileMode(filePerm)
	if hdr.Mode&0o111 != 0 {
		mode = dirPerm
	}

	if err = os.WriteFile(full, body, mode); err != nil {
		return fmt.Errorf("write %s: %w", full, err)
	}

	return os.Chmod(full, mode)
}

// invalidateEngineCache removes a project's local caches and empties its S3 cache archive.
func (b *InMemoryBackend) invalidateEngineCache(ctx context.Context, proj *Project) error {
	eng := b.engine
	if eng == nil {
		return nil
	}

	key := filepath.Base(b.accountID + "-" + b.region + "-" + proj.Name)
	if err := os.RemoveAll(filepath.Join(eng.cfg.CacheDir, key)); err != nil {
		return fmt.Errorf("invalidate local cache: %w", err)
	}

	if proj.Cache == nil || proj.Cache.Type != cacheTypeS3 {
		return nil
	}

	bucket, objKey, ok := s3CacheLocation(proj.Cache.Location, proj.Name)
	store := eng.svc().Store

	if !ok || store == nil {
		return nil
	}

	if err := store.PutObjectBytes(ctx, bucket, objKey, emptyArchive()); err != nil {
		return err
	}

	return emptyKeyedObjects(ctx, store, proj)
}

func emptyArchive() []byte {
	var buf bytes.Buffer

	gz := gzip.NewWriter(&buf)
	_ = tar.NewWriter(gz).Close()
	_ = gz.Close()

	return buf.Bytes()
}
