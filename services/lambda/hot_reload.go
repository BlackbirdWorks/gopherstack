package lambda

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/labstack/echo/v5"
)

// hotReloadCodeSha is the CodeSha256 LocalStack reports for hot-reload functions.
const hotReloadCodeSha = "hot-reloading-hash-not-available"

// ErrInvalidHotReloadPath is returned when a hot-reload S3Key is not a usable local directory.
var ErrInvalidHotReloadPath = errors.New("invalid hot-reload path")

// forbiddenHotReloadRoots lists host trees that must never be mounted into a function.
func forbiddenHotReloadRoots() []string {
	return []string{
		"/proc", "/sys", "/dev", "/run", "/var/run", "/boot", "/etc", "/root",
		"/var/lib/docker", "/var/lib/containerd", "/var/lib/podman", "/var/lib/containers",
	}
}

func isForbiddenHotReloadPath(p string) bool {
	if p == "/" {
		return true
	}

	for _, root := range forbiddenHotReloadRoots() {
		if p == root || strings.HasPrefix(p, root+"/") {
			return true
		}
	}

	return false
}

// hotReloadMarker returns the magic bucket name; env, not Settings, to keep Settings pointer-free.
func hotReloadMarker() string {
	if v := os.Getenv("LAMBDA_HOT_RELOAD_BUCKET"); v != "" {
		return v
	}

	return defaultHotReloadBucket
}

// IsHotReloadBucket reports whether bucket is the enabled hot-reload magic bucket.
func (b *InMemoryBackend) IsHotReloadBucket(bucket string) bool {
	return !b.settings.DisableHotReload && bucket != "" && bucket == hotReloadMarker()
}

func (b *InMemoryBackend) isHotReloadFunction(fn *FunctionConfiguration) bool {
	return fn.PackageType == PackageTypeZip && len(fn.ZipData) == 0 && b.IsHotReloadBucket(fn.S3BucketCode)
}

// hotReloadRootsEnv lists the directories hot-reload mounts must live under.
const hotReloadRootsEnv = "LAMBDA_HOT_RELOAD_ROOTS"

// configuredHotReloadRoots returns LAMBDA_HOT_RELOAD_ROOTS, or the home dir and temp dir when unset.
func configuredHotReloadRoots() []string {
	if v := os.Getenv(hotReloadRootsEnv); v != "" {
		return filepath.SplitList(v)
	}

	roots := []string{os.TempDir()}
	if home, err := os.UserHomeDir(); err == nil {
		roots = append(roots, home)
	}

	return roots
}

// resolveHotReloadRoots returns the absolute, symlink-resolved existing roots and the ones skipped.
func resolveHotReloadRoots(roots []string) ([]string, []string) {
	var resolved, skipped []string

	for _, r := range roots {
		if r == "" {
			continue
		}

		abs, err := filepath.Abs(r)
		if err == nil {
			abs, err = filepath.EvalSymlinks(abs)
		}

		if err != nil {
			skipped = append(skipped, r)

			continue
		}

		resolved = append(resolved, filepath.Clean(abs))
	}

	return resolved, skipped
}

// withinHotReloadRoot reports whether the cleaned absolute path p is a root or lies under one.
func withinHotReloadRoot(p string, roots []string) bool {
	for _, root := range roots {
		prefix := strings.TrimSuffix(root, string(filepath.Separator)) + string(filepath.Separator)
		if p == root || strings.HasPrefix(p, prefix) {
			return true
		}
	}

	return false
}

// sanitizeHotReloadPath returns key's env-expanded, cleaned, symlink-resolved absolute path,
// rejecting relative, traversing, forbidden-root and out-of-roots paths. Only the returned value may reach file ops.
func sanitizeHotReloadPath(key string, roots []string) (string, error) {
	expanded := os.ExpandEnv(key)

	if !filepath.IsAbs(expanded) {
		return "", fmt.Errorf("%w: S3Key must be an absolute path, got %q", ErrInvalidHotReloadPath, key)
	}

	if slices.Contains(strings.Split(filepath.ToSlash(expanded), "/"), "..") {
		return "", fmt.Errorf("%w: path traversal is not allowed in %q", ErrInvalidHotReloadPath, key)
	}

	clean := filepath.Clean(expanded)

	resolved, err := filepath.EvalSymlinks(clean)
	if err != nil {
		return "", fmt.Errorf("%w: %q is not accessible: %w", ErrInvalidHotReloadPath, clean, err)
	}

	if isForbiddenHotReloadPath(clean) || isForbiddenHotReloadPath(resolved) {
		return "", fmt.Errorf("%w: mounting %q is not allowed", ErrInvalidHotReloadPath, clean)
	}

	safe := filepath.Clean(resolved)
	if !withinHotReloadRoot(safe, roots) {
		return "", fmt.Errorf(
			"%w: %q is outside the allowed roots; set %s to permit it",
			ErrInvalidHotReloadPath, safe, hotReloadRootsEnv,
		)
	}

	return safe, nil
}

// ResolveHotReloadPath expands env placeholders in key and validates it as an
// absolute, traversal-free path to an existing directory.
func ResolveHotReloadPath(key string) (string, error) {
	return ResolveHotReloadPathIn(key, configuredHotReloadRoots())
}

// ResolveHotReloadPathIn is ResolveHotReloadPath restricted to the given allowed root directories.
func ResolveHotReloadPathIn(key string, roots []string) (string, error) {
	allowed, _ := resolveHotReloadRoots(roots)

	safe, err := sanitizeHotReloadPath(key, allowed)
	if err != nil {
		return "", err
	}

	info, err := os.Stat(safe)
	if err != nil {
		return "", fmt.Errorf("%w: %q is not accessible: %w", ErrInvalidHotReloadPath, safe, err)
	}

	if !info.IsDir() {
		return "", fmt.Errorf("%w: %q is not a directory", ErrInvalidHotReloadPath, safe)
	}

	return safe, nil
}

// ValidateHotReloadCode validates a hot-reload S3Key when bucket is the magic bucket.
// It is a no-op (false) for ordinary S3 buckets.
func (b *InMemoryBackend) ValidateHotReloadCode(bucket, key string) (bool, error) {
	if !b.IsHotReloadBucket(bucket) {
		return false, nil
	}

	dir, err := ResolveHotReloadPath(key)
	if err != nil {
		return true, err
	}

	if _, err = hotReloadFingerprint(dir); err != nil {
		return true, err
	}

	return true, nil
}

// hotReloadFingerprint hashes path, size, mtime and mode of every entry under root.
func hotReloadFingerprint(root string) (string, error) {
	h := sha256.New()

	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}

		info, infoErr := d.Info()
		if infoErr != nil {
			return infoErr
		}

		if info.Mode()&(fs.ModeSocket|fs.ModeDevice|fs.ModeCharDevice) != 0 {
			return fmt.Errorf("%w: %q is a socket or device file", ErrInvalidHotReloadPath, path)
		}

		fmt.Fprintf(h, "%s|%d|%d|%d\n", path, info.Size(), info.ModTime().UnixNano(), info.Mode())

		return nil
	})
	if err != nil {
		return "", fmt.Errorf("scan %q: %w", root, err)
	}

	return hex.EncodeToString(h.Sum(nil)), nil
}

// hotReloadFingerprintFor fingerprints fn's live directory; "" with nil error when fn is not hot-reload.
func (b *InMemoryBackend) hotReloadFingerprintFor(fn *FunctionConfiguration) (string, error) {
	if !b.isHotReloadFunction(fn) {
		return "", nil
	}

	dir, err := ResolveHotReloadPath(fn.S3KeyCode)
	if err != nil {
		return "", err
	}

	return hotReloadFingerprint(dir)
}

// recycleStaleHotReloadRuntime stops fn's warm runtime when its live directory changed,
// so the next invocation starts a fresh container on the new code.
func (b *InMemoryBackend) recycleStaleHotReloadRuntime(fn *FunctionConfiguration) {
	if !b.isHotReloadFunction(fn) {
		return
	}

	b.mu.RLock("recycleStaleHotReloadRuntime")
	rt := b.runtimes[fn.FunctionName]
	b.mu.RUnlock()

	if rt == nil {
		return
	}

	now := time.Now()
	interval := time.Duration(b.settings.HotReloadIntervalMS) * time.Millisecond

	rt.mu.Lock("recycleStaleHotReloadRuntime")

	if !rt.started || rt.containerID == "" || (interval > 0 && now.Sub(rt.hotCheckedAt) < interval) {
		rt.mu.Unlock()

		return
	}

	rt.hotCheckedAt = now
	known := rt.hotFP
	rt.mu.Unlock()

	if fp, fpErr := b.hotReloadFingerprintFor(fn); fpErr == nil && fp == known {
		return
	}

	b.mu.Lock("recycleStaleHotReloadRuntime")

	owned := b.runtimes[fn.FunctionName] == rt
	if owned {
		delete(b.runtimes, fn.FunctionName)
	}

	b.mu.Unlock()

	if !owned {
		return
	}

	ctx, cancel := context.WithTimeout(b.ctx, containerShutdownTimeout)
	defer cancel()

	b.cleanupRuntime(ctx, rt)
}

// HotReloadResolver is implemented by backends that support the hot-reload magic bucket.
type HotReloadResolver interface {
	IsHotReloadBucket(bucket string) bool
	ValidateHotReloadCode(bucket, key string) (bool, error)
}

// validateHotReloadCode writes a 400 and returns false when bucket is the hot-reload bucket and key is invalid.
func (h *Handler) validateHotReloadCode(c *echo.Context, bucket, key string) bool {
	r, ok := h.Backend.(HotReloadResolver)
	if !ok {
		return true
	}

	if _, err := r.ValidateHotReloadCode(bucket, key); err != nil {
		_ = h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", err.Error())

		return false
	}

	return true
}

// applyHotReloadDigest sets LocalStack's placeholder digest and zero size for hot-reload functions.
func (h *Handler) applyHotReloadDigest(fn *FunctionConfiguration) {
	r, ok := h.Backend.(HotReloadResolver)
	if !ok || len(fn.ZipData) > 0 || !r.IsHotReloadBucket(fn.S3BucketCode) {
		return
	}

	fn.CodeSha256 = hotReloadCodeSha
	fn.CodeSize = 0
}
