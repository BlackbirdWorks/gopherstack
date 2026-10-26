package codebuild

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
)

const (
	cacheActionRestore = "restore"
	cacheActionSave    = "save"
	cacheKeyedDir      = "keyed"
	maxFallbackKeys    = 5
	cacheVersionLen    = 16
)

var hashFilesRe = regexp.MustCompile(`\$\(\s*codebuild-hash-files([^)]*)\)`)

func (r *buildRunner) cacheAction() string {
	if r.spec == nil || r.spec.Cache == nil {
		return ""
	}

	return strings.ToLower(strings.TrimSpace(r.spec.Cache.Action))
}

// resolveCacheKey evaluates the buildspec cache key, or leaves it empty for the single per-project archive.
func (r *buildRunner) resolveCacheKey() {
	r.cacheKey = ""

	if r.spec == nil || r.spec.Cache == nil {
		return
	}

	if k := strings.TrimSpace(r.spec.Cache.Key); k != "" {
		r.cacheKey = r.expandCacheKey(k)
	}
}

func (r *buildRunner) expandCacheKey(k string) string {
	k = hashFilesRe.ReplaceAllStringFunc(k, func(m string) string {
		args := hashFilesRe.FindStringSubmatch(m)[1]

		return r.hashFiles(strings.FieldsFunc(args, func(c rune) bool { return c == ' ' || c == '\t' }))
	})

	return strings.TrimSpace(os.Expand(k, func(name string) string { return r.envMap[name] }))
}

// hashFiles is the SHA-256 over the content hashes of every source file matching the patterns.
func (r *buildRunner) hashFiles(patterns []string) string {
	for i, p := range patterns {
		patterns[i] = strings.Trim(p, `'"`)
	}

	files, err := matchFiles(r.workDir, patterns)
	if err != nil || len(files) == 0 {
		return ""
	}

	outer := sha256.New()

	for _, f := range files {
		data, rerr := os.ReadFile(filepath.Join(r.workDir, filepath.FromSlash(f)))
		if rerr != nil {
			continue
		}

		sum := sha256.Sum256(data)
		outer.Write(sum[:])
	}

	return hex.EncodeToString(outer.Sum(nil))
}

// cacheVersion hashes the cached paths: caches with different paths never match each other.
func (r *buildRunner) cacheVersion() string {
	paths := slices.Clone(r.cachePaths())
	for i := range paths {
		paths[i] = strings.TrimSpace(paths[i])
	}

	sort.Strings(paths)
	sum := sha256.Sum256([]byte(strings.Join(paths, "\n")))

	return hex.EncodeToString(sum[:])[:cacheVersionLen]
}

func (r *buildRunner) fallbackKeys() []string {
	var out []string

	for _, k := range r.spec.Cache.FallbackKeys {
		if k = r.expandCacheKey(k); k != "" {
			out = append(out, k)
		}
	}

	if len(out) > maxFallbackKeys {
		out = out[:maxFallbackKeys]
	}

	return out
}

func (r *buildRunner) localKeyedDir() string {
	return filepath.Join(r.cacheRoot(), cacheKeyedDir, r.cacheVersion())
}

func (r *buildRunner) localKeyedEntry(key string) string {
	return filepath.Join(r.localKeyedDir(), url.PathEscape(key))
}

func (r *buildRunner) s3KeyedBase() (string, string, bool) {
	c := r.cacheCfg()
	if c == nil || c.Type != cacheTypeS3 {
		return "", "", false
	}

	bucket, prefix, _ := strings.Cut(strings.TrimPrefix(c.Location, s3ARNPrefix), "/")
	if bucket == "" {
		return "", "", false
	}

	return bucket, strings.Trim(prefix+"/"+r.build.ProjectName+"/"+cacheKeyedDir+"/"+r.cacheVersion(), "/") + "/", true
}

func (r *buildRunner) s3KeyedObject(key string) (string, string, bool) {
	bucket, base, ok := r.s3KeyedBase()

	return bucket, base + url.PathEscape(key) + "/" + cacheS3Object, ok
}

func (r *buildRunner) keyedExists(key string) bool {
	if c := r.cacheCfg(); c != nil && c.Type == cacheTypeS3 {
		bucket, obj, ok := r.s3KeyedObject(key)
		if !ok || r.svc.Store == nil {
			return false
		}

		_, err := r.svc.Store.GetObjectBytes(r.ctx, bucket, obj)

		return err == nil
	}

	_, err := os.Stat(filepath.Join(r.localKeyedDir(), url.PathEscape(key)))

	return err == nil
}

// populateStage fills the cache stage with the entry for the resolved key (exact first, then fallbacks by prefix).
func (r *buildRunner) populateStage() {
	c := r.cacheCfg()

	switch {
	case r.cacheKey != "":
		r.restoreKeyed(c)
	case c.Type == cacheTypeS3:
		if bucket, key, ok := s3CacheLocation(c.Location, r.build.ProjectName); ok {
			r.restoreS3Object(bucket, key)
		}
	default:
		if err := copyTree(filepath.Join(r.cacheRoot(), cacheCustomDir), r.cacheStage, false); err != nil {
			r.warnCache("restore local cache", err)
		}
	}
}

func (r *buildRunner) restoreKeyed(c *ProjectCache) {
	candidates := append([]string{r.cacheKey}, r.fallbackKeys()...)

	for i, cand := range candidates {
		matched := r.findKeyed(c, cand, i == 0)
		if matched == "" {
			continue
		}

		r.log.write(r.ctx, marker("Cache restored from key %q", matched))

		if c.Type == cacheTypeS3 {
			bucket, obj, _ := r.s3KeyedObject(matched)
			r.restoreS3Object(bucket, obj)
		} else if err := copyTree(r.localKeyedEntry(matched), r.cacheStage, false); err != nil {
			r.warnCache("restore local cache", err)
		}

		return
	}

	r.log.write(r.ctx, marker("No cache found for key %q", r.cacheKey))
}

// findKeyed returns the stored key matching cand: exactly for the primary key, by prefix for fallbacks.
func (r *buildRunner) findKeyed(c *ProjectCache, cand string, exact bool) string {
	if exact {
		if r.keyedExists(cand) {
			return cand
		}

		return ""
	}

	var keys []string

	if c.Type == cacheTypeS3 {
		keys = r.listS3Keyed(cand)
	} else {
		keys = r.listLocalKeyed(cand)
	}

	if len(keys) == 0 {
		return ""
	}

	return keys[len(keys)-1]
}

// listLocalKeyed returns stored keys with the prefix, oldest first.
func (r *buildRunner) listLocalKeyed(prefix string) []string {
	entries, err := os.ReadDir(r.localKeyedDir())
	if err != nil {
		return nil
	}

	type found struct {
		key string
		mod int64
	}

	var hits []found

	for _, e := range entries {
		key, uerr := url.PathUnescape(e.Name())
		if uerr != nil || !strings.HasPrefix(key, prefix) {
			continue
		}

		if info, ierr := e.Info(); ierr == nil {
			hits = append(hits, found{key: key, mod: info.ModTime().UnixNano()})
		}
	}

	sort.Slice(hits, func(i, j int) bool {
		if hits[i].mod != hits[j].mod {
			return hits[i].mod < hits[j].mod
		}

		return hits[i].key < hits[j].key
	})

	out := make([]string, len(hits))
	for i, h := range hits {
		out[i] = h.key
	}

	return out
}

// listS3Keyed returns stored keys with the prefix in lexical order.
func (r *buildRunner) listS3Keyed(prefix string) []string {
	bucket, base, ok := r.s3KeyedBase()
	if !ok || r.svc.Store == nil {
		return nil
	}

	objs, err := r.svc.Store.ListObjectKeys(r.ctx, bucket, base+url.PathEscape(prefix))
	if err != nil {
		return nil
	}

	var out []string

	for _, o := range objs {
		rest, cut := strings.CutSuffix(strings.TrimPrefix(o, base), "/"+cacheS3Object)
		if !cut {
			continue
		}

		if key, uerr := url.PathUnescape(rest); uerr == nil {
			out = append(out, key)
		}
	}

	sort.Strings(out)

	return out
}

func (r *buildRunner) restoreS3Object(bucket, key string) {
	if r.svc.Store == nil {
		return
	}

	data, err := r.svc.Store.GetObjectBytes(r.ctx, bucket, key)
	if err != nil {
		return
	}

	if uerr := untarGz(data, r.cacheStage); uerr != nil {
		r.warnCache("restore s3 cache", uerr)
	}
}

// persistStage writes the staged cache to the keyed entry, or the project's single archive when keyless.
func (r *buildRunner) persistStage() error {
	c := r.cacheCfg()

	switch {
	case c.Type == cacheTypeS3 && r.cacheKey != "":
		bucket, obj, _ := r.s3KeyedObject(r.cacheKey)

		return r.uploadS3Cache(bucket, obj)
	case c.Type == cacheTypeS3:
		bucket, key, ok := s3CacheLocation(c.Location, r.build.ProjectName)
		if !ok {
			return fmt.Errorf("%w: S3 cache location", errSourceDependency)
		}

		return r.uploadS3Cache(bucket, key)
	case r.cacheKey != "":
		return replaceTree(r.cacheStage, filepath.Join(r.localKeyedDir(), url.PathEscape(r.cacheKey)))
	default:
		return replaceTree(r.cacheStage, filepath.Join(r.cacheRoot(), cacheCustomDir))
	}
}

// emptyKeyedObjects empties every keyed S3 cache archive of the project.
func emptyKeyedObjects(ctx context.Context, store ObjectStore, proj *Project) error {
	bucket, prefix, _ := strings.Cut(strings.TrimPrefix(proj.Cache.Location, s3ARNPrefix), "/")
	base := strings.Trim(prefix+"/"+proj.Name+"/"+cacheKeyedDir, "/") + "/"

	keys, err := store.ListObjectKeys(ctx, bucket, base)
	if err != nil {
		return fmt.Errorf("list keyed caches: %w", err)
	}

	for _, k := range keys {
		if perr := store.PutObjectBytes(ctx, bucket, k, emptyArchive()); perr != nil {
			return perr
		}
	}

	return nil
}
