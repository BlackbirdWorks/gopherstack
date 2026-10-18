package codebuild

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
)

const (
	artifactTypeS3     = "S3"
	packagingZip       = "ZIP"
	namespaceBuildID   = "BUILD_ID"
	artifactsHostRoot  = "."
	globRecursiveToken = "**"
)

var errNoArtifactFiles = errors.New("no matching artifact paths found")

// globMatch matches a slash-separated path against a pattern where "**" spans directories.
func globMatch(pattern, name string) bool {
	return matchSegments(strings.Split(strings.TrimPrefix(pattern, "./"), "/"), strings.Split(name, "/"))
}

func matchSegments(pat, segs []string) bool {
	for len(pat) > 0 {
		if pat[0] == globRecursiveToken {
			for i := 0; i <= len(segs); i++ {
				if matchSegments(pat[1:], segs[i:]) {
					return true
				}
			}

			return false
		}

		if len(segs) == 0 {
			return false
		}

		if ok, err := path.Match(pat[0], segs[0]); err != nil || !ok {
			return false
		}

		pat, segs = pat[1:], segs[1:]
	}

	return len(segs) == 0
}

// matchFiles returns the sorted slash-relative regular files under base matching any pattern.
func matchFiles(base string, patterns []string) ([]string, error) {
	expanded := make([]string, 0, len(patterns))

	for _, p := range patterns {
		p = strings.TrimPrefix(strings.TrimSpace(p), "./")

		switch {
		case p == "" || p == artifactsHostRoot:
			expanded = append(expanded, "**/*")
		case strings.HasSuffix(p, "/"):
			expanded = append(expanded, p+"**/*")
		default:
			if st, err := os.Stat(filepath.Join(base, filepath.FromSlash(p))); err == nil && st.IsDir() {
				p += "/**/*"
			}

			expanded = append(expanded, p)
		}
	}

	var out []string

	err := filepath.WalkDir(base, func(p string, d fs.DirEntry, werr error) error {
		if werr != nil {
			return werr
		}

		if !d.Type().IsRegular() {
			return nil
		}

		rel, rerr := filepath.Rel(base, p)
		if rerr != nil {
			return rerr
		}

		rel = filepath.ToSlash(rel)

		for _, pat := range expanded {
			if globMatch(pat, rel) {
				out = append(out, rel)

				break
			}
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk artifacts: %w", err)
	}

	sort.Strings(out)

	return out, nil
}

// artifactKeys maps each matched file to its destination relative path, applying discard-paths.
func artifactKeys(files []string, discard bool) map[string]string {
	out := make(map[string]string, len(files))

	for _, f := range files {
		dst := f
		if discard {
			dst = path.Base(f)
		}

		out[f] = dst
	}

	return out
}

// artifactKeyPrefix builds <path>/<namespace>/<name> as AWS lays out S3 artifacts.
func artifactKeyPrefix(cfg ProjectArtifacts, buildUUID, name string) string {
	parts := []string{}

	if p := strings.Trim(cfg.Path, "/"); p != "" {
		parts = append(parts, p)
	}

	if cfg.NamespaceType == namespaceBuildID {
		parts = append(parts, buildUUID)
	}

	if name != "" {
		parts = append(parts, strings.Trim(name, "/"))
	}

	return strings.Join(parts, "/")
}

func zipFiles(base string, keys map[string]string) ([]byte, error) {
	var buf bytes.Buffer

	zw := zip.NewWriter(&buf)

	names := make([]string, 0, len(keys))
	for src := range keys {
		names = append(names, src)
	}

	sort.Strings(names)

	for _, src := range names {
		data, err := os.ReadFile(
			filepath.Join(base, filepath.FromSlash(src)),
		)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", src, err)
		}

		w, err := zw.Create(keys[src])
		if err != nil {
			return nil, fmt.Errorf("zip %s: %w", src, err)
		}

		if _, err = w.Write(data); err != nil {
			return nil, fmt.Errorf("zip %s: %w", src, err)
		}
	}

	if err := zw.Close(); err != nil {
		return nil, fmt.Errorf("close zip: %w", err)
	}

	return buf.Bytes(), nil
}

// uploadOne uploads one artifact set to the project's S3 location.
func (r *buildRunner) uploadOne(ctx context.Context, dir string, cfg ProjectArtifacts, sa specArtifact) error {
	if len(sa.Files) == 0 {
		return nil
	}

	base, err := safeJoin(dir, sa.BaseDirectory)
	if err != nil {
		return err
	}

	files, err := matchFiles(base, sa.Files)
	if err != nil {
		return err
	}

	if len(files) == 0 {
		return errNoArtifactFiles
	}

	if r.svc.Store == nil {
		return fmt.Errorf("%w: S3", errSourceDependency)
	}

	keys := artifactKeys(files, yamlTruthy(sa.DiscardPaths))

	name := cfg.Name
	if cfg.OverrideArtifactName && sa.Name != "" {
		name = sa.Name
	}

	uuid := r.buildUUID()

	if cfg.Packaging == packagingZip {
		if name == "" {
			name = r.build.ProjectName + ".zip"
		}

		data, zerr := zipFiles(base, keys)
		if zerr != nil {
			return zerr
		}

		return r.svc.Store.PutObjectBytes(ctx, cfg.Location, artifactKeyPrefix(cfg, uuid, name), data)
	}

	prefix := artifactKeyPrefix(cfg, uuid, name)

	for _, src := range files {
		data, rerr := os.ReadFile(
			filepath.Join(base, filepath.FromSlash(src)),
		)
		if rerr != nil {
			return fmt.Errorf("read %s: %w", src, rerr)
		}

		key := keys[src]
		if prefix != "" {
			key = prefix + "/" + key
		}

		if perr := r.svc.Store.PutObjectBytes(ctx, cfg.Location, key, data); perr != nil {
			return fmt.Errorf("upload %s: %w", key, perr)
		}
	}

	return nil
}

// uploadArtifacts uploads the primary and secondary artifacts declared by the buildspec.
func (r *buildRunner) uploadArtifacts(ctx context.Context, dir string) error {
	if r.spec == nil || r.spec.Artifacts == nil {
		return nil
	}

	if a := r.build.Artifacts; a != nil && a.Type == artifactTypeS3 {
		if err := r.uploadOne(ctx, dir, *a, r.spec.Artifacts.specArtifact); err != nil {
			return err
		}
	}

	ids := make([]string, 0, len(r.spec.Artifacts.Secondary))
	for id := range r.spec.Artifacts.Secondary {
		ids = append(ids, id)
	}

	sort.Strings(ids)

	for _, id := range ids {
		for _, cfg := range r.build.SecondaryArtifacts {
			if cfg.ArtifactIdentifier == id && cfg.Type == artifactTypeS3 {
				if err := r.uploadOne(ctx, dir, cfg, r.spec.Artifacts.Secondary[id]); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (r *buildRunner) buildUUID() string {
	_, id, _ := strings.Cut(r.build.ID, ":")

	return id
}
