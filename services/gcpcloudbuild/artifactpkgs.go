package gcpcloudbuild

import (
	"archive/tar"
	"bytes"
	"cmp"
	"compress/gzip"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"

	"cloud.google.com/go/cloudbuild/apiv1/v2/cloudbuildpb"
	"golang.org/x/mod/module"
	modzip "golang.org/x/mod/zip"
)

// PackageRegistry stores build outputs in the Artifact Registry emulator.
type PackageRegistry interface {
	PublishMaven(project, repo, group, artifact, version, ext string, data []byte) (string, error)
	PublishPython(project, repo, name, version, filename string, data []byte) (string, error)
	PublishNPM(project, repo string, tarball []byte) (pkg, name, version string, err error)
	PublishGoModule(project, repo string, zipData []byte) (pkg, modulePath, version string, err error)
}

const (
	repoURLSegs   = 2
	wheelMinParts = 2
	tarFileMode   = 0o644
)

var (
	errNoPackages = errors.New("the Artifact Registry emulator is not available to this build")
	errBadRepoURL = errors.New("repository must be https://<location>-<format>.pkg.dev/<project>/<repository>")
	errNoMatch    = errors.New("no file matches")
)

// repoURL splits https://$REGION-<format>.pkg.dev/$PROJECT/$REPOSITORY.
func arRepoURL(raw string) (string, string, error) {
	u, perr := url.Parse(raw)
	if perr != nil || !strings.HasSuffix(u.Host, ".pkg.dev") {
		return "", "", fmt.Errorf("%w: got %q", errBadRepoURL, raw)
	}

	parts := strings.Split(strings.Trim(u.Path, "/"), "/")
	if len(parts) != repoURLSegs || parts[0] == "" || parts[1] == "" {
		return "", "", fmt.Errorf("%w: got %q", errBadRepoURL, raw)
	}

	return parts[0], parts[1], nil
}

func sha256Hashes(data []byte) *cloudbuildpb.FileHashes {
	sum := sha256.Sum256(data)

	return &cloudbuildpb.FileHashes{FileHash: []*cloudbuildpb.Hash{{Type: cloudbuildpb.Hash_SHA256, Value: sum[:]}}}
}

// uploadPackages publishes artifacts.maven_artifacts, python_packages, npm_packages and go_modules.
func (r *runner) uploadPackages(res *cloudbuildpb.Results) error {
	a := r.build.GetArtifacts()
	if len(a.GetMavenArtifacts())+len(a.GetPythonPackages())+len(a.GetNpmPackages())+len(a.GetGoModules()) == 0 {
		return nil
	}

	if r.svc.Packages == nil {
		return errNoPackages
	}

	start := r.b.ts()

	steps := []func(*cloudbuildpb.Results) error{r.uploadMaven, r.uploadPython, r.uploadNPM, r.uploadGo}
	for _, step := range steps {
		if err := step(res); err != nil {
			return err
		}
	}

	if res.GetArtifactTiming() == nil {
		res.ArtifactTiming = &cloudbuildpb.TimeSpan{StartTime: start, EndTime: r.b.ts()}
	} else {
		res.ArtifactTiming.StartTime = start
	}

	return nil
}

func (r *runner) workFile(p string) (string, error) {
	files, err := r.matchArtifacts([]string{p})
	if err != nil {
		return "", err
	}

	if len(files) != 1 {
		return "", fmt.Errorf("%w %q in the workspace", errNoMatch, p)
	}

	return files[0], nil
}

func (r *runner) uploadMaven(res *cloudbuildpb.Results) error {
	for _, m := range r.build.GetArtifacts().GetMavenArtifacts() {
		start := r.b.ts()

		project, repo, err := arRepoURL(m.GetRepository())
		if err != nil {
			return err
		}

		rel, err := r.workFile(m.GetPath())
		if err != nil {
			return err
		}

		data, err := readBounded(filepath.Join(r.work, rel))
		if err != nil {
			return err
		}

		ext := filepath.Ext(rel)

		pkg, err := r.svc.Packages.PublishMaven(
			project,
			repo,
			m.GetGroupId(),
			m.GetArtifactId(),
			m.GetVersion(),
			ext,
			data,
		)
		if err != nil {
			return fmt.Errorf("maven artifact %s:%s:%s: %w", m.GetGroupId(), m.GetArtifactId(), m.GetVersion(), err)
		}

		uri := strings.TrimRight(m.GetRepository(), "/") + "/" + strings.ReplaceAll(m.GetGroupId(), ".", "/") + "/" +
			m.GetArtifactId() + "/" + m.GetVersion() + "/" + m.GetArtifactId() + "-" + m.GetVersion() + ext

		res.MavenArtifacts = append(res.MavenArtifacts, &cloudbuildpb.UploadedMavenArtifact{
			Uri: uri, FileHashes: sha256Hashes(data), ArtifactRegistryPackage: pkg,
			PushTiming: &cloudbuildpb.TimeSpan{StartTime: start, EndTime: r.b.ts()},
		})
	}

	return nil
}

// pyNameVersion reads the project name and version from a wheel or sdist file name.
func pyNameVersion(filename string) (string, string, bool) {
	stem, isWheel := strings.CutSuffix(filename, ".whl")
	if isWheel {
		parts := strings.Split(stem, "-")
		if len(parts) < wheelMinParts {
			return "", "", false
		}

		return parts[0], parts[1], true
	}

	for _, ext := range []string{".tar.gz", ".tar.bz2", ".tar.xz", ".zip", ".egg"} {
		if s, ok := strings.CutSuffix(filename, ext); ok {
			i := strings.LastIndex(s, "-")

			return s[:max(i, 0)], s[i+1:], i > 0
		}
	}

	return "", "", false
}

func (r *runner) uploadPython(res *cloudbuildpb.Results) error {
	for _, p := range r.build.GetArtifacts().GetPythonPackages() {
		project, repo, err := arRepoURL(p.GetRepository())
		if err != nil {
			return err
		}

		files, err := r.matchArtifacts(p.GetPaths())
		if err != nil {
			return err
		}

		for _, rel := range files {
			filename := filepath.Base(rel)

			name, version, ok := pyNameVersion(filename)
			if !ok {
				continue
			}

			start := r.b.ts()

			data, rerr := readBounded(filepath.Join(r.work, rel))
			if rerr != nil {
				return rerr
			}

			pkg, perr := r.svc.Packages.PublishPython(project, repo, name, version, filename, data)
			if perr != nil {
				return fmt.Errorf("python package %s: %w", filename, perr)
			}

			res.PythonPackages = append(res.PythonPackages, &cloudbuildpb.UploadedPythonPackage{
				Uri: strings.TrimRight(p.GetRepository(), "/") + "/" + filename, FileHashes: sha256Hashes(data),
				ArtifactRegistryPackage: pkg, PushTiming: &cloudbuildpb.TimeSpan{StartTime: start, EndTime: r.b.ts()},
			})
		}
	}

	return nil
}

// workDir resolves a workspace-relative directory, refusing paths that leave the workspace.
func (r *runner) workDir(p string) (string, error) {
	clean := path.Clean(strings.TrimPrefix(strings.TrimPrefix(p, workspace), "/"))
	if clean == ".." || strings.HasPrefix(clean, "../") || path.IsAbs(clean) {
		return "", fmt.Errorf("%w: %q", errUnsafeArtifact, p)
	}

	return filepath.Join(r.work, filepath.FromSlash(clean)), nil
}

// packNPM writes the directory as an npm tarball: every file under package/, without node_modules and .git.
func packNPM(dir string) ([]byte, error) {
	var buf bytes.Buffer

	zw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(zw)

	err := filepath.WalkDir(dir, func(p string, d os.DirEntry, werr error) error {
		if werr != nil {
			return werr
		}

		if d.IsDir() && (d.Name() == "node_modules" || d.Name() == ".git") {
			return filepath.SkipDir
		}

		if !d.Type().IsRegular() {
			return nil
		}

		rel, _ := filepath.Rel(dir, p)

		data, rerr := readBounded(p)
		if rerr != nil {
			return rerr
		}

		if werr = tw.WriteHeader(
			&tar.Header{Name: "package/" + filepath.ToSlash(rel), Mode: tarFileMode, Size: int64(len(data))},
		); werr != nil {
			return werr //nolint:wrapcheck // wrapped by the caller
		}

		_, werr = tw.Write(data)

		return werr //nolint:wrapcheck // wrapped by the caller
	})
	if err != nil {
		return nil, fmt.Errorf("pack npm package: %w", err)
	}

	if err = tw.Close(); err != nil {
		return nil, fmt.Errorf("pack npm package: %w", err)
	}

	if err = zw.Close(); err != nil {
		return nil, fmt.Errorf("pack npm package: %w", err)
	}

	return buf.Bytes(), nil
}

func (r *runner) uploadNPM(res *cloudbuildpb.Results) error {
	for _, n := range r.build.GetArtifacts().GetNpmPackages() {
		start := r.b.ts()

		project, repo, err := arRepoURL(n.GetRepository())
		if err != nil {
			return err
		}

		dir, err := r.workDir(n.GetPackagePath())
		if err != nil {
			return err
		}

		tarball, err := packNPM(dir)
		if err != nil {
			return err
		}

		pkg, name, version, err := r.svc.Packages.PublishNPM(project, repo, tarball)
		if err != nil {
			return fmt.Errorf("npm package %s: %w", n.GetPackagePath(), err)
		}

		base := name
		if _, after, ok := strings.Cut(name, "/"); ok {
			base = after
		}

		res.NpmPackages = append(res.NpmPackages, &cloudbuildpb.UploadedNpmPackage{
			Uri:        strings.TrimRight(n.GetRepository(), "/") + "/" + name + "/-/" + base + "-" + version + ".tgz",
			FileHashes: sha256Hashes(tarball), ArtifactRegistryPackage: pkg,
			PushTiming: &cloudbuildpb.TimeSpan{StartTime: start, EndTime: r.b.ts()},
		})
	}

	return nil
}

func (r *runner) uploadGo(res *cloudbuildpb.Results) error {
	for _, g := range r.build.GetArtifacts().GetGoModules() {
		start := r.b.ts()

		dir, err := r.workDir(cmp.Or(g.GetSourcePath(), "."))
		if err != nil {
			return err
		}

		var buf bytes.Buffer
		if err = modzip.CreateFromDir(
			&buf,
			module.Version{Path: g.GetModulePath(), Version: g.GetModuleVersion()},
			dir,
		); err != nil {
			return fmt.Errorf("zip go module %s: %w", g.GetModulePath(), err)
		}

		project := cmp.Or(g.GetRepositoryProjectId(), r.build.GetProjectId())

		pkg, mod, version, err := r.svc.Packages.PublishGoModule(project, g.GetRepositoryName(), buf.Bytes())
		if err != nil {
			return fmt.Errorf("go module %s: %w", g.GetModulePath(), err)
		}

		location := cmp.Or(g.GetRepositoryLocation(), r.ref.location)

		res.GoModules = append(res.GoModules, &cloudbuildpb.UploadedGoModule{
			Uri: "https://" + location + "-go.pkg.dev/" + project + "/" + g.GetRepositoryName() + "/" +
				escapeModule(mod) + "/@v/" + version + ".zip",
			FileHashes: sha256Hashes(buf.Bytes()), ArtifactRegistryPackage: pkg,
			PushTiming: &cloudbuildpb.TimeSpan{StartTime: start, EndTime: r.b.ts()},
		})
	}

	return nil
}

func escapeModule(p string) string {
	esc, err := module.EscapePath(p)
	if err != nil {
		return p
	}

	return esc
}
