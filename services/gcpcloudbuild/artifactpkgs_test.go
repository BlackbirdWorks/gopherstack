package gcpcloudbuild_test

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"
	"cloud.google.com/go/cloudbuild/apiv1/v2/cloudbuildpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/services/gcpartifactregistry"
	"github.com/blackbirdworks/gopherstack/services/gcpcloudbuild"
)

const arParent = "projects/" + project + "/locations/us-central1"

func newRegistry(
	t *testing.T,
	formats map[string]artifactregistrypb.Repository_Format,
) *gcpartifactregistry.InMemoryBackend {
	t.Helper()

	ar := gcpartifactregistry.NewInMemoryBackend()
	ar.SetContext(t.Context())

	for id, f := range formats {
		_, err := ar.CreateRepository(&artifactregistrypb.CreateRepositoryRequest{
			Parent: arParent, RepositoryId: id, Repository: &artifactregistrypb.Repository{Format: f},
		})
		require.NoError(t, err)
	}

	return ar
}

func listPackages(t *testing.T, ar *gcpartifactregistry.InMemoryBackend, repo string) []string {
	t.Helper()

	pkgs, _, err := ar.ListPackages(&artifactregistrypb.ListPackagesRequest{Parent: arParent + "/repositories/" + repo})
	require.NoError(t, err)

	out := make([]string, 0, len(pkgs))
	for _, p := range pkgs {
		out = append(out, p.GetDisplayName())
	}

	return out
}

func writeFiles(t *testing.T, host string, files map[string]string) {
	t.Helper()

	for name, body := range files {
		p := filepath.Join(host, filepath.FromSlash(name))
		require.NoError(t, os.MkdirAll(filepath.Dir(p), 0o750))
		require.NoError(t, os.WriteFile(p, []byte(body), 0o600))
	}
}

func TestArtifactsUploadedToArtifactRegistry(t *testing.T) {
	t.Parallel()

	files := map[string]string{
		"target/app-1.0.jar":                 "jar bytes",
		"dist/my_pkg-0.3.0-py3-none-any.whl": "wheel bytes",
		"dist/my_pkg-0.3.0.tar.gz":           "sdist bytes",
		"web/package.json":                   `{"name":"@acme/web","version":"2.1.0"}`,
		"web/index.js":                       "module.exports = 1\n",
		"web/node_modules/dep/index.js":      "ignored",
		"mod/go.mod":                         "module example.com/mod\n\ngo 1.21\n",
		"mod/mod.go":                         "package mod\n",
	}

	build := func(_ context.Context, spec container.Spec, _ io.Writer) int {
		host, _, _ := strings.Cut(spec.Mounts[0], ":")
		writeFiles(t, host, files)

		return 0
	}

	ar := newRegistry(t, map[string]artifactregistrypb.Repository_Format{
		"mvn": artifactregistrypb.Repository_MAVEN, "py": artifactregistrypb.Repository_PYTHON,
		"npm": artifactregistrypb.Repository_NPM, "go": artifactregistrypb.Repository_GO,
	})

	e := newEnv(t, map[string]behavior{"builder": build})
	e.configure(func(s *gcpcloudbuild.Services) { s.Packages = ar })

	b := buildOf(step("builder"))
	b.Artifacts = &cloudbuildpb.Artifacts{
		MavenArtifacts: []*cloudbuildpb.Artifacts_MavenArtifact{{
			Repository: "https://us-central1-maven.pkg.dev/" + project + "/mvn", Path: "/workspace/target/app-1.0.jar",
			GroupId: "com.acme", ArtifactId: "app", Version: "1.0",
		}},
		PythonPackages: []*cloudbuildpb.Artifacts_PythonPackage{{
			Repository: "https://us-central1-python.pkg.dev/" + project + "/py", Paths: []string{"dist/*"},
		}},
		NpmPackages: []*cloudbuildpb.Artifacts_NpmPackage{{
			Repository: "https://us-central1-npm.pkg.dev/" + project + "/npm", PackagePath: "web",
		}},
		GoModules: []*cloudbuildpb.Artifacts_GoModule{{
			RepositoryName: "go", RepositoryLocation: "us-central1", SourcePath: "mod",
			ModulePath: "example.com/mod", ModuleVersion: "v0.1.0",
		}},
	}

	got := e.run(t, b)
	require.Equal(t, cloudbuildpb.Build_SUCCESS, got.GetStatus(), got.GetStatusDetail())

	res := got.GetResults()

	require.Len(t, res.GetMavenArtifacts(), 1)
	mvn := res.GetMavenArtifacts()[0]
	assert.Equal(t, "https://us-central1-maven.pkg.dev/"+project+"/mvn/com/acme/app/1.0/app-1.0.jar", mvn.GetUri())
	assert.Equal(t, arParent+"/repositories/mvn/packages/com.acme:app", mvn.GetArtifactRegistryPackage())
	assert.Len(t, mvn.GetFileHashes().GetFileHash()[0].GetValue(), 32)
	assert.NotNil(t, mvn.GetPushTiming().GetEndTime())

	require.Len(t, res.GetPythonPackages(), 2)
	assert.Equal(
		t,
		"https://us-central1-python.pkg.dev/"+project+"/py/my_pkg-0.3.0-py3-none-any.whl",
		res.GetPythonPackages()[0].GetUri(),
	)

	require.Len(t, res.GetNpmPackages(), 1)
	assert.Equal(
		t,
		"https://us-central1-npm.pkg.dev/"+project+"/npm/@acme/web/-/web-2.1.0.tgz",
		res.GetNpmPackages()[0].GetUri(),
	)

	require.Len(t, res.GetGoModules(), 1)
	assert.Contains(
		t,
		res.GetGoModules()[0].GetUri(),
		"us-central1-go.pkg.dev/"+project+"/go/example.com/mod/@v/v0.1.0.zip",
	)

	assert.NotNil(t, res.GetArtifactTiming())

	assert.Equal(t, []string{"com.acme:app"}, listPackages(t, ar, "mvn"))
	assert.Equal(t, []string{"my_pkg"}, listPackages(t, ar, "py"))
	assert.Equal(t, []string{"@acme/web"}, listPackages(t, ar, "npm"))
	assert.Equal(t, []string{"example.com/mod"}, listPackages(t, ar, "go"))
}

func TestArtifactUploadFailures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		artifacts *cloudbuildpb.Artifacts
		name      string
		want      string
		withAR    bool
	}{
		{
			name: "registry unavailable",
			artifacts: &cloudbuildpb.Artifacts{MavenArtifacts: []*cloudbuildpb.Artifacts_MavenArtifact{
				{
					Repository: "https://us-central1-maven.pkg.dev/" + project + "/mvn",
					Path:       "a.jar",
					GroupId:    "g",
					ArtifactId: "a",
					Version:    "1",
				},
			}},
			want: "not available",
		},
		{
			name: "repository missing",
			artifacts: &cloudbuildpb.Artifacts{MavenArtifacts: []*cloudbuildpb.Artifacts_MavenArtifact{
				{
					Repository: "https://us-central1-maven.pkg.dev/" + project + "/nope",
					Path:       "a.jar",
					GroupId:    "g",
					ArtifactId: "a",
					Version:    "1",
				},
			}},
			withAR: true, want: "nope",
		},
		{
			name: "file missing",
			artifacts: &cloudbuildpb.Artifacts{MavenArtifacts: []*cloudbuildpb.Artifacts_MavenArtifact{
				{
					Repository: "https://us-central1-maven.pkg.dev/" + project + "/mvn",
					Path:       "missing.jar",
					GroupId:    "g",
					ArtifactId: "a",
					Version:    "1",
				},
			}},
			withAR: true, want: "no file matches",
		},
		{
			name: "bad repository url",
			artifacts: &cloudbuildpb.Artifacts{NpmPackages: []*cloudbuildpb.Artifacts_NpmPackage{{
				Repository: "https://example.com/x", PackagePath: ".",
			}}},
			withAR: true, want: "pkg.dev",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			build := func(_ context.Context, spec container.Spec, _ io.Writer) int {
				host, _, _ := strings.Cut(spec.Mounts[0], ":")
				writeFiles(t, host, map[string]string{"a.jar": "x"})

				return 0
			}

			e := newEnv(t, map[string]behavior{"builder": build})
			if tt.withAR {
				ar := newRegistry(
					t,
					map[string]artifactregistrypb.Repository_Format{"mvn": artifactregistrypb.Repository_MAVEN},
				)
				e.configure(func(s *gcpcloudbuild.Services) { s.Packages = ar })
			}

			b := buildOf(step("builder"))
			b.Artifacts = tt.artifacts

			got := e.run(t, b)
			assert.Equal(t, cloudbuildpb.Build_FAILURE, got.GetStatus())
			assert.Contains(t, got.GetStatusDetail(), tt.want)
		})
	}
}
