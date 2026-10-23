package gcpcloudbuild_test

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"cloud.google.com/go/cloudbuild/apiv1/v2/cloudbuildpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

func tarGz(t *testing.T, files map[string]string) []byte {
	t.Helper()

	var buf bytes.Buffer

	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)

	for name, body := range files {
		require.NoError(
			t,
			tw.WriteHeader(
				&tar.Header{Name: name, Mode: 0o644, Size: int64(len(body)), Typeflag: tar.TypeReg},
			),
		)
		_, err := tw.Write([]byte(body))
		require.NoError(t, err)
	}

	require.NoError(t, tw.Close())
	require.NoError(t, gz.Close())

	return buf.Bytes()
}

func TestStorageSourceAndArtifacts(t *testing.T) {
	t.Parallel()

	build := func(_ context.Context, spec container.Spec, out io.Writer) int {
		host, _, _ := strings.Cut(spec.Mounts[0], ":")

		src, err := os.ReadFile(filepath.Join(host, "app", "main.txt"))
		if err != nil {
			return 2
		}

		_, _ = io.WriteString(out, "source says "+string(src)+"\n")

		return exitCode(
			os.WriteFile(
				filepath.Join(host, "result.bin"),
				append([]byte("built:"), src...),
				0o600,
			),
		)
	}

	e := newEnv(t, map[string]behavior{"builder": build})
	require.NoError(
		t,
		e.store.WriteObject(
			"src-bucket",
			"src.tar.gz",
			tarGz(t, map[string]string{"app/main.txt": "v1"}),
			"",
		),
	)

	b := buildOf(step("builder"))
	b.Source = &cloudbuildpb.Source{Source: &cloudbuildpb.Source_StorageSource{
		StorageSource: &cloudbuildpb.StorageSource{Bucket: "src-bucket", Object: "src.tar.gz"},
	}}
	b.Artifacts = &cloudbuildpb.Artifacts{Objects: &cloudbuildpb.Artifacts_ArtifactObjects{
		Location: "gs://out-bucket/builds/", Paths: []string{"result.*", "missing.txt"},
	}}
	b.LogsBucket = "gs://logs-bucket"

	got := e.run(t, b)
	require.Equal(t, cloudbuildpb.Build_SUCCESS, got.GetStatus(), got.GetStatusDetail())

	data, ok := e.store.get("out-bucket/builds/result.bin")
	require.True(t, ok)
	assert.Equal(t, "built:v1", string(data))

	assert.EqualValues(t, 1, got.GetResults().GetNumArtifacts())
	assert.Equal(
		t,
		"gs://out-bucket/builds/artifacts-"+got.GetId()+".json",
		got.GetResults().GetArtifactManifest(),
	)

	manifest, ok := e.store.get("out-bucket/builds/artifacts-" + got.GetId() + ".json")
	require.True(t, ok)
	assert.Contains(t, string(manifest), `"location":"gs://out-bucket/builds/result.bin"`)

	_, ok = e.store.get("logs-bucket/log-" + got.GetId() + ".txt")
	assert.True(t, ok)
	assert.Contains(t, e.logs.lines(), "Step #0: source says v1")
}

func TestSourceFailures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		source *cloudbuildpb.Source
		want   string
	}{
		{
			name: "missing object",
			source: &cloudbuildpb.Source{Source: &cloudbuildpb.Source_StorageSource{
				StorageSource: &cloudbuildpb.StorageSource{Bucket: "b", Object: "nope.tgz"},
			}},
			want: "failed to fetch source: read gs://b/nope.tgz",
		},
		{
			name: "connected repository is not supported",
			source: &cloudbuildpb.Source{Source: &cloudbuildpb.Source_ConnectedRepository{
				ConnectedRepository: &cloudbuildpb.ConnectedRepository{Repository: "r"},
			}},
			want: "failed to fetch source: only storageSource, gitSource and repoSource are supported",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, map[string]behavior{"x": exits(0)})
			b := buildOf(step("x"))
			b.Source = tt.source

			got := e.run(t, b)
			assert.Equal(t, cloudbuildpb.Build_FAILURE, got.GetStatus())
			assert.Contains(t, got.GetStatusDetail(), tt.want)
			assert.Equal(
				t,
				cloudbuildpb.Build_FailureInfo_FETCH_SOURCE_FAILED,
				got.GetFailureInfo().GetType(),
			)
			assert.Empty(t, e.rt.startedImages())
		})
	}
}

func TestImagePush(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantDetail string
		images     []string
		wantPush   []string
		wantStatus cloudbuildpb.Build_Status
		wantType   cloudbuildpb.Build_FailureInfo_FailureType
	}{
		{
			name:       "artifact registry image",
			images:     []string{"us-central1-docker.pkg.dev/p/repo/app:v1"},
			wantStatus: cloudbuildpb.Build_SUCCESS,
			wantPush: []string{
				"us-central1-docker.pkg.dev/p/repo/app:v1 -> localhost:8134/p/repo/app:v1",
			},
		},
		{
			name:       "untagged image gets latest",
			images:     []string{"europe-docker.pkg.dev/p/repo/app"},
			wantStatus: cloudbuildpb.Build_SUCCESS,
			wantPush: []string{
				"europe-docker.pkg.dev/p/repo/app:latest -> localhost:8134/p/repo/app:latest",
			},
		},
		{
			name:       "other registry",
			images:     []string{"gcr.io/p/app"},
			wantStatus: cloudbuildpb.Build_FAILURE,
			wantDetail: "must be pushed to <location>-docker.pkg.dev",
			wantType:   cloudbuildpb.Build_FailureInfo_PUSH_FAILED,
		},
		{
			name:       "image not built",
			images:     []string{"us-docker.pkg.dev/p/repo/missing"},
			wantStatus: cloudbuildpb.Build_FAILURE,
			wantDetail: "No such image",
			wantType:   cloudbuildpb.Build_FailureInfo_PUSH_IMAGE_NOT_FOUND,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, map[string]behavior{"x": exits(0)})
			b := buildOf(step("x"))
			b.Images = tt.images

			got := e.run(t, b)
			assert.Equal(t, tt.wantStatus, got.GetStatus(), got.GetStatusDetail())
			assert.Contains(t, got.GetStatusDetail(), tt.wantDetail)
			assert.Equal(t, tt.wantPush, e.rt.pushes())
			assert.Equal(t, tt.wantType, got.GetFailureInfo().GetType())

			if tt.wantStatus == cloudbuildpb.Build_SUCCESS {
				require.Len(t, got.GetResults().GetImages(), len(tt.images))
				assert.Equal(t, "sha256:feedface", got.GetResults().GetImages()[0].GetDigest())
			}
		})
	}
}

func TestSecrets(t *testing.T) {
	t.Parallel()

	e := newEnv(t, map[string]behavior{
		"x": func(_ context.Context, spec container.Spec, out io.Writer) int {
			for _, kv := range spec.Env {
				if after, ok := strings.CutPrefix(kv, "TOKEN="); ok {
					_, _ = io.WriteString(out, "token is "+after+"\n")
				}
			}

			return 0
		},
	})

	b := buildOf(step("x", func(s *cloudbuildpb.BuildStep) { s.SecretEnv = []string{"TOKEN"} }))
	b.AvailableSecrets = &cloudbuildpb.Secrets{SecretManager: []*cloudbuildpb.SecretManagerSecret{
		{VersionName: "projects/p/secrets/s/versions/1", Env: "TOKEN"},
	}}

	got := e.run(t, b)
	require.Equal(t, cloudbuildpb.Build_SUCCESS, got.GetStatus(), got.GetStatusDetail())

	lines := e.logs.lines()
	assert.Contains(t, lines, "Step #0: token is ******")

	for _, l := range lines {
		assert.NotContains(t, l, "hunter2")
	}
}

func TestSubstitutionsInSteps(t *testing.T) {
	t.Parallel()

	e := newEnv(t, map[string]behavior{"img": exits(0)})

	b := buildOf(step("img", withArgs("$PROJECT_ID", "${_TARGET}", "$$HOME", "$BUILD_ID")))
	b.Substitutions = map[string]string{"_TARGET": "prod"}

	got := e.run(t, b)
	require.Equal(t, cloudbuildpb.Build_SUCCESS, got.GetStatus(), got.GetStatusDetail())

	args := got.GetSteps()[0].GetArgs()
	assert.Equal(t, []string{project, "prod", "$HOME", got.GetId()}, args)
	assert.Equal(t, args, e.rt.specOf("img").Cmd)
}
