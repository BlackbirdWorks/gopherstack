package gcpartifactregistry_test

import (
	"testing"
	"time"

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/blackbirdworks/gopherstack/services/gcpartifactregistry"
)

func TestExportArtifact(t *testing.T) {
	t.Parallel()

	store := memStore{}
	e, version, _ := attachEnv(t, func(b *gcpartifactregistry.InMemoryBackend) { b.SetObjectStore(store) })
	ctx := t.Context()
	content := []byte(`{"bomFormat":"CycloneDX"}`)

	e.createRepo(t, "dock", dockerRepo())

	tests := []struct {
		req  *artifactregistrypb.ExportArtifactRequest
		name string
		obj  string
		want codes.Code
	}{
		{
			name: "version", obj: "bkt-out/sbom.json",
			req: &artifactregistrypb.ExportArtifactRequest{
				Repository:     repoName("gen"),
				SourceArtifact: &artifactregistrypb.ExportArtifactRequest_SourceVersion{SourceVersion: version},
				Destination:    &artifactregistrypb.ExportArtifactRequest_GcsPath{GcsPath: "bkt-out"},
			},
		},
		{
			name: "subdirectory", obj: "bkt-out/a/b/sbom.json",
			req: &artifactregistrypb.ExportArtifactRequest{
				Repository:     repoName("gen"),
				SourceArtifact: &artifactregistrypb.ExportArtifactRequest_SourceVersion{SourceVersion: version},
				Destination:    &artifactregistrypb.ExportArtifactRequest_GcsPath{GcsPath: "bkt-out/a/b"},
			},
		},
		{
			name: "missing bucket path", want: codes.InvalidArgument,
			req: &artifactregistrypb.ExportArtifactRequest{
				Repository:     repoName("gen"),
				SourceArtifact: &artifactregistrypb.ExportArtifactRequest_SourceVersion{SourceVersion: version},
			},
		},
		{
			name: "no source", want: codes.InvalidArgument,
			req: &artifactregistrypb.ExportArtifactRequest{
				Repository: repoName(
					"gen",
				),
				Destination: &artifactregistrypb.ExportArtifactRequest_GcsPath{GcsPath: "bkt-out"},
			},
		},
		{
			name: "unknown version", want: codes.NotFound,
			req: &artifactregistrypb.ExportArtifactRequest{
				Repository: repoName("gen"),
				SourceArtifact: &artifactregistrypb.ExportArtifactRequest_SourceVersion{
					SourceVersion: pkgName("gen", "tool") + "/versions/9",
				},
				Destination: &artifactregistrypb.ExportArtifactRequest_GcsPath{GcsPath: "bkt-out"},
			},
		},
		{
			name: "generic repositories have no tags", want: codes.FailedPrecondition,
			req: &artifactregistrypb.ExportArtifactRequest{
				Repository: repoName("gen"),
				SourceArtifact: &artifactregistrypb.ExportArtifactRequest_SourceTag{
					SourceTag: pkgName("gen", "tool") + "/tags/none",
				},
				Destination: &artifactregistrypb.ExportArtifactRequest_GcsPath{GcsPath: "bkt-out"},
			},
		},
		{
			name: "docker repository", want: codes.FailedPrecondition,
			req: &artifactregistrypb.ExportArtifactRequest{
				Repository: repoName("dock"),
				SourceArtifact: &artifactregistrypb.ExportArtifactRequest_SourceVersion{
					SourceVersion: pkgName("dock", "x") + "/versions/sha256:1",
				},
				Destination: &artifactregistrypb.ExportArtifactRequest_GcsPath{GcsPath: "bkt-out"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			op, err := e.client.ExportArtifact(ctx, tt.req)
			if tt.want != codes.OK {
				assert.Equal(t, tt.want, status.Code(err), "%v", err)

				return
			}

			require.NoError(t, err)

			resp, err := op.Wait(ctx)
			require.NoError(t, err)
			assert.Equal(t, version, resp.GetExportedVersion().GetName())

			meta, err := op.Metadata()
			require.NoError(t, err)
			require.Len(t, meta.GetExportedFiles(), 1)
			assert.Equal(t, tt.obj, meta.GetExportedFiles()[0].GetGcsObjectPath())
			assert.Equal(t, sha256Of(content), "sha256:"+hexOf(meta.GetExportedFiles()[0].GetHashes()[0].GetValue()))
			assert.Equal(t, content, store[tt.obj])
		})
	}
}

func TestExportArtifactByTagAndFailures(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	e.createRepo(t, "kfp", kfpRepo())

	spec := pipelineSpec("exported-pipe")
	code, _ := kfpPost(t, e, "kfp", kfpUpload{content: spec, tags: "v1"})
	require.Equal(t, 200, code)

	req := &artifactregistrypb.ExportArtifactRequest{
		Repository: repoName("kfp"),
		SourceArtifact: &artifactregistrypb.ExportArtifactRequest_SourceTag{
			SourceTag: pkgName("kfp", "exported-pipe") + "/tags/v1",
		},
		Destination: &artifactregistrypb.ExportArtifactRequest_GcsPath{GcsPath: "bkt-t"},
	}

	_, err := e.client.ExportArtifact(t.Context(), req)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err), "no Cloud Storage emulator wired")

	store := memStore{}
	e.handler.Backend.SetObjectStore(store)

	op, err := e.client.ExportArtifact(t.Context(), req)
	require.NoError(t, err)
	_, err = op.Wait(t.Context())
	require.NoError(t, err)
	assert.Equal(t, spec, store["bkt-t/pipeline_spec.yaml"])

	req.Destination = &artifactregistrypb.ExportArtifactRequest_GcsPath{GcsPath: "missing-bucket"}
	op, err = e.client.ExportArtifact(t.Context(), req)
	require.NoError(t, err)

	_, err = op.Wait(t.Context())
	assert.Equal(t, codes.NotFound, status.Code(err), "an unwritable destination fails the operation")
}

func TestCancelledExportWritesNothing(t *testing.T) {
	t.Parallel()

	clock := newFakeClock(epochTime())
	store := memStore{}
	e, version, _ := attachEnv(t, func(b *gcpartifactregistry.InMemoryBackend) {
		b.SetObjectStore(store)
		b.SetClock(clock.now)
		b.SetOperationDelay(time.Minute)
	})
	ctx := t.Context()

	op, err := e.client.ExportArtifact(ctx, &artifactregistrypb.ExportArtifactRequest{
		Repository:     repoName("gen"),
		SourceArtifact: &artifactregistrypb.ExportArtifactRequest_SourceVersion{SourceVersion: version},
		Destination:    &artifactregistrypb.ExportArtifactRequest_GcsPath{GcsPath: "bkt-c"},
	})
	require.NoError(t, err)
	assert.False(t, op.Done())
	assert.Empty(t, store, "nothing is written while the operation is pending")

	require.NoError(t, e.client.LROClient.CancelOperation(ctx, &longrunningpb.CancelOperationRequest{Name: op.Name()}))
	clock.advance(time.Hour)

	got, err := e.client.LROClient.GetOperation(ctx, &longrunningpb.GetOperationRequest{Name: op.Name()})
	require.NoError(t, err)
	assert.EqualValues(t, codes.Canceled, got.GetError().GetCode())
	assert.Empty(t, store)
}
