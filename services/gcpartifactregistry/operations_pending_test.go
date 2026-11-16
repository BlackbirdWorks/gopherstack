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

	arapi "google.golang.org/api/artifactregistry/v1"

	"github.com/blackbirdworks/gopherstack/services/gcpartifactregistry"
)

func TestPendingCreateRepositoryAppearsWhenDone(t *testing.T) {
	t.Parallel()

	clock := newFakeClock(epochTime())
	e := newEnvWith(t, "", func(b *gcpartifactregistry.InMemoryBackend) {
		b.SetClock(clock.now)
		b.SetOperationDelay(time.Minute)
	})
	ctx := t.Context()

	op, err := e.client.CreateRepository(ctx, &artifactregistrypb.CreateRepositoryRequest{
		Parent: loc, RepositoryId: "slow", Repository: genericRepo(),
	})
	require.NoError(t, err)
	assert.False(t, op.Done())

	_, err = e.client.GetRepository(ctx, &artifactregistrypb.GetRepositoryRequest{Name: repoName("slow")})
	assert.Equal(t, codes.NotFound, status.Code(err), "the repository exists only once the operation finishes")

	clock.advance(2 * time.Minute)

	got, err := e.client.LROClient.GetOperation(ctx, &longrunningpb.GetOperationRequest{Name: op.Name()})
	require.NoError(t, err)
	assert.True(t, got.GetDone())

	_, err = e.client.GetRepository(ctx, &artifactregistrypb.GetRepositoryRequest{Name: repoName("slow")})
	require.NoError(t, err)
}

func TestPendingDeletesRemoveOnCompletion(t *testing.T) {
	t.Parallel()

	const fileID = "tool:1.0.0:tool.zip"

	pkg := repoName("gen") + "/packages/tool"
	version := pkg + "/versions/1.0.0"

	tests := []struct {
		start func(t *testing.T, e *env) (opName string)
		get   func(t *testing.T, e *env) error
		name  string
	}{
		{
			name: "file",
			start: func(t *testing.T, e *env) string {
				t.Helper()

				req := &artifactregistrypb.DeleteFileRequest{Name: fileName("gen", fileID)}
				op, err := e.client.DeleteFile(t.Context(), req)
				require.NoError(t, err)

				return op.Name()
			},
			get: func(t *testing.T, e *env) error {
				t.Helper()

				_, err := e.client.GetFile(
					t.Context(), &artifactregistrypb.GetFileRequest{Name: fileName("gen", fileID)},
				)

				return err
			},
		},
		{
			name: "version",
			start: func(t *testing.T, e *env) string {
				t.Helper()

				op, err := e.client.DeleteVersion(t.Context(), &artifactregistrypb.DeleteVersionRequest{Name: version})
				require.NoError(t, err)

				return op.Name()
			},
			get: func(t *testing.T, e *env) error {
				t.Helper()

				_, err := e.client.GetVersion(t.Context(), &artifactregistrypb.GetVersionRequest{Name: version})

				return err
			},
		},
		{
			name: "batch",
			start: func(t *testing.T, e *env) string {
				t.Helper()

				op, err := e.client.BatchDeleteVersions(t.Context(), &artifactregistrypb.BatchDeleteVersionsRequest{
					Parent: pkg, Names: []string{version},
				})
				require.NoError(t, err)

				return op.Name()
			},
			get: func(t *testing.T, e *env) error {
				t.Helper()

				_, err := e.client.GetVersion(t.Context(), &artifactregistrypb.GetVersionRequest{Name: version})

				return err
			},
		},
		{
			name: "package",
			start: func(t *testing.T, e *env) string {
				t.Helper()

				op, err := e.client.DeletePackage(t.Context(), &artifactregistrypb.DeletePackageRequest{Name: pkg})
				require.NoError(t, err)

				return op.Name()
			},
			get: func(t *testing.T, e *env) error {
				t.Helper()

				_, err := e.client.GetPackage(t.Context(), &artifactregistrypb.GetPackageRequest{Name: pkg})

				return err
			},
		},
		{
			name: "repository",
			start: func(t *testing.T, e *env) string {
				t.Helper()

				req := &artifactregistrypb.DeleteRepositoryRequest{Name: repoName("gen")}
				op, err := e.client.DeleteRepository(t.Context(), req)
				require.NoError(t, err)

				return op.Name()
			},
			get: func(t *testing.T, e *env) error {
				t.Helper()

				req := &artifactregistrypb.GetRepositoryRequest{Name: repoName("gen")}
				_, err := e.client.GetRepository(t.Context(), req)

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			clock := newFakeClock(epochTime())
			e := newEnvWith(t, "", func(b *gcpartifactregistry.InMemoryBackend) { b.SetClock(clock.now) })
			e.createRepo(t, "gen", genericRepo())

			_, err := upload(t, restService(t, e), "gen", &arapi.UploadGenericArtifactRequest{
				PackageId: "tool", VersionId: "1.0.0", Filename: "tool.zip",
			}, []byte("payload"))
			require.NoError(t, err)

			e.handler.Backend.SetOperationDelay(time.Minute)

			opName := tt.start(t, e)
			ctx := t.Context()

			pending, err := e.client.LROClient.GetOperation(ctx, &longrunningpb.GetOperationRequest{Name: opName})
			require.NoError(t, err)
			assert.False(t, pending.GetDone())
			require.NoError(t, tt.get(t, e), "the resource stays until the operation finishes")

			clock.advance(2 * time.Minute)

			finished, err := e.client.LROClient.GetOperation(ctx, &longrunningpb.GetOperationRequest{Name: opName})
			require.NoError(t, err)
			assert.True(t, finished.GetDone())
			assert.Nil(t, finished.GetError())
			assert.Equal(t, codes.NotFound, status.Code(tt.get(t, e)))
		})
	}
}
