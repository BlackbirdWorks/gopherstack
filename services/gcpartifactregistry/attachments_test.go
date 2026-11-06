package gcpartifactregistry_test

import (
	"errors"
	"testing"
	"time"

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	arapi "google.golang.org/api/artifactregistry/v1"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/blackbirdworks/gopherstack/services/gcpartifactregistry"
)

const attachTarget = "tool:1.0.0:sbom.json"

// attachEnv is a GENERIC repository holding tool 1.0.0 with one file; setup runs once that exists.
func attachEnv(t *testing.T, setup func(*gcpartifactregistry.InMemoryBackend)) (*env, string, string) {
	t.Helper()

	e := newEnv(t, "")
	e.createRepo(t, "gen", genericRepo())

	_, err := upload(
		t,
		restService(t, e),
		"gen",
		&arapi.UploadGenericArtifactRequest{PackageId: "tool", VersionId: "1.0.0", Filename: "sbom.json"},
		[]byte(`{"bomFormat":"CycloneDX"}`),
	)
	require.NoError(t, err)

	if setup != nil {
		setup(e.handler.Backend)
	}

	return e, pkgName("gen", "tool") + "/versions/1.0.0", fileName("gen", attachTarget)
}

func TestAttachmentLifecycle(t *testing.T) {
	t.Parallel()

	e, version, file := attachEnv(t, nil)
	ctx := t.Context()
	parent := repoName("gen")

	create := func(id string, att *artifactregistrypb.Attachment) (*artifactregistrypb.Attachment, error) {
		op, err := e.client.CreateAttachment(ctx, &artifactregistrypb.CreateAttachmentRequest{
			Parent: parent, AttachmentId: id, Attachment: att,
		})
		if err != nil {
			return nil, err
		}

		return op.Wait(ctx)
	}

	got, err := create("sbom", &artifactregistrypb.Attachment{
		Target: version, Type: "application/vnd.cyclonedx+json", AttachmentNamespace: "example.com",
		Files: []string{file}, Annotations: map[string]string{"k": "v"},
	})
	require.NoError(t, err)
	assert.Equal(t, parent+"/attachments/sbom", got.GetName())
	assert.NotNil(t, got.GetCreateTime())
	assert.Equal(t, map[string]string{"k": "v"}, got.GetAnnotations())

	_, err = create("other", &artifactregistrypb.Attachment{Target: parent, Type: "text/plain", Files: []string{file}})
	require.NoError(t, err)

	t.Run("get", func(t *testing.T) {
		t.Parallel()

		a, getErr := e.client.GetAttachment(ctx, &artifactregistrypb.GetAttachmentRequest{Name: got.GetName()})
		require.NoError(t, getErr)
		assert.Equal(t, version, a.GetTarget())
	})

	tests := []struct {
		name   string
		filter string
		want   []string
	}{
		{name: "all", want: []string{"other", "sbom"}},
		{name: "by target", filter: `target="` + version + `"`, want: []string{"sbom"}},
		{name: "by type case insensitive", filter: `type="TEXT/PLAIN"`, want: []string{"other"}},
		{
			name:   "and",
			filter: `attachment_namespace="example.com" AND type="application/vnd.cyclonedx+json"`,
			want:   []string{"sbom"},
		},
		{name: "none", filter: `type="nope"`},
	}

	for _, tt := range tests {
		t.Run("list "+tt.name, func(t *testing.T) {
			t.Parallel()

			it := e.client.ListAttachments(
				ctx,
				&artifactregistrypb.ListAttachmentsRequest{Parent: parent, Filter: tt.filter},
			)

			var ids []string

			for {
				a, nerr := it.Next()
				if errors.Is(nerr, iterator.Done) {
					break
				}

				require.NoError(t, nerr)

				ids = append(ids, a.GetName()[len(parent+"/attachments/"):])
			}

			assert.Equal(t, tt.want, ids)
		})
	}

	errTests := []struct {
		att  *artifactregistrypb.Attachment
		name string
		id   string
		want codes.Code
	}{
		{
			name: "duplicate",
			id:   "sbom",
			att:  &artifactregistrypb.Attachment{Target: version, Files: []string{file}},
			want: codes.AlreadyExists,
		},
		{
			name: "bad id",
			id:   "-bad",
			att:  &artifactregistrypb.Attachment{Target: version, Files: []string{file}},
			want: codes.InvalidArgument,
		},
		{
			name: "no files",
			id:   "nf",
			att:  &artifactregistrypb.Attachment{Target: version},
			want: codes.InvalidArgument,
		},
		{
			name: "missing file",
			id:   "mf",
			att:  &artifactregistrypb.Attachment{Target: version, Files: []string{fileName("gen", "x:y:z")}},
			want: codes.NotFound,
		},
		{
			name: "missing target",
			id:   "mt",
			att: &artifactregistrypb.Attachment{
				Target: pkgName("gen", "tool") + "/versions/9",
				Files:  []string{file},
			},
			want: codes.NotFound,
		},
		{
			name: "foreign target",
			id:   "ft",
			att:  &artifactregistrypb.Attachment{Target: repoName("other"), Files: []string{file}},
			want: codes.InvalidArgument,
		},
	}

	for _, tt := range errTests {
		t.Run("create "+tt.name, func(t *testing.T) {
			t.Parallel()

			_, cerr := e.client.CreateAttachment(ctx, &artifactregistrypb.CreateAttachmentRequest{
				Parent: parent, AttachmentId: tt.id, Attachment: tt.att,
			})
			assert.Equal(t, tt.want, status.Code(cerr), "%v", cerr)
		})
	}
}

func TestAttachmentDelete(t *testing.T) {
	t.Parallel()

	e, version, file := attachEnv(t, nil)
	ctx := t.Context()

	op, err := e.client.CreateAttachment(ctx, &artifactregistrypb.CreateAttachmentRequest{
		Parent: repoName("gen"), AttachmentId: "gone",
		Attachment: &artifactregistrypb.Attachment{Target: version, Files: []string{file}},
	})
	require.NoError(t, err)

	got, err := op.Wait(ctx)
	require.NoError(t, err)

	del, err := e.client.DeleteAttachment(ctx, &artifactregistrypb.DeleteAttachmentRequest{Name: got.GetName()})
	require.NoError(t, err)
	require.NoError(t, del.Wait(ctx))

	_, err = e.client.GetAttachment(ctx, &artifactregistrypb.GetAttachmentRequest{Name: got.GetName()})
	assert.Equal(t, codes.NotFound, status.Code(err))

	_, err = e.client.DeleteAttachment(ctx, &artifactregistrypb.DeleteAttachmentRequest{Name: got.GetName()})
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestAttachmentsSurviveSnapshotRestore(t *testing.T) {
	t.Parallel()

	e, version, file := attachEnv(t, nil)
	ctx := t.Context()

	op, err := e.client.CreateAttachment(ctx, &artifactregistrypb.CreateAttachmentRequest{
		Parent: repoName("gen"), AttachmentId: "keep",
		Attachment: &artifactregistrypb.Attachment{Target: version, Files: []string{file}},
	})
	require.NoError(t, err)

	_, err = op.Wait(ctx)
	require.NoError(t, err)

	require.NoError(t, e.handler.Backend.Restore(ctx, e.handler.Backend.Snapshot(ctx)))

	got, err := e.client.GetAttachment(
		ctx,
		&artifactregistrypb.GetAttachmentRequest{Name: repoName("gen") + "/attachments/keep"},
	)
	require.NoError(t, err)
	assert.Equal(t, version, got.GetTarget())
}

func TestPendingOperationsFinishOnTheClock(t *testing.T) {
	t.Parallel()

	clock := newFakeClock(epochTime())
	e, version, file := attachEnv(t, func(b *gcpartifactregistry.InMemoryBackend) {
		b.SetClock(clock.now)
		b.SetOperationDelay(time.Minute)
	})
	ctx := t.Context()
	name := repoName("gen") + "/attachments/slow"

	op, err := e.client.CreateAttachment(ctx, &artifactregistrypb.CreateAttachmentRequest{
		Parent: repoName("gen"), AttachmentId: "slow",
		Attachment: &artifactregistrypb.Attachment{Target: version, Files: []string{file}},
	})
	require.NoError(t, err)
	assert.False(t, op.Done())

	_, err = e.client.GetAttachment(ctx, &artifactregistrypb.GetAttachmentRequest{Name: name})
	assert.Equal(t, codes.NotFound, status.Code(err), "the attachment exists only once the operation finishes")

	clock.advance(59 * time.Second)

	pending, err := e.client.LROClient.GetOperation(ctx, &longrunningpb.GetOperationRequest{Name: op.Name()})
	require.NoError(t, err)
	assert.False(t, pending.GetDone())

	clock.advance(2 * time.Second)

	finished, err := e.client.LROClient.GetOperation(ctx, &longrunningpb.GetOperationRequest{Name: op.Name()})
	require.NoError(t, err)
	assert.True(t, finished.GetDone())
	assert.NotNil(t, finished.GetResponse())

	_, err = e.client.GetAttachment(ctx, &artifactregistrypb.GetAttachmentRequest{Name: name})
	require.NoError(t, err)
}

func TestCancelPendingOperation(t *testing.T) {
	t.Parallel()

	clock := newFakeClock(epochTime())
	e, version, file := attachEnv(t, func(b *gcpartifactregistry.InMemoryBackend) {
		b.SetClock(clock.now)
		b.SetOperationDelay(time.Minute)
	})
	ctx := t.Context()

	op, err := e.client.CreateAttachment(ctx, &artifactregistrypb.CreateAttachmentRequest{
		Parent: repoName("gen"), AttachmentId: "never",
		Attachment: &artifactregistrypb.Attachment{Target: version, Files: []string{file}},
	})
	require.NoError(t, err)

	require.NoError(t, e.client.LROClient.CancelOperation(ctx, &longrunningpb.CancelOperationRequest{Name: op.Name()}))

	got, err := e.client.LROClient.GetOperation(ctx, &longrunningpb.GetOperationRequest{Name: op.Name()})
	require.NoError(t, err)
	assert.True(t, got.GetDone())
	assert.EqualValues(t, codes.Canceled, got.GetError().GetCode())

	clock.advance(time.Hour)

	_, err = e.client.GetAttachment(
		ctx,
		&artifactregistrypb.GetAttachmentRequest{Name: repoName("gen") + "/attachments/never"},
	)
	assert.Equal(t, codes.NotFound, status.Code(err), "a cancelled create never applies")

	require.NoError(t, e.client.LROClient.CancelOperation(ctx, &longrunningpb.CancelOperationRequest{Name: op.Name()}),
		"cancelling a finished operation is a no-op")
}
