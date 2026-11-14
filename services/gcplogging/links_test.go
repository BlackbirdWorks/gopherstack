package gcplogging_test

import (
	"errors"
	"testing"
	"time"

	"cloud.google.com/go/logging/apiv2/loggingpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	ltype "google.golang.org/genproto/googleapis/logging/type"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (e *env) analyticsBucket(t *testing.T, id string) string {
	t.Helper()

	b, err := e.config(t).CreateBucket(t.Context(), &loggingpb.CreateBucketRequest{
		Parent: loc, BucketId: id, Bucket: &loggingpb.LogBucket{AnalyticsEnabled: true},
	})
	require.NoError(t, err)

	return b.GetName()
}

func TestLinkLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	cfg := e.config(t)
	bucket := e.analyticsBucket(t, "an")

	op, err := cfg.CreateLink(t.Context(), &loggingpb.CreateLinkRequest{
		Parent: bucket, LinkId: "lnk", Link: &loggingpb.Link{Description: "d"},
	})
	require.NoError(t, err)

	link, err := op.Wait(t.Context())
	require.NoError(t, err)
	assert.Equal(t, bucket+"/links/lnk", link.GetName())
	assert.Equal(t, "d", link.GetDescription())
	assert.Equal(t, loggingpb.LifecycleState_ACTIVE, link.GetLifecycleState())
	assert.Equal(
		t,
		"bigquery.googleapis.com/projects/"+testProject+"/datasets/lnk",
		link.GetBigqueryDataset().GetDatasetId(),
	)
	assert.True(t, e.tables.hasDataset(testProject+".lnk"))

	got, err := cfg.GetLink(t.Context(), &loggingpb.GetLinkRequest{Name: link.GetName()})
	require.NoError(t, err)
	assert.Equal(t, "d", got.GetDescription())

	_, err = cfg.CreateLink(t.Context(), &loggingpb.CreateLinkRequest{Parent: bucket, LinkId: "lnk"})
	assert.Equal(t, codes.AlreadyExists, status.Code(err))

	it := cfg.ListLinks(t.Context(), &loggingpb.ListLinksRequest{Parent: bucket})
	l, err := it.Next()
	require.NoError(t, err)
	assert.Equal(t, link.GetName(), l.GetName())

	_, err = it.Next()
	require.ErrorIs(t, err, iterator.Done)

	err = cfg.DeleteBucket(t.Context(), &loggingpb.DeleteBucketRequest{Name: bucket})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))

	del, err := cfg.DeleteLink(t.Context(), &loggingpb.DeleteLinkRequest{Name: link.GetName()})
	require.NoError(t, err)
	require.NoError(t, del.Wait(t.Context()))
	assert.False(t, e.tables.hasDataset(testProject+".lnk"))

	_, err = cfg.GetLink(t.Context(), &loggingpb.GetLinkRequest{Name: link.GetName()})
	assert.Equal(t, codes.NotFound, status.Code(err))
	require.NoError(t, cfg.DeleteBucket(t.Context(), &loggingpb.DeleteBucketRequest{Name: bucket}))
}

func TestLinkErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		req  func(bucket string) *loggingpb.CreateLinkRequest
		name string
		want codes.Code
	}{
		{
			name: "bad id", want: codes.InvalidArgument,
			req: func(b string) *loggingpb.CreateLinkRequest {
				return &loggingpb.CreateLinkRequest{Parent: b, LinkId: "a-b"}
			},
		},
		{
			name: "no analytics", want: codes.FailedPrecondition,
			req: func(string) *loggingpb.CreateLinkRequest {
				return &loggingpb.CreateLinkRequest{Parent: loc + "/buckets/_Default", LinkId: "ok"}
			},
		},
		{
			name: "missing bucket", want: codes.NotFound,
			req: func(string) *loggingpb.CreateLinkRequest {
				return &loggingpb.CreateLinkRequest{Parent: loc + "/buckets/ghost", LinkId: "ok"}
			},
		},
		{
			name: "bad parent", want: codes.InvalidArgument,
			req: func(string) *loggingpb.CreateLinkRequest {
				return &loggingpb.CreateLinkRequest{Parent: "x", LinkId: "ok"}
			},
		},
		{
			name: "dataset exists", want: codes.AlreadyExists,
			req: func(b string) *loggingpb.CreateLinkRequest {
				return &loggingpb.CreateLinkRequest{Parent: b, LinkId: "taken"}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, true)
			bucket := e.analyticsBucket(t, "an")
			require.NoError(t, e.tables.CreateDataset(testProject, "taken"))

			_, err := e.config(t).CreateLink(t.Context(), tt.req(bucket))
			assert.Equal(t, tt.want, status.Code(err))
		})
	}

	cfg := newEnv(t, true).config(t)

	_, err := cfg.GetLink(t.Context(), &loggingpb.GetLinkRequest{Name: "bad"})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))

	_, err = cfg.DeleteLink(t.Context(), &loggingpb.DeleteLinkRequest{Name: loc + "/buckets/_Default/links/nope"})
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestLinkSurvivesSnapshot(t *testing.T) {
	t.Parallel()

	src := newEnv(t, true)
	bucket := src.analyticsBucket(t, "an")

	op, err := src.config(t).CreateLink(t.Context(), &loggingpb.CreateLinkRequest{Parent: bucket, LinkId: "lnk"})
	require.NoError(t, err)

	_, err = op.Wait(t.Context())
	require.NoError(t, err)

	dst := newEnv(t, true)
	require.NoError(t, dst.handler.Restore(t.Context(), src.handler.Snapshot(t.Context())))

	got, err := dst.config(t).GetLink(t.Context(), &loggingpb.GetLinkRequest{Name: bucket + "/links/lnk"})
	require.NoError(t, err)
	assert.Equal(t, bucket+"/links/lnk", got.GetName())
}

func TestCopyLogEntries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		filter string
		dest   string
		want   codes.Code
		copied int64
	}{
		{name: "all", dest: "storage.googleapis.com/out", copied: 2},
		{name: "filtered", filter: "severity>=ERROR", dest: "storage.googleapis.com/out", copied: 1},
		{name: "bad filter", filter: "severity >=", dest: "storage.googleapis.com/out", want: codes.InvalidArgument},
		{name: "bad destination", dest: "pubsub.googleapis.com/t", want: codes.InvalidArgument},
		{name: "empty destination", want: codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, true)
			now := e.clk.now()
			e.write(t,
				textEntry("app", "fine", ltype.LogSeverity_INFO, now.Add(-time.Minute)),
				textEntry("app", "bad", ltype.LogSeverity_ERROR, now.Add(-time.Second)))

			op, err := e.config(t).CopyLogEntries(t.Context(), &loggingpb.CopyLogEntriesRequest{
				Name: loc + "/buckets/_Default", Filter: tt.filter, Destination: tt.dest,
			})
			if tt.want != codes.OK {
				assert.Equal(t, tt.want, status.Code(err))

				return
			}

			require.NoError(t, err)

			resp, err := op.Wait(t.Context())
			require.NoError(t, err)
			assert.Equal(t, tt.copied, resp.GetLogEntriesCopiedCount())

			meta, err := op.Metadata()
			require.NoError(t, err)
			assert.Equal(t, loggingpb.OperationState_OPERATION_STATE_SUCCEEDED, meta.GetState())
			assert.NotEmpty(t, meta.GetWriterIdentity())

			names := e.objs.names()
			require.Len(t, names, 1)
			assert.Contains(t, names[0], "out/app/2030/01/01/")
			assert.Contains(t, e.objs.get(names[0]), "bad")
		})
	}
}

func TestCopyLogEntriesMissingBucket(t *testing.T) {
	t.Parallel()

	_, err := newEnv(t, true).config(t).CopyLogEntries(t.Context(), &loggingpb.CopyLogEntriesRequest{
		Name: loc + "/buckets/ghost", Destination: "storage.googleapis.com/out",
	})
	assert.Equal(t, codes.NotFound, status.Code(err))

	_, err = newEnv(t, true).config(t).CopyLogEntries(t.Context(), &loggingpb.CopyLogEntriesRequest{
		Name: "bad", Destination: "storage.googleapis.com/out",
	})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
	require.NotErrorIs(t, err, errors.ErrUnsupported)
}
