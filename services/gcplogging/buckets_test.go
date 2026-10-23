package gcplogging_test

import (
	"errors"
	"testing"
	"time"

	"cloud.google.com/go/logging/apiv2/loggingpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

const loc = proj + "/locations/global"

func mask(paths ...string) *fieldmaskpb.FieldMask { return &fieldmaskpb.FieldMask{Paths: paths} }

func TestBucketLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	cfg := e.config(t)

	b, err := cfg.CreateBucket(t.Context(), &loggingpb.CreateBucketRequest{
		Parent: loc, BucketId: "audit", Bucket: &loggingpb.LogBucket{Description: "d", RetentionDays: 60},
	})
	require.NoError(t, err)
	assert.Equal(t, loc+"/buckets/audit", b.GetName())
	assert.Equal(t, loggingpb.LifecycleState_ACTIVE, b.GetLifecycleState())
	assert.EqualValues(t, 60, b.GetRetentionDays())

	_, err = cfg.CreateBucket(t.Context(), &loggingpb.CreateBucketRequest{Parent: loc, BucketId: "audit"})
	assert.Equal(t, codes.AlreadyExists, status.Code(err))

	upd, err := cfg.UpdateBucket(t.Context(), &loggingpb.UpdateBucketRequest{
		Name:       b.GetName(),
		Bucket:     &loggingpb.LogBucket{RetentionDays: 120, Description: "new"},
		UpdateMask: mask("retention_days"),
	})
	require.NoError(t, err)
	assert.EqualValues(t, 120, upd.GetRetentionDays())
	assert.Equal(t, "d", upd.GetDescription())

	var ids []string

	it := cfg.ListBuckets(t.Context(), &loggingpb.ListBucketsRequest{Parent: proj + "/locations/-"})

	for {
		bk, nerr := it.Next()
		if errors.Is(nerr, iterator.Done) {
			break
		}

		require.NoError(t, nerr)

		ids = append(ids, bk.GetName())
	}

	assert.Equal(t, []string{loc + "/buckets/_Default", loc + "/buckets/_Required", loc + "/buckets/audit"}, ids)

	require.NoError(t, cfg.DeleteBucket(t.Context(), &loggingpb.DeleteBucketRequest{Name: b.GetName()}))

	pending, err := cfg.GetBucket(t.Context(), &loggingpb.GetBucketRequest{Name: b.GetName()})
	require.NoError(t, err)
	assert.Equal(t, loggingpb.LifecycleState_DELETE_REQUESTED, pending.GetLifecycleState())

	require.NoError(t, cfg.UndeleteBucket(t.Context(), &loggingpb.UndeleteBucketRequest{Name: b.GetName()}))

	e.clk.advance(8 * 24 * time.Hour)
	require.NoError(t, cfg.DeleteBucket(t.Context(), &loggingpb.DeleteBucketRequest{Name: b.GetName()}))
	e.clk.advance(8 * 24 * time.Hour)

	_, err = cfg.GetBucket(t.Context(), &loggingpb.GetBucketRequest{Name: b.GetName()})
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestBucketProtections(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	cfg := e.config(t)

	_, err := cfg.CreateBucket(t.Context(), &loggingpb.CreateBucketRequest{
		Parent: loc, BucketId: "locked", Bucket: &loggingpb.LogBucket{Locked: true, RetentionDays: 100},
	})
	require.NoError(t, err)

	tests := []struct {
		call func() error
		name string
		code codes.Code
	}{
		{func() error {
			return cfg.DeleteBucket(t.Context(), &loggingpb.DeleteBucketRequest{Name: loc + "/buckets/_Default"})
		}, "delete default", codes.InvalidArgument},
		{func() error {
			_, e := cfg.UpdateBucket(t.Context(), &loggingpb.UpdateBucketRequest{
				Name:       loc + "/buckets/_Required",
				Bucket:     &loggingpb.LogBucket{RetentionDays: 1},
				UpdateMask: mask("retention_days"),
			})

			return e
		}, "update required", codes.InvalidArgument},
		{func() error {
			_, e := cfg.UpdateBucket(t.Context(), &loggingpb.UpdateBucketRequest{
				Name:       loc + "/buckets/locked",
				Bucket:     &loggingpb.LogBucket{RetentionDays: 10},
				UpdateMask: mask("retention_days"),
			})

			return e
		}, "shorten locked retention", codes.FailedPrecondition},
		{func() error {
			return cfg.DeleteBucket(t.Context(), &loggingpb.DeleteBucketRequest{Name: loc + "/buckets/locked"})
		}, "delete locked", codes.FailedPrecondition},
		{func() error {
			_, e := cfg.CreateBucket(t.Context(), &loggingpb.CreateBucketRequest{
				Parent: loc, BucketId: "x", Bucket: &loggingpb.LogBucket{RetentionDays: 99999},
			})

			return e
		}, "bad retention", codes.InvalidArgument},
		{func() error {
			_, e := cfg.UpdateBucket(
				t.Context(),
				&loggingpb.UpdateBucketRequest{Name: loc + "/buckets/locked", Bucket: &loggingpb.LogBucket{}},
			)

			return e
		}, "empty mask", codes.InvalidArgument},
		{func() error {
			_, e := cfg.GetBucket(t.Context(), &loggingpb.GetBucketRequest{Name: loc + "/buckets/none"})

			return e
		}, "missing", codes.NotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.code, status.Code(tt.call()))
		})
	}
}

func TestBucketAsyncOperations(t *testing.T) {
	t.Parallel()

	cfg := newEnv(t, true).config(t)

	op, err := cfg.CreateBucketAsync(t.Context(), &loggingpb.CreateBucketRequest{
		Parent: loc, BucketId: "async", Bucket: &loggingpb.LogBucket{AnalyticsEnabled: true},
	})
	require.NoError(t, err)

	got, err := op.Wait(t.Context())
	require.NoError(t, err)
	assert.Equal(t, loc+"/buckets/async", got.GetName())

	meta, err := op.Metadata()
	require.NoError(t, err)
	assert.Equal(t, loggingpb.OperationState_OPERATION_STATE_SUCCEEDED, meta.GetState())

	op2, err := cfg.UpdateBucketAsync(t.Context(), &loggingpb.UpdateBucketRequest{
		Name: got.GetName(), Bucket: &loggingpb.LogBucket{RetentionDays: 45}, UpdateMask: mask("retention_days"),
	})
	require.NoError(t, err)

	upd, err := op2.Wait(t.Context())
	require.NoError(t, err)
	assert.EqualValues(t, 45, upd.GetRetentionDays())

	polled := cfg.CreateBucketAsyncOperation(op.Name())
	_, err = polled.Poll(t.Context())
	require.NoError(t, err)
	assert.True(t, polled.Done())
}

func TestViews(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	cfg := e.config(t)
	bucket := loc + "/buckets/_Default"

	v, err := cfg.CreateView(t.Context(), &loggingpb.CreateViewRequest{
		Parent: bucket, ViewId: "errors", View: &loggingpb.LogView{Filter: "severity>=ERROR"},
	})
	require.NoError(t, err)
	assert.Equal(t, bucket+"/views/errors", v.GetName())

	_, err = cfg.CreateView(
		t.Context(),
		&loggingpb.CreateViewRequest{Parent: bucket, ViewId: "bad", View: &loggingpb.LogView{Filter: "severity>="}},
	)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))

	now := e.clk.now()
	e.write(t, textEntry("v", "ok", 200, now), textEntry("v", "bad", 500, now))
	assert.Equal(
		t,
		[]string{"bad"},
		texts(e.list(t, &loggingpb.ListLogEntriesRequest{ResourceNames: []string{v.GetName()}})),
	)
	all := e.list(t, &loggingpb.ListLogEntriesRequest{ResourceNames: []string{bucket + "/views/_AllLogs"}})
	assert.ElementsMatch(t, []string{"ok", "bad"}, texts(all))

	upd, err := cfg.UpdateView(t.Context(), &loggingpb.UpdateViewRequest{
		Name: v.GetName(), View: &loggingpb.LogView{Description: "new"}, UpdateMask: mask("description"),
	})
	require.NoError(t, err)
	assert.Equal(t, "new", upd.GetDescription())
	assert.Equal(t, "severity>=ERROR", upd.GetFilter())

	var names []string

	it := cfg.ListViews(t.Context(), &loggingpb.ListViewsRequest{Parent: bucket})

	for {
		x, nerr := it.Next()
		if errors.Is(nerr, iterator.Done) {
			break
		}

		require.NoError(t, nerr)

		names = append(names, x.GetName())
	}

	assert.Equal(t, []string{bucket + "/views/_AllLogs", bucket + "/views/errors"}, names)
	assert.Equal(
		t,
		codes.InvalidArgument,
		status.Code(cfg.DeleteView(t.Context(), &loggingpb.DeleteViewRequest{Name: bucket + "/views/_AllLogs"})),
	)
	require.NoError(t, cfg.DeleteView(t.Context(), &loggingpb.DeleteViewRequest{Name: v.GetName()}))
	assert.Equal(
		t,
		codes.NotFound,
		status.Code(cfg.DeleteView(t.Context(), &loggingpb.DeleteViewRequest{Name: v.GetName()})),
	)
}

func TestCmekAndSettings(t *testing.T) {
	t.Parallel()

	cfg := newEnv(t, true).config(t)
	key := "projects/k/locations/global/keyRings/r/cryptoKeys/c"

	empty, err := cfg.GetCmekSettings(t.Context(), &loggingpb.GetCmekSettingsRequest{Name: proj + "/cmekSettings"})
	require.NoError(t, err)
	assert.Empty(t, empty.GetKmsKeyName())

	set, err := cfg.UpdateCmekSettings(t.Context(), &loggingpb.UpdateCmekSettingsRequest{
		Name: proj + "/cmekSettings", CmekSettings: &loggingpb.CmekSettings{KmsKeyName: key},
	})
	require.NoError(t, err)
	assert.Equal(t, key, set.GetKmsKeyName())
	assert.Contains(t, set.GetServiceAccountId(), "gcp-sa-logging")

	got, err := cfg.GetCmekSettings(t.Context(), &loggingpb.GetCmekSettingsRequest{Name: proj + "/cmekSettings"})
	require.NoError(t, err)
	assert.Equal(t, key, got.GetKmsKeyName())

	_, err = cfg.UpdateCmekSettings(t.Context(), &loggingpb.UpdateCmekSettingsRequest{
		Name: proj + "/cmekSettings", CmekSettings: &loggingpb.CmekSettings{KmsKeyName: "nope"},
	})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))

	s, err := cfg.UpdateSettings(t.Context(), &loggingpb.UpdateSettingsRequest{
		Name:     proj + "/settings",
		Settings: &loggingpb.Settings{StorageLocation: "us-east1", DisableDefaultSink: true},
	})
	require.NoError(t, err)
	assert.Equal(t, "us-east1", s.GetStorageLocation())

	sink, err := cfg.GetSink(t.Context(), &loggingpb.GetSinkRequest{SinkName: proj + "/sinks/_Default"})
	require.NoError(t, err)
	assert.True(t, sink.GetDisabled())

	_, err = cfg.GetSettings(t.Context(), &loggingpb.GetSettingsRequest{Name: "bogus"})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestUnsupportedLinks(t *testing.T) {
	t.Parallel()

	cfg := newEnv(t, true).config(t)

	_, err := cfg.GetLink(t.Context(), &loggingpb.GetLinkRequest{Name: loc + "/buckets/_Default/links/l"})
	assert.Equal(t, codes.Unimplemented, status.Code(err))
}
