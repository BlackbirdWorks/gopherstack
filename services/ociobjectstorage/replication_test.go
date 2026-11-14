package ociobjectstorage_test

import (
	"io"
	"strings"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/objectstorage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	oci "github.com/blackbirdworks/gopherstack/services/ociobjectstorage"
)

const replRegion = "us-phoenix-1"

func replEnv(t *testing.T) (*env, objectstorage.ReplicationPolicy) {
	t.Helper()

	e := newEnv(t, envOpts{})
	e.mkBucket(t, "src", nil)
	e.mkBucket(t, "dst", nil)

	p, err := e.client.CreateReplicationPolicy(t.Context(), objectstorage.CreateReplicationPolicyRequest{
		NamespaceName: new(e.ns), BucketName: new("src"),
		Name: new("to-phx"), DestinationRegionName: new(replRegion), DestinationBucketName: new("dst"),
	})
	require.NoError(t, err)

	return e, p.ReplicationPolicy
}

func headOf(t *testing.T, e *env, bucket, name string) (objectstorage.HeadObjectResponse, error) {
	t.Helper()

	return e.client.HeadObject(t.Context(), objectstorage.HeadObjectRequest{
		NamespaceName: new(e.ns), BucketName: new(bucket), ObjectName: new(name),
	})
}

func TestReplicationCopiesNewObjectsAfterClockAdvance(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	e.mkBucket(t, "src", nil)
	e.mkBucket(t, "dst", nil)
	e.put(t, "src", "before", "old")

	p, err := e.client.CreateReplicationPolicy(t.Context(), objectstorage.CreateReplicationPolicyRequest{
		NamespaceName: new(e.ns), BucketName: new("src"),
		Name: new("to-phx"), DestinationRegionName: new(replRegion), DestinationBucketName: new("dst"),
	})
	require.NoError(t, err)
	assert.Equal(t, objectstorage.ReplicationPolicyStatusActive, p.Status)
	assert.Equal(t, "to-phx", *p.Name)
	assert.Equal(t, "dst", *p.DestinationBucketName)

	put := e.put(t, "src", "after", "new")

	assert.Zero(t, e.backend.SweepAsync(), "nothing is due yet")

	_, err = headOf(t, e, "dst", "after")
	assert.Equal(t, 404, svcErr(t, err).GetHTTPStatusCode())

	e.clock.Advance(time.Minute)
	assert.Equal(t, 1, e.backend.SweepAsync())

	head, err := headOf(t, e, "dst", "after")
	require.NoError(t, err)
	assert.Equal(t, *put.ETag, *head.ETag, "replicated objects keep their ETag")
	assert.Equal(t, *put.OpcContentMd5, *head.ContentMd5)
	assert.Equal(t, "new", e.get(t, "dst", "after"))

	_, err = headOf(t, e, "dst", "before")
	assert.Equal(t, 404, svcErr(t, err).GetHTTPStatusCode(), "objects older than the policy are not replicated")

	got, err := e.client.GetReplicationPolicy(t.Context(), objectstorage.GetReplicationPolicyRequest{
		NamespaceName: new(e.ns), BucketName: new("src"), ReplicationId: p.Id,
	})
	require.NoError(t, err)
	assert.True(t, got.TimeLastSync.After(p.TimeLastSync.Time), "timeLastSync advances after a sync")

	_, err = e.client.DeleteObject(t.Context(), objectstorage.DeleteObjectRequest{
		NamespaceName: new(e.ns), BucketName: new("src"), ObjectName: new("after"),
	})
	require.NoError(t, err)
	e.clock.Advance(time.Minute)
	e.backend.SweepAsync()

	_, err = headOf(t, e, "dst", "after")
	assert.Equal(t, 404, svcErr(t, err).GetHTTPStatusCode(), "deletes replicate")
}

func TestReplicationDestinationIsReadOnly(t *testing.T) {
	t.Parallel()

	e, _ := replEnv(t)

	_, err := e.client.PutObject(t.Context(), objectstorage.PutObjectRequest{
		NamespaceName: new(e.ns), BucketName: new("dst"), ObjectName: new("x"),
		ContentLength: new(int64(1)), PutObjectBody: io.NopCloser(strings.NewReader("x")),
	})
	assert.Equal(t, 409, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.client.DeleteBucket(t.Context(), objectstorage.DeleteBucketRequest{
		NamespaceName: new(e.ns), BucketName: new("dst"),
	})
	assert.Equal(t, 409, svcErr(t, err).GetHTTPStatusCode())

	srcs, err := e.client.ListReplicationSources(t.Context(), objectstorage.ListReplicationSourcesRequest{
		NamespaceName: new(e.ns), BucketName: new("dst"),
	})
	require.NoError(t, err)
	require.Len(t, srcs.Items, 1)
	assert.Equal(t, "to-phx", *srcs.Items[0].PolicyName)
	assert.Equal(t, "src", *srcs.Items[0].SourceBucketName)
	assert.Equal(t, oci.Region, *srcs.Items[0].SourceRegionName)

	_, err = e.client.MakeBucketWritable(t.Context(), objectstorage.MakeBucketWritableRequest{
		NamespaceName: new(e.ns), BucketName: new("dst"),
	})
	require.NoError(t, err)

	e.put(t, "dst", "x", "now writable")

	pols, err := e.client.ListReplicationPolicies(t.Context(), objectstorage.ListReplicationPoliciesRequest{
		NamespaceName: new(e.ns), BucketName: new("src"),
	})
	require.NoError(t, err)
	require.Len(t, pols.Items, 1)
	assert.Equal(t, objectstorage.ReplicationPolicySummaryStatusClientError, pols.Items[0].Status)

	e.put(t, "src", "late", "y")
	e.clock.Advance(time.Minute)
	assert.Zero(t, e.backend.SweepAsync(), "a policy in CLIENT_ERROR no longer replicates")
}

func TestReplicationPolicyRejections(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		src     string
		dst     string
		wantHTT int
	}{
		{name: "second policy on a source", src: "src", dst: "other", wantHTT: 409},
		{name: "destination already used", src: "other", dst: "dst", wantHTT: 409},
		{name: "chained from a destination", src: "dst", dst: "other", wantHTT: 409},
		{name: "chained into a source", src: "other", dst: "src", wantHTT: 409},
		{name: "same bucket", src: "other", dst: "other", wantHTT: 400},
		{name: "missing destination", src: "other", dst: "nope", wantHTT: 404},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e, _ := replEnv(t)
			e.mkBucket(t, "other", nil)

			_, err := e.client.CreateReplicationPolicy(t.Context(), objectstorage.CreateReplicationPolicyRequest{
				NamespaceName: new(e.ns), BucketName: new(tt.src),
				Name: new("p2"), DestinationRegionName: new(replRegion), DestinationBucketName: new(tt.dst),
			})
			assert.Equal(t, tt.wantHTT, svcErr(t, err).GetHTTPStatusCode())
		})
	}
}

func TestReplicationPolicyLifecycle(t *testing.T) {
	t.Parallel()

	e, p := replEnv(t)

	_, err := e.client.GetReplicationPolicy(t.Context(), objectstorage.GetReplicationPolicyRequest{
		NamespaceName: new(e.ns), BucketName: new("src"), ReplicationId: new("nope"),
	})
	assert.Equal(t, 404, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.client.DeleteReplicationPolicy(t.Context(), objectstorage.DeleteReplicationPolicyRequest{
		NamespaceName: new(e.ns), BucketName: new("src"), ReplicationId: p.Id,
	})
	require.NoError(t, err)

	e.put(t, "src", "after-delete", "x")
	e.clock.Advance(time.Minute)
	assert.Zero(t, e.backend.SweepAsync(), "a deleted policy replicates nothing")

	pols, err := e.client.ListReplicationPolicies(t.Context(), objectstorage.ListReplicationPoliciesRequest{
		NamespaceName: new(e.ns), BucketName: new("src"),
	})
	require.NoError(t, err)
	assert.Empty(t, pols.Items)
}

func TestReplicationSurvivesSnapshot(t *testing.T) {
	t.Parallel()

	e, _ := replEnv(t)
	e.put(t, "src", "k", "v")

	fresh := oci.NewInMemoryBackend()
	fresh.SetClock(e.clock.Now)
	require.NoError(t, fresh.Restore(t.Context(), e.backend.Snapshot(t.Context())))

	e.clock.Advance(time.Minute)
	assert.Equal(t, 1, fresh.SweepAsync())

	o, err := fresh.GetObject(e.ns, "dst", "k", "", oci.Preconds{}, true)
	require.NoError(t, err)
	assert.Equal(t, "v", string(o.Data))
}

func copyTo(t *testing.T, e *env, region, dst, name string) (string, error) {
	t.Helper()

	r, err := e.client.CopyObject(t.Context(), objectstorage.CopyObjectRequest{
		NamespaceName: new(e.ns), BucketName: new("src"),
		SourceObjectName: new("orig"), DestinationRegion: new(region), DestinationNamespace: new(e.ns),
		DestinationBucket: new(dst), DestinationObjectName: new(name),
	})
	if err != nil {
		return "", err
	}

	return *r.OpcWorkRequestId, nil
}

func wrStatus(t *testing.T, e *env, id string) objectstorage.WorkRequestStatusEnum {
	t.Helper()

	wr, err := e.client.GetWorkRequest(t.Context(), objectstorage.GetWorkRequestRequest{WorkRequestId: new(id)})
	require.NoError(t, err)

	return wr.Status
}

func TestCrossRegionCopyWorkRequest(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	e.mkBucket(t, "src", nil)
	e.mkBucket(t, "far", nil)
	e.put(t, "src", "orig", "payload")

	id, err := copyTo(t, e, replRegion, "far", "copy")
	require.NoError(t, err)
	assert.Equal(t, objectstorage.WorkRequestStatusAccepted, wrStatus(t, e, id))

	_, err = headOf(t, e, "far", "copy")
	assert.Equal(t, 404, svcErr(t, err).GetHTTPStatusCode())

	e.clock.Advance(20 * time.Second)
	assert.Equal(t, objectstorage.WorkRequestStatusInProgress, wrStatus(t, e, id))

	e.clock.Advance(time.Minute)
	e.backend.SweepAsync()
	assert.Equal(t, objectstorage.WorkRequestStatusCompleted, wrStatus(t, e, id))
	assert.Equal(t, "payload", e.get(t, "far", "copy"))

	_, err = e.client.CancelWorkRequest(t.Context(), objectstorage.CancelWorkRequestRequest{WorkRequestId: new(id)})
	assert.Equal(t, 409, svcErr(t, err).GetHTTPStatusCode())

	same, err := copyTo(t, e, oci.Region, "far", "sync-copy")
	require.NoError(t, err)
	assert.Equal(
		t,
		objectstorage.WorkRequestStatusCompleted,
		wrStatus(t, e, same),
		"same-region copy completes at once",
	)
}

func TestCrossRegionCopyCancelAndFailure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		act  func(t *testing.T, e *env, id string)
		want objectstorage.WorkRequestStatusEnum
	}{
		{
			name: "canceled", want: objectstorage.WorkRequestStatusCanceled,
			act: func(t *testing.T, e *env, id string) {
				t.Helper()

				_, err := e.client.CancelWorkRequest(t.Context(), objectstorage.CancelWorkRequestRequest{
					WorkRequestId: new(id),
				})
				require.NoError(t, err)
			},
		},
		{
			name: "source deleted before the copy runs", want: objectstorage.WorkRequestStatusFailed,
			act: func(t *testing.T, e *env, _ string) {
				t.Helper()

				_, err := e.client.DeleteObject(t.Context(), objectstorage.DeleteObjectRequest{
					NamespaceName: new(e.ns), BucketName: new("src"), ObjectName: new("orig"),
				})
				require.NoError(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			e.mkBucket(t, "src", nil)
			e.mkBucket(t, "far", nil)
			e.put(t, "src", "orig", "payload")

			id, err := copyTo(t, e, replRegion, "far", "copy")
			require.NoError(t, err)

			tt.act(t, e, id)
			e.clock.Advance(time.Minute)
			e.backend.SweepAsync()

			assert.Equal(t, tt.want, wrStatus(t, e, id))

			_, err = headOf(t, e, "far", "copy")
			assert.Equal(t, 404, svcErr(t, err).GetHTTPStatusCode())
		})
	}
}

func TestCrossRegionCopyErrors(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	e.mkBucket(t, "src", nil)
	e.put(t, "src", "orig", "payload")

	_, err := copyTo(t, e, replRegion, "nope", "copy")
	assert.Equal(t, 404, svcErr(t, err).GetHTTPStatusCode(), "destination bucket must exist at accept time")
}

func TestRestoreTimingPerTier(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	e.mkBucket(t, "arch", nil)

	put := func(name string, tier objectstorage.PutObjectStorageTierEnum) {
		t.Helper()

		_, err := e.client.PutObject(t.Context(), objectstorage.PutObjectRequest{
			NamespaceName: new(e.ns), BucketName: new("arch"), ObjectName: new(name),
			ContentLength: new(int64(1)), PutObjectBody: io.NopCloser(strings.NewReader("x")), StorageTier: tier,
		})
		require.NoError(t, err)
	}
	restore := func(name string) error {
		t.Helper()

		_, err := e.client.RestoreObjects(t.Context(), objectstorage.RestoreObjectsRequest{
			NamespaceName: new(e.ns), BucketName: new("arch"),
			ObjectName: new(name), Hours: new(3),
		})

		return err
	}

	put("std", objectstorage.PutObjectStorageTierStandard)
	put("ia", objectstorage.PutObjectStorageTierInfrequentaccess)
	put("cold", objectstorage.PutObjectStorageTierArchive)

	for _, n := range []string{"std", "ia"} {
		assert.Equal(t, "Conflict", svcErr(t, restore(n)).GetCode(), n+" is readable and needs no restore")
	}

	archivedAt := e.clock.Now()
	h, err := headOf(t, e, "arch", "cold")
	require.NoError(t, err)
	require.NotNil(t, h.TimeOfArchival)
	assert.WithinDuration(t, archivedAt, h.TimeOfArchival.Time, time.Second)

	std, err := headOf(t, e, "arch", "std")
	require.NoError(t, err)
	assert.Nil(t, std.TimeOfArchival)
	assert.Empty(t, std.ArchivalState)

	require.NoError(t, restore("cold"))
	e.clock.Advance(59 * time.Minute)

	h, err = headOf(t, e, "arch", "cold")
	require.NoError(t, err)
	assert.Equal(t, objectstorage.HeadObjectArchivalStateRestoring, h.ArchivalState, "restore takes up to an hour")

	e.clock.Advance(time.Minute)

	h, err = headOf(t, e, "arch", "cold")
	require.NoError(t, err)
	assert.Equal(t, objectstorage.HeadObjectArchivalStateRestored, h.ArchivalState)

	e.clock.Advance(3 * time.Hour)

	h, err = headOf(t, e, "arch", "cold")
	require.NoError(t, err)
	assert.Equal(
		t,
		objectstorage.HeadObjectArchivalStateArchived,
		h.ArchivalState,
		"window of hours then archived again",
	)
}
