package s3control_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3control"
)

func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *s3control.InMemoryBackend) string
		verify func(t *testing.T, b *s3control.InMemoryBackend, id string)
		name   string
	}{
		{
			name: "round_trip_preserves_state",
			setup: func(b *s3control.InMemoryBackend) string {
				b.PutPublicAccessBlock(s3control.PublicAccessBlock{
					AccountID:       "000000000000",
					BlockPublicAcls: true,
				})

				return "000000000000"
			},
			verify: func(t *testing.T, b *s3control.InMemoryBackend, id string) {
				t.Helper()

				cfg, err := b.GetPublicAccessBlock(id)
				require.NoError(t, err)
				assert.True(t, cfg.BlockPublicAcls)
				assert.Equal(t, id, cfg.AccountID)
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *s3control.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *s3control.InMemoryBackend, _ string) {
				t.Helper()

				_, err := b.GetPublicAccessBlock("nonexistent")
				require.Error(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := s3control.NewInMemoryBackend()
			id := tt.setup(original)

			snap := original.Snapshot(t.Context())
			require.NotNil(t, snap)

			fresh := s3control.NewInMemoryBackend()
			require.NoError(t, fresh.Restore(t.Context(), snap))

			tt.verify(t, fresh, id)
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

// TestInMemoryBackend_FullStateSnapshotRestore seeds every store.Table-backed
// resource (Phase 3.3) -- both the "clean" tables registered on the
// backend's registry and the "dirty" tables that round-trip through an
// ephemeral DTO registry (mrapRequests, accessPointPABs) -- plus every raw
// (non-*T) map that remains persisted, then verifies a Snapshot/Restore round
// trip preserves all of it byte-for-byte.
func TestInMemoryBackend_FullStateSnapshotRestore(t *testing.T) {
	t.Parallel()

	const accountID = "000000000000"

	original := s3control.NewInMemoryBackend()

	// "Clean" tables.
	original.PutPublicAccessBlock(s3control.PublicAccessBlock{
		AccountID:       accountID,
		BlockPublicAcls: true,
	})
	agi := original.AddAccessGrantsInstanceInternal(accountID, "arn:aws:sso:::instance/ins-1")
	require.NotNil(t, agi)
	grant := original.AddAccessGrantInternal(accountID, "loc1", "DIRECTORY_USER", "u@x.com", "READ")
	require.NotNil(t, grant)
	loc := original.CreateAccessGrantsLocation(accountID, "s3://bucket/*", "arn:aws:iam::000000000000:role/r")
	require.NotNil(t, loc)
	ap := original.CreateAccessPoint(accountID, "my-ap", "my-bucket")
	require.NotNil(t, ap)
	olap := original.CreateAccessPointForObjectLambda(accountID, "my-olap")
	require.NotNil(t, olap)
	bucket := original.CreateBucket(accountID, "my-outposts-bucket")
	require.NotNil(t, bucket)
	job, jobErr := original.CreateJob(accountID, "arn:aws:iam::000000000000:role/batch-role", 5)
	require.NoError(t, jobErr)
	require.NotNil(t, job)
	slg := original.CreateStorageLensGroup(accountID, "my-slg")
	require.NotNil(t, slg)

	// "Dirty" tables (mrapRequests, accessPointPABs).
	mrapReq := original.CreateMultiRegionAccessPoint(accountID, "my-mrap", "")
	require.NotNil(t, mrapReq)
	pabErr := original.PutAccessPointPublicAccessBlock(accountID, "my-ap", s3control.PublicAccessBlock{
		BlockPublicAcls: true,
	})
	require.NoError(t, pabErr)

	// Raw (non-*T) maps that remain persisted.
	require.NoError(t, original.PutAccessPointPolicy(accountID, "my-ap", `{"Version":"2012-10-17"}`))
	require.NoError(t, original.PutBucketReplication(accountID, "my-outposts-bucket", "<ReplicationConfiguration/>"))
	require.NoError(t, original.PutStorageLensConfiguration(accountID, "my-config", "<StorageLensConfiguration/>"))
	require.NoError(t, original.PutStorageLensConfigurationTagging(accountID, "my-config", s3control.TagSet{"k": "v"}))
	original.TagResource("arn:aws:s3:::my-outposts-bucket", map[string]string{"env": "test"})

	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := s3control.NewInMemoryBackend()
	require.NoError(t, fresh.Restore(t.Context(), snap))

	// Verify every table's count round-tripped exactly.
	assert.Equal(t, 1, s3control.AccessBlockCount(fresh))
	assert.Equal(t, 1, s3control.AccessGrantsInstanceCount(fresh))
	assert.Equal(t, 1, s3control.AccessGrantCount(fresh))
	assert.Equal(t, 1, s3control.AccessGrantsLocationCount(fresh))
	assert.Equal(t, 1, s3control.AccessPointCount(fresh))
	assert.Equal(t, 1, s3control.ObjectLambdaAccessPointCount(fresh))
	assert.Equal(t, 1, s3control.OutpostsBucketCount(fresh))
	assert.Equal(t, 1, s3control.BatchJobCount(fresh))
	assert.Equal(t, 1, s3control.StorageLensGroupCount(fresh))
	assert.Equal(t, 1, s3control.MRAPRequestCount(fresh))
	assert.Equal(t, 1, s3control.MRAPCount(fresh))
	assert.Equal(t, 1, s3control.AccessPointPABCount(fresh))

	// Spot-check values, not just counts, for every table.
	cfg, err := fresh.GetPublicAccessBlock(accountID)
	require.NoError(t, err)
	assert.True(t, cfg.BlockPublicAcls)

	gotAGI, err := fresh.GetAccessGrantsInstance(accountID)
	require.NoError(t, err)
	assert.Equal(t, agi.AccessGrantsInstanceID, gotAGI.AccessGrantsInstanceID)

	gotGrant, err := fresh.GetAccessGrant(accountID, grant.AccessGrantID)
	require.NoError(t, err)
	assert.Equal(t, "READ", gotGrant.Permission)

	gotLoc, err := fresh.GetAccessGrantsLocation(accountID, loc.AccessGrantsLocationID)
	require.NoError(t, err)
	assert.Equal(t, loc.IAMRoleArn, gotLoc.IAMRoleArn)

	gotAP, err := fresh.GetAccessPoint(accountID, "my-ap")
	require.NoError(t, err)
	assert.Equal(t, "my-bucket", gotAP.Bucket)

	gotOLAP, err := fresh.GetAccessPointForObjectLambda(accountID, "my-olap")
	require.NoError(t, err)
	assert.Equal(t, olap.ObjectLambdaAccessPointArn, gotOLAP.ObjectLambdaAccessPointArn)

	gotBucket, err := fresh.GetBucket(accountID, "my-outposts-bucket")
	require.NoError(t, err)
	assert.Equal(t, bucket.BucketArn, gotBucket.BucketArn)

	gotJob, err := fresh.GetJob(accountID, job.JobID)
	require.NoError(t, err)
	assert.Equal(t, int32(5), gotJob.Priority)

	gotSLG, err := fresh.GetStorageLensGroup(accountID, "my-slg")
	require.NoError(t, err)
	assert.Equal(t, slg.StorageLensGroupArn, gotSLG.StorageLensGroupArn)

	gotMRAP, err := fresh.GetMultiRegionAccessPoint(accountID, "my-mrap")
	require.NoError(t, err)
	assert.Equal(t, "READY", gotMRAP.Status)

	gotMRAPReq, err := fresh.DescribeMultiRegionAccessPointOperation(accountID, mrapReq.RequestTokenARN)
	require.NoError(t, err)
	assert.Equal(t, mrapReq.RequestTokenARN, gotMRAPReq.RequestTokenARN)

	gotPAB, err := fresh.GetAccessPointPublicAccessBlock(accountID, "my-ap")
	require.NoError(t, err)
	assert.True(t, gotPAB.BlockPublicAcls)

	// Raw maps round-trip too.
	gotPolicy, err := fresh.GetAccessPointPolicy(accountID, "my-ap")
	require.NoError(t, err)
	assert.JSONEq(t, `{"Version":"2012-10-17"}`, gotPolicy)

	gotRepl, err := fresh.GetBucketReplication(accountID, "my-outposts-bucket")
	require.NoError(t, err)
	assert.Equal(t, "<ReplicationConfiguration/>", gotRepl)

	gotSLC, err := fresh.GetStorageLensConfiguration(accountID, "my-config")
	require.NoError(t, err)
	assert.Equal(t, "<StorageLensConfiguration/>", gotSLC)

	gotSLCTags, err := fresh.GetStorageLensConfigurationTagging(accountID, "my-config")
	require.NoError(t, err)
	assert.Equal(t, s3control.TagSet{"k": "v"}, gotSLCTags)

	gotTags := fresh.ListTagsForResource("arn:aws:s3:::my-outposts-bucket")
	assert.Equal(t, map[string]string{"env": "test"}, gotTags)
}
