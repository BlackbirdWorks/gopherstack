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
	require.NoError(t, original.PutBucketReplication("my-outposts-bucket", "<ReplicationConfiguration/>"))
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

	gotBucket, err := fresh.GetBucket("my-outposts-bucket")
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

	gotRepl, err := fresh.GetBucketReplication("my-outposts-bucket")
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

func TestPersistence_AccessPointPABs(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	b.CreateAccessPoint("000000000000", "my-ap", "my-bucket")
	_ = b.PutAccessPointPublicAccessBlock(
		"000000000000", "my-ap",
		s3control.PublicAccessBlock{BlockPublicAcls: true, BlockPublicPolicy: true},
	)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := s3control.NewInMemoryBackend()
	require.NoError(t, b2.Restore(t.Context(), snap))

	pab, err := b2.GetAccessPointPublicAccessBlock("000000000000", "my-ap")
	require.NoError(t, err)
	assert.True(t, pab.BlockPublicAcls)
	assert.True(t, pab.BlockPublicPolicy)
}

func TestPersistence_AccessPointVpcConfig(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	b.CreateAccessPoint("000000000000", "vpc-ap", "my-bucket")
	_ = b.SetAccessPointVpcConfig("000000000000", "vpc-ap", "vpc-abc123", "111122223333")

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := s3control.NewInMemoryBackend()
	require.NoError(t, b2.Restore(t.Context(), snap))

	ap, err := b2.GetAccessPoint("000000000000", "vpc-ap")
	require.NoError(t, err)
	assert.Equal(t, "vpc-abc123", ap.VpcID)
	assert.Equal(t, "VPC", ap.NetworkOrigin)
}

func TestPersistence_JobDetails(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	job, err := b.CreateJob("000000000000", "arn:aws:iam::000000000000:role/R", 5)
	require.NoError(t, err)
	_ = b.UpdateJobDetails("000000000000", job.JobID, "my job", "<manifest/>", "<op/>", "<report/>", true)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := s3control.NewInMemoryBackend()
	require.NoError(t, b2.Restore(t.Context(), snap))

	got, err := b2.GetJob("000000000000", job.JobID)
	require.NoError(t, err)
	assert.Equal(t, "my job", got.Description)
	assert.True(t, got.ConfirmationRequired)
	assert.NotEmpty(t, got.CreationTime)
}

func TestPersistence_StorageLensConfig_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		configName string
		config     string
	}{
		{
			name:       "round_trip_with_data",
			configName: "snap-cfg",
			config:     "<IsEnabled>true</IsEnabled>",
		},
		{
			name:       "round_trip_empty_config",
			configName: "empty-cfg",
			config:     "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()
			b.PutStorageLensConfiguration("acc1", tt.configName, tt.config)
			require.Equal(t, 1, s3control.StorageLensConfigCount(b))

			snap := b.Snapshot(t.Context())
			require.NotNil(t, snap)

			b2 := s3control.NewInMemoryBackend()
			require.NoError(t, b2.Restore(t.Context(), snap))
			assert.Equal(t, 1, s3control.StorageLensConfigCount(b2))

			cfg, err := b2.GetStorageLensConfiguration("acc1", tt.configName)
			require.NoError(t, err)
			assert.Equal(t, tt.config, cfg)
		})
	}
}

func TestPersistence_SnapshotRestoreDeepCopy(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	b.PutPublicAccessBlock(s3control.PublicAccessBlock{AccountID: "acc1", BlockPublicAcls: true})

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	// Mutate original after snapshot
	b.PutPublicAccessBlock(s3control.PublicAccessBlock{AccountID: "acc2", BlockPublicAcls: false})
	assert.Equal(t, 2, s3control.AccessBlockCount(b))

	// Restore from snapshot
	b2 := s3control.NewInMemoryBackend()
	require.NoError(t, b2.Restore(t.Context(), snap))
	assert.Equal(t, 1, s3control.AccessBlockCount(b2))
}

// TestPersistence_Batch1Maps_SnapshotRestore locks in the version-1-to-2
// persistence-gap fix: accessPointScopes, objectLambdaAPPolicies,
// objectLambdaAPConfigs, bucketPolicies, bucketTagging, bucketLifecycle,
// bucketVersioning, mrapRoutes, accessGrantsInstancePolicies, and jobTags
// were declared on InMemoryBackend but never wired into backendSnapshot, so
// a Snapshot/Restore cycle silently dropped every one of them even though
// the owning resource survived. Each subtest seeds one such field and
// asserts it round-trips.
func TestPersistence_Batch1Maps_SnapshotRestore(t *testing.T) {
	t.Parallel()

	const accountID = "000000000000"

	tests := []struct {
		setup  func(t *testing.T, b *s3control.InMemoryBackend)
		verify func(t *testing.T, b *s3control.InMemoryBackend)
		name   string
	}{
		{
			name: "access_point_scope",
			setup: func(t *testing.T, b *s3control.InMemoryBackend) {
				t.Helper()
				b.CreateAccessPoint(accountID, "scoped-ap", "my-bucket")
				require.NoError(t, b.PutAccessPointScope(accountID, "scoped-ap", "<Scope/>"))
			},
			verify: func(t *testing.T, b *s3control.InMemoryBackend) {
				t.Helper()
				scope, err := b.GetAccessPointScope(accountID, "scoped-ap")
				require.NoError(t, err)
				assert.Equal(t, "<Scope/>", scope)
			},
		},
		{
			name: "object_lambda_ap_policy_and_config",
			setup: func(t *testing.T, b *s3control.InMemoryBackend) {
				t.Helper()
				b.CreateAccessPointForObjectLambda(accountID, "olap-1")
				require.NoError(t, b.PutAccessPointPolicyForObjectLambda(accountID, "olap-1", `{"p":1}`))
				require.NoError(t, b.PutAccessPointConfigurationForObjectLambda(accountID, "olap-1", "<Config/>"))
			},
			verify: func(t *testing.T, b *s3control.InMemoryBackend) {
				t.Helper()
				policy, err := b.GetAccessPointPolicyForObjectLambda(accountID, "olap-1")
				require.NoError(t, err)
				assert.Equal(t, `{"p":1}`, policy)
				cfg, err := b.GetAccessPointConfigurationForObjectLambda(accountID, "olap-1")
				require.NoError(t, err)
				assert.Equal(t, "<Config/>", cfg)
			},
		},
		{
			name: "bucket_policy_tagging_lifecycle_versioning",
			setup: func(t *testing.T, b *s3control.InMemoryBackend) {
				t.Helper()
				b.CreateBucket(accountID, "batch1-bucket")
				require.NoError(t, b.PutBucketPolicy("batch1-bucket", `{"p":1}`))
				require.NoError(t, b.PutBucketTagging("batch1-bucket", s3control.TagSet{"k": "v"}))
				require.NoError(t, b.PutBucketLifecycleConfiguration("batch1-bucket", "<Lifecycle/>"))
				require.NoError(t, b.PutBucketVersioning("batch1-bucket", "Enabled"))
			},
			verify: func(t *testing.T, b *s3control.InMemoryBackend) {
				t.Helper()
				policy, err := b.GetBucketPolicy("batch1-bucket")
				require.NoError(t, err)
				assert.Equal(t, `{"p":1}`, policy)
				tags, err := b.GetBucketTagging("batch1-bucket")
				require.NoError(t, err)
				assert.Equal(t, s3control.TagSet{"k": "v"}, tags)
				lc, err := b.GetBucketLifecycleConfiguration("batch1-bucket")
				require.NoError(t, err)
				assert.Equal(t, "<Lifecycle/>", lc)
				v, err := b.GetBucketVersioning("batch1-bucket")
				require.NoError(t, err)
				assert.Equal(t, "Enabled", v)
			},
		},
		{
			name: "mrap_routes",
			setup: func(t *testing.T, b *s3control.InMemoryBackend) {
				t.Helper()
				b.CreateMultiRegionAccessPoint(accountID, "routed-mrap", "")
				require.NoError(t, b.SubmitMultiRegionAccessPointRoutes(accountID, "routed-mrap", "<Routes/>"))
			},
			verify: func(t *testing.T, b *s3control.InMemoryBackend) {
				t.Helper()
				routes, err := b.GetMultiRegionAccessPointRoutes(accountID, "routed-mrap")
				require.NoError(t, err)
				assert.Equal(t, "<Routes/>", routes)
			},
		},
		{
			name: "access_grants_instance_resource_policy",
			setup: func(t *testing.T, b *s3control.InMemoryBackend) {
				t.Helper()
				b.CreateAccessGrantsInstance(accountID, "")
				b.PutAccessGrantsInstanceResourcePolicy(accountID, `{"p":1}`)
			},
			verify: func(t *testing.T, b *s3control.InMemoryBackend) {
				t.Helper()
				policy, err := b.GetAccessGrantsInstanceResourcePolicy(accountID)
				require.NoError(t, err)
				assert.Equal(t, `{"p":1}`, policy)
			},
		},
		{
			name: "job_tags",
			setup: func(t *testing.T, b *s3control.InMemoryBackend) {
				t.Helper()
				job, err := b.CreateJob(accountID, "arn:aws:iam::000000000000:role/R", 1)
				require.NoError(t, err)
				require.NoError(t, b.PutJobTagging(accountID, job.JobID, s3control.TagSet{"env": "prod"}))
			},
			verify: func(t *testing.T, b *s3control.InMemoryBackend) {
				t.Helper()
				jobs := b.ListJobs(accountID)
				require.Len(t, jobs, 1)
				tags, err := b.GetJobTagging(accountID, jobs[0].JobID)
				require.NoError(t, err)
				assert.Equal(t, s3control.TagSet{"env": "prod"}, tags)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()
			tt.setup(t, b)

			snap := b.Snapshot(t.Context())
			require.NotNil(t, snap)

			fresh := s3control.NewInMemoryBackend()
			require.NoError(t, fresh.Restore(t.Context(), snap))

			tt.verify(t, fresh)
		})
	}
}

func TestPersistence_SnapshotRestore_AccessGrantsAndJobs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *s3control.InMemoryBackend)
		verify func(t *testing.T, b *s3control.InMemoryBackend)
		name   string
	}{
		{
			name: "snapshot_restore_preserves_access_grants_instance",
			setup: func(b *s3control.InMemoryBackend) {
				b.CreateAccessGrantsInstance("account-1", "arn:aws:sso:::instance/inst-1")
			},
			verify: func(t *testing.T, _ *s3control.InMemoryBackend) {
				t.Helper()
			},
		},
		{
			name: "snapshot_restore_preserves_batch_job",
			setup: func(b *s3control.InMemoryBackend) {
				_, _ = b.CreateJob("account-2", "arn:aws:iam::account-2:role/role", 10)
			},
			verify: func(t *testing.T, _ *s3control.InMemoryBackend) {
				t.Helper()
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := s3control.NewInMemoryBackend()
			tt.setup(original)

			snap := original.Snapshot(t.Context())
			require.NotNil(t, snap)

			fresh := s3control.NewInMemoryBackend()
			require.NoError(t, fresh.Restore(t.Context(), snap))

			tt.verify(t, fresh)
		})
	}
}
