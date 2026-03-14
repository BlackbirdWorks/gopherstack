package integration_test

import (
	"testing"

	smithy "github.com/aws/smithy-go"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_S3_PublicAccessBlock verifies Put/Get/DeletePublicAccessBlock operations.
func TestIntegration_S3_PublicAccessBlock(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		setup   func(t *testing.T, client *s3.Client, bucket string)
		name    string
		want    types.PublicAccessBlockConfiguration
		wantErr bool
	}{
		{
			name: "put and get public access block with all fields true",
			setup: func(t *testing.T, client *s3.Client, bucket string) {
				t.Helper()
				_, err := client.PutPublicAccessBlock(t.Context(), &s3.PutPublicAccessBlockInput{
					Bucket: aws.String(bucket),
					PublicAccessBlockConfiguration: &types.PublicAccessBlockConfiguration{
						BlockPublicAcls:       aws.Bool(true),
						IgnorePublicAcls:      aws.Bool(true),
						BlockPublicPolicy:     aws.Bool(true),
						RestrictPublicBuckets: aws.Bool(true),
					},
				})
				require.NoError(t, err)
			},
			want: types.PublicAccessBlockConfiguration{
				BlockPublicAcls:       aws.Bool(true),
				IgnorePublicAcls:      aws.Bool(true),
				BlockPublicPolicy:     aws.Bool(true),
				RestrictPublicBuckets: aws.Bool(true),
			},
		},
		{
			name: "put and get public access block with mixed fields",
			setup: func(t *testing.T, client *s3.Client, bucket string) {
				t.Helper()
				_, err := client.PutPublicAccessBlock(t.Context(), &s3.PutPublicAccessBlockInput{
					Bucket: aws.String(bucket),
					PublicAccessBlockConfiguration: &types.PublicAccessBlockConfiguration{
						BlockPublicAcls:       aws.Bool(true),
						IgnorePublicAcls:      aws.Bool(false),
						BlockPublicPolicy:     aws.Bool(true),
						RestrictPublicBuckets: aws.Bool(false),
					},
				})
				require.NoError(t, err)
			},
			want: types.PublicAccessBlockConfiguration{
				BlockPublicAcls:       aws.Bool(true),
				IgnorePublicAcls:      aws.Bool(false),
				BlockPublicPolicy:     aws.Bool(true),
				RestrictPublicBuckets: aws.Bool(false),
			},
		},
		{
			name: "get returns NoSuchPublicAccessBlockConfiguration when not set",
			setup: func(_ *testing.T, _ *s3.Client, _ string) {
				// No config stored.
			},
			wantErr: true,
		},
	}

	client := createS3Client(t)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			bucket := "pab-integ-" + uuid.NewString()

			_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
			require.NoError(t, err)

			t.Cleanup(func() {
				_, _ = client.DeleteBucket(t.Context(), &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
			})

			tt.setup(t, client, bucket)

			out, err := client.GetPublicAccessBlock(ctx, &s3.GetPublicAccessBlockInput{
				Bucket: aws.String(bucket),
			})

			if tt.wantErr {
				require.Error(t, err)

				var apiErr smithy.APIError
				require.ErrorAs(t, err, &apiErr)
				assert.Equal(t, "NoSuchPublicAccessBlockConfiguration", apiErr.ErrorCode())

				return
			}

			require.NoError(t, err)
			require.NotNil(t, out.PublicAccessBlockConfiguration)

			cfg := out.PublicAccessBlockConfiguration
			assert.Equal(t, aws.ToBool(tt.want.BlockPublicAcls), aws.ToBool(cfg.BlockPublicAcls))
			assert.Equal(t, aws.ToBool(tt.want.IgnorePublicAcls), aws.ToBool(cfg.IgnorePublicAcls))
			assert.Equal(t, aws.ToBool(tt.want.BlockPublicPolicy), aws.ToBool(cfg.BlockPublicPolicy))
			assert.Equal(t, aws.ToBool(tt.want.RestrictPublicBuckets), aws.ToBool(cfg.RestrictPublicBuckets))
		})
	}
}

// TestIntegration_S3_DeletePublicAccessBlock verifies that deleting the public access block config works.
func TestIntegration_S3_DeletePublicAccessBlock(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createS3Client(t)
	ctx := t.Context()

	bucket := "pab-delete-integ-" + uuid.NewString()

	_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	t.Cleanup(func() {
		_, _ = client.DeleteBucket(t.Context(), &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
	})

	_, err = client.PutPublicAccessBlock(ctx, &s3.PutPublicAccessBlockInput{
		Bucket: aws.String(bucket),
		PublicAccessBlockConfiguration: &types.PublicAccessBlockConfiguration{
			BlockPublicAcls: aws.Bool(true),
		},
	})
	require.NoError(t, err)

	out, err := client.GetPublicAccessBlock(ctx, &s3.GetPublicAccessBlockInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)
	assert.True(t, aws.ToBool(out.PublicAccessBlockConfiguration.BlockPublicAcls))

	_, err = client.DeletePublicAccessBlock(ctx, &s3.DeletePublicAccessBlockInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	_, err = client.GetPublicAccessBlock(ctx, &s3.GetPublicAccessBlockInput{Bucket: aws.String(bucket)})

	var apiErr smithy.APIError
	require.Error(t, err)
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "NoSuchPublicAccessBlockConfiguration", apiErr.ErrorCode())
}

// TestIntegration_S3_OwnershipControls verifies Put/Get/DeleteBucketOwnershipControls.
func TestIntegration_S3_OwnershipControls(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		setup     func(t *testing.T, client *s3.Client, bucket string)
		name      string
		wantOwner types.ObjectOwnership
		wantErr   bool
	}{
		{
			name:      "put and get BucketOwnerEnforced",
			wantOwner: types.ObjectOwnershipBucketOwnerEnforced,
			setup: func(t *testing.T, client *s3.Client, bucket string) {
				t.Helper()
				_, err := client.PutBucketOwnershipControls(t.Context(), &s3.PutBucketOwnershipControlsInput{
					Bucket: aws.String(bucket),
					OwnershipControls: &types.OwnershipControls{
						Rules: []types.OwnershipControlsRule{
							{ObjectOwnership: types.ObjectOwnershipBucketOwnerEnforced},
						},
					},
				})
				require.NoError(t, err)
			},
		},
		{
			name:      "put and get BucketOwnerPreferred",
			wantOwner: types.ObjectOwnershipBucketOwnerPreferred,
			setup: func(t *testing.T, client *s3.Client, bucket string) {
				t.Helper()
				_, err := client.PutBucketOwnershipControls(t.Context(), &s3.PutBucketOwnershipControlsInput{
					Bucket: aws.String(bucket),
					OwnershipControls: &types.OwnershipControls{
						Rules: []types.OwnershipControlsRule{
							{ObjectOwnership: types.ObjectOwnershipBucketOwnerPreferred},
						},
					},
				})
				require.NoError(t, err)
			},
		},
		{
			name: "get returns OwnershipControlsNotFoundError when not set",
			setup: func(_ *testing.T, _ *s3.Client, _ string) {
				// No config stored.
			},
			wantErr: true,
		},
	}

	client := createS3Client(t)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			bucket := "ownership-integ-" + uuid.NewString()

			_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
			require.NoError(t, err)

			t.Cleanup(func() {
				_, _ = client.DeleteBucket(t.Context(), &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
			})

			tt.setup(t, client, bucket)

			out, err := client.GetBucketOwnershipControls(ctx, &s3.GetBucketOwnershipControlsInput{
				Bucket: aws.String(bucket),
			})

			if tt.wantErr {
				require.Error(t, err)

				var apiErr smithy.APIError
				require.ErrorAs(t, err, &apiErr)
				assert.Equal(t, "OwnershipControlsNotFoundError", apiErr.ErrorCode())

				return
			}

			require.NoError(t, err)
			require.NotNil(t, out.OwnershipControls)
			require.NotEmpty(t, out.OwnershipControls.Rules)
			assert.Equal(t, tt.wantOwner, out.OwnershipControls.Rules[0].ObjectOwnership)
		})
	}
}

// TestIntegration_S3_DeleteOwnershipControls verifies that deleting ownership controls works.
func TestIntegration_S3_DeleteOwnershipControls(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createS3Client(t)
	ctx := t.Context()

	bucket := "ownership-delete-integ-" + uuid.NewString()

	_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	t.Cleanup(func() {
		_, _ = client.DeleteBucket(t.Context(), &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
	})

	_, err = client.PutBucketOwnershipControls(ctx, &s3.PutBucketOwnershipControlsInput{
		Bucket: aws.String(bucket),
		OwnershipControls: &types.OwnershipControls{
			Rules: []types.OwnershipControlsRule{
				{ObjectOwnership: types.ObjectOwnershipBucketOwnerEnforced},
			},
		},
	})
	require.NoError(t, err)

	out, err := client.GetBucketOwnershipControls(ctx, &s3.GetBucketOwnershipControlsInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)
	require.NotEmpty(t, out.OwnershipControls.Rules)
	assert.Equal(t, types.ObjectOwnershipBucketOwnerEnforced, out.OwnershipControls.Rules[0].ObjectOwnership)

	_, err = client.DeleteBucketOwnershipControls(
		ctx,
		&s3.DeleteBucketOwnershipControlsInput{Bucket: aws.String(bucket)},
	)
	require.NoError(t, err)

	_, err = client.GetBucketOwnershipControls(ctx, &s3.GetBucketOwnershipControlsInput{Bucket: aws.String(bucket)})

	var apiErr smithy.APIError
	require.Error(t, err)
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "OwnershipControlsNotFoundError", apiErr.ErrorCode())
}

// TestIntegration_S3_BucketLogging verifies Put/GetBucketLogging operations.
func TestIntegration_S3_BucketLogging(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		setup          func(t *testing.T, client *s3.Client, bucket string)
		name           string
		wantTargetBkt  string
		wantEmptyRules bool
	}{
		{
			name:          "put and get logging configuration",
			wantTargetBkt: "logs-target",
			setup: func(t *testing.T, client *s3.Client, bucket string) {
				t.Helper()
				_, err := client.PutBucketLogging(t.Context(), &s3.PutBucketLoggingInput{
					Bucket: aws.String(bucket),
					BucketLoggingStatus: &types.BucketLoggingStatus{
						LoggingEnabled: &types.LoggingEnabled{
							TargetBucket: aws.String("logs-target"),
							TargetPrefix: aws.String("logs/"),
						},
					},
				})
				require.NoError(t, err)
			},
		},
		{
			name:           "get returns empty logging status when not configured",
			wantEmptyRules: true,
			setup: func(_ *testing.T, _ *s3.Client, _ string) {
				// No logging config stored.
			},
		},
	}

	client := createS3Client(t)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			bucket := "logging-integ-" + uuid.NewString()

			_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
			require.NoError(t, err)

			t.Cleanup(func() {
				_, _ = client.DeleteBucket(t.Context(), &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
			})

			tt.setup(t, client, bucket)

			out, err := client.GetBucketLogging(ctx, &s3.GetBucketLoggingInput{
				Bucket: aws.String(bucket),
			})
			require.NoError(t, err)

			if tt.wantEmptyRules {
				assert.Nil(t, out.LoggingEnabled)

				return
			}

			require.NotNil(t, out.LoggingEnabled)
			assert.Equal(t, tt.wantTargetBkt, aws.ToString(out.LoggingEnabled.TargetBucket))
			assert.Equal(t, "logs/", aws.ToString(out.LoggingEnabled.TargetPrefix))
		})
	}
}

// TestIntegration_S3_BucketReplication verifies Put/Get/DeleteBucketReplication.
func TestIntegration_S3_BucketReplication(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	const replicationRoleARN = "arn:aws:iam::000000000000:role/replication-role"

	tests := []struct {
		setup    func(t *testing.T, client *s3.Client, bucket string)
		name     string
		wantRole string
		wantErr  bool
	}{
		{
			name:     "put and get replication configuration",
			wantRole: replicationRoleARN,
			setup: func(t *testing.T, client *s3.Client, bucket string) {
				t.Helper()
				_, err := client.PutBucketVersioning(t.Context(), &s3.PutBucketVersioningInput{
					Bucket: aws.String(bucket),
					VersioningConfiguration: &types.VersioningConfiguration{
						Status: types.BucketVersioningStatusEnabled,
					},
				})
				require.NoError(t, err)

				_, err = client.PutBucketReplication(t.Context(), &s3.PutBucketReplicationInput{
					Bucket: aws.String(bucket),
					ReplicationConfiguration: &types.ReplicationConfiguration{
						Role: aws.String(replicationRoleARN),
						Rules: []types.ReplicationRule{
							{
								ID:     aws.String("rule-1"),
								Status: types.ReplicationRuleStatusEnabled,
								Destination: &types.Destination{
									Bucket: aws.String("arn:aws:s3:::dest-bucket"),
								},
								Filter: &types.ReplicationRuleFilter{},
							},
						},
					},
				})
				require.NoError(t, err)
			},
		},
		{
			name: "get returns ReplicationConfigurationNotFoundError when not set",
			setup: func(_ *testing.T, _ *s3.Client, _ string) {
				// No replication config stored.
			},
			wantErr: true,
		},
	}

	client := createS3Client(t)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			bucket := "replication-integ-" + uuid.NewString()

			_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
			require.NoError(t, err)

			t.Cleanup(func() {
				_, _ = client.DeleteBucket(t.Context(), &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
			})

			tt.setup(t, client, bucket)

			out, err := client.GetBucketReplication(ctx, &s3.GetBucketReplicationInput{
				Bucket: aws.String(bucket),
			})

			if tt.wantErr {
				require.Error(t, err)

				var apiErr smithy.APIError
				require.ErrorAs(t, err, &apiErr)
				assert.Equal(t, "ReplicationConfigurationNotFoundError", apiErr.ErrorCode())

				return
			}

			require.NoError(t, err)
			require.NotNil(t, out.ReplicationConfiguration)
			assert.Equal(t, tt.wantRole, aws.ToString(out.ReplicationConfiguration.Role))
			require.NotEmpty(t, out.ReplicationConfiguration.Rules)
		})
	}
}

// TestIntegration_S3_DeleteBucketReplication verifies that deleting replication configuration works.
func TestIntegration_S3_DeleteBucketReplication(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createS3Client(t)
	ctx := t.Context()

	bucket := "replication-delete-integ-" + uuid.NewString()

	_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	t.Cleanup(func() {
		_, _ = client.DeleteBucket(t.Context(), &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
	})

	_, err = client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
		Bucket: aws.String(bucket),
		VersioningConfiguration: &types.VersioningConfiguration{
			Status: types.BucketVersioningStatusEnabled,
		},
	})
	require.NoError(t, err)

	_, err = client.PutBucketReplication(ctx, &s3.PutBucketReplicationInput{
		Bucket: aws.String(bucket),
		ReplicationConfiguration: &types.ReplicationConfiguration{
			Role: aws.String("arn:aws:iam::000000000000:role/replication-role"),
			Rules: []types.ReplicationRule{
				{
					ID:     aws.String("rule-1"),
					Status: types.ReplicationRuleStatusEnabled,
					Destination: &types.Destination{
						Bucket: aws.String("arn:aws:s3:::dest-bucket"),
					},
					Filter: &types.ReplicationRuleFilter{},
				},
			},
		},
	})
	require.NoError(t, err)

	out, err := client.GetBucketReplication(ctx, &s3.GetBucketReplicationInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)
	require.NotNil(t, out.ReplicationConfiguration)

	_, err = client.DeleteBucketReplication(ctx, &s3.DeleteBucketReplicationInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	_, err = client.GetBucketReplication(ctx, &s3.GetBucketReplicationInput{Bucket: aws.String(bucket)})

	var apiErr smithy.APIError
	require.Error(t, err)
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "ReplicationConfigurationNotFoundError", apiErr.ErrorCode())
}
