package athena_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	athenasdk "github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/athena"
)

func TestCreateWorkGroup_FeatureConfigs_RealClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		cfg     *types.WorkGroupConfiguration
		check   func(t *testing.T, got *types.WorkGroupConfiguration)
		name    string
		wantErr string
	}{
		{
			name: "round trip",
			cfg: &types.WorkGroupConfiguration{
				ExecutionRole: aws.String("arn:aws:iam::123456789012:role/r"),
				IdentityCenterConfiguration: &types.IdentityCenterConfiguration{
					EnableIdentityCenter:      aws.Bool(true),
					IdentityCenterInstanceArn: aws.String("arn:aws:sso:::instance/ssoins-1"),
				},
				QueryResultsS3AccessGrantsConfiguration: &types.QueryResultsS3AccessGrantsConfiguration{
					AuthenticationType:    types.AuthenticationTypeDirectoryIdentity,
					EnableS3AccessGrants:  aws.Bool(true),
					CreateUserLevelPrefix: aws.Bool(true),
				},
			},
			check: func(t *testing.T, got *types.WorkGroupConfiguration) {
				t.Helper()
				require.NotNil(t, got.IdentityCenterConfiguration)
				assert.True(t, aws.ToBool(got.IdentityCenterConfiguration.EnableIdentityCenter))
				assert.Equal(t, "arn:aws:sso:::instance/ssoins-1",
					aws.ToString(got.IdentityCenterConfiguration.IdentityCenterInstanceArn))
				require.NotNil(t, got.QueryResultsS3AccessGrantsConfiguration)
				assert.True(t, aws.ToBool(got.QueryResultsS3AccessGrantsConfiguration.CreateUserLevelPrefix))
				assert.Equal(t, types.AuthenticationTypeDirectoryIdentity,
					got.QueryResultsS3AccessGrantsConfiguration.AuthenticationType)
			},
		},
		{
			name: "managed results round trip",
			cfg: &types.WorkGroupConfiguration{
				ManagedQueryResultsConfiguration: &types.ManagedQueryResultsConfiguration{
					Enabled: true,
					EncryptionConfiguration: &types.ManagedQueryResultsEncryptionConfiguration{
						KmsKey: aws.String("arn:aws:kms:us-east-1:123456789012:key/k"),
					},
				},
			},
			check: func(t *testing.T, got *types.WorkGroupConfiguration) {
				t.Helper()
				require.NotNil(t, got.ManagedQueryResultsConfiguration)
				assert.True(t, got.ManagedQueryResultsConfiguration.Enabled)
				require.NotNil(t, got.ManagedQueryResultsConfiguration.EncryptionConfiguration)
				assert.Equal(t, "arn:aws:kms:us-east-1:123456789012:key/k",
					aws.ToString(got.ManagedQueryResultsConfiguration.EncryptionConfiguration.KmsKey))
			},
		},
		{
			name: "identity center needs role",
			cfg: &types.WorkGroupConfiguration{
				IdentityCenterConfiguration: &types.IdentityCenterConfiguration{EnableIdentityCenter: aws.Bool(true)},
			},
			wantErr: "ExecutionRole",
		},
		{
			name: "managed excludes output location",
			cfg: &types.WorkGroupConfiguration{
				ManagedQueryResultsConfiguration: &types.ManagedQueryResultsConfiguration{Enabled: true},
				ResultConfiguration:              &types.ResultConfiguration{OutputLocation: aws.String("s3://b/p/")},
			},
			wantErr: "OutputLocation",
		},
		{
			name: "grants bad auth type",
			cfg: &types.WorkGroupConfiguration{
				QueryResultsS3AccessGrantsConfiguration: &types.QueryResultsS3AccessGrantsConfiguration{
					AuthenticationType:   types.AuthenticationType("BOGUS"),
					EnableS3AccessGrants: aws.Bool(true),
				},
			},
			wantErr: "AuthenticationType",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := athena.NewInMemoryBackend("123456789012", config.DefaultRegion)
			client := newTestAthenaClient(t, athena.NewHandler(backend))
			ctx := t.Context()

			_, err := client.CreateWorkGroup(ctx, &athenasdk.CreateWorkGroupInput{
				Name:          aws.String("wg"),
				Configuration: tt.cfg,
			})
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)

				return
			}

			require.NoError(t, err)

			got, err := client.GetWorkGroup(ctx, &athenasdk.GetWorkGroupInput{WorkGroup: aws.String("wg")})
			require.NoError(t, err)
			tt.check(t, got.WorkGroup.Configuration)
		})
	}
}

func TestUpdateWorkGroup_FeatureConfigs_RealClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		update *types.WorkGroupConfigurationUpdates
		check  func(t *testing.T, got *types.WorkGroupConfiguration)
		name   string
	}{
		{
			name: "remove bytes cutoff",
			update: &types.WorkGroupConfigurationUpdates{
				RemoveBytesScannedCutoffPerQuery: aws.Bool(true),
			},
			check: func(t *testing.T, got *types.WorkGroupConfiguration) {
				t.Helper()
				assert.Zero(t, aws.ToInt64(got.BytesScannedCutoffPerQuery))
			},
		},
		{
			name: "managed results enable",
			update: &types.WorkGroupConfigurationUpdates{
				ManagedQueryResultsConfigurationUpdates: &types.ManagedQueryResultsConfigurationUpdates{
					Enabled: aws.Bool(true),
					EncryptionConfiguration: &types.ManagedQueryResultsEncryptionConfiguration{
						KmsKey: aws.String("arn:aws:kms:us-east-1:123456789012:key/k"),
					},
				},
				ResultConfigurationUpdates: &types.ResultConfigurationUpdates{RemoveOutputLocation: aws.Bool(true)},
			},
			check: func(t *testing.T, got *types.WorkGroupConfiguration) {
				t.Helper()
				require.NotNil(t, got.ManagedQueryResultsConfiguration)
				assert.True(t, got.ManagedQueryResultsConfiguration.Enabled)
			},
		},
		{
			name: "s3 access grants",
			update: &types.WorkGroupConfigurationUpdates{
				QueryResultsS3AccessGrantsConfiguration: &types.QueryResultsS3AccessGrantsConfiguration{
					AuthenticationType:   types.AuthenticationTypeDirectoryIdentity,
					EnableS3AccessGrants: aws.Bool(true),
				},
			},
			check: func(t *testing.T, got *types.WorkGroupConfiguration) {
				t.Helper()
				require.NotNil(t, got.QueryResultsS3AccessGrantsConfiguration)
				assert.True(t, aws.ToBool(got.QueryResultsS3AccessGrantsConfiguration.EnableS3AccessGrants))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := athena.NewInMemoryBackend("123456789012", config.DefaultRegion)
			client := newTestAthenaClient(t, athena.NewHandler(backend))
			ctx := t.Context()

			_, err := client.CreateWorkGroup(ctx, &athenasdk.CreateWorkGroupInput{
				Name: aws.String("wg"),
				Configuration: &types.WorkGroupConfiguration{
					BytesScannedCutoffPerQuery: aws.Int64(20 * 1024 * 1024),
					ResultConfiguration:        &types.ResultConfiguration{OutputLocation: aws.String("s3://b/p/")},
				},
			})
			require.NoError(t, err)

			_, err = client.UpdateWorkGroup(ctx, &athenasdk.UpdateWorkGroupInput{
				WorkGroup:            aws.String("wg"),
				ConfigurationUpdates: tt.update,
			})
			require.NoError(t, err)

			got, err := client.GetWorkGroup(ctx, &athenasdk.GetWorkGroupInput{WorkGroup: aws.String("wg")})
			require.NoError(t, err)
			tt.check(t, got.WorkGroup.Configuration)
		})
	}
}

func TestStartQueryExecution_EngineConfigurationValidation_RealClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantErr string
		cls     []types.Classification
	}{
		{name: "valid dpu range", cls: []types.Classification{{
			Name:       aws.String("athena-query-engine-properties"),
			Properties: map[string]string{"min-dpu-count": "4", "max-dpu-count": "24"},
		}}},
		{name: "bad classification", cls: []types.Classification{{
			Name: aws.String("spark-defaults"),
		}}, wantErr: "Classification name"},
		{name: "bad property", cls: []types.Classification{{
			Name:       aws.String("athena-query-engine-properties"),
			Properties: map[string]string{"foo": "1"},
		}}, wantErr: "not allowed"},
		{name: "non numeric", cls: []types.Classification{{
			Name:       aws.String("athena-query-engine-properties"),
			Properties: map[string]string{"min-dpu-count": "x"},
		}}, wantErr: "positive integer"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := athena.NewInMemoryBackend("123456789012", config.DefaultRegion)
			client := newTestAthenaClient(t, athena.NewHandler(backend))

			_, err := client.StartQueryExecution(t.Context(), &athenasdk.StartQueryExecutionInput{
				QueryString:         aws.String("SELECT 1"),
				EngineConfiguration: &types.EngineConfiguration{Classifications: tt.cls},
				ResultConfiguration: &types.ResultConfiguration{OutputLocation: aws.String("s3://b/p/")},
			})
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestGetQueryExecution_ManagedQueryResults_RealClient(t *testing.T) {
	t.Parallel()

	backend := athena.NewInMemoryBackend("123456789012", config.DefaultRegion)
	client := newTestAthenaClient(t, athena.NewHandler(backend))
	ctx := t.Context()

	_, err := client.CreateWorkGroup(ctx, &athenasdk.CreateWorkGroupInput{
		Name: aws.String("managed"),
		Configuration: &types.WorkGroupConfiguration{
			ManagedQueryResultsConfiguration: &types.ManagedQueryResultsConfiguration{Enabled: true},
		},
	})
	require.NoError(t, err)

	start, err := client.StartQueryExecution(ctx, &athenasdk.StartQueryExecutionInput{
		QueryString: aws.String("SELECT 1"),
		WorkGroup:   aws.String("managed"),
	})
	require.NoError(t, err)

	got, err := client.GetQueryExecution(ctx, &athenasdk.GetQueryExecutionInput{
		QueryExecutionId: start.QueryExecutionId,
	})
	require.NoError(t, err)
	require.NotNil(t, got.QueryExecution.ManagedQueryResultsConfiguration)
	assert.True(t, got.QueryExecution.ManagedQueryResultsConfiguration.Enabled)
}
