package awsconfig_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/awsconfig"
)

func TestAWSConfigBackend_PutConfigurationRecorder(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		recName    string
		roleARN    string
		wantName   string
		wantStatus string
		wantLen    int
	}{
		{
			name:       "success",
			recName:    "default",
			roleARN:    "arn:aws:iam::000000000000:role/config",
			wantLen:    1,
			wantName:   "default",
			wantStatus: "PENDING",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := awsconfig.NewInMemoryBackend()
			err := b.PutConfigurationRecorder(tt.recName, tt.roleARN)
			require.NoError(t, err)

			recorders := b.DescribeConfigurationRecorders()
			require.Len(t, recorders, tt.wantLen)
			assert.Equal(t, tt.wantName, recorders[0].Name)
			assert.Equal(t, tt.wantStatus, recorders[0].Status)
		})
	}
}

func TestAWSConfigBackend_StartConfigurationRecorder(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		recName    string
		setup      func(t *testing.T, b *awsconfig.InMemoryBackend)
		wantErr    error
		wantStatus string
	}{
		{
			name:    "success",
			recName: "default",
			setup: func(t *testing.T, b *awsconfig.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.PutConfigurationRecorder("default", "arn:aws:iam::000000000000:role/config"))
				require.NoError(t, b.PutDeliveryChannel("default", "my-bucket", ""))
			},
			wantStatus: "ACTIVE",
		},
		{
			name:    "not_found",
			recName: "nonexistent",
			wantErr: awsconfig.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := awsconfig.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(t, b)
			}

			err := b.StartConfigurationRecorder(tt.recName)

			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)

			recorders := b.DescribeConfigurationRecorders()
			require.Len(t, recorders, 1)
			assert.Equal(t, tt.wantStatus, recorders[0].Status)
		})
	}
}

func TestAWSConfigBackend_PutDeliveryChannel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		chanName   string
		bucket     string
		topic      string
		wantName   string
		wantBucket string
		wantLen    int
	}{
		{
			name:       "success",
			chanName:   "default",
			bucket:     "my-bucket",
			topic:      "arn:aws:sns:us-east-1:000000000000:my-topic",
			wantLen:    1,
			wantName:   "default",
			wantBucket: "my-bucket",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := awsconfig.NewInMemoryBackend()
			err := b.PutDeliveryChannel(tt.chanName, tt.bucket, tt.topic)
			require.NoError(t, err)

			channels := b.DescribeDeliveryChannels()
			require.Len(t, channels, tt.wantLen)
			assert.Equal(t, tt.wantName, channels[0].Name)
			assert.Equal(t, tt.wantBucket, channels[0].S3Bucket)
		})
	}
}

func TestAWSConfigBackend_DescribeDeliveryChannels(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(t *testing.T, b *awsconfig.InMemoryBackend)
		name      string
		wantCount int
	}{
		{
			name:      "empty",
			wantCount: 0,
		},
		{
			name: "one_channel",
			setup: func(t *testing.T, b *awsconfig.InMemoryBackend) {
				t.Helper()
				require.NoError(
					t,
					b.PutDeliveryChannel("default", "my-bucket", "arn:aws:sns:us-east-1:000000000000:my-topic"),
				)
			},
			wantCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := awsconfig.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(t, b)
			}

			channels := b.DescribeDeliveryChannels()
			assert.Len(t, channels, tt.wantCount)
		})
	}
}

func TestAWSConfigBackend_DescribeConfigurationRecorders(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(t *testing.T, b *awsconfig.InMemoryBackend)
		name      string
		wantCount int
	}{
		{
			name:      "empty",
			wantCount: 0,
		},
		{
			name: "one_recorder",
			setup: func(t *testing.T, b *awsconfig.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.PutConfigurationRecorder("default", "arn:aws:iam::000000000000:role/config"))
			},
			wantCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := awsconfig.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(t, b)
			}

			recorders := b.DescribeConfigurationRecorders()
			assert.Len(t, recorders, tt.wantCount)
		})
	}
}

func TestAWSConfigBackend_DeleteAggregationAuthorization(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(t *testing.T, b *awsconfig.InMemoryBackend)
		name      string
		accountID string
		region    string
		wantErr   bool
	}{
		{
			name: "success",
			setup: func(t *testing.T, b *awsconfig.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.PutAggregationAuthorization("123456789012", "us-east-1"))
			},
			accountID: "123456789012",
			region:    "us-east-1",
		},
		{
			name:      "not_found",
			accountID: "999999999999",
			region:    "eu-west-1",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := awsconfig.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(t, b)
			}

			err := b.DeleteAggregationAuthorization(tt.accountID, tt.region)
			if tt.wantErr {
				require.Error(t, err)

				return
			}
			require.NoError(t, err)
		})
	}
}

func TestAWSConfigBackend_DeleteConfigRule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(t *testing.T, b *awsconfig.InMemoryBackend)
		name    string
		delName string
		wantErr bool
	}{
		{
			name: "success",
			setup: func(t *testing.T, b *awsconfig.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.PutConfigRule("my-rule"))
			},
			delName: "my-rule",
		},
		{
			name:    "not_found",
			delName: "nonexistent",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := awsconfig.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(t, b)
			}

			err := b.DeleteConfigRule(tt.delName)
			if tt.wantErr {
				require.Error(t, err)

				return
			}
			require.NoError(t, err)
		})
	}
}

func TestAWSConfigBackend_DeleteConfigurationAggregator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(t *testing.T, b *awsconfig.InMemoryBackend)
		name    string
		delName string
		wantErr bool
	}{
		{
			name: "success",
			setup: func(t *testing.T, b *awsconfig.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.PutConfigurationAggregator("agg1"))
			},
			delName: "agg1",
		},
		{
			name:    "not_found",
			delName: "nonexistent",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := awsconfig.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(t, b)
			}

			err := b.DeleteConfigurationAggregator(tt.delName)
			if tt.wantErr {
				require.Error(t, err)

				return
			}
			require.NoError(t, err)
		})
	}
}

func TestAWSConfigBackend_DeleteConformancePack(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(t *testing.T, b *awsconfig.InMemoryBackend)
		name    string
		delName string
		wantErr bool
	}{
		{
			name: "success",
			setup: func(t *testing.T, b *awsconfig.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.PutConformancePack("my-pack"))
			},
			delName: "my-pack",
		},
		{
			name:    "not_found",
			delName: "nonexistent",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := awsconfig.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(t, b)
			}

			err := b.DeleteConformancePack(tt.delName)
			if tt.wantErr {
				require.Error(t, err)

				return
			}
			require.NoError(t, err)
		})
	}
}

func TestAWSConfigBackend_DeleteEvaluationResults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		configRuleName string
	}{
		{
			name:           "success_always",
			configRuleName: "my-rule",
		},
		{
			name:           "nonexistent_rule_still_succeeds",
			configRuleName: "nonexistent",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := awsconfig.NewInMemoryBackend()
			require.NoError(t, b.DeleteEvaluationResults(tt.configRuleName))
		})
	}
}

func TestAWSConfigBackend_DeleteOrganizationConfigRule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(t *testing.T, b *awsconfig.InMemoryBackend)
		name    string
		delName string
		wantErr bool
	}{
		{
			name: "success",
			setup: func(t *testing.T, b *awsconfig.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.PutOrganizationConfigRule("org-rule"))
			},
			delName: "org-rule",
		},
		{
			name:    "not_found",
			delName: "nonexistent",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := awsconfig.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(t, b)
			}

			err := b.DeleteOrganizationConfigRule(tt.delName)
			if tt.wantErr {
				require.Error(t, err)

				return
			}
			require.NoError(t, err)
		})
	}
}

func TestAWSConfigBackend_DeleteOrganizationConformancePack(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(t *testing.T, b *awsconfig.InMemoryBackend)
		name    string
		delName string
		wantErr bool
	}{
		{
			name: "success",
			setup: func(t *testing.T, b *awsconfig.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.PutOrganizationConformancePack("org-pack"))
			},
			delName: "org-pack",
		},
		{
			name:    "not_found",
			delName: "nonexistent",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := awsconfig.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(t, b)
			}

			err := b.DeleteOrganizationConformancePack(tt.delName)
			if tt.wantErr {
				require.Error(t, err)

				return
			}
			require.NoError(t, err)
		})
	}
}

func TestAWSConfigBackend_AssociateResourceTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup         func(t *testing.T, b *awsconfig.InMemoryBackend)
		name          string
		recorderARN   string
		wantName      string
		resourceTypes []string
	}{
		{
			name: "known_recorder_by_role_arn",
			setup: func(t *testing.T, b *awsconfig.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.PutConfigurationRecorder("default", "arn:aws:iam::000000000000:role/config"))
			},
			recorderARN:   "arn:aws:iam::000000000000:role/config",
			resourceTypes: []string{"AWS::EC2::Instance"},
			wantName:      "default",
		},
		{
			name:          "unknown_arn_returns_stub",
			recorderARN:   "arn:aws:config:us-east-1:000000000000:config-recorder/unknown",
			resourceTypes: []string{"AWS::S3::Bucket"},
			wantName:      "arn:aws:config:us-east-1:000000000000:config-recorder/unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := awsconfig.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(t, b)
			}

			recorder, err := b.AssociateResourceTypes(tt.recorderARN, tt.resourceTypes)
			require.NoError(t, err)
			require.NotNil(t, recorder)
			assert.Equal(t, tt.wantName, recorder.Name)
		})
	}
}

func TestAWSConfigBackend_BatchGetAggregateResourceConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                 string
		aggregatorName       string
		identifiers          []awsconfig.AggregateResourceIdentifier
		wantItemCount        int
		wantUnprocessedCount int
	}{
		{
			name:           "returns_all_as_unprocessed",
			aggregatorName: "my-aggregator",
			identifiers: []awsconfig.AggregateResourceIdentifier{
				{
					SourceAccountID: "000000000000",
					SourceRegion:    "us-east-1",
					ResourceID:      "i-abc",
					ResourceType:    "AWS::EC2::Instance",
				},
			},
			wantItemCount:        0,
			wantUnprocessedCount: 1,
		},
		{
			name:                 "empty_identifiers",
			aggregatorName:       "my-aggregator",
			identifiers:          []awsconfig.AggregateResourceIdentifier{},
			wantItemCount:        0,
			wantUnprocessedCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := awsconfig.NewInMemoryBackend()
			items, unprocessed := b.BatchGetAggregateResourceConfig(tt.aggregatorName, tt.identifiers)
			assert.Len(t, items, tt.wantItemCount)
			assert.Len(t, unprocessed, tt.wantUnprocessedCount)
		})
	}
}

func TestAWSConfigBackend_BatchGetResourceConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                 string
		keys                 []awsconfig.ResourceKey
		wantItemCount        int
		wantUnprocessedCount int
	}{
		{
			name: "returns_all_as_unprocessed",
			keys: []awsconfig.ResourceKey{
				{ResourceType: "AWS::EC2::Instance", ResourceID: "i-abc"},
			},
			wantItemCount:        0,
			wantUnprocessedCount: 1,
		},
		{
			name:                 "empty_keys",
			keys:                 []awsconfig.ResourceKey{},
			wantItemCount:        0,
			wantUnprocessedCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := awsconfig.NewInMemoryBackend()
			items, unprocessed := b.BatchGetResourceConfig(tt.keys)
			assert.Len(t, items, tt.wantItemCount)
			assert.Len(t, unprocessed, tt.wantUnprocessedCount)
		})
	}
}
