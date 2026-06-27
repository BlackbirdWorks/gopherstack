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
			err := b.PutConfigurationRecorder(tt.recName, tt.roleARN, nil)
			require.NoError(t, err)

			recorders := b.DescribeConfigurationRecorders(nil)
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
				require.NoError(t, b.PutConfigurationRecorder("default", "arn:aws:iam::000000000000:role/config", nil))
				require.NoError(t, b.PutDeliveryChannel("default", "my-bucket", "", "", nil))
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

			recorders := b.DescribeConfigurationRecorders(nil)
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
			err := b.PutDeliveryChannel(tt.chanName, tt.bucket, tt.topic, "", nil)
			require.NoError(t, err)

			channels := b.DescribeDeliveryChannels(nil)
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
					b.PutDeliveryChannel(
						"default",
						"my-bucket",
						"arn:aws:sns:us-east-1:000000000000:my-topic",
						"",
						nil,
					),
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

			channels := b.DescribeDeliveryChannels(nil)
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
				require.NoError(t, b.PutConfigurationRecorder("default", "arn:aws:iam::000000000000:role/config", nil))
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

			recorders := b.DescribeConfigurationRecorders(nil)
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
				require.NoError(t, b.PutConfigRule(&awsconfig.ConfigRule{ConfigRuleName: "my-rule"}))
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
				require.NoError(t, b.PutConfigurationAggregator("agg1", nil, nil))
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
				require.NoError(t, b.PutConformancePack("my-pack", "", ""))
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
			name: "known_recorder_by_name",
			setup: func(t *testing.T, b *awsconfig.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.PutConfigurationRecorder("default", "arn:aws:iam::000000000000:role/config", nil))
			},
			recorderARN:   "default",
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

func TestAWSConfigBackend_StopConfigurationRecorder(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, b *awsconfig.InMemoryBackend)
		name       string
		recName    string
		wantErr    error
		wantStatus string
	}{
		{
			name: "stops_active_recorder",
			setup: func(t *testing.T, b *awsconfig.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.PutConfigurationRecorder("default", "arn:aws:iam::123:role/r", nil))
				require.NoError(t, b.PutDeliveryChannel("default", "my-bucket", "", "", nil))
				require.NoError(t, b.StartConfigurationRecorder("default"))
			},
			recName:    "default",
			wantStatus: "PENDING",
		},
		{
			name:    "not_found",
			recName: "nonexistent",
			wantErr: awsconfig.ErrNotFound,
		},
		{
			name:    "empty_name_returns_validation_error",
			recName: "",
			wantErr: awsconfig.ErrValidation,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := awsconfig.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(t, b)
			}

			err := b.StopConfigurationRecorder(tt.recName)
			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)

			statuses := b.DescribeConfigurationRecorderStatus(nil)
			require.Len(t, statuses, 1)
			assert.False(t, statuses[0].Recording)
		})
	}
}

func TestAWSConfigBackend_PutConfigurationRecorder_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		name    string
		recName string
		roleARN string
	}{
		{
			name:    "empty_name_fails",
			recName: "",
			roleARN: "arn:aws:iam::000000000000:role/r",
			wantErr: awsconfig.ErrValidation,
		},
		{
			name:    "empty_roleARN_fails",
			recName: "default",
			roleARN: "",
			wantErr: awsconfig.ErrValidation,
		},
		{
			name:    "update_preserves_status",
			recName: "default",
			roleARN: "arn:aws:iam::000000000000:role/new",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := awsconfig.NewInMemoryBackend()
			if tt.name == "update_preserves_status" {
				require.NoError(t, b.PutConfigurationRecorder("default", "arn:aws:iam::000000000000:role/old", nil))
				require.NoError(t, b.PutDeliveryChannel("default", "bucket", "", "", nil))
				require.NoError(t, b.StartConfigurationRecorder("default"))
			}

			err := b.PutConfigurationRecorder(tt.recName, tt.roleARN, nil)
			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)

			if tt.name == "update_preserves_status" {
				recorders := b.DescribeConfigurationRecorders(nil)
				require.Len(t, recorders, 1)
				assert.Equal(t, "ACTIVE", recorders[0].Status)
				assert.Equal(t, tt.roleARN, recorders[0].RoleARN)
			}
		})
	}
}

func TestAWSConfigBackend_PutDeliveryChannel_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr  error
		name     string
		chanName string
		bucket   string
	}{
		{
			name:     "empty_name_fails",
			chanName: "",
			bucket:   "my-bucket",
			wantErr:  awsconfig.ErrValidation,
		},
		{
			name:     "empty_bucket_fails",
			chanName: "default",
			bucket:   "",
			wantErr:  awsconfig.ErrValidation,
		},
		{
			name:     "success",
			chanName: "default",
			bucket:   "my-bucket",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := awsconfig.NewInMemoryBackend()
			err := b.PutDeliveryChannel(tt.chanName, tt.bucket, "", "", nil)
			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestAWSConfigBackend_DescribeConfigRules_WithStoredRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(t *testing.T, b *awsconfig.InMemoryBackend)
		name      string
		filter    []string
		wantNames []string
	}{
		{
			name: "returns_all_sorted",
			setup: func(t *testing.T, b *awsconfig.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.PutConfigRule(&awsconfig.ConfigRule{ConfigRuleName: "rule-b"}))
				require.NoError(t, b.PutConfigRule(&awsconfig.ConfigRule{ConfigRuleName: "rule-a"}))
				require.NoError(t, b.PutConfigRule(&awsconfig.ConfigRule{ConfigRuleName: "rule-c"}))
			},
			wantNames: []string{"rule-a", "rule-b", "rule-c"},
		},
		{
			name: "filter_by_name",
			setup: func(t *testing.T, b *awsconfig.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.PutConfigRule(&awsconfig.ConfigRule{ConfigRuleName: "rule-a"}))
				require.NoError(t, b.PutConfigRule(&awsconfig.ConfigRule{ConfigRuleName: "rule-b"}))
			},
			filter:    []string{"rule-a"},
			wantNames: []string{"rule-a"},
		},
		{
			name:      "empty_backend",
			wantNames: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := awsconfig.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(t, b)
			}

			rules := b.DescribeConfigRules(tt.filter)
			names := make([]string, len(rules))
			for i, r := range rules {
				names[i] = r.ConfigRuleName
			}
			assert.Equal(t, tt.wantNames, names)
		})
	}
}

func TestAWSConfigBackend_DescribeConfigurationRecorders_NameFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		filter    []string
		wantCount int
	}{
		{
			name:      "no_filter_returns_all_sorted",
			wantCount: 3,
		},
		{
			name:      "filter_one",
			filter:    []string{"rec-a"},
			wantCount: 1,
		},
		{
			name:      "filter_nonexistent",
			filter:    []string{"no-such"},
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := awsconfig.NewInMemoryBackend()
			require.NoError(t, b.PutConfigurationRecorder("rec-c", "arn:aws:iam::123:role/r", nil))
			require.NoError(t, b.PutConfigurationRecorder("rec-a", "arn:aws:iam::123:role/r", nil))
			require.NoError(t, b.PutConfigurationRecorder("rec-b", "arn:aws:iam::123:role/r", nil))

			recs := b.DescribeConfigurationRecorders(tt.filter)
			assert.Len(t, recs, tt.wantCount)

			if tt.wantCount > 1 && len(tt.filter) == 0 {
				// verify sorted
				for i := 1; i < len(recs); i++ {
					assert.Less(t, recs[i-1].Name, recs[i].Name)
				}
			}
		})
	}
}

func TestAWSConfigBackend_DescribeAggregationAuthorizations(t *testing.T) {
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
			name: "multiple_sorted",
			setup: func(t *testing.T, b *awsconfig.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.PutAggregationAuthorization("222222222222", "us-west-2"))
				require.NoError(t, b.PutAggregationAuthorization("111111111111", "us-east-1"))
				require.NoError(t, b.PutAggregationAuthorization("111111111111", "eu-west-1"))
			},
			wantCount: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := awsconfig.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(t, b)
			}

			auths := b.DescribeAggregationAuthorizations()
			assert.Len(t, auths, tt.wantCount)

			for i := 1; i < len(auths); i++ {
				prev := auths[i-1].AuthorizedAccountID + "#" + auths[i-1].AuthorizedAwsRegion
				curr := auths[i].AuthorizedAccountID + "#" + auths[i].AuthorizedAwsRegion
				assert.LessOrEqual(t, prev, curr)
			}
		})
	}
}

func TestAWSConfigBackend_DeleteAggregationAuthorization_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr   error
		name      string
		accountID string
		region    string
	}{
		{
			name:      "empty_account_id_fails",
			accountID: "",
			region:    "us-east-1",
			wantErr:   awsconfig.ErrValidation,
		},
		{
			name:      "empty_region_fails",
			accountID: "123456789012",
			region:    "",
			wantErr:   awsconfig.ErrValidation,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := awsconfig.NewInMemoryBackend()
			err := b.DeleteAggregationAuthorization(tt.accountID, tt.region)
			require.Error(t, err)
			assert.ErrorIs(t, err, tt.wantErr)
		})
	}
}

func TestAWSConfigBackend_DeleteErrors_SpecificTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		setup   func(t *testing.T, b *awsconfig.InMemoryBackend)
		del     func(b *awsconfig.InMemoryBackend) error
		name    string
	}{
		{
			name:    "delete_delivery_channel_not_found",
			del:     func(b *awsconfig.InMemoryBackend) error { return b.DeleteDeliveryChannel("x") },
			wantErr: awsconfig.ErrNoSuchDeliveryChannel,
		},
		{
			name:    "delete_config_rule_not_found",
			del:     func(b *awsconfig.InMemoryBackend) error { return b.DeleteConfigRule("x") },
			wantErr: awsconfig.ErrNoSuchConfigRule,
		},
		{
			name:    "delete_aggregator_not_found",
			del:     func(b *awsconfig.InMemoryBackend) error { return b.DeleteConfigurationAggregator("x") },
			wantErr: awsconfig.ErrNoSuchAggregator,
		},
		{
			name:    "delete_conformance_pack_not_found",
			del:     func(b *awsconfig.InMemoryBackend) error { return b.DeleteConformancePack("x") },
			wantErr: awsconfig.ErrNoSuchConformancePack,
		},
		{
			name:    "delete_org_config_rule_not_found",
			del:     func(b *awsconfig.InMemoryBackend) error { return b.DeleteOrganizationConfigRule("x") },
			wantErr: awsconfig.ErrNoSuchOrganizationConfigRule,
		},
		{
			name:    "delete_org_conformance_pack_not_found",
			del:     func(b *awsconfig.InMemoryBackend) error { return b.DeleteOrganizationConformancePack("x") },
			wantErr: awsconfig.ErrNoSuchOrganizationConformancePack,
		},
		{
			name: "delete_aggregation_auth_not_found",
			del: func(b *awsconfig.InMemoryBackend) error {
				return b.DeleteAggregationAuthorization("123456789012", "us-east-1")
			},
			wantErr: awsconfig.ErrNoSuchAggregationAuthorization,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := awsconfig.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(t, b)
			}

			err := tt.del(b)
			require.Error(t, err)
			assert.ErrorIs(t, err, tt.wantErr)
		})
	}
}

func TestAWSConfigBackend_AssociateResourceTypes_EmptyARN(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()
	_, err := b.AssociateResourceTypes("", nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, awsconfig.ErrValidation)
}
