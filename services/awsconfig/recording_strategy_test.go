package awsconfig_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	configservicesdk "github.com/aws/aws-sdk-go-v2/service/configservice"
	"github.com/aws/aws-sdk-go-v2/service/configservice/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func strategy(u types.RecordingStrategyType) *types.RecordingStrategy {
	return &types.RecordingStrategy{UseOnly: u}
}

func TestRealClient_RecordingGroupStrategy(t *testing.T) {
	t.Parallel()

	exclude := &types.ExclusionByResourceTypes{ResourceTypes: []types.ResourceType{types.ResourceTypeInstance}}

	tests := []struct {
		group   *types.RecordingGroup
		name    string
		invalid bool
	}{
		{
			name: "exclusion_ok",
			group: &types.RecordingGroup{
				ExclusionByResourceTypes: exclude,
				RecordingStrategy:        strategy(types.RecordingStrategyTypeExclusionByResourceTypes),
			},
		},
		{
			name:    "exclusion_without_strategy",
			group:   &types.RecordingGroup{ExclusionByResourceTypes: exclude},
			invalid: true,
		},
		{
			name: "exclusion_with_inclusion_strategy",
			group: &types.RecordingGroup{
				ExclusionByResourceTypes: exclude,
				RecordingStrategy:        strategy(types.RecordingStrategyTypeInclusionByResourceTypes),
			},
			invalid: true,
		},
		{
			name: "exclusion_with_all_supported",
			group: &types.RecordingGroup{
				AllSupported:             true,
				ExclusionByResourceTypes: exclude,
				RecordingStrategy:        strategy(types.RecordingStrategyTypeExclusionByResourceTypes),
			},
			invalid: true,
		},
		{
			name: "all_supported_strategy_without_all_supported",
			group: &types.RecordingGroup{
				RecordingStrategy: strategy(types.RecordingStrategyTypeAllSupportedResourceTypes),
			},
			invalid: true,
		},
		{
			name:    "empty_group",
			group:   &types.RecordingGroup{},
			invalid: true,
		},
		{
			name: "all_supported_with_exclusion_strategy",
			group: &types.RecordingGroup{
				AllSupported:      true,
				RecordingStrategy: strategy(types.RecordingStrategyTypeExclusionByResourceTypes),
			},
			invalid: true,
		},
		{
			name:    "unknown_resource_type",
			group:   &types.RecordingGroup{ResourceTypes: []types.ResourceType{"AWS::Nope::Thing"}},
			invalid: true,
		},
		{
			name: "unknown_excluded_resource_type",
			group: &types.RecordingGroup{
				ExclusionByResourceTypes: &types.ExclusionByResourceTypes{ResourceTypes: []types.ResourceType{"Nope"}},
				RecordingStrategy:        strategy(types.RecordingStrategyTypeExclusionByResourceTypes),
			},
			invalid: true,
		},
		{
			name: "unknown_strategy",
			group: &types.RecordingGroup{
				ResourceTypes:     []types.ResourceType{types.ResourceTypeInstance},
				RecordingStrategy: strategy("BOGUS"),
			},
			invalid: true,
		},
		{
			name:  "inclusion_ok",
			group: &types.RecordingGroup{ResourceTypes: []types.ResourceType{types.ResourceTypeInstance}},
		},
		{
			name: "all_supported_strategy_ok",
			group: &types.RecordingGroup{
				AllSupported:      true,
				RecordingStrategy: strategy(types.RecordingStrategyTypeAllSupportedResourceTypes),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, client := newOpenItemsClient(t)
			_, err := client.PutConfigurationRecorder(t.Context(), &configservicesdk.PutConfigurationRecorderInput{
				ConfigurationRecorder: &types.ConfigurationRecorder{
					Name:           aws.String("rec"),
					RoleARN:        aws.String("arn:aws:iam::000000000000:role/r"),
					RecordingGroup: tt.group,
				},
			})

			if tt.invalid {
				var want *types.InvalidRecordingGroupException
				require.ErrorAs(t, err, &want)

				return
			}

			require.NoError(t, err)

			got, err := client.DescribeConfigurationRecorders(
				t.Context(), &configservicesdk.DescribeConfigurationRecordersInput{},
			)
			require.NoError(t, err)
			require.Len(t, got.ConfigurationRecorders, 1)
			assert.Equal(t, tt.group.RecordingStrategy, got.ConfigurationRecorders[0].RecordingGroup.RecordingStrategy)
			assert.Equal(
				t, tt.group.ExclusionByResourceTypes,
				got.ConfigurationRecorders[0].RecordingGroup.ExclusionByResourceTypes,
			)
		})
	}
}
