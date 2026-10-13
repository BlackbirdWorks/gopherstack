package opensearch_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	opensearchsdk "github.com/aws/aws-sdk-go-v2/service/opensearch"
	ostypes "github.com/aws/aws-sdk-go-v2/service/opensearch/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUpdateDomainConfig_ClusterConfigPartialUpdate_SDK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		update func() *ostypes.ClusterConfig
		check  func(t *testing.T, cc *ostypes.ClusterConfig)
		name   string
	}{
		{
			name:   "count only keeps type and master",
			update: func() *ostypes.ClusterConfig { return &ostypes.ClusterConfig{InstanceCount: aws.Int32(3)} },
			check: func(t *testing.T, cc *ostypes.ClusterConfig) {
				t.Helper()
				assert.Equal(t, int32(3), aws.ToInt32(cc.InstanceCount))
				assert.Equal(t, ostypes.OpenSearchPartitionInstanceTypeR5LargeSearch, cc.InstanceType)
				assert.True(t, aws.ToBool(cc.DedicatedMasterEnabled))
				assert.Equal(t, int32(3), aws.ToInt32(cc.DedicatedMasterCount))
			},
		},
		{
			name: "explicit false clears flag",
			update: func() *ostypes.ClusterConfig {
				return &ostypes.ClusterConfig{DedicatedMasterEnabled: aws.Bool(false)}
			},
			check: func(t *testing.T, cc *ostypes.ClusterConfig) {
				t.Helper()
				assert.False(t, aws.ToBool(cc.DedicatedMasterEnabled))
				assert.Equal(t, ostypes.OpenSearchPartitionInstanceTypeR5LargeSearch, cc.InstanceType)
				assert.Equal(t, int32(2), aws.ToInt32(cc.InstanceCount))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, client := newRealClient(t)
			_, err := client.CreateDomain(t.Context(), &opensearchsdk.CreateDomainInput{
				DomainName: aws.String("merge-dom"),
				ClusterConfig: &ostypes.ClusterConfig{
					InstanceType:           ostypes.OpenSearchPartitionInstanceTypeR5LargeSearch,
					InstanceCount:          aws.Int32(2),
					DedicatedMasterEnabled: aws.Bool(true),
					DedicatedMasterType:    ostypes.OpenSearchPartitionInstanceTypeR5LargeSearch,
					DedicatedMasterCount:   aws.Int32(3),
				},
			})
			require.NoError(t, err)

			out, err := client.UpdateDomainConfig(t.Context(), &opensearchsdk.UpdateDomainConfigInput{
				DomainName: aws.String("merge-dom"), ClusterConfig: tt.update(),
			})
			require.NoError(t, err)
			tt.check(t, out.DomainConfig.ClusterConfig.Options)

			desc, err := client.DescribeDomain(t.Context(), &opensearchsdk.DescribeDomainInput{
				DomainName: aws.String("merge-dom"),
			})
			require.NoError(t, err)
			tt.check(t, desc.DomainStatus.ClusterConfig)
		})
	}
}
