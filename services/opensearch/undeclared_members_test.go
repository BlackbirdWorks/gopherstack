package opensearch_test

import (
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	opensearchsdk "github.com/aws/aws-sdk-go-v2/service/opensearch"
	"github.com/aws/aws-sdk-go-v2/service/opensearch/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSDK_ClusterConfigColdStorageOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		enabled bool
	}{
		{name: "enabled", enabled: true},
		{name: "disabled", enabled: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newTestOpenSearchClient(t, newTestHandler())

			out, err := client.CreateDomain(t.Context(), &opensearchsdk.CreateDomainInput{
				DomainName: aws.String("cold"),
				ClusterConfig: &types.ClusterConfig{
					InstanceType:       types.OpenSearchPartitionInstanceTypeR6gLargeSearch,
					WarmEnabled:        aws.Bool(true),
					WarmType:           types.OpenSearchWarmPartitionInstanceTypeUltrawarm1MediumSearch,
					WarmCount:          aws.Int32(2),
					ColdStorageOptions: &types.ColdStorageOptions{Enabled: aws.Bool(tc.enabled)},
				},
			})
			require.NoError(t, err)

			cc := out.DomainStatus.ClusterConfig
			require.NotNil(t, cc)
			require.NotNil(t, cc.ColdStorageOptions)
			assert.Equal(t, tc.enabled, aws.ToBool(cc.ColdStorageOptions.Enabled))
		})
	}
}

func TestHandler_ClusterConfigOmitsFlatColdStorage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		absent string
	}{
		{name: "flat_cold_storage", absent: "ColdStorageEnabled"},
		{name: "blue_green", absent: "BlueGreenDeploymentOptions"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			createTestDomain(t, h, "raw-cold")

			resp := doRequest(t, h, "GET", "/2021-01-01/opensearch/domain/raw-cold", nil)
			defer resp.Body.Close()

			buf := new(strings.Builder)
			_, err := io.Copy(buf, resp.Body)
			require.NoError(t, err)
			assert.NotContains(t, buf.String(), tc.absent)
		})
	}
}
