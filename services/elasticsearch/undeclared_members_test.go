package elasticsearch_test

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	elasticsearchsdk "github.com/aws/aws-sdk-go-v2/service/elasticsearchservice"
	"github.com/aws/aws-sdk-go-v2/service/elasticsearchservice/types"
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

			client := newTestElasticsearchClient(t, newTestHandler())

			out, err := client.CreateElasticsearchDomain(t.Context(), &elasticsearchsdk.CreateElasticsearchDomainInput{
				DomainName: aws.String("cold"),
				ElasticsearchClusterConfig: &types.ElasticsearchClusterConfig{
					InstanceType:       types.ESPartitionInstanceTypeR5LargeElasticsearch,
					WarmEnabled:        aws.Bool(true),
					WarmType:           types.ESWarmPartitionInstanceTypeUltrawarm1MediumElasticsearch,
					WarmCount:          aws.Int32(2),
					ColdStorageOptions: &types.ColdStorageOptions{Enabled: aws.Bool(tc.enabled)},
				},
			})
			require.NoError(t, err)

			cc := out.DomainStatus.ElasticsearchClusterConfig
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
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			doRequest(t, h, http.MethodPost, "/2015-01-01/es/domain",
				map[string]any{"DomainName": "raw-cold"}).Body.Close()

			resp := doRequest(t, h, http.MethodGet, "/2015-01-01/es/domain/raw-cold", nil)
			defer resp.Body.Close()

			buf := new(strings.Builder)
			_, err := io.Copy(buf, resp.Body)
			require.NoError(t, err)
			assert.Contains(t, buf.String(), "ElasticsearchClusterConfig")
			assert.NotContains(t, buf.String(), tc.absent)
		})
	}
}
