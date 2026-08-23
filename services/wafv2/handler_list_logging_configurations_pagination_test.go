package wafv2_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	wafv2sdk "github.com/aws/aws-sdk-go-v2/service/wafv2"
	"github.com/aws/aws-sdk-go-v2/service/wafv2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/wafv2"
)

// TestListLoggingConfigurations_SDKRoundTrip_Pagination drives the real SDK client
// across two pages of ListLoggingConfigurations and asserts the pages are disjoint.
// Before the fix, handleListLoggingConfigurations never even parsed the request body
// (Scope, Limit, LogScope, NextMarker -- all real ListLoggingConfigurationsInput members,
// wafv2@v1.77.3 api_op_ListLoggingConfigurations.go), so it always returned every
// configuration in one unbounded page with no NextMarker.
func TestListLoggingConfigurations_SDKRoundTrip_Pagination(t *testing.T) {
	t.Parallel()

	backend := wafv2.NewInMemoryBackend("123456789012", "us-east-1")
	h := wafv2.NewHandler(backend)
	client := newTestWAFV2Client(t, h)

	const total = 25

	for i := range total {
		resourceARN := fmt.Sprintf("arn:aws:wafv2:us-east-1:123456789012:regional/webacl/wa-%02d/id-%02d", i, i)
		_, err := client.PutLoggingConfiguration(t.Context(), &wafv2sdk.PutLoggingConfigurationInput{
			LoggingConfiguration: &types.LoggingConfiguration{
				ResourceArn:           aws.String(resourceARN),
				LogDestinationConfigs: []string{"arn:aws:s3:::log-bucket"},
			},
		})
		require.NoError(t, err)
	}

	page1, err := client.ListLoggingConfigurations(t.Context(), &wafv2sdk.ListLoggingConfigurationsInput{
		Scope: types.ScopeRegional,
		Limit: aws.Int32(10),
	})
	require.NoError(t, err)
	require.Len(t, page1.LoggingConfigurations, 10)
	require.NotNil(t, page1.NextMarker)

	page2, err := client.ListLoggingConfigurations(t.Context(), &wafv2sdk.ListLoggingConfigurationsInput{
		Scope:      types.ScopeRegional,
		Limit:      aws.Int32(10),
		NextMarker: page1.NextMarker,
	})
	require.NoError(t, err)
	require.Len(t, page2.LoggingConfigurations, 10)

	seen := make(map[string]bool, 20)
	for _, cfg := range page1.LoggingConfigurations {
		seen[aws.ToString(cfg.ResourceArn)] = true
	}

	for _, cfg := range page2.LoggingConfigurations {
		assert.False(t, seen[aws.ToString(cfg.ResourceArn)],
			"page 2 repeated a config from page 1: %s", aws.ToString(cfg.ResourceArn))
		seen[aws.ToString(cfg.ResourceArn)] = true
	}

	assert.Len(t, seen, 20)
}
