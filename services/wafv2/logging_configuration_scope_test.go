package wafv2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	wafv2sdk "github.com/aws/aws-sdk-go-v2/service/wafv2"
	"github.com/aws/aws-sdk-go-v2/service/wafv2/types"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/wafv2"
)

// GetLoggingConfigurationInput and DeleteLoggingConfigurationInput both declare a
// LogScope member (wafv2@v1.77.3 api_op_{Get,Delete}LoggingConfiguration.go: "Default:
// CUSTOMER") that the handlers parsed nowhere -- a request for a non-CUSTOMER scope
// always resolved the CUSTOMER-scoped config instead of reporting it not-found.
func TestLoggingConfiguration_LogScopeSelectsDistinctConfig(t *testing.T) {
	t.Parallel()

	backend := wafv2.NewInMemoryBackend("123456789012", "us-east-1")
	client := newTestWAFV2Client(t, wafv2.NewHandler(backend))
	ctx := t.Context()

	const resourceARN = "arn:aws:wafv2:us-east-1:123456789012:regional/webacl/scope-wa/id-1"

	_, putErr := client.PutLoggingConfiguration(ctx, &wafv2sdk.PutLoggingConfigurationInput{
		LoggingConfiguration: &types.LoggingConfiguration{
			ResourceArn:           aws.String(resourceARN),
			LogDestinationConfigs: []string{"arn:aws:s3:::log-bucket"},
			LogScope:              types.LogScopeCustomer,
		},
	})
	require.NoError(t, putErr)

	t.Run("get with mismatched scope is not found", func(t *testing.T) {
		t.Parallel()

		_, err := client.GetLoggingConfiguration(ctx, &wafv2sdk.GetLoggingConfigurationInput{
			ResourceArn: aws.String(resourceARN),
			LogScope:    types.LogScopeSecurityLake,
		})
		require.Error(t, err)

		var apiErr smithy.APIError
		require.ErrorAs(t, err, &apiErr)
		assert.Equal(t, "WAFNonexistentItemException", apiErr.ErrorCode())
	})

	t.Run("get with matching default scope succeeds", func(t *testing.T) {
		t.Parallel()

		out, err := client.GetLoggingConfiguration(ctx, &wafv2sdk.GetLoggingConfigurationInput{
			ResourceArn: aws.String(resourceARN),
		})
		require.NoError(t, err)
		assert.Equal(t, resourceARN, aws.ToString(out.LoggingConfiguration.ResourceArn))
	})

	t.Run("delete with mismatched scope is not found and leaves the config intact", func(t *testing.T) {
		t.Parallel()

		_, err := client.DeleteLoggingConfiguration(ctx, &wafv2sdk.DeleteLoggingConfigurationInput{
			ResourceArn: aws.String(resourceARN),
			LogScope:    types.LogScopeCloudwatchTelemetryRuleManaged,
		})
		require.Error(t, err)

		var apiErr smithy.APIError
		require.ErrorAs(t, err, &apiErr)
		assert.Equal(t, "WAFNonexistentItemException", apiErr.ErrorCode())

		_, getErr := client.GetLoggingConfiguration(ctx, &wafv2sdk.GetLoggingConfigurationInput{
			ResourceArn: aws.String(resourceARN),
		})
		require.NoError(t, getErr, "the CUSTOMER-scoped config must survive a delete under a different scope")
	})
}

// LogType (currently a single documented value, WAF_LOGS) was accepted nowhere on
// GetLoggingConfiguration/DeleteLoggingConfiguration -- an unrecognized value was
// silently ignored instead of rejected.
func TestLoggingConfiguration_InvalidLogTypeRejected(t *testing.T) {
	t.Parallel()

	backend := wafv2.NewInMemoryBackend("123456789012", "us-east-1")
	client := newTestWAFV2Client(t, wafv2.NewHandler(backend))
	ctx := t.Context()

	const resourceARN = "arn:aws:wafv2:us-east-1:123456789012:regional/webacl/logtype-wa/id-1"

	_, putErr := client.PutLoggingConfiguration(ctx, &wafv2sdk.PutLoggingConfigurationInput{
		LoggingConfiguration: &types.LoggingConfiguration{
			ResourceArn:           aws.String(resourceARN),
			LogDestinationConfigs: []string{"arn:aws:s3:::log-bucket"},
		},
	})
	require.NoError(t, putErr)

	tests := []struct {
		call func() error
		name string
	}{
		{
			name: "get",
			call: func() error {
				_, err := client.GetLoggingConfiguration(ctx, &wafv2sdk.GetLoggingConfigurationInput{
					ResourceArn: aws.String(resourceARN),
					LogType:     "BOGUS_LOGS",
				})

				return err
			},
		},
		{
			name: "delete",
			call: func() error {
				_, err := client.DeleteLoggingConfiguration(ctx, &wafv2sdk.DeleteLoggingConfigurationInput{
					ResourceArn: aws.String(resourceARN),
					LogType:     "BOGUS_LOGS",
				})

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.call()
			require.Error(t, err)

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, "WAFInvalidParameterException", apiErr.ErrorCode())
		})
	}
}
