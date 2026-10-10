package wafv2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	wafv2sdk "github.com/aws/aws-sdk-go-v2/service/wafv2"
	"github.com/aws/aws-sdk-go-v2/service/wafv2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/wafv2"
)

// TestListLoggingConfigurations_LogScopeDefault pins "Default: CUSTOMER":
// an unset LogScope lists only customer-managed configurations.
func TestListLoggingConfigurations_LogScopeDefault(t *testing.T) {
	t.Parallel()

	const (
		customerARN = "arn:aws:wafv2:us-east-1:123456789012:regional/webacl/customer-wa/id-1"
		seclakeARN  = "arn:aws:wafv2:us-east-1:123456789012:regional/webacl/seclake-wa/id-2"
	)

	tests := []struct {
		name     string
		logScope types.LogScope
		want     []string
	}{
		{name: "unset_defaults_to_customer", want: []string{customerARN}},
		{name: "customer", logScope: types.LogScopeCustomer, want: []string{customerARN}},
		{name: "security_lake", logScope: types.LogScopeSecurityLake, want: []string{seclakeARN}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newTestWAFV2Client(t, wafv2.NewHandler(wafv2.NewInMemoryBackend("123456789012", "us-east-1")))

			for arn, scope := range map[string]types.LogScope{
				customerARN: types.LogScopeCustomer,
				seclakeARN:  types.LogScopeSecurityLake,
			} {
				_, err := client.PutLoggingConfiguration(t.Context(), &wafv2sdk.PutLoggingConfigurationInput{
					LoggingConfiguration: &types.LoggingConfiguration{
						ResourceArn:           aws.String(arn),
						LogDestinationConfigs: []string{"arn:aws:s3:::aws-waf-logs-bucket"},
						LogScope:              scope,
					},
				})
				require.NoError(t, err)
			}

			out, err := client.ListLoggingConfigurations(t.Context(), &wafv2sdk.ListLoggingConfigurationsInput{
				Scope: types.ScopeRegional, LogScope: tc.logScope,
			})
			require.NoError(t, err)

			got := make([]string, 0, len(out.LoggingConfigurations))
			for _, c := range out.LoggingConfigurations {
				got = append(got, aws.ToString(c.ResourceArn))
			}

			assert.ElementsMatch(t, tc.want, got)
		})
	}
}
