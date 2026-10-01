package organizations_test

import (
	"testing"

	organizationssdk "github.com/aws/aws-sdk-go-v2/service/organizations"
	organizationstypes "github.com/aws/aws-sdk-go-v2/service/organizations/types"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRealClient_DescribeEffectivePolicy_TypeEnum(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		typ      organizationstypes.EffectivePolicyType
		wantCode string
	}{
		{name: "scp_not_an_effective_type", typ: "SERVICE_CONTROL_POLICY", wantCode: "InvalidInputException"},
		{name: "unknown_value", typ: "BOGUS_POLICY", wantCode: "InvalidInputException"},
		{
			name:     "valid_type_without_policy",
			typ:      organizationstypes.EffectivePolicyTypeS3Policy,
			wantCode: "EffectivePolicyNotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, org := newRealClient(t)

			_, err := client.DescribeEffectivePolicy(t.Context(), &organizationssdk.DescribeEffectivePolicyInput{
				PolicyType: tt.typ,
				TargetId:   org.Organization.MasterAccountId,
			})
			require.Error(t, err)

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, tt.wantCode, apiErr.ErrorCode())
		})
	}
}

func TestRealClient_ListInvalidEffectivePolicy_TypeEnum(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		typ      organizationstypes.EffectivePolicyType
		wantCode string
	}{
		{name: "scp_rejected", typ: "SERVICE_CONTROL_POLICY", wantCode: "InvalidInputException"},
		{name: "tag_policy_accepted", typ: organizationstypes.EffectivePolicyTypeTagPolicy},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, org := newRealClient(t)

			_, accErr := client.ListAccountsWithInvalidEffectivePolicy(
				t.Context(), &organizationssdk.ListAccountsWithInvalidEffectivePolicyInput{PolicyType: tt.typ},
			)
			_, valErr := client.ListEffectivePolicyValidationErrors(
				t.Context(), &organizationssdk.ListEffectivePolicyValidationErrorsInput{
					PolicyType: tt.typ, AccountId: org.Organization.MasterAccountId,
				},
			)

			if tt.wantCode == "" {
				require.NoError(t, accErr)
				require.NoError(t, valErr)

				return
			}

			for _, err := range []error{accErr, valErr} {
				var apiErr smithy.APIError
				require.ErrorAs(t, err, &apiErr)
				assert.Equal(t, tt.wantCode, apiErr.ErrorCode())
			}
		})
	}
}
