package organizations_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	organizationssdk "github.com/aws/aws-sdk-go-v2/service/organizations"
	organizationstypes "github.com/aws/aws-sdk-go-v2/service/organizations/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/organizations"
)

// TestSDKRoundTrip_ListEffectivePolicyValidationErrors_WireKey proves,
// through the real aws-sdk-go-v2 organizations client
// (organizations@v1.53.5), that ListEffectivePolicyValidationErrors'
// response decodes under the real wire key. Before the fix gopherstack
// wrapped the list under "ValidationErrors"; the real deserializer switches
// on "EffectivePolicyValidationErrors" (confirmed against
// ListEffectivePolicyValidationErrorsOutput in
// api_op_ListEffectivePolicyValidationErrors.go), so a real client's
// EffectivePolicyValidationErrors field stayed nil (its Go zero value,
// never decoded) rather than an initialized empty slice decoded from "[]".
// gopherstack's backend always returns zero validation errors (it has no
// policy-validation engine), so the signal this test proves is
// nil-because-never-decoded versus non-nil-because-decoded-under-the-right-
// key, not element count.
func TestSDKRoundTrip_ListEffectivePolicyValidationErrors_WireKey(t *testing.T) {
	t.Parallel()

	h := organizations.NewHandler(organizations.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestOrganizationsClient(t, h)

	_, err := client.CreateOrganization(t.Context(), &organizationssdk.CreateOrganizationInput{
		FeatureSet: organizationstypes.OrganizationFeatureSetAll,
	})
	require.NoError(t, err)

	acctOut, err := client.CreateAccount(t.Context(), &organizationssdk.CreateAccountInput{
		AccountName: aws.String("effective-policy-wire-test"),
		Email:       aws.String("effective-policy-wire-test@example.com"),
	})
	require.NoError(t, err)

	out, err := client.ListEffectivePolicyValidationErrors(
		t.Context(),
		&organizationssdk.ListEffectivePolicyValidationErrorsInput{
			AccountId:  acctOut.CreateAccountStatus.AccountId,
			PolicyType: organizationstypes.EffectivePolicyTypeTagPolicy,
		},
	)
	require.NoError(t, err)
	require.NotNil(t, out.EffectivePolicyValidationErrors,
		"ListEffectivePolicyValidationErrorsOutput.EffectivePolicyValidationErrors must "+
			"decode (non-nil) under the real wire key")
}
