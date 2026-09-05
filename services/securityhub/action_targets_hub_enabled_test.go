package securityhub_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	securityhubsdk "github.com/aws/aws-sdk-go-v2/service/securityhub"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/securityhub"
)

// TestActionTargetOps_HubEnabledPrecondition guards gopherstack-02oa:
// UpdateActionTarget, DeleteActionTarget and DisableImportFindingsForProduct
// never checked b.hubEnabled, unlike every sibling create/enable path in the
// same service. securityhub@v1.75.4 deserializers.go models
// InvalidAccessException on all three paths
// (deserializeOpErrorUpdateActionTarget:16987, deserializeOpErrorDeleteActionTarget:4539,
// deserializeOpErrorDisableImportFindingsForProduct:7344), so real AWS enforces
// the hub-enabled precondition here too. The "after hub enabled" cases confirm
// the hubEnabled check didn't shadow the pre-existing not-found path once the
// hub is actually enabled.
func TestActionTargetOps_HubEnabledPrecondition(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call        func(*testing.T, *securityhubsdk.Client) error
		name        string
		wantErrCode string
		enableHub   bool
	}{
		{
			name: "UpdateActionTarget hub not enabled",
			call: func(t *testing.T, client *securityhubsdk.Client) error {
				t.Helper()

				_, err := client.UpdateActionTarget(t.Context(), &securityhubsdk.UpdateActionTargetInput{
					ActionTargetArn: aws.String("arn:aws:securityhub:us-east-1:000000000000:action/custom/x"),
					Name:            aws.String("NewName"),
				})

				return err
			},
			wantErrCode: "InvalidAccessException",
		},
		{
			name: "DeleteActionTarget hub not enabled",
			call: func(t *testing.T, client *securityhubsdk.Client) error {
				t.Helper()

				_, err := client.DeleteActionTarget(t.Context(), &securityhubsdk.DeleteActionTargetInput{
					ActionTargetArn: aws.String("arn:aws:securityhub:us-east-1:000000000000:action/custom/x"),
				})

				return err
			},
			wantErrCode: "InvalidAccessException",
		},
		{
			name: "DisableImportFindingsForProduct hub not enabled",
			call: func(t *testing.T, client *securityhubsdk.Client) error {
				t.Helper()

				_, err := client.DisableImportFindingsForProduct(
					t.Context(),
					&securityhubsdk.DisableImportFindingsForProductInput{
						ProductSubscriptionArn: aws.String(
							"arn:aws:securityhub:us-east-1:000000000000:product-subscription/x",
						),
					},
				)

				return err
			},
			wantErrCode: "InvalidAccessException",
		},
		{
			name:      "UpdateActionTarget not found after hub enabled",
			enableHub: true,
			call: func(t *testing.T, client *securityhubsdk.Client) error {
				t.Helper()

				_, err := client.UpdateActionTarget(t.Context(), &securityhubsdk.UpdateActionTargetInput{
					ActionTargetArn: aws.String("arn:aws:securityhub:us-east-1:000000000000:action/custom/missing"),
					Name:            aws.String("NewName"),
				})

				return err
			},
			wantErrCode: "ResourceNotFoundException",
		},
		{
			name:      "DisableImportFindingsForProduct not found after hub enabled",
			enableHub: true,
			call: func(t *testing.T, client *securityhubsdk.Client) error {
				t.Helper()

				_, err := client.DisableImportFindingsForProduct(
					t.Context(),
					&securityhubsdk.DisableImportFindingsForProductInput{
						ProductSubscriptionArn: aws.String(
							"arn:aws:securityhub:us-east-1:000000000000:product-subscription/missing",
						),
					},
				)

				return err
			},
			wantErrCode: "ResourceNotFoundException",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := securityhub.NewInMemoryBackend("000000000000", "us-east-1")
			if tc.enableHub {
				require.NoError(t, backend.EnableHub(false, nil))
			}

			client := newTestSecurityHubClient(t, securityhub.NewHandler(backend))

			err := tc.call(t, client)
			require.Error(t, err)

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
			assert.Equal(t, tc.wantErrCode, apiErr.ErrorCode())
		})
	}
}
