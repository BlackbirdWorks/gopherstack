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

// TestUpdateActionTarget_HubNotEnabled, TestDeleteActionTarget_HubNotEnabled
// and TestDisableImportFindingsForProduct_HubNotEnabled guard
// gopherstack-02oa: these three ops never checked b.hubEnabled, unlike every
// sibling create/enable path in the same service. securityhub@v1.75.4
// deserializers.go models InvalidAccessException on all three paths
// (deserializeOpErrorUpdateActionTarget:16987, deserializeOpErrorDeleteActionTarget:4539,
// deserializeOpErrorDisableImportFindingsForProduct:7344), so real AWS enforces
// the hub-enabled precondition here too.
func TestUpdateActionTarget_HubNotEnabled(t *testing.T) {
	t.Parallel()

	backend := securityhub.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestSecurityHubClient(t, securityhub.NewHandler(backend))

	_, err := client.UpdateActionTarget(t.Context(), &securityhubsdk.UpdateActionTargetInput{
		ActionTargetArn: aws.String("arn:aws:securityhub:us-east-1:000000000000:action/custom/x"),
		Name:            aws.String("NewName"),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "InvalidAccessException", apiErr.ErrorCode())
}

func TestDeleteActionTarget_HubNotEnabled(t *testing.T) {
	t.Parallel()

	backend := securityhub.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestSecurityHubClient(t, securityhub.NewHandler(backend))

	_, err := client.DeleteActionTarget(t.Context(), &securityhubsdk.DeleteActionTargetInput{
		ActionTargetArn: aws.String("arn:aws:securityhub:us-east-1:000000000000:action/custom/x"),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "InvalidAccessException", apiErr.ErrorCode())
}

func TestDisableImportFindingsForProduct_HubNotEnabled(t *testing.T) {
	t.Parallel()

	backend := securityhub.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestSecurityHubClient(t, securityhub.NewHandler(backend))

	_, err := client.DisableImportFindingsForProduct(
		t.Context(),
		&securityhubsdk.DisableImportFindingsForProductInput{
			ProductSubscriptionArn: aws.String(
				"arn:aws:securityhub:us-east-1:000000000000:product-subscription/x",
			),
		},
	)
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "InvalidAccessException", apiErr.ErrorCode())
}

// TestUpdateActionTarget_NotFoundAfterHubEnabled and its DisableImportFindingsForProduct
// sibling confirm the hubEnabled check didn't shadow the pre-existing not-found
// path once the hub is actually enabled.
func TestUpdateActionTarget_NotFoundAfterHubEnabled(t *testing.T) {
	t.Parallel()

	backend := securityhub.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, backend.EnableHub(false, nil))
	client := newTestSecurityHubClient(t, securityhub.NewHandler(backend))

	_, err := client.UpdateActionTarget(t.Context(), &securityhubsdk.UpdateActionTargetInput{
		ActionTargetArn: aws.String("arn:aws:securityhub:us-east-1:000000000000:action/custom/missing"),
		Name:            aws.String("NewName"),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "ResourceNotFoundException", apiErr.ErrorCode())
}

func TestDisableImportFindingsForProduct_NotFoundAfterHubEnabled(t *testing.T) {
	t.Parallel()

	backend := securityhub.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, backend.EnableHub(false, nil))
	client := newTestSecurityHubClient(t, securityhub.NewHandler(backend))

	_, err := client.DisableImportFindingsForProduct(
		t.Context(),
		&securityhubsdk.DisableImportFindingsForProductInput{
			ProductSubscriptionArn: aws.String(
				"arn:aws:securityhub:us-east-1:000000000000:product-subscription/missing",
			),
		},
	)
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "ResourceNotFoundException", apiErr.ErrorCode())
}
