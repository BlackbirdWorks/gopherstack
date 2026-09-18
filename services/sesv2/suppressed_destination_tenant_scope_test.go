package sesv2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sesv2sdk "github.com/aws/aws-sdk-go-v2/service/sesv2"
	sesv2types "github.com/aws/aws-sdk-go-v2/service/sesv2/types"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sesv2"
)

// PutSuppressedDestinationInput/GetSuppressedDestinationInput/
// DeleteSuppressedDestinationInput/ListSuppressedDestinationsInput each
// declare a TenantName member (sesv2@v1.66.4: "To target a tenant's
// suppression list, specify the TenantName parameter. If you omit
// TenantName, the operation targets the account-level suppression list.")
// that was parsed nowhere -- every call operated on one single, shared
// suppression list regardless of TenantName, so a tenant-scoped Put could be
// read back (and deleted) through the account-level list and vice versa.
func TestSuppressedDestination_TenantNameScopesIndependentLists(t *testing.T) {
	t.Parallel()

	backend := sesv2.NewInMemoryBackend()
	client := newSESv2SDKClient(t, sesv2.NewHandler(backend))
	ctx := t.Context()

	const email = "shared@example.com"

	_, putErr := client.PutSuppressedDestination(ctx, &sesv2sdk.PutSuppressedDestinationInput{
		EmailAddress: aws.String(email),
		Reason:       sesv2types.SuppressionListReasonBounce,
	})
	require.NoError(t, putErr)

	_, putErr = client.PutSuppressedDestination(ctx, &sesv2sdk.PutSuppressedDestinationInput{
		EmailAddress: aws.String(email),
		Reason:       sesv2types.SuppressionListReasonComplaint,
		TenantName:   aws.String("tenant-a"),
	})
	require.NoError(t, putErr)

	t.Run("get without TenantName returns the account-level reason", func(t *testing.T) {
		t.Parallel()

		out, err := client.GetSuppressedDestination(ctx, &sesv2sdk.GetSuppressedDestinationInput{
			EmailAddress: aws.String(email),
		})
		require.NoError(t, err)
		assert.Equal(t, sesv2types.SuppressionListReasonBounce, out.SuppressedDestination.Reason)
	})

	t.Run("get with TenantName returns the tenant's own reason", func(t *testing.T) {
		t.Parallel()

		out, err := client.GetSuppressedDestination(ctx, &sesv2sdk.GetSuppressedDestinationInput{
			EmailAddress: aws.String(email),
			TenantName:   aws.String("tenant-a"),
		})
		require.NoError(t, err)
		assert.Equal(t, sesv2types.SuppressionListReasonComplaint, out.SuppressedDestination.Reason)
	})

	t.Run("get with an unrelated tenant is not found", func(t *testing.T) {
		t.Parallel()

		_, err := client.GetSuppressedDestination(ctx, &sesv2sdk.GetSuppressedDestinationInput{
			EmailAddress: aws.String(email),
			TenantName:   aws.String("tenant-b"),
		})
		require.Error(t, err)

		var apiErr smithy.APIError
		require.ErrorAs(t, err, &apiErr)
		assert.Equal(t, "NotFoundException", apiErr.ErrorCode())
	})

	t.Run("list scopes by TenantName", func(t *testing.T) {
		t.Parallel()

		accountList, err := client.ListSuppressedDestinations(ctx, &sesv2sdk.ListSuppressedDestinationsInput{})
		require.NoError(t, err)
		require.Len(t, accountList.SuppressedDestinationSummaries, 1)
		assert.Equal(t, sesv2types.SuppressionListReasonBounce,
			accountList.SuppressedDestinationSummaries[0].Reason)

		tenantList, err := client.ListSuppressedDestinations(ctx, &sesv2sdk.ListSuppressedDestinationsInput{
			TenantName: aws.String("tenant-a"),
		})
		require.NoError(t, err)
		require.Len(t, tenantList.SuppressedDestinationSummaries, 1)
		assert.Equal(t, sesv2types.SuppressionListReasonComplaint,
			tenantList.SuppressedDestinationSummaries[0].Reason)
	})

	t.Run("delete with the wrong scope leaves both lists intact", func(t *testing.T) {
		t.Parallel()

		_, err := client.DeleteSuppressedDestination(ctx, &sesv2sdk.DeleteSuppressedDestinationInput{
			EmailAddress: aws.String(email),
			TenantName:   aws.String("tenant-b"),
		})
		require.Error(t, err)

		_, getErr := client.GetSuppressedDestination(ctx, &sesv2sdk.GetSuppressedDestinationInput{
			EmailAddress: aws.String(email),
		})
		require.NoError(t, getErr, "account-level entry must survive a delete under an unrelated tenant scope")

		_, getErr = client.GetSuppressedDestination(ctx, &sesv2sdk.GetSuppressedDestinationInput{
			EmailAddress: aws.String(email),
			TenantName:   aws.String("tenant-a"),
		})
		require.NoError(t, getErr, "tenant-a's entry must survive a delete under an unrelated tenant scope")
	})
}
