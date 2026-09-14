package outposts_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	outpostssdk "github.com/aws/aws-sdk-go-v2/service/outposts"
	"github.com/aws/aws-sdk-go-v2/service/outposts/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestUpdateSiteAddress_RequiredEmptyString_RealClient drives
// UpdateSiteAddress then GetSiteAddress through the real SDK client with
// AddressLine1 set to a legitimate empty string. types.Address.AddressLine1
// is "This member is required" (outposts@v1.66.1 types/types.go:11-16),
// but *string on the wire, so the client-side required check
// (validators.go:877-899) only rejects nil, not an empty string. The
// pre-fix wire struct tagged all seven required Address fields
// `omitempty`, silently dropping them instead of echoing the empty value --
// gopherstack-mven required-output nested-domain-struct sweep.
func TestUpdateSiteAddress_RequiredEmptyString_RealClient(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	siteID := createTestSite(t, client)
	ctx := t.Context()

	_, err := client.UpdateSiteAddress(ctx, &outpostssdk.UpdateSiteAddressInput{
		SiteId:      aws.String(siteID),
		AddressType: types.AddressTypeShippingAddress,
		Address: &types.Address{
			AddressLine1: aws.String(""), City: aws.String("NewCity"),
			ContactName: aws.String("C"), ContactPhoneNumber: aws.String("+12065550102"),
			CountryCode: aws.String("US"), PostalCode: aws.String("33333"), StateOrRegion: aws.String("WA"),
		},
	})
	require.NoError(t, err)

	got, err := client.GetSiteAddress(ctx, &outpostssdk.GetSiteAddressInput{
		SiteId:      aws.String(siteID),
		AddressType: types.AddressTypeShippingAddress,
	})
	require.NoError(t, err)
	require.NotNil(t, got.Address)
	require.NotNil(t, got.Address.AddressLine1,
		"AddressLine1 is required and must round-trip even when empty")
	assert.Empty(t, aws.ToString(got.Address.AddressLine1))
}

// Note: InstanceTypeCapacity.Count/InstanceType's own omitempty removal
// (wire.go) has no dedicated real-client test -- this backend's own
// StartCapacityTask validation ("InstancePools[].Count must be positive",
// "InstancePools[].InstanceType is required", capacity_tasks.go:17-21)
// already rejects the zero/empty values that would trigger it, so the fix
// is defensive wire-accuracy rather than a currently reachable bug.
