package outposts_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	outpostssdk "github.com/aws/aws-sdk-go-v2/service/outposts"
	"github.com/aws/aws-sdk-go-v2/service/outposts/types"
	"github.com/stretchr/testify/require"
)

func TestCreateSite(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)

	out, err := client.CreateSite(t.Context(), &outpostssdk.CreateSiteInput{
		Name:        aws.String("my-site"),
		Description: aws.String("a test site"),
		OperatingAddress: &types.Address{
			AddressLine1:       aws.String("123 Main St"),
			City:               aws.String("Seattle"),
			ContactName:        aws.String("Jane Doe"),
			ContactPhoneNumber: aws.String("+12065550100"),
			CountryCode:        aws.String("US"),
			PostalCode:         aws.String("98101"),
			StateOrRegion:      aws.String("WA"),
		},
		ShippingAddress: &types.Address{
			AddressLine1:       aws.String("456 Oak Ave"),
			City:               aws.String("Tacoma"),
			ContactName:        aws.String("Jane Doe"),
			ContactPhoneNumber: aws.String("+12065550100"),
			CountryCode:        aws.String("US"),
			PostalCode:         aws.String("98402"),
			StateOrRegion:      aws.String("WA"),
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(out.Site.SiteId))
	require.Equal(t, "Seattle", aws.ToString(out.Site.OperatingAddressCity))
	require.Equal(t, "US", aws.ToString(out.Site.OperatingAddressCountryCode))
}

func TestGetSiteAddress_ShippingVsOperating(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)

	created, err := client.CreateSite(t.Context(), &outpostssdk.CreateSiteInput{
		Name: aws.String("my-site"),
		OperatingAddress: &types.Address{
			AddressLine1: aws.String("Operating Line 1"), City: aws.String("OpCity"),
			ContactName: aws.String("A"), ContactPhoneNumber: aws.String("+12065550100"),
			CountryCode: aws.String("US"), PostalCode: aws.String("11111"), StateOrRegion: aws.String("WA"),
		},
		ShippingAddress: &types.Address{
			AddressLine1: aws.String("Shipping Line 1"), City: aws.String("ShipCity"),
			ContactName: aws.String("B"), ContactPhoneNumber: aws.String("+12065550101"),
			CountryCode: aws.String("US"), PostalCode: aws.String("22222"), StateOrRegion: aws.String("WA"),
		},
	})
	require.NoError(t, err)

	op, err := client.GetSiteAddress(t.Context(), &outpostssdk.GetSiteAddressInput{
		SiteId:      created.Site.SiteId,
		AddressType: types.AddressTypeOperatingAddress,
	})
	require.NoError(t, err)
	require.Equal(t, "OpCity", aws.ToString(op.Address.City))

	ship, err := client.GetSiteAddress(t.Context(), &outpostssdk.GetSiteAddressInput{
		SiteId:      created.Site.SiteId,
		AddressType: types.AddressTypeShippingAddress,
	})
	require.NoError(t, err)
	require.Equal(t, "ShipCity", aws.ToString(ship.Address.City))
}

func TestUpdateSiteAddress_FullReplacement(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	siteID := createTestSite(t, client)

	updated, err := client.UpdateSiteAddress(t.Context(), &outpostssdk.UpdateSiteAddressInput{
		SiteId:      aws.String(siteID),
		AddressType: types.AddressTypeShippingAddress,
		Address: &types.Address{
			AddressLine1: aws.String("New Address"), City: aws.String("NewCity"),
			ContactName: aws.String("C"), ContactPhoneNumber: aws.String("+12065550102"),
			CountryCode: aws.String("US"), PostalCode: aws.String("33333"), StateOrRegion: aws.String("WA"),
		},
	})
	require.NoError(t, err)
	require.Equal(t, "NewCity", aws.ToString(updated.Address.City))

	got, err := client.GetSiteAddress(t.Context(), &outpostssdk.GetSiteAddressInput{
		SiteId:      aws.String(siteID),
		AddressType: types.AddressTypeShippingAddress,
	})
	require.NoError(t, err)
	require.Equal(t, "NewCity", aws.ToString(got.Address.City))
}

func TestUpdateSiteRackPhysicalProperties_MergesFields(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	siteID := createTestSite(t, client)

	first, err := client.UpdateSiteRackPhysicalProperties(
		t.Context(),
		&outpostssdk.UpdateSiteRackPhysicalPropertiesInput{
			SiteId:         aws.String(siteID),
			PowerConnector: types.PowerConnectorL630p,
		},
	)
	require.NoError(t, err)
	require.Equal(t, types.PowerConnectorL630p, first.Site.RackPhysicalProperties.PowerConnector)

	second, err := client.UpdateSiteRackPhysicalProperties(
		t.Context(),
		&outpostssdk.UpdateSiteRackPhysicalPropertiesInput{
			SiteId:     aws.String(siteID),
			UplinkGbps: types.UplinkGbpsUplink10g,
		},
	)
	require.NoError(t, err)
	require.Equal(t, types.PowerConnectorL630p, second.Site.RackPhysicalProperties.PowerConnector,
		"a merge must not clobber a previously-set field")
	require.Equal(t, types.UplinkGbpsUplink10g, second.Site.RackPhysicalProperties.UplinkGbps)
}

func TestDeleteSite_RejectedWhileOutpostExists(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	siteID := createTestSite(t, client)
	createTestOutpost(t, client, siteID)

	_, err := client.DeleteSite(t.Context(), &outpostssdk.DeleteSiteInput{SiteId: aws.String(siteID)})
	require.Error(t, err)

	var ce *types.ConflictException
	require.ErrorAs(t, err, &ce)
}

func TestListSites_FiltersByCountryCode(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)

	_, err := client.CreateSite(t.Context(), &outpostssdk.CreateSiteInput{
		Name: aws.String("us-site"),
		OperatingAddress: &types.Address{
			AddressLine1: aws.String("L1"), City: aws.String("City"),
			ContactName: aws.String("A"), ContactPhoneNumber: aws.String("+12065550100"),
			CountryCode: aws.String("US"), PostalCode: aws.String("11111"), StateOrRegion: aws.String("WA"),
		},
	})
	require.NoError(t, err)

	out, err := client.ListSites(t.Context(), &outpostssdk.ListSitesInput{
		OperatingAddressCountryCodeFilter: []string{"US"},
	})
	require.NoError(t, err)
	require.NotEmpty(t, out.Sites)

	out, err = client.ListSites(t.Context(), &outpostssdk.ListSitesInput{
		OperatingAddressCountryCodeFilter: []string{"FR"},
	})
	require.NoError(t, err)
	require.Empty(t, out.Sites)
}
