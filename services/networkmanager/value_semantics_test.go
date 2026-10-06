package networkmanager_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	networkmanagersdk "github.com/aws/aws-sdk-go-v2/service/networkmanager"
	"github.com/aws/aws-sdk-go-v2/service/networkmanager/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUpdate_OmittedKeepsEmptyStringClears(t *testing.T) {
	t.Parallel()

	cases := []struct {
		update   *string
		name     string
		wantDesc string
	}{
		{name: "omitted keeps", update: nil, wantDesc: "d"},
		{name: "empty string clears", update: aws.String(""), wantDesc: ""},
		{name: "value replaces", update: aws.String("n"), wantDesc: "n"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, c := newTestHandlerAndClient(t)
			ctx := t.Context()

			gn, err := c.CreateGlobalNetwork(
				ctx, &networkmanagersdk.CreateGlobalNetworkInput{Description: aws.String("d")},
			)
			require.NoError(t, err)

			gnID := gn.GlobalNetwork.GlobalNetworkId

			site, err := c.CreateSite(ctx, &networkmanagersdk.CreateSiteInput{
				GlobalNetworkId: gnID, Description: aws.String("d"),
			})
			require.NoError(t, err)

			dev, err := c.CreateDevice(ctx, &networkmanagersdk.CreateDeviceInput{
				GlobalNetworkId: gnID, Description: aws.String("d"), Vendor: aws.String("v"),
			})
			require.NoError(t, err)

			link, err := c.CreateLink(ctx, &networkmanagersdk.CreateLinkInput{
				GlobalNetworkId: gnID, SiteId: site.Site.SiteId,
				Description: aws.String("d"), Provider: aws.String("p"),
				Bandwidth: &types.Bandwidth{UploadSpeed: aws.Int32(1), DownloadSpeed: aws.Int32(1)},
			})
			require.NoError(t, err)

			ugn, err := c.UpdateGlobalNetwork(ctx, &networkmanagersdk.UpdateGlobalNetworkInput{
				GlobalNetworkId: gnID, Description: tc.update,
			})
			require.NoError(t, err)
			assert.Equal(t, tc.wantDesc, aws.ToString(ugn.GlobalNetwork.Description))

			us, err := c.UpdateSite(ctx, &networkmanagersdk.UpdateSiteInput{
				GlobalNetworkId: gnID, SiteId: site.Site.SiteId, Description: tc.update,
			})
			require.NoError(t, err)
			assert.Equal(t, tc.wantDesc, aws.ToString(us.Site.Description))

			ud, err := c.UpdateDevice(ctx, &networkmanagersdk.UpdateDeviceInput{
				GlobalNetworkId: gnID, DeviceId: dev.Device.DeviceId, Description: tc.update,
			})
			require.NoError(t, err)
			assert.Equal(t, tc.wantDesc, aws.ToString(ud.Device.Description))
			assert.Equal(t, "v", aws.ToString(ud.Device.Vendor))

			ul, err := c.UpdateLink(ctx, &networkmanagersdk.UpdateLinkInput{
				GlobalNetworkId: gnID, LinkId: link.Link.LinkId, Description: tc.update,
			})
			require.NoError(t, err)
			assert.Equal(t, tc.wantDesc, aws.ToString(ul.Link.Description))
			assert.Equal(t, "p", aws.ToString(ul.Link.Provider))
		})
	}
}
