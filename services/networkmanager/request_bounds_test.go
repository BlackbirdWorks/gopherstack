package networkmanager_test

import (
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	networkmanagersdk "github.com/aws/aws-sdk-go-v2/service/networkmanager"
	"github.com/aws/aws-sdk-go-v2/service/networkmanager/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/networkmanager"
)

func TestRoundTrip_RequestBounds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call func(t *testing.T, c *networkmanagersdk.Client, gn string) error
		name string
	}{
		{name: "description_257", call: func(t *testing.T, c *networkmanagersdk.Client, _ string) error {
			t.Helper()
			_, err := c.CreateGlobalNetwork(t.Context(), &networkmanagersdk.CreateGlobalNetworkInput{
				Description: aws.String(strings.Repeat("d", 257)),
			})

			return err
		}},
		{name: "site_address_257", call: func(t *testing.T, c *networkmanagersdk.Client, gn string) error {
			t.Helper()
			_, err := c.CreateSite(t.Context(), &networkmanagersdk.CreateSiteInput{
				GlobalNetworkId: aws.String(gn),
				Location:        &types.Location{Address: aws.String(strings.Repeat("a", 257))},
			})

			return err
		}},
		{name: "device_model_129", call: func(t *testing.T, c *networkmanagersdk.Client, gn string) error {
			t.Helper()
			_, err := c.CreateDevice(t.Context(), &networkmanagersdk.CreateDeviceInput{
				GlobalNetworkId: aws.String(gn), Model: aws.String(strings.Repeat("m", 129)),
			})

			return err
		}},
		{name: "max_results_501", call: func(t *testing.T, c *networkmanagersdk.Client, _ string) error {
			t.Helper()
			_, err := c.DescribeGlobalNetworks(t.Context(), &networkmanagersdk.DescribeGlobalNetworksInput{
				MaxResults: aws.Int32(501),
			})

			return err
		}},
		{name: "junk_token", call: func(t *testing.T, c *networkmanagersdk.Client, _ string) error {
			t.Helper()
			_, err := c.DescribeGlobalNetworks(t.Context(), &networkmanagersdk.DescribeGlobalNetworksInput{
				NextToken: aws.String("junk-token!"),
			})

			return err
		}},
		{name: "eleven_site_ids", call: func(t *testing.T, c *networkmanagersdk.Client, gn string) error {
			t.Helper()
			ids := make([]string, 11)
			for i := range ids {
				ids[i] = "site-x"
			}
			_, err := c.GetSites(
				t.Context(),
				&networkmanagersdk.GetSitesInput{GlobalNetworkId: aws.String(gn), SiteIds: ids},
			)

			return err
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, client := newTestHandlerAndClient(t)
			gn, err := client.CreateGlobalNetwork(t.Context(), &networkmanagersdk.CreateGlobalNetworkInput{})
			require.NoError(t, err)

			err = tt.call(t, client, aws.ToString(gn.GlobalNetwork.GlobalNetworkId))

			var ve *types.ValidationException
			require.ErrorAs(t, err, &ve)
		})
	}
}

func TestBackend_SetLifecycleDelay(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		want  string
		delay time.Duration
		wait  time.Duration
	}{
		{name: "pending", delay: 5 * time.Second, wait: time.Second, want: "PENDING"},
		{name: "available", delay: 5 * time.Second, wait: 6 * time.Second, want: "AVAILABLE"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := networkmanager.NewInMemoryBackend(t.Context(), rtTestAccountID, rtTestRegion)
				defer b.Close()
				b.SetLifecycleDelay(tt.delay)

				created := b.CreateGlobalNetwork("", nil)
				time.Sleep(tt.wait)

				p, err := b.DescribeGlobalNetworks([]string{created.GlobalNetworkID}, "", 0)
				require.NoError(t, err)
				require.Len(t, p.Data, 1)
				assert.Equal(t, tt.want, p.Data[0].State)
			})
		})
	}
}
