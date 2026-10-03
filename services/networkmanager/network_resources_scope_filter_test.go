package networkmanager_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	networkmanagersdk "github.com/aws/aws-sdk-go-v2/service/networkmanager"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetNetworkResources_AccountAndRegionFilters_RealClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		account *string
		region  *string
		name    string
		want    int
	}{
		{name: "none", want: 1},
		{name: "matching account", account: aws.String(rtTestAccountID), want: 1},
		{name: "other account", account: aws.String("999999999999"), want: 0},
		{name: "matching region", region: aws.String(rtTestRegion), want: 1},
		{name: "other region", region: aws.String("eu-west-3"), want: 0},
		{name: "both and", account: aws.String(rtTestAccountID), region: aws.String("eu-west-3"), want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, client := newTestHandlerAndClient(t)
			ctx := t.Context()

			gn, err := client.CreateGlobalNetwork(ctx, &networkmanagersdk.CreateGlobalNetworkInput{})
			require.NoError(t, err)

			_, err = client.CreateSite(
				ctx,
				&networkmanagersdk.CreateSiteInput{GlobalNetworkId: gn.GlobalNetwork.GlobalNetworkId},
			)
			require.NoError(t, err)

			out, err := client.GetNetworkResources(ctx, &networkmanagersdk.GetNetworkResourcesInput{
				GlobalNetworkId: gn.GlobalNetwork.GlobalNetworkId,
				AccountId:       tt.account,
				AwsRegion:       tt.region,
			})
			require.NoError(t, err)
			assert.Len(t, out.NetworkResources, tt.want)
		})
	}
}
