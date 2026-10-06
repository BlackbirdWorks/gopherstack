package networkmanager_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	networkmanagersdk "github.com/aws/aws-sdk-go-v2/service/networkmanager"
	nmtypes "github.com/aws/aws-sdk-go-v2/service/networkmanager/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateCoreNetwork_ClientTokenReplay(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		firstToken   string
		secondToken  string
		secondDesc   string
		wantSame     bool
		wantConflict bool
	}{
		{name: "same token and params", firstToken: "tok-1", secondToken: "tok-1", secondDesc: "d", wantSame: true},
		{name: "different token", firstToken: "tok-1", secondToken: "tok-2", secondDesc: "d"},
		{
			name:         "same token other params",
			firstToken:   "tok-1",
			secondToken:  "tok-1",
			secondDesc:   "x",
			wantConflict: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, client := newTestHandlerAndClient(t)
			ctx := t.Context()

			gn, err := client.CreateGlobalNetwork(ctx, &networkmanagersdk.CreateGlobalNetworkInput{})
			require.NoError(t, err)

			first, err := client.CreateCoreNetwork(ctx, &networkmanagersdk.CreateCoreNetworkInput{
				GlobalNetworkId: gn.GlobalNetwork.GlobalNetworkId, Description: aws.String("d"),
				ClientToken: aws.String(tt.firstToken),
			})
			require.NoError(t, err)

			second, err := client.CreateCoreNetwork(ctx, &networkmanagersdk.CreateCoreNetworkInput{
				GlobalNetworkId: gn.GlobalNetwork.GlobalNetworkId, Description: aws.String(tt.secondDesc),
				ClientToken: aws.String(tt.secondToken),
			})

			if tt.wantConflict {
				var conflict *nmtypes.ConflictException
				require.ErrorAs(t, err, &conflict)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantSame,
				aws.ToString(first.CoreNetwork.CoreNetworkId) == aws.ToString(second.CoreNetwork.CoreNetworkId))
		})
	}
}

func TestPutCoreNetworkPolicy_ClientTokenReplay(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		second   string
		wantSame bool
	}{
		{name: "same token", second: "tok", wantSame: true},
		{name: "new token", second: "tok-2"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, client := newTestHandlerAndClient(t)
			ctx := t.Context()

			gn, err := client.CreateGlobalNetwork(ctx, &networkmanagersdk.CreateGlobalNetworkInput{})
			require.NoError(t, err)

			cn, err := client.CreateCoreNetwork(ctx, &networkmanagersdk.CreateCoreNetworkInput{
				GlobalNetworkId: gn.GlobalNetwork.GlobalNetworkId,
			})
			require.NoError(t, err)

			put := func(token string) int32 {
				out, putErr := client.PutCoreNetworkPolicy(ctx, &networkmanagersdk.PutCoreNetworkPolicyInput{
					CoreNetworkId:  cn.CoreNetwork.CoreNetworkId,
					PolicyDocument: aws.String(`{"version":"2021.12"}`),
					ClientToken:    aws.String(token),
				})
				require.NoError(t, putErr)

				return aws.ToInt32(out.CoreNetworkPolicy.PolicyVersionId)
			}

			assert.Equal(t, tt.wantSame, put("tok") == put(tt.second))
		})
	}
}

func TestGetNetworkResources_RegisteredGatewayArnFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		gateway *string
		name    string
		want    int
	}{
		{name: "no filter", want: 1},
		{
			name:    "gateway no resource is registered under",
			gateway: aws.String("arn:aws:ec2:us-east-1:000000000000:transit-gateway/tgw-1"),
			want:    0,
		},
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
				GlobalNetworkId: gn.GlobalNetwork.GlobalNetworkId, RegisteredGatewayArn: tt.gateway,
			})
			require.NoError(t, err)
			assert.Len(t, out.NetworkResources, tt.want)
		})
	}
}

func TestGetNetworkResourceRelationships_AppliesFilters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		resourceType *string
		account      *string
		gateway      *string
		name         string
		want         int
	}{
		{name: "no filter", want: 1},
		{name: "device type", resourceType: aws.String("device"), want: 1},
		{name: "type with no endpoint", resourceType: aws.String("link"), want: 0},
		{name: "other account", account: aws.String("999999999999"), want: 0},
		{name: "gateway", gateway: aws.String("arn:aws:ec2:us-east-1:000000000000:transit-gateway/tgw-1"), want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, client := newTestHandlerAndClient(t)
			ctx := t.Context()

			gn, err := client.CreateGlobalNetwork(ctx, &networkmanagersdk.CreateGlobalNetworkInput{})
			require.NoError(t, err)

			site, err := client.CreateSite(
				ctx,
				&networkmanagersdk.CreateSiteInput{GlobalNetworkId: gn.GlobalNetwork.GlobalNetworkId},
			)
			require.NoError(t, err)

			_, err = client.CreateDevice(ctx, &networkmanagersdk.CreateDeviceInput{
				GlobalNetworkId: gn.GlobalNetwork.GlobalNetworkId, SiteId: site.Site.SiteId,
			})
			require.NoError(t, err)

			out, err := client.GetNetworkResourceRelationships(
				ctx,
				&networkmanagersdk.GetNetworkResourceRelationshipsInput{
					GlobalNetworkId: gn.GlobalNetwork.GlobalNetworkId, ResourceType: tt.resourceType,
					AccountId: tt.account, RegisteredGatewayArn: tt.gateway,
				},
			)
			require.NoError(t, err)
			assert.Len(t, out.Relationships, tt.want)
		})
	}
}
