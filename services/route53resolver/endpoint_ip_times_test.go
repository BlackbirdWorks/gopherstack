package route53resolver_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	route53resolversdk "github.com/aws/aws-sdk-go-v2/service/route53resolver"
	"github.com/aws/aws-sdk-go-v2/service/route53resolver/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListResolverEndpointIpAddresses_Timestamps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		associate bool
		wantIPs   int
	}{
		{name: "created_ip", wantIPs: 1},
		{name: "associated_ip", associate: true, wantIPs: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)
			created, err := client.CreateResolverEndpoint(t.Context(), &route53resolversdk.CreateResolverEndpointInput{
				CreatorRequestId: aws.String("cr-ip-times"),
				Direction:        types.ResolverEndpointDirectionInbound,
				Name:             aws.String("ep-ip-times"),
				IpAddresses:      []types.IpAddressRequest{{SubnetId: aws.String("subnet-11111111")}},
				SecurityGroupIds: []string{"sg-11111111"},
			})
			require.NoError(t, err)
			id := aws.ToString(created.ResolverEndpoint.Id)

			if tt.associate {
				_, err = client.AssociateResolverEndpointIpAddress(
					t.Context(),
					&route53resolversdk.AssociateResolverEndpointIpAddressInput{
						ResolverEndpointId: aws.String(id),
						IpAddress:          &types.IpAddressUpdate{SubnetId: aws.String("subnet-22222222")},
					},
				)
				require.NoError(t, err)
			}

			out, err := client.ListResolverEndpointIpAddresses(
				t.Context(),
				&route53resolversdk.ListResolverEndpointIpAddressesInput{ResolverEndpointId: aws.String(id)},
			)
			require.NoError(t, err)
			require.Len(t, out.IpAddresses, tt.wantIPs)

			for _, ip := range out.IpAddresses {
				c, cerr := time.Parse(time.RFC3339, aws.ToString(ip.CreationTime))
				require.NoError(t, cerr)
				m, merr := time.Parse(time.RFC3339, aws.ToString(ip.ModificationTime))
				require.NoError(t, merr)
				assert.False(t, m.Before(c))
			}
		})
	}
}
