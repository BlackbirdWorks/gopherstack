package ec2_test

// Pagination fixes for handler_client_vpn.go, following the same
// "unbounded single page" shape ec2sweep11 fixed elsewhere in this package:
// DescribeClientVpnEndpoints, DescribeClientVpnTargetNetworks,
// DescribeClientVpnRoutes, and DescribeClientVpnAuthorizationRules all
// declare real MaxResults/NextToken on their Input/Output (confirmed against
// each api_op_*.go, ec2@v1.319.1) but the handlers returned every item in one
// unbounded page, no NextToken.

import (
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/stretchr/testify/require"
)

const ec2sweep23SeedCount = 5

func TestDescribeClientVpnEndpoints_Pagination_RealClient(t *testing.T) {
	t.Parallel()

	b, client := newTestBackendAndClient(t)

	for i := range ec2sweep23SeedCount {
		_, err := b.CreateClientVpnEndpoint(
			"10.70."+strconv.Itoa(i)+".0/24", "sweep23 endpoint", nil,
		)
		require.NoError(t, err)
	}

	paginator := ec2sdk.NewDescribeClientVpnEndpointsPaginator(
		client, &ec2sdk.DescribeClientVpnEndpointsInput{},
		func(o *ec2sdk.DescribeClientVpnEndpointsPaginatorOptions) { o.Limit = ec2sweep11MaxResults },
	)

	var pages [][]string
	for pageNum := 0; paginator.HasMorePages() && pageNum < ec2sweep11LoopGuard; pageNum++ {
		page, err := paginator.NextPage(t.Context())
		require.NoError(t, err)

		ids := make([]string, 0, len(page.ClientVpnEndpoints))
		for _, ep := range page.ClientVpnEndpoints {
			ids = append(ids, aws.ToString(ep.ClientVpnEndpointId))
		}
		pages = append(pages, ids)
	}

	assertDisjointPages(t, pages, ec2sweep23SeedCount)
}

func TestDescribeClientVpnRoutes_Pagination_RealClient(t *testing.T) {
	t.Parallel()

	b, client := newTestBackendAndClient(t)

	ep, err := b.CreateClientVpnEndpoint("10.71.0.0/22", "sweep23 route endpoint", nil)
	require.NoError(t, err)

	for i := range ec2sweep23SeedCount {
		require.NoError(t, b.CreateClientVpnRoute(
			ep.ClientVpnEndpointID, "192.168."+strconv.Itoa(i)+".0/24", "sweep23",
		))
	}

	paginator := ec2sdk.NewDescribeClientVpnRoutesPaginator(
		client,
		&ec2sdk.DescribeClientVpnRoutesInput{ClientVpnEndpointId: aws.String(ep.ClientVpnEndpointID)},
		func(o *ec2sdk.DescribeClientVpnRoutesPaginatorOptions) { o.Limit = ec2sweep11MaxResults },
	)

	var pages [][]string
	for pageNum := 0; paginator.HasMorePages() && pageNum < ec2sweep11LoopGuard; pageNum++ {
		page, pageErr := paginator.NextPage(t.Context())
		require.NoError(t, pageErr)

		ids := make([]string, 0, len(page.Routes))
		for _, r := range page.Routes {
			ids = append(ids, aws.ToString(r.DestinationCidr))
		}
		pages = append(pages, ids)
	}

	assertDisjointPages(t, pages, ec2sweep23SeedCount)
}

func TestDescribeClientVpnAuthorizationRules_Pagination_RealClient(t *testing.T) {
	t.Parallel()

	b, client := newTestBackendAndClient(t)

	ep, err := b.CreateClientVpnEndpoint("10.72.0.0/22", "sweep23 authz endpoint", nil)
	require.NoError(t, err)

	for i := range ec2sweep23SeedCount {
		require.NoError(t, b.AuthorizeClientVpnIngress(
			ep.ClientVpnEndpointID, "172.16."+strconv.Itoa(i)+".0/24", "sweep23",
		))
	}

	paginator := ec2sdk.NewDescribeClientVpnAuthorizationRulesPaginator(
		client,
		&ec2sdk.DescribeClientVpnAuthorizationRulesInput{ClientVpnEndpointId: aws.String(ep.ClientVpnEndpointID)},
		func(o *ec2sdk.DescribeClientVpnAuthorizationRulesPaginatorOptions) { o.Limit = ec2sweep11MaxResults },
	)

	var pages [][]string
	for pageNum := 0; paginator.HasMorePages() && pageNum < ec2sweep11LoopGuard; pageNum++ {
		page, pageErr := paginator.NextPage(t.Context())
		require.NoError(t, pageErr)

		ids := make([]string, 0, len(page.AuthorizationRules))
		for _, r := range page.AuthorizationRules {
			ids = append(ids, aws.ToString(r.DestinationCidr))
		}
		pages = append(pages, ids)
	}

	assertDisjointPages(t, pages, ec2sweep23SeedCount)
}

func TestDescribeClientVpnTargetNetworks_Pagination_RealClient(t *testing.T) {
	t.Parallel()

	b, client := newTestBackendAndClient(t)

	ep, err := b.CreateClientVpnEndpoint("10.73.0.0/22", "sweep23 tn endpoint", nil)
	require.NoError(t, err)

	for i := range ec2sweep23SeedCount {
		_, assocErr := b.AssociateClientVpnTargetNetwork(
			ep.ClientVpnEndpointID, "subnet-sweep23-"+strconv.Itoa(i),
		)
		require.NoError(t, assocErr)
	}

	paginator := ec2sdk.NewDescribeClientVpnTargetNetworksPaginator(
		client,
		&ec2sdk.DescribeClientVpnTargetNetworksInput{ClientVpnEndpointId: aws.String(ep.ClientVpnEndpointID)},
		func(o *ec2sdk.DescribeClientVpnTargetNetworksPaginatorOptions) { o.Limit = ec2sweep11MaxResults },
	)

	var pages [][]string
	for pageNum := 0; paginator.HasMorePages() && pageNum < ec2sweep11LoopGuard; pageNum++ {
		page, pageErr := paginator.NextPage(t.Context())
		require.NoError(t, pageErr)

		ids := make([]string, 0, len(page.ClientVpnTargetNetworks))
		for _, n := range page.ClientVpnTargetNetworks {
			ids = append(ids, aws.ToString(n.AssociationId))
		}
		pages = append(pages, ids)
	}

	assertDisjointPages(t, pages, ec2sweep23SeedCount)
}
