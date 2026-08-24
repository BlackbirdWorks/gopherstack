package ec2_test

// Fixes for handler_tgw_multicast.go (ec2sweep25):
//
//   - SearchTransitGatewayMulticastGroups copied NetworkInterfaceId into the
//     response's TransitGatewayAttachmentId field. The real
//     TransitGatewayMulticastGroup type (types/types.go:24172) declares
//     NetworkInterfaceId and TransitGatewayAttachmentId as two distinct
//     fields; a client reading TransitGatewayAttachmentId got back an ENI ID
//     instead.
//   - Four List/Get ops declare real MaxResults/NextToken on their SDK
//     Input/Output but the handlers returned every item in one unbounded
//     page: DescribeTransitGatewayMulticastDomains,
//     DescribeTransitGatewayMeteringPolicies,
//     GetTransitGatewayMulticastDomainAssociations,
//     SearchTransitGatewayMulticastGroups.
//   - CreateTransitGatewayMulticastDomain / CreateTransitGatewayMeteringPolicy
//     accepted-and-dropped TagSpecifications entirely: the tag was recognised
//     as valid (resource_types.go already lists both IDs for CreateTags) but
//     never applied, so a domain/policy tagged at creation came back
//     untagged from Describe.

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const ec2sweep25SeedCount = 5

func createTestTGW(t *testing.T, client *ec2sdk.Client) string {
	t.Helper()

	tgw, err := client.CreateTransitGateway(t.Context(), &ec2sdk.CreateTransitGatewayInput{})
	require.NoError(t, err)

	return aws.ToString(tgw.TransitGateway.TransitGatewayId)
}

func TestSearchTransitGatewayMulticastGroups_TransitGatewayAttachmentId_RealClient(t *testing.T) {
	t.Parallel()

	_, client := newTestBackendAndClient(t)
	tgwID := createTestTGW(t, client)

	domain, err := client.CreateTransitGatewayMulticastDomain(
		t.Context(),
		&ec2sdk.CreateTransitGatewayMulticastDomainInput{
			TransitGatewayId: aws.String(tgwID),
		},
	)
	require.NoError(t, err)
	domainID := domain.TransitGatewayMulticastDomain.TransitGatewayMulticastDomainId

	_, err = client.RegisterTransitGatewayMulticastGroupMembers(
		t.Context(), &ec2sdk.RegisterTransitGatewayMulticastGroupMembersInput{
			TransitGatewayMulticastDomainId: domainID,
			GroupIpAddress:                  aws.String("224.0.1.5"),
			NetworkInterfaceIds:             []string{"eni-mcast-1"},
		},
	)
	require.NoError(t, err)

	out, err := client.SearchTransitGatewayMulticastGroups(
		t.Context(), &ec2sdk.SearchTransitGatewayMulticastGroupsInput{
			TransitGatewayMulticastDomainId: domainID,
		},
	)
	require.NoError(t, err)
	require.Len(t, out.MulticastGroups, 1)

	group := out.MulticastGroups[0]
	assert.Equal(t, "eni-mcast-1", aws.ToString(group.NetworkInterfaceId))
	assert.Nil(
		t, group.TransitGatewayAttachmentId,
		"TransitGatewayAttachmentId set - handler copied NetworkInterfaceId into a distinct wire field",
	)
}

func TestDescribeTransitGatewayMulticastDomains_Pagination_RealClient(t *testing.T) {
	t.Parallel()

	_, client := newTestBackendAndClient(t)
	tgwID := createTestTGW(t, client)

	for range ec2sweep25SeedCount {
		_, err := client.CreateTransitGatewayMulticastDomain(
			t.Context(), &ec2sdk.CreateTransitGatewayMulticastDomainInput{TransitGatewayId: aws.String(tgwID)},
		)
		require.NoError(t, err)
	}

	paginator := ec2sdk.NewDescribeTransitGatewayMulticastDomainsPaginator(
		client, &ec2sdk.DescribeTransitGatewayMulticastDomainsInput{},
		func(o *ec2sdk.DescribeTransitGatewayMulticastDomainsPaginatorOptions) { o.Limit = ec2sweep11MaxResults },
	)

	var pages [][]string
	for pageNum := 0; paginator.HasMorePages() && pageNum < ec2sweep11LoopGuard; pageNum++ {
		p, pageErr := paginator.NextPage(t.Context())
		require.NoError(t, pageErr)

		ids := make([]string, 0, len(p.TransitGatewayMulticastDomains))
		for _, d := range p.TransitGatewayMulticastDomains {
			ids = append(ids, aws.ToString(d.TransitGatewayMulticastDomainId))
		}
		pages = append(pages, ids)
	}

	assertDisjointPages(t, pages, ec2sweep25SeedCount)
}

func TestDescribeTransitGatewayMeteringPolicies_Pagination_RealClient(t *testing.T) {
	t.Parallel()

	// DescribeTransitGatewayMeteringPolicies has no generated Paginator (the
	// SDK's smithy model doesn't mark it paginated despite the Input/Output
	// declaring MaxResults/NextToken), so drive the pages by hand.
	_, client := newTestBackendAndClient(t)
	tgwID := createTestTGW(t, client)

	for range ec2sweep25SeedCount {
		_, err := client.CreateTransitGatewayMeteringPolicy(
			t.Context(), &ec2sdk.CreateTransitGatewayMeteringPolicyInput{TransitGatewayId: aws.String(tgwID)},
		)
		require.NoError(t, err)
	}

	var (
		pages     [][]string
		nextToken *string
	)

	for range ec2sweep11LoopGuard {
		out, err := client.DescribeTransitGatewayMeteringPolicies(
			t.Context(), &ec2sdk.DescribeTransitGatewayMeteringPoliciesInput{
				MaxResults: aws.Int32(ec2sweep11MaxResults),
				NextToken:  nextToken,
			},
		)
		require.NoError(t, err)

		ids := make([]string, 0, len(out.TransitGatewayMeteringPolicies))
		for _, p := range out.TransitGatewayMeteringPolicies {
			ids = append(ids, aws.ToString(p.TransitGatewayMeteringPolicyId))
		}
		pages = append(pages, ids)

		if aws.ToString(out.NextToken) == "" {
			break
		}
		nextToken = out.NextToken
	}

	assertDisjointPages(t, pages, ec2sweep25SeedCount)
}

func TestGetTransitGatewayMulticastDomainAssociations_Pagination_RealClient(t *testing.T) {
	t.Parallel()

	_, client := newTestBackendAndClient(t)
	tgwID := createTestTGW(t, client)

	domain, err := client.CreateTransitGatewayMulticastDomain(
		t.Context(), &ec2sdk.CreateTransitGatewayMulticastDomainInput{TransitGatewayId: aws.String(tgwID)},
	)
	require.NoError(t, err)
	domainID := domain.TransitGatewayMulticastDomain.TransitGatewayMulticastDomainId

	subnetIDs := make([]string, 0, ec2sweep25SeedCount)
	for i := range ec2sweep25SeedCount {
		subnetIDs = append(subnetIDs, "subnet-mcast-"+string(rune('a'+i)))
	}

	_, err = client.AssociateTransitGatewayMulticastDomain(
		t.Context(), &ec2sdk.AssociateTransitGatewayMulticastDomainInput{
			TransitGatewayMulticastDomainId: domainID,
			TransitGatewayAttachmentId:      aws.String("tgw-attach-mcast-1"),
			SubnetIds:                       subnetIDs,
		},
	)
	require.NoError(t, err)

	paginator := ec2sdk.NewGetTransitGatewayMulticastDomainAssociationsPaginator(
		client, &ec2sdk.GetTransitGatewayMulticastDomainAssociationsInput{
			TransitGatewayMulticastDomainId: domainID,
		},
		func(o *ec2sdk.GetTransitGatewayMulticastDomainAssociationsPaginatorOptions) {
			o.Limit = ec2sweep11MaxResults
		},
	)

	var pages [][]string
	for pageNum := 0; paginator.HasMorePages() && pageNum < ec2sweep11LoopGuard; pageNum++ {
		p, pageErr := paginator.NextPage(t.Context())
		require.NoError(t, pageErr)

		ids := make([]string, 0, len(p.MulticastDomainAssociations))
		for _, a := range p.MulticastDomainAssociations {
			ids = append(ids, aws.ToString(a.Subnet.SubnetId))
		}
		pages = append(pages, ids)
	}

	assertDisjointPages(t, pages, ec2sweep25SeedCount)
}

func TestSearchTransitGatewayMulticastGroups_Pagination_RealClient(t *testing.T) {
	t.Parallel()

	_, client := newTestBackendAndClient(t)
	tgwID := createTestTGW(t, client)

	domain, err := client.CreateTransitGatewayMulticastDomain(
		t.Context(), &ec2sdk.CreateTransitGatewayMulticastDomainInput{TransitGatewayId: aws.String(tgwID)},
	)
	require.NoError(t, err)
	domainID := domain.TransitGatewayMulticastDomain.TransitGatewayMulticastDomainId

	eniIDs := make([]string, 0, ec2sweep25SeedCount)
	for i := range ec2sweep25SeedCount {
		eniIDs = append(eniIDs, "eni-mcast-page-"+string(rune('a'+i)))
	}

	_, err = client.RegisterTransitGatewayMulticastGroupMembers(
		t.Context(), &ec2sdk.RegisterTransitGatewayMulticastGroupMembersInput{
			TransitGatewayMulticastDomainId: domainID,
			GroupIpAddress:                  aws.String("224.0.1.6"),
			NetworkInterfaceIds:             eniIDs,
		},
	)
	require.NoError(t, err)

	paginator := ec2sdk.NewSearchTransitGatewayMulticastGroupsPaginator(
		client, &ec2sdk.SearchTransitGatewayMulticastGroupsInput{
			TransitGatewayMulticastDomainId: domainID,
		},
		func(o *ec2sdk.SearchTransitGatewayMulticastGroupsPaginatorOptions) { o.Limit = ec2sweep11MaxResults },
	)

	var pages [][]string
	for pageNum := 0; paginator.HasMorePages() && pageNum < ec2sweep11LoopGuard; pageNum++ {
		p, pageErr := paginator.NextPage(t.Context())
		require.NoError(t, pageErr)

		ids := make([]string, 0, len(p.MulticastGroups))
		for _, g := range p.MulticastGroups {
			ids = append(ids, aws.ToString(g.NetworkInterfaceId))
		}
		pages = append(pages, ids)
	}

	assertDisjointPages(t, pages, ec2sweep25SeedCount)
}

func TestCreateTransitGatewayMulticastDomain_Tags_RealClient(t *testing.T) {
	t.Parallel()

	_, client := newTestBackendAndClient(t)
	tgwID := createTestTGW(t, client)

	created, err := client.CreateTransitGatewayMulticastDomain(
		t.Context(), &ec2sdk.CreateTransitGatewayMulticastDomainInput{
			TransitGatewayId: aws.String(tgwID),
			TagSpecifications: []types.TagSpecification{{
				ResourceType: types.ResourceTypeTransitGatewayMulticastDomain,
				Tags:         []types.Tag{{Key: aws.String("Team"), Value: aws.String("sweep25")}},
			}},
		},
	)
	require.NoError(t, err)
	require.NotEmpty(
		t, created.TransitGatewayMulticastDomain.Tags,
		"Tags empty on create response - TagSpecification accepted but never applied",
	)

	domainID := created.TransitGatewayMulticastDomain.TransitGatewayMulticastDomainId

	out, err := client.DescribeTransitGatewayMulticastDomains(
		t.Context(), &ec2sdk.DescribeTransitGatewayMulticastDomainsInput{
			TransitGatewayMulticastDomainIds: []string{aws.ToString(domainID)},
		},
	)
	require.NoError(t, err)
	require.Len(t, out.TransitGatewayMulticastDomains, 1)
	require.NotEmpty(
		t, out.TransitGatewayMulticastDomains[0].Tags,
		"Tags empty on describe - TagSpecification accepted at create but dropped from Describe",
	)
	assert.Equal(t, "Team", aws.ToString(out.TransitGatewayMulticastDomains[0].Tags[0].Key))
	assert.Equal(t, "sweep25", aws.ToString(out.TransitGatewayMulticastDomains[0].Tags[0].Value))
}

func TestCreateTransitGatewayMeteringPolicy_Tags_RealClient(t *testing.T) {
	t.Parallel()

	_, client := newTestBackendAndClient(t)
	tgwID := createTestTGW(t, client)

	created, err := client.CreateTransitGatewayMeteringPolicy(
		t.Context(), &ec2sdk.CreateTransitGatewayMeteringPolicyInput{
			TransitGatewayId: aws.String(tgwID),
			TagSpecifications: []types.TagSpecification{{
				ResourceType: types.ResourceTypeTransitGatewayMeteringPolicy,
				Tags:         []types.Tag{{Key: aws.String("Team"), Value: aws.String("sweep25")}},
			}},
		},
	)
	require.NoError(t, err)

	policyID := created.TransitGatewayMeteringPolicy.TransitGatewayMeteringPolicyId

	out, err := client.DescribeTransitGatewayMeteringPolicies(
		t.Context(), &ec2sdk.DescribeTransitGatewayMeteringPoliciesInput{
			TransitGatewayMeteringPolicyIds: []string{aws.ToString(policyID)},
		},
	)
	require.NoError(t, err)
	require.Len(t, out.TransitGatewayMeteringPolicies, 1)
	require.NotEmpty(
		t, out.TransitGatewayMeteringPolicies[0].Tags,
		"Tags empty on describe - TagSpecification accepted at create but dropped from Describe",
	)
	assert.Equal(t, "Team", aws.ToString(out.TransitGatewayMeteringPolicies[0].Tags[0].Key))
	assert.Equal(t, "sweep25", aws.ToString(out.TransitGatewayMeteringPolicies[0].Tags[0].Value))
}
