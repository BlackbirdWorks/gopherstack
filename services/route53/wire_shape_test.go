package route53_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	route53sdk "github.com/aws/aws-sdk-go-v2/service/route53"
	"github.com/aws/aws-sdk-go-v2/service/route53/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/route53"
)

// TestListCidrBlocks_WireShape and TestListCidrLocations_WireShape guard
// against gopherstack-6flj: route53's ListCidrBlocks/ListCidrLocations
// wrapped each block/location as a bare <member>string</member>, but the
// real shapes (route53@v1.65.6 deserializers.go:
// awsRestxml_deserializeDocumentCidrBlockSummary /
// awsRestxml_deserializeDocumentLocationSummary) nest CidrBlock/LocationName
// as child elements of member. A real client parsing the old bare-string
// shape decoded every CidrBlock/LocationName field to nil.
func TestListCidrBlocks_WireShape(t *testing.T) {
	t.Parallel()

	h := route53.NewHandler(route53.NewInMemoryBackend())
	client := newTestRoute53Client(t, h)

	col, err := client.CreateCidrCollection(t.Context(), &route53sdk.CreateCidrCollectionInput{
		Name:            aws.String("wire-shape-cidrs"),
		CallerReference: aws.String("cidr-caller-ref-1"),
	})
	require.NoError(t, err)
	colID := aws.ToString(col.Collection.Id)

	_, err = client.ChangeCidrCollection(t.Context(), &route53sdk.ChangeCidrCollectionInput{
		Id: aws.String(colID),
		Changes: []types.CidrCollectionChange{
			{
				Action:       types.CidrCollectionChangeActionPut,
				LocationName: aws.String("office"),
				CidrList:     []string{"192.168.1.0/24"},
			},
		},
	})
	require.NoError(t, err)

	out, err := client.ListCidrBlocks(t.Context(), &route53sdk.ListCidrBlocksInput{
		CollectionId: aws.String(colID),
		LocationName: aws.String("office"),
	})
	require.NoError(t, err)
	require.Len(t, out.CidrBlocks, 1)
	assert.Equal(t, "192.168.1.0/24", aws.ToString(out.CidrBlocks[0].CidrBlock))
	assert.Equal(t, "office", aws.ToString(out.CidrBlocks[0].LocationName))
}

func TestListCidrLocations_WireShape(t *testing.T) {
	t.Parallel()

	h := route53.NewHandler(route53.NewInMemoryBackend())
	client := newTestRoute53Client(t, h)

	col, err := client.CreateCidrCollection(t.Context(), &route53sdk.CreateCidrCollectionInput{
		Name:            aws.String("wire-shape-locations"),
		CallerReference: aws.String("cidr-caller-ref-2"),
	})
	require.NoError(t, err)
	colID := aws.ToString(col.Collection.Id)

	_, err = client.ChangeCidrCollection(t.Context(), &route53sdk.ChangeCidrCollectionInput{
		Id: aws.String(colID),
		Changes: []types.CidrCollectionChange{
			{
				Action:       types.CidrCollectionChangeActionPut,
				LocationName: aws.String("datacenter"),
				CidrList:     []string{"10.1.0.0/16"},
			},
		},
	})
	require.NoError(t, err)

	out, err := client.ListCidrLocations(t.Context(), &route53sdk.ListCidrLocationsInput{
		CollectionId: aws.String(colID),
	})
	require.NoError(t, err)
	require.Len(t, out.CidrLocations, 1)
	assert.Equal(t, "datacenter", aws.ToString(out.CidrLocations[0].LocationName))
}

// TestListHostedZonesByVPC_WireShape guards against gopherstack-6flj:
// ListHostedZonesByVPC reused the full xmlHostedZone shape (element "Id"),
// but the real HostedZoneSummaries member (route53@v1.65.6 deserializers.go:
// awsRestxml_deserializeDocumentHostedZoneSummary) is a distinct type whose
// id element is "HostedZoneId" and which also carries a required nested
// Owner. A real client decoded HostedZoneId and Owner to nil on every zone.
func TestListHostedZonesByVPC_WireShape(t *testing.T) {
	t.Parallel()

	h := route53.NewHandler(route53.NewInMemoryBackend())
	client := newTestRoute53Client(t, h)

	zone, err := client.CreateHostedZone(t.Context(), &route53sdk.CreateHostedZoneInput{
		Name:            aws.String("private.example.com."),
		CallerReference: aws.String("vpc-zone-ref-1"),
		HostedZoneConfig: &types.HostedZoneConfig{
			PrivateZone: true,
		},
	})
	require.NoError(t, err)
	wantZoneID := aws.ToString(zone.HostedZone.Id)

	_, err = client.AssociateVPCWithHostedZone(t.Context(), &route53sdk.AssociateVPCWithHostedZoneInput{
		HostedZoneId: zone.HostedZone.Id,
		VPC: &types.VPC{
			VPCId:     aws.String("vpc-abc123"),
			VPCRegion: types.VPCRegionUsEast1,
		},
	})
	require.NoError(t, err)

	out, err := client.ListHostedZonesByVPC(t.Context(), &route53sdk.ListHostedZonesByVPCInput{
		VPCId:     aws.String("vpc-abc123"),
		VPCRegion: types.VPCRegionUsEast1,
	})
	require.NoError(t, err)
	require.Len(t, out.HostedZoneSummaries, 1)
	assert.Equal(t, wantZoneID, aws.ToString(out.HostedZoneSummaries[0].HostedZoneId))
	assert.Equal(t, "private.example.com.", aws.ToString(out.HostedZoneSummaries[0].Name))
	require.NotNil(t, out.HostedZoneSummaries[0].Owner)
	assert.NotEmpty(t, aws.ToString(out.HostedZoneSummaries[0].Owner.OwningAccount))
}

// TestListReusableDelegationSets_WireShape guards against gopherstack-6flj:
// xmlDelegationSet had no CallerReference field at all, so
// ListReusableDelegationSets (and Create/GetReusableDelegationSet) silently
// dropped it even though the backend tracks it.
func TestListReusableDelegationSets_WireShape(t *testing.T) {
	t.Parallel()

	h := route53.NewHandler(route53.NewInMemoryBackend())
	client := newTestRoute53Client(t, h)

	_, err := client.CreateReusableDelegationSet(t.Context(), &route53sdk.CreateReusableDelegationSetInput{
		CallerReference: aws.String("reusable-ds-ref-1"),
	})
	require.NoError(t, err)

	out, err := client.ListReusableDelegationSets(t.Context(), &route53sdk.ListReusableDelegationSetsInput{})
	require.NoError(t, err)
	require.Len(t, out.DelegationSets, 1)
	assert.Equal(t, "reusable-ds-ref-1", aws.ToString(out.DelegationSets[0].CallerReference))
}

// TestGetDNSSEC_WireShape guards against gopherstack-m1gl: xmlKSK carries its
// own struct-level XMLName ("KeySigningKey"), which silently overrides the
// parent field's "member" tag when reused directly as GetDNSSEC's list item
// type. The real KeySigningKeys member (route53@v1.65.6 deserializers.go:
// awsRestxml_deserializeDocumentKeySigningKeys) is "member", not
// "KeySigningKey" -- a real client decoded zero KSKs no matter how many
// existed.
func TestGetDNSSEC_WireShape(t *testing.T) {
	t.Parallel()

	h := route53.NewHandler(route53.NewInMemoryBackend())
	client := newTestRoute53Client(t, h)

	zone, err := client.CreateHostedZone(t.Context(), &route53sdk.CreateHostedZoneInput{
		Name:            aws.String("dnssec-wire-shape.example.com."),
		CallerReference: aws.String("dnssec-wire-shape-ref-1"),
	})
	require.NoError(t, err)

	_, err = client.CreateKeySigningKey(t.Context(), &route53sdk.CreateKeySigningKeyInput{
		HostedZoneId:            zone.HostedZone.Id,
		CallerReference:         aws.String("ksk-caller-ref-1"),
		Name:                    aws.String("wire-shape-ksk"),
		KeyManagementServiceArn: aws.String("arn:aws:kms:us-east-1:123456789012:key/test-ksk"),
		Status:                  aws.String("ACTIVE"),
	})
	require.NoError(t, err)

	out, err := client.GetDNSSEC(t.Context(), &route53sdk.GetDNSSECInput{
		HostedZoneId: zone.HostedZone.Id,
	})
	require.NoError(t, err)
	require.Len(t, out.KeySigningKeys, 1)
	assert.Equal(t, "wire-shape-ksk", aws.ToString(out.KeySigningKeys[0].Name))
	assert.Equal(t, "ACTIVE", aws.ToString(out.KeySigningKeys[0].Status))
}
