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

// TestActivateKeySigningKey_NoInventedField_RealClient drives
// ActivateKeySigningKey through the real aws-sdk-go-v2 client
// (gopherstack-7185). The real output carries only ChangeInfo
// (route53@v1.65.6 deserializers.go:
// awsRestxml_deserializeOpDocumentActivateKeySigningKeyOutput); gopherstack
// additionally echoed a KeySigningKey element that isn't on the wire,
// confirmed by hand-reverting. The activated status is verified via a
// follow-up GetDNSSEC call.
func TestActivateKeySigningKey_NoInventedField_RealClient(t *testing.T) {
	t.Parallel()

	h := route53.NewHandler(route53.NewInMemoryBackend())
	client := newTestRoute53Client(t, h)

	zone, err := client.CreateHostedZone(t.Context(), &route53sdk.CreateHostedZoneInput{
		Name:            aws.String("sweep1-ksk.example.com."),
		CallerReference: aws.String("sweep1-ksk-ref"),
	})
	require.NoError(t, err)
	zoneID := aws.ToString(zone.HostedZone.Id)

	_, err = client.CreateKeySigningKey(t.Context(), &route53sdk.CreateKeySigningKeyInput{
		HostedZoneId:            aws.String(zoneID),
		CallerReference:         aws.String("sweep1-ksk-create-ref"),
		Name:                    aws.String("sweep1key"),
		KeyManagementServiceArn: aws.String("arn:aws:kms:us-east-1:123456789012:key/sweep1-ksk"),
		Status:                  aws.String("INACTIVE"),
	})
	require.NoError(t, err)

	out, err := client.ActivateKeySigningKey(t.Context(), &route53sdk.ActivateKeySigningKeyInput{
		HostedZoneId: aws.String(zoneID),
		Name:         aws.String("sweep1key"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.ChangeInfo)
	assert.NotEmpty(t, aws.ToString(out.ChangeInfo.Id))

	dnssec, err := client.GetDNSSEC(t.Context(), &route53sdk.GetDNSSECInput{HostedZoneId: aws.String(zoneID)})
	require.NoError(t, err)
	require.Len(t, dnssec.KeySigningKeys, 1)
	assert.Equal(t, "ACTIVE", aws.ToString(dnssec.KeySigningKeys[0].Status))
}

// TestChangeCidrCollection_NoInventedVersion_RealClient drives
// ChangeCidrCollection through the real client. The real output carries only
// Id (route53@v1.65.6 deserializers.go:
// awsRestxml_deserializeOpDocumentChangeCidrCollectionOutput); gopherstack
// additionally echoed a Version element that isn't on the wire, confirmed by
// hand-reverting. The incremented version is verified via a follow-up
// ListCidrCollections call.
func TestChangeCidrCollection_NoInventedVersion_RealClient(t *testing.T) {
	t.Parallel()

	h := route53.NewHandler(route53.NewInMemoryBackend())
	client := newTestRoute53Client(t, h)

	col, err := client.CreateCidrCollection(t.Context(), &route53sdk.CreateCidrCollectionInput{
		Name:            aws.String("sweep1-cidrs"),
		CallerReference: aws.String("sweep1-cidr-ref"),
	})
	require.NoError(t, err)
	colID := aws.ToString(col.Collection.Id)
	require.EqualValues(t, 1, aws.ToInt64(col.Collection.Version))

	out, err := client.ChangeCidrCollection(t.Context(), &route53sdk.ChangeCidrCollectionInput{
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
	assert.Equal(t, colID, aws.ToString(out.Id))

	list, err := client.ListCidrCollections(t.Context(), &route53sdk.ListCidrCollectionsInput{})
	require.NoError(t, err)
	require.Len(t, list.CidrCollections, 1)
	assert.EqualValues(t, 2, aws.ToInt64(list.CidrCollections[0].Version),
		"ListCidrCollections: Version did not increment after ChangeCidrCollection")
}

// TestListHostedZonesByName_ItemShape_RealClient drives ListHostedZonesByName
// through the real client (gopherstack-21my). Its own ad-hoc HostedZone item
// builder (handler_hosted_zones.go's listHostedZonesByName, distinct from
// toXMLHostedZone which GetHostedZone/ListHostedZones both use) dropped
// Config.PrivateZone and ResourceRecordSetCount entirely -- both are real
// HostedZone members (route53@v1.65.6 deserializers.go:
// awsRestxml_deserializeDocumentHostedZone /
// awsRestxml_deserializeDocumentHostedZoneConfig) and both are backed by
// state this backend already tracks (confirmed correct through
// GetHostedZone), so the list variant returned the right zone count with
// two blank/false fields regardless of the real values.
func TestListHostedZonesByName_ItemShape_RealClient(t *testing.T) {
	t.Parallel()

	h := route53.NewHandler(route53.NewInMemoryBackend())
	client := newTestRoute53Client(t, h)

	zone, err := client.CreateHostedZone(t.Context(), &route53sdk.CreateHostedZoneInput{
		Name:            aws.String("sweep1-byname.example.com."),
		CallerReference: aws.String("sweep1-byname-ref"),
		HostedZoneConfig: &types.HostedZoneConfig{
			Comment:     aws.String("private test zone"),
			PrivateZone: true,
		},
		VPC: &types.VPC{
			VPCId:     aws.String("vpc-sweep1byname"),
			VPCRegion: types.VPCRegionUsEast1,
		},
	})
	require.NoError(t, err)
	zoneID := aws.ToString(zone.HostedZone.Id)

	_, err = client.ChangeResourceRecordSets(t.Context(), &route53sdk.ChangeResourceRecordSetsInput{
		HostedZoneId: aws.String(zoneID),
		ChangeBatch: &types.ChangeBatch{
			Changes: []types.Change{{
				Action: types.ChangeActionCreate,
				ResourceRecordSet: &types.ResourceRecordSet{
					Name: aws.String("sweep1-byname.example.com."),
					Type: types.RRTypeA,
					TTL:  aws.Int64(300),
					ResourceRecords: []types.ResourceRecord{
						{Value: aws.String("10.0.0.1")},
					},
				},
			}},
		},
	})
	require.NoError(t, err)

	out, err := client.ListHostedZonesByName(t.Context(), &route53sdk.ListHostedZonesByNameInput{
		DNSName: aws.String("sweep1-byname.example.com."),
	})
	require.NoError(t, err)
	require.NotEmpty(t, out.HostedZones)

	var found *types.HostedZone
	for i := range out.HostedZones {
		if aws.ToString(out.HostedZones[i].Id) == zoneID {
			found = &out.HostedZones[i]
		}
	}
	require.NotNil(t, found, "created zone must appear in the list")

	require.NotNil(t, found.Config, "Config must round-trip, not decode nil")
	assert.True(t, found.Config.PrivateZone,
		"ListHostedZonesByName: Config.PrivateZone did not round-trip")
	assert.Equal(t, "private test zone", aws.ToString(found.Config.Comment))
	assert.EqualValues(t, 3, aws.ToInt64(found.ResourceRecordSetCount),
		"ListHostedZonesByName: ResourceRecordSetCount did not round-trip (auto NS+SOA plus the created A record)")
}
