package integration_test

import (
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	route53sdk "github.com/aws/aws-sdk-go-v2/service/route53"
	route53types "github.com/aws/aws-sdk-go-v2/service/route53/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_Route53_ResourceRecordSetsRoundTrip drives ChangeResourceRecordSets
// through a real client and reads the actual record values back via
// ListResourceRecordSets -- the read side of the record lifecycle that no typed
// client had exercised before this test (only the write side, ChangeResourceRecordSets,
// had coverage).
func TestIntegration_Route53_ResourceRecordSetsRoundTrip(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createRoute53Client(t)
	ctx := t.Context()

	suffix := time.Now().Format("20060102150405.000000")
	zoneName := "rrs-roundtrip-" + suffix + ".example.com"

	createZoneOut, err := client.CreateHostedZone(ctx, &route53sdk.CreateHostedZoneInput{
		Name:            aws.String(zoneName),
		CallerReference: aws.String("rrs-roundtrip-" + suffix),
	})
	require.NoError(t, err)
	zoneID := aws.ToString(createZoneOut.HostedZone.Id)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()
		_, _ = client.DeleteHostedZone(cleanupCtx, &route53sdk.DeleteHostedZoneInput{Id: aws.String(zoneID)})
	})

	recordName := "www." + zoneName
	change := route53types.Change{
		Action: route53types.ChangeActionCreate,
		ResourceRecordSet: &route53types.ResourceRecordSet{
			Name: aws.String(recordName),
			Type: route53types.RRTypeA,
			TTL:  aws.Int64(120),
			ResourceRecords: []route53types.ResourceRecord{
				{Value: aws.String("10.0.0.1")},
				{Value: aws.String("10.0.0.2")},
			},
		},
	}

	_, err = client.ChangeResourceRecordSets(ctx, &route53sdk.ChangeResourceRecordSetsInput{
		HostedZoneId: aws.String(zoneID),
		ChangeBatch:  &route53types.ChangeBatch{Changes: []route53types.Change{change}},
	})
	require.NoError(t, err)

	listOut, err := client.ListResourceRecordSets(ctx, &route53sdk.ListResourceRecordSetsInput{
		HostedZoneId: aws.String(zoneID),
	})
	require.NoError(t, err)

	var found *route53types.ResourceRecordSet

	for i := range listOut.ResourceRecordSets {
		if aws.ToString(listOut.ResourceRecordSets[i].Name) == recordName+"." {
			found = &listOut.ResourceRecordSets[i]
		}
	}

	require.NotNil(t, found, "created record set should appear in ListResourceRecordSets")
	assert.Equal(t, route53types.RRTypeA, found.Type)
	assert.Equal(t, int64(120), aws.ToInt64(found.TTL))

	gotValues := make([]string, len(found.ResourceRecords))
	for i, rr := range found.ResourceRecords {
		gotValues[i] = aws.ToString(rr.Value)
	}

	assert.ElementsMatch(t, []string{"10.0.0.1", "10.0.0.2"}, gotValues)

	_, err = client.ChangeResourceRecordSets(ctx, &route53sdk.ChangeResourceRecordSetsInput{
		HostedZoneId: aws.String(zoneID),
		ChangeBatch: &route53types.ChangeBatch{Changes: []route53types.Change{
			{Action: route53types.ChangeActionDelete, ResourceRecordSet: change.ResourceRecordSet},
		}},
	})
	require.NoError(t, err)

	listOut2, err := client.ListResourceRecordSets(ctx, &route53sdk.ListResourceRecordSetsInput{
		HostedZoneId: aws.String(zoneID),
	})
	require.NoError(t, err)

	for _, rrs := range listOut2.ResourceRecordSets {
		assert.NotEqual(
			t, recordName+".", aws.ToString(rrs.Name),
			"deleted record set should not appear in ListResourceRecordSets",
		)
	}
}

// TestIntegration_Route53_HealthCheckRoundTrip drives CreateHealthCheck through a
// real client and reads the actual config back via GetHealthCheck -- neither op
// had any typed-client coverage before this test.
func TestIntegration_Route53_HealthCheckRoundTrip(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createRoute53Client(t)
	ctx := t.Context()

	callerRef := "hc-roundtrip-" + uuid.NewString()

	createOut, err := client.CreateHealthCheck(ctx, &route53sdk.CreateHealthCheckInput{
		CallerReference: aws.String(callerRef),
		HealthCheckConfig: &route53types.HealthCheckConfig{
			IPAddress:        aws.String("192.0.2.44"),
			Port:             aws.Int32(8080),
			Type:             route53types.HealthCheckTypeHttp,
			ResourcePath:     aws.String("/healthz"),
			RequestInterval:  aws.Int32(30),
			FailureThreshold: aws.Int32(3),
		},
	})
	require.NoError(t, err)
	require.NotNil(t, createOut.HealthCheck)
	hcID := aws.ToString(createOut.HealthCheck.Id)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()
		_, _ = client.DeleteHealthCheck(cleanupCtx, &route53sdk.DeleteHealthCheckInput{HealthCheckId: aws.String(hcID)})
	})

	getOut, err := client.GetHealthCheck(ctx, &route53sdk.GetHealthCheckInput{HealthCheckId: aws.String(hcID)})
	require.NoError(t, err)
	require.NotNil(t, getOut.HealthCheck)

	cfg := getOut.HealthCheck.HealthCheckConfig
	require.NotNil(t, cfg)
	assert.Equal(t, "192.0.2.44", aws.ToString(cfg.IPAddress))
	assert.Equal(t, int32(8080), aws.ToInt32(cfg.Port))
	assert.Equal(t, route53types.HealthCheckTypeHttp, cfg.Type)
	assert.Equal(t, "/healthz", aws.ToString(cfg.ResourcePath))
	assert.Equal(t, int32(30), aws.ToInt32(cfg.RequestInterval))
	assert.Equal(t, int32(3), aws.ToInt32(cfg.FailureThreshold))
	assert.Equal(t, callerRef, aws.ToString(getOut.HealthCheck.CallerReference))
}

// TestIntegration_Route53_ChangeTagsForResourceRoundTrip drives ChangeTagsForResource
// through a real client and reads the actual tag set back via ListTagsForResource --
// neither op had any typed-client coverage before this test.
func TestIntegration_Route53_ChangeTagsForResourceRoundTrip(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createRoute53Client(t)
	ctx := t.Context()

	suffix := time.Now().Format("20060102150405.000000")
	zoneName := "tags-roundtrip-" + suffix + ".example.com"

	createZoneOut, err := client.CreateHostedZone(ctx, &route53sdk.CreateHostedZoneInput{
		Name:            aws.String(zoneName),
		CallerReference: aws.String("tags-roundtrip-" + suffix),
	})
	require.NoError(t, err)
	zoneID := aws.ToString(createZoneOut.HostedZone.Id)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()
		_, _ = client.DeleteHostedZone(cleanupCtx, &route53sdk.DeleteHostedZoneInput{Id: aws.String(zoneID)})
	})

	_, err = client.ChangeTagsForResource(ctx, &route53sdk.ChangeTagsForResourceInput{
		ResourceType: route53types.TagResourceTypeHostedzone,
		ResourceId:   aws.String(zoneID),
		AddTags: []route53types.Tag{
			{Key: aws.String("env"), Value: aws.String("test")},
			{Key: aws.String("owner"), Value: aws.String("gopherstack")},
		},
	})
	require.NoError(t, err)

	listOut, err := client.ListTagsForResource(ctx, &route53sdk.ListTagsForResourceInput{
		ResourceType: route53types.TagResourceTypeHostedzone,
		ResourceId:   aws.String(zoneID),
	})
	require.NoError(t, err)
	require.NotNil(t, listOut.ResourceTagSet)

	got := make(map[string]string, len(listOut.ResourceTagSet.Tags))
	for _, tag := range listOut.ResourceTagSet.Tags {
		got[aws.ToString(tag.Key)] = aws.ToString(tag.Value)
	}

	assert.Equal(t, map[string]string{"env": "test", "owner": "gopherstack"}, got)

	_, err = client.ChangeTagsForResource(ctx, &route53sdk.ChangeTagsForResourceInput{
		ResourceType:  route53types.TagResourceTypeHostedzone,
		ResourceId:    aws.String(zoneID),
		RemoveTagKeys: []string{"env"},
	})
	require.NoError(t, err)

	listOut2, err := client.ListTagsForResource(ctx, &route53sdk.ListTagsForResourceInput{
		ResourceType: route53types.TagResourceTypeHostedzone,
		ResourceId:   aws.String(zoneID),
	})
	require.NoError(t, err)

	got2 := make(map[string]string, len(listOut2.ResourceTagSet.Tags))
	for _, tag := range listOut2.ResourceTagSet.Tags {
		got2[aws.ToString(tag.Key)] = aws.ToString(tag.Value)
	}

	assert.Equal(t, map[string]string{"owner": "gopherstack"}, got2, "removing env should leave owner intact")
}

// TestIntegration_Route53_DNSSECKeySigningKeyRoundTrip drives the full
// DNSSEC/KSK lifecycle through a real client -- CreateKeySigningKey,
// ActivateKeySigningKey, EnableHostedZoneDNSSEC, GetDNSSEC,
// DisableHostedZoneDNSSEC, DeactivateKeySigningKey, DeleteKeySigningKey --
// and asserts the actual KSK field values GetDNSSEC returns match what was
// created, not just a 200. This is the path gopherstack-6flj found
// returning zero key-signing keys to every client because a reused type's
// struct-level XMLName silently overrode the enclosing field tag; the fix
// (a separate xmlKSKMember type with no XMLName) had never been driven by a
// typed client since.
func TestIntegration_Route53_DNSSECKeySigningKeyRoundTrip(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createRoute53Client(t)
	ctx := t.Context()

	suffix := time.Now().Format("20060102150405.000000")
	zoneName := "dnssec-roundtrip-" + suffix + ".example.com"

	createZoneOut, err := client.CreateHostedZone(ctx, &route53sdk.CreateHostedZoneInput{
		Name:            aws.String(zoneName),
		CallerReference: aws.String("dnssec-roundtrip-" + suffix),
	})
	require.NoError(t, err)
	zoneID := aws.ToString(createZoneOut.HostedZone.Id)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()
		_, _ = client.DeleteHostedZone(cleanupCtx, &route53sdk.DeleteHostedZoneInput{Id: aws.String(zoneID)})
	})

	kskName := "ksk-" + uuid.NewString()[:8]
	kmsArn := "arn:aws:kms:us-east-1:123456789012:key/mrk-" + uuid.NewString()

	createKSKOut, err := client.CreateKeySigningKey(ctx, &route53sdk.CreateKeySigningKeyInput{
		CallerReference:         aws.String("ksk-ref-" + suffix),
		HostedZoneId:            aws.String(zoneID),
		Name:                    aws.String(kskName),
		KeyManagementServiceArn: aws.String(kmsArn),
		Status:                  aws.String("INACTIVE"),
	})
	require.NoError(t, err)
	require.NotNil(t, createKSKOut.KeySigningKey)
	assert.Equal(t, kskName, aws.ToString(createKSKOut.KeySigningKey.Name))
	assert.Equal(t, kmsArn, aws.ToString(createKSKOut.KeySigningKey.KmsArn))

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()
		_, _ = client.DeactivateKeySigningKey(cleanupCtx, &route53sdk.DeactivateKeySigningKeyInput{
			HostedZoneId: aws.String(zoneID), Name: aws.String(kskName),
		})
		_, _ = client.DeleteKeySigningKey(cleanupCtx, &route53sdk.DeleteKeySigningKeyInput{
			HostedZoneId: aws.String(zoneID), Name: aws.String(kskName),
		})
	})

	_, err = client.ActivateKeySigningKey(ctx, &route53sdk.ActivateKeySigningKeyInput{
		HostedZoneId: aws.String(zoneID), Name: aws.String(kskName),
	})
	require.NoError(t, err)

	_, err = client.EnableHostedZoneDNSSEC(ctx, &route53sdk.EnableHostedZoneDNSSECInput{
		HostedZoneId: aws.String(zoneID),
	})
	require.NoError(t, err)

	getOut, err := client.GetDNSSEC(ctx, &route53sdk.GetDNSSECInput{HostedZoneId: aws.String(zoneID)})
	require.NoError(t, err)
	require.NotNil(t, getOut.Status)
	assert.Equal(t, "SIGNING", aws.ToString(getOut.Status.ServeSignature))

	require.NotEmpty(t, getOut.KeySigningKeys, "GetDNSSEC must return the KSK just created and activated")

	var found *route53types.KeySigningKey

	for i := range getOut.KeySigningKeys {
		if aws.ToString(getOut.KeySigningKeys[i].Name) == kskName {
			found = &getOut.KeySigningKeys[i]
		}
	}

	require.NotNil(t, found, "created KSK %q should appear in GetDNSSEC's KeySigningKeys", kskName)
	assert.Equal(t, "ACTIVE", aws.ToString(found.Status))
	assert.Equal(t, kmsArn, aws.ToString(found.KmsArn))
	assert.NotEmpty(t, aws.ToString(found.DSRecord))
	assert.NotEmpty(t, aws.ToString(found.PublicKey))
	assert.NotEmpty(t, aws.ToString(found.DigestValue))
	assert.NotZero(t, found.KeyTag)
	assert.Equal(t, "ECDSAP256SHA256", aws.ToString(found.SigningAlgorithmMnemonic))

	_, err = client.DisableHostedZoneDNSSEC(ctx, &route53sdk.DisableHostedZoneDNSSECInput{
		HostedZoneId: aws.String(zoneID),
	})
	require.NoError(t, err)

	getOut2, err := client.GetDNSSEC(ctx, &route53sdk.GetDNSSECInput{HostedZoneId: aws.String(zoneID)})
	require.NoError(t, err)
	require.NotNil(t, getOut2.Status)
	assert.Equal(t, "NOT_SIGNING", aws.ToString(getOut2.Status.ServeSignature))

	_, err = client.DeactivateKeySigningKey(ctx, &route53sdk.DeactivateKeySigningKeyInput{
		HostedZoneId: aws.String(zoneID), Name: aws.String(kskName),
	})
	require.NoError(t, err)

	_, err = client.DeleteKeySigningKey(ctx, &route53sdk.DeleteKeySigningKeyInput{
		HostedZoneId: aws.String(zoneID), Name: aws.String(kskName),
	})
	require.NoError(t, err)

	getOut3, err := client.GetDNSSEC(ctx, &route53sdk.GetDNSSECInput{HostedZoneId: aws.String(zoneID)})
	require.NoError(t, err)

	for _, ksk := range getOut3.KeySigningKeys {
		assert.NotEqual(t, kskName, aws.ToString(ksk.Name), "deleted KSK should not appear in GetDNSSEC")
	}
}

// TestIntegration_Route53_PrivateHostedZoneVPCRoundTrip drives
// CreateHostedZone with an initial VPC (the standard way every typed
// client creates a private zone) and reads it back through GetHostedZone
// and ListHostedZonesByVPC, then exercises AssociateVPCWithHostedZone,
// DisassociateVPCFromHostedZone and the VPC association authorization
// family. Neither GetHostedZone.VPCs nor CreateHostedZone's VPC member had
// ever been driven by a typed client before this test.
func TestIntegration_Route53_PrivateHostedZoneVPCRoundTrip(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createRoute53Client(t)
	ctx := t.Context()

	suffix := time.Now().Format("20060102150405.000000")
	zoneName := "vpc-roundtrip-" + suffix + ".example.com"
	vpc1 := "vpc-" + uuid.NewString()[:12]
	vpc2 := "vpc-" + uuid.NewString()[:12]

	createZoneOut, err := client.CreateHostedZone(ctx, &route53sdk.CreateHostedZoneInput{
		Name:            aws.String(zoneName),
		CallerReference: aws.String("vpc-roundtrip-" + suffix),
		HostedZoneConfig: &route53types.HostedZoneConfig{
			PrivateZone: true,
		},
		VPC: &route53types.VPC{
			VPCId:     aws.String(vpc1),
			VPCRegion: route53types.VPCRegionUsEast1,
		},
	})
	require.NoError(t, err)
	zoneID := aws.ToString(createZoneOut.HostedZone.Id)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()
		_, _ = client.DeleteHostedZone(cleanupCtx, &route53sdk.DeleteHostedZoneInput{Id: aws.String(zoneID)})
	})

	getOut, err := client.GetHostedZone(ctx, &route53sdk.GetHostedZoneInput{Id: aws.String(zoneID)})
	require.NoError(t, err)
	require.Len(t, getOut.VPCs, 1, "the VPC supplied at creation should be recorded, not silently dropped")
	assert.Equal(t, vpc1, aws.ToString(getOut.VPCs[0].VPCId))
	assert.Equal(t, route53types.VPCRegionUsEast1, getOut.VPCs[0].VPCRegion)

	listByVPCOut, err := client.ListHostedZonesByVPC(ctx, &route53sdk.ListHostedZonesByVPCInput{
		VPCId:     aws.String(vpc1),
		VPCRegion: route53types.VPCRegionUsEast1,
	})
	require.NoError(t, err)

	var foundByVPC bool

	for _, s := range listByVPCOut.HostedZoneSummaries {
		if aws.ToString(s.HostedZoneId) == zoneID {
			foundByVPC = true
		}
	}

	assert.True(t, foundByVPC, "ListHostedZonesByVPC should find the zone via its creation-time VPC")

	_, err = client.AssociateVPCWithHostedZone(ctx, &route53sdk.AssociateVPCWithHostedZoneInput{
		HostedZoneId: aws.String(zoneID),
		VPC: &route53types.VPC{
			VPCId:     aws.String(vpc2),
			VPCRegion: route53types.VPCRegionUsWest2,
		},
	})
	require.NoError(t, err)

	getOut2, err := client.GetHostedZone(ctx, &route53sdk.GetHostedZoneInput{Id: aws.String(zoneID)})
	require.NoError(t, err)
	require.Len(t, getOut2.VPCs, 2, "both the creation-time VPC and the associated one should be present")

	gotVPCIDs := make([]string, len(getOut2.VPCs))
	for i, v := range getOut2.VPCs {
		gotVPCIDs[i] = aws.ToString(v.VPCId)
	}

	assert.ElementsMatch(t, []string{vpc1, vpc2}, gotVPCIDs)

	_, err = client.DisassociateVPCFromHostedZone(ctx, &route53sdk.DisassociateVPCFromHostedZoneInput{
		HostedZoneId: aws.String(zoneID),
		VPC: &route53types.VPC{
			VPCId:     aws.String(vpc2),
			VPCRegion: route53types.VPCRegionUsWest2,
		},
	})
	require.NoError(t, err)

	getOut3, err := client.GetHostedZone(ctx, &route53sdk.GetHostedZoneInput{Id: aws.String(zoneID)})
	require.NoError(t, err)
	require.Len(t, getOut3.VPCs, 1)
	assert.Equal(t, vpc1, aws.ToString(getOut3.VPCs[0].VPCId))

	authVPC := "vpc-" + uuid.NewString()[:12]

	createAuthOut, err := client.CreateVPCAssociationAuthorization(
		ctx,
		&route53sdk.CreateVPCAssociationAuthorizationInput{
			HostedZoneId: aws.String(zoneID),
			VPC: &route53types.VPC{
				VPCId:     aws.String(authVPC),
				VPCRegion: route53types.VPCRegionUsEast1,
			},
		},
	)
	require.NoError(t, err)
	assert.Equal(t, authVPC, aws.ToString(createAuthOut.VPC.VPCId))

	listAuthOut, err := client.ListVPCAssociationAuthorizations(ctx, &route53sdk.ListVPCAssociationAuthorizationsInput{
		HostedZoneId: aws.String(zoneID),
	})
	require.NoError(t, err)

	var foundAuth bool

	for _, v := range listAuthOut.VPCs {
		if aws.ToString(v.VPCId) == authVPC {
			foundAuth = true
		}
	}

	assert.True(t, foundAuth, "ListVPCAssociationAuthorizations should return the authorization just created")

	_, err = client.DeleteVPCAssociationAuthorization(ctx, &route53sdk.DeleteVPCAssociationAuthorizationInput{
		HostedZoneId: aws.String(zoneID),
		VPC: &route53types.VPC{
			VPCId:     aws.String(authVPC),
			VPCRegion: route53types.VPCRegionUsEast1,
		},
	})
	require.NoError(t, err)

	listAuthOut2, err := client.ListVPCAssociationAuthorizations(ctx, &route53sdk.ListVPCAssociationAuthorizationsInput{
		HostedZoneId: aws.String(zoneID),
	})
	require.NoError(t, err)

	for _, v := range listAuthOut2.VPCs {
		assert.NotEqual(t, authVPC, aws.ToString(v.VPCId), "deleted authorization should not appear in the list")
	}
}

// TestIntegration_Route53_CidrCollectionRoundTrip drives
// CreateCidrCollection, ChangeCidrCollection (PUT and DELETE_IF_EXISTS),
// ListCidrCollections, ListCidrBlocks and DeleteCidrCollection through a
// real client and asserts the actual CIDR blocks and collection metadata
// match, not just a 200. None of these ops had typed-client coverage
// before this test.
func TestIntegration_Route53_CidrCollectionRoundTrip(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createRoute53Client(t)
	ctx := t.Context()

	colName := "cidr-roundtrip-" + strings.ToLower(uuid.NewString()[:8])

	createOut, err := client.CreateCidrCollection(ctx, &route53sdk.CreateCidrCollectionInput{
		Name:            aws.String(colName),
		CallerReference: aws.String("cidr-ref-" + uuid.NewString()),
	})
	require.NoError(t, err)
	require.NotNil(t, createOut.Collection)
	colID := aws.ToString(createOut.Collection.Id)
	assert.Equal(t, colName, aws.ToString(createOut.Collection.Name))
	assert.Equal(t, int64(1), aws.ToInt64(createOut.Collection.Version))

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()
		_, _ = client.DeleteCidrCollection(cleanupCtx, &route53sdk.DeleteCidrCollectionInput{Id: aws.String(colID)})
	})

	changeOut, err := client.ChangeCidrCollection(ctx, &route53sdk.ChangeCidrCollectionInput{
		Id: aws.String(colID),
		Changes: []route53types.CidrCollectionChange{
			{
				Action:       route53types.CidrCollectionChangeActionPut,
				LocationName: aws.String("us-datacenter"),
				CidrList:     []string{"10.5.0.0/16", "10.6.0.0/16"},
			},
		},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(changeOut.Id))

	listOut, err := client.ListCidrCollections(ctx, &route53sdk.ListCidrCollectionsInput{})
	require.NoError(t, err)

	var foundSummary *route53types.CollectionSummary

	for i := range listOut.CidrCollections {
		if aws.ToString(listOut.CidrCollections[i].Id) == colID {
			foundSummary = &listOut.CidrCollections[i]
		}
	}

	require.NotNil(t, foundSummary, "created collection should appear in ListCidrCollections")
	assert.Equal(t, colName, aws.ToString(foundSummary.Name))
	assert.Equal(
		t,
		int64(2),
		aws.ToInt64(foundSummary.Version),
		"collection version should have incremented after ChangeCidrCollection",
	)

	blocksOut, err := client.ListCidrBlocks(ctx, &route53sdk.ListCidrBlocksInput{
		CollectionId: aws.String(colID),
		LocationName: aws.String("us-datacenter"),
	})
	require.NoError(t, err)
	require.Len(t, blocksOut.CidrBlocks, 2)

	gotCidrs := make([]string, len(blocksOut.CidrBlocks))
	for i, b := range blocksOut.CidrBlocks {
		gotCidrs[i] = aws.ToString(b.CidrBlock)
	}

	assert.ElementsMatch(t, []string{"10.5.0.0/16", "10.6.0.0/16"}, gotCidrs)

	_, err = client.ChangeCidrCollection(ctx, &route53sdk.ChangeCidrCollectionInput{
		Id: aws.String(colID),
		Changes: []route53types.CidrCollectionChange{
			{
				Action:       route53types.CidrCollectionChangeActionDeleteIfExists,
				LocationName: aws.String("us-datacenter"),
				CidrList:     []string{"10.5.0.0/16", "10.6.0.0/16"},
			},
		},
	})
	require.NoError(t, err)

	_, err = client.DeleteCidrCollection(ctx, &route53sdk.DeleteCidrCollectionInput{Id: aws.String(colID)})
	require.NoError(t, err, "an emptied collection should be deletable")

	_, err = client.ListCidrBlocks(ctx, &route53sdk.ListCidrBlocksInput{
		CollectionId: aws.String(colID),
		LocationName: aws.String("us-datacenter"),
	})
	assert.Error(t, err, "the collection should be gone after DeleteCidrCollection")
}

// TestIntegration_Route53_QueryLoggingConfigRoundTrip drives
// CreateQueryLoggingConfig, GetQueryLoggingConfig, ListQueryLoggingConfigs
// and DeleteQueryLoggingConfig through a real client. None had typed-client
// coverage before this test.
func TestIntegration_Route53_QueryLoggingConfigRoundTrip(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createRoute53Client(t)
	ctx := t.Context()

	suffix := time.Now().Format("20060102150405.000000")
	zoneName := "qlog-roundtrip-" + suffix + ".example.com"

	createZoneOut, err := client.CreateHostedZone(ctx, &route53sdk.CreateHostedZoneInput{
		Name:            aws.String(zoneName),
		CallerReference: aws.String("qlog-roundtrip-" + suffix),
	})
	require.NoError(t, err)
	zoneID := aws.ToString(createZoneOut.HostedZone.Id)
	// Unlike HostedZone.Id itself, QueryLoggingConfig.HostedZoneId is documented
	// (and returned) bare, with no "/hostedzone/" prefix -- confirmed against the
	// AWS API reference's GetQueryLoggingConfig example response.
	bareZoneID := strings.TrimPrefix(zoneID, "/hostedzone/")

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()
		_, _ = client.DeleteHostedZone(cleanupCtx, &route53sdk.DeleteHostedZoneInput{Id: aws.String(zoneID)})
	})

	logGroupArn := "arn:aws:logs:us-east-1:123456789012:log-group:/aws/route53/" + zoneName

	createOut, err := client.CreateQueryLoggingConfig(ctx, &route53sdk.CreateQueryLoggingConfigInput{
		HostedZoneId:              aws.String(zoneID),
		CloudWatchLogsLogGroupArn: aws.String(logGroupArn),
	})
	require.NoError(t, err)
	require.NotNil(t, createOut.QueryLoggingConfig)
	cfgID := aws.ToString(createOut.QueryLoggingConfig.Id)
	assert.Equal(t, bareZoneID, aws.ToString(createOut.QueryLoggingConfig.HostedZoneId))
	assert.Equal(t, logGroupArn, aws.ToString(createOut.QueryLoggingConfig.CloudWatchLogsLogGroupArn))

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()
		_, _ = client.DeleteQueryLoggingConfig(
			cleanupCtx,
			&route53sdk.DeleteQueryLoggingConfigInput{Id: aws.String(cfgID)},
		)
	})

	getOut, err := client.GetQueryLoggingConfig(ctx, &route53sdk.GetQueryLoggingConfigInput{Id: aws.String(cfgID)})
	require.NoError(t, err)
	require.NotNil(t, getOut.QueryLoggingConfig)
	assert.Equal(t, logGroupArn, aws.ToString(getOut.QueryLoggingConfig.CloudWatchLogsLogGroupArn))
	assert.Equal(t, bareZoneID, aws.ToString(getOut.QueryLoggingConfig.HostedZoneId))

	listOut, err := client.ListQueryLoggingConfigs(ctx, &route53sdk.ListQueryLoggingConfigsInput{
		HostedZoneId: aws.String(zoneID),
	})
	require.NoError(t, err)
	require.Len(t, listOut.QueryLoggingConfigs, 1)
	assert.Equal(t, cfgID, aws.ToString(listOut.QueryLoggingConfigs[0].Id))

	_, err = client.DeleteQueryLoggingConfig(ctx, &route53sdk.DeleteQueryLoggingConfigInput{Id: aws.String(cfgID)})
	require.NoError(t, err)

	_, err = client.GetQueryLoggingConfig(ctx, &route53sdk.GetQueryLoggingConfigInput{Id: aws.String(cfgID)})
	assert.Error(t, err, "the config should be gone after DeleteQueryLoggingConfig")
}

// TestIntegration_Route53_ReusableDelegationSetRoundTrip drives
// CreateReusableDelegationSet, GetReusableDelegationSet,
// ListReusableDelegationSets and DeleteReusableDelegationSet through a real
// client and asserts the actual name servers and CallerReference match.
// None had typed-client coverage before this test.
func TestIntegration_Route53_ReusableDelegationSetRoundTrip(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createRoute53Client(t)
	ctx := t.Context()

	callerRef := "rds-roundtrip-" + uuid.NewString()

	createOut, err := client.CreateReusableDelegationSet(ctx, &route53sdk.CreateReusableDelegationSetInput{
		CallerReference: aws.String(callerRef),
	})
	require.NoError(t, err)
	require.NotNil(t, createOut.DelegationSet)
	dsID := aws.ToString(createOut.DelegationSet.Id)
	require.NotEmpty(t, dsID)
	require.NotEmpty(t, createOut.DelegationSet.NameServers)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()
		_, _ = client.DeleteReusableDelegationSet(
			cleanupCtx,
			&route53sdk.DeleteReusableDelegationSetInput{Id: aws.String(dsID)},
		)
	})

	getOut, err := client.GetReusableDelegationSet(ctx, &route53sdk.GetReusableDelegationSetInput{Id: aws.String(dsID)})
	require.NoError(t, err)
	require.NotNil(t, getOut.DelegationSet)
	assert.Equal(t, callerRef, aws.ToString(getOut.DelegationSet.CallerReference))
	assert.Equal(t, createOut.DelegationSet.NameServers, getOut.DelegationSet.NameServers)

	listOut, err := client.ListReusableDelegationSets(ctx, &route53sdk.ListReusableDelegationSetsInput{})
	require.NoError(t, err)

	var found *route53types.DelegationSet

	for i := range listOut.DelegationSets {
		if aws.ToString(listOut.DelegationSets[i].Id) == dsID {
			found = &listOut.DelegationSets[i]
		}
	}

	require.NotNil(t, found, "created delegation set should appear in ListReusableDelegationSets")
	assert.Equal(t, callerRef, aws.ToString(found.CallerReference))

	_, err = client.DeleteReusableDelegationSet(ctx, &route53sdk.DeleteReusableDelegationSetInput{Id: aws.String(dsID)})
	require.NoError(t, err)

	_, err = client.GetReusableDelegationSet(ctx, &route53sdk.GetReusableDelegationSetInput{Id: aws.String(dsID)})
	assert.Error(t, err, "the delegation set should be gone after DeleteReusableDelegationSet")
}

// TestIntegration_Route53_TrafficPolicyInstanceRoundTrip drives
// CreateTrafficPolicyInstance, GetTrafficPolicyInstance,
// ListTrafficPolicyInstances, GetTrafficPolicyInstanceCount,
// UpdateTrafficPolicyInstance and DeleteTrafficPolicyInstance through a
// real client and asserts the actual field values, not just a 200. None
// had typed-client coverage before this test (a raw-body test covers the
// same lifecycle in route53_new_ops_test.go, but never through a typed
// client that has to decode the response).
func TestIntegration_Route53_TrafficPolicyInstanceRoundTrip(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createRoute53Client(t)
	ctx := t.Context()

	suffix := time.Now().Format("20060102150405.000000")
	zoneName := "tpi-roundtrip-" + suffix + ".example.com"

	createZoneOut, err := client.CreateHostedZone(ctx, &route53sdk.CreateHostedZoneInput{
		Name:            aws.String(zoneName),
		CallerReference: aws.String("tpi-roundtrip-" + suffix),
	})
	require.NoError(t, err)
	zoneID := aws.ToString(createZoneOut.HostedZone.Id)
	// TrafficPolicyInstance.HostedZoneId is documented bare (max length 32,
	// too short to fit the "/hostedzone/" prefix plus a real zone ID), the
	// same convention confirmed for QueryLoggingConfig.HostedZoneId above.
	bareZoneID := strings.TrimPrefix(zoneID, "/hostedzone/")

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()
		_, _ = client.DeleteHostedZone(cleanupCtx, &route53sdk.DeleteHostedZoneInput{Id: aws.String(zoneID)})
	})

	tpOut, err := client.CreateTrafficPolicy(ctx, &route53sdk.CreateTrafficPolicyInput{
		Name:     aws.String("tpi-roundtrip-policy-" + suffix),
		Document: aws.String(`{"AWSPolicyFormatVersion":"2015-10-01","RecordType":"A","Endpoints":{},"Rules":{}}`),
	})
	require.NoError(t, err)
	tpID := aws.ToString(tpOut.TrafficPolicy.Id)

	beforeCountOut, err := client.GetTrafficPolicyInstanceCount(ctx, &route53sdk.GetTrafficPolicyInstanceCountInput{})
	require.NoError(t, err)
	beforeCount := aws.ToInt32(beforeCountOut.TrafficPolicyInstanceCount)

	recordName := "tpi." + zoneName

	createInstOut, err := client.CreateTrafficPolicyInstance(ctx, &route53sdk.CreateTrafficPolicyInstanceInput{
		HostedZoneId:         aws.String(zoneID),
		Name:                 aws.String(recordName),
		TrafficPolicyId:      aws.String(tpID),
		TrafficPolicyVersion: aws.Int32(1),
		TTL:                  aws.Int64(60),
	})
	require.NoError(t, err)
	require.NotNil(t, createInstOut.TrafficPolicyInstance)
	instID := aws.ToString(createInstOut.TrafficPolicyInstance.Id)
	assert.Equal(t, int64(60), aws.ToInt64(createInstOut.TrafficPolicyInstance.TTL))

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()
		_, _ = client.DeleteTrafficPolicyInstance(
			cleanupCtx,
			&route53sdk.DeleteTrafficPolicyInstanceInput{Id: aws.String(instID)},
		)
	})

	getOut, err := client.GetTrafficPolicyInstance(
		ctx,
		&route53sdk.GetTrafficPolicyInstanceInput{Id: aws.String(instID)},
	)
	require.NoError(t, err)
	require.NotNil(t, getOut.TrafficPolicyInstance)
	assert.Equal(t, recordName+".", aws.ToString(getOut.TrafficPolicyInstance.Name))
	assert.Equal(t, bareZoneID, aws.ToString(getOut.TrafficPolicyInstance.HostedZoneId))
	assert.Equal(t, tpID, aws.ToString(getOut.TrafficPolicyInstance.TrafficPolicyId))

	listOut, err := client.ListTrafficPolicyInstances(ctx, &route53sdk.ListTrafficPolicyInstancesInput{})
	require.NoError(t, err)

	var found bool

	for _, inst := range listOut.TrafficPolicyInstances {
		if aws.ToString(inst.Id) == instID {
			found = true
		}
	}

	assert.True(t, found, "created instance should appear in ListTrafficPolicyInstances")

	afterCountOut, err := client.GetTrafficPolicyInstanceCount(ctx, &route53sdk.GetTrafficPolicyInstanceCountInput{})
	require.NoError(t, err)
	assert.Equal(t, beforeCount+1, aws.ToInt32(afterCountOut.TrafficPolicyInstanceCount))

	updateOut, err := client.UpdateTrafficPolicyInstance(ctx, &route53sdk.UpdateTrafficPolicyInstanceInput{
		Id:                   aws.String(instID),
		TrafficPolicyId:      aws.String(tpID),
		TrafficPolicyVersion: aws.Int32(1),
		TTL:                  aws.Int64(120),
	})
	require.NoError(t, err)
	assert.Equal(t, int64(120), aws.ToInt64(updateOut.TrafficPolicyInstance.TTL))

	getOut2, err := client.GetTrafficPolicyInstance(
		ctx,
		&route53sdk.GetTrafficPolicyInstanceInput{Id: aws.String(instID)},
	)
	require.NoError(t, err)
	assert.Equal(
		t,
		int64(120),
		aws.ToInt64(getOut2.TrafficPolicyInstance.TTL),
		"GetTrafficPolicyInstance should observe the TTL update",
	)

	_, err = client.DeleteTrafficPolicyInstance(
		ctx,
		&route53sdk.DeleteTrafficPolicyInstanceInput{Id: aws.String(instID)},
	)
	require.NoError(t, err)

	_, err = client.GetTrafficPolicyInstance(ctx, &route53sdk.GetTrafficPolicyInstanceInput{Id: aws.String(instID)})
	assert.Error(t, err, "the instance should be gone after DeleteTrafficPolicyInstance")
}
