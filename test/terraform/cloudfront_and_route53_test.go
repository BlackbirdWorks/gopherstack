package terraform_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cloudfrontsvc "github.com/aws/aws-sdk-go-v2/service/cloudfront"
	cftypes "github.com/aws/aws-sdk-go-v2/service/cloudfront/types"
	route53svc "github.com/aws/aws-sdk-go-v2/service/route53"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTerraform_CloudfrontAndRoute53 provisions the 14 CloudFront resource types
// (cache policy, continuous deployment policy, distribution, field-level
// encryption config/profile, function, key group, monitoring subscription,
// origin access control, origin request policy, public key, real-time log
// config, response headers policy, VPC origin) and the 12 Route53 resource
// types (CIDR collection/location, delegation set, health check, hosted
// zone DNSSEC, key signing key, query log, records exclusive, traffic
// policy/instance, VPC association authorization, zone association) that
// had no Terraform fixture coverage, and verifies each via its own SDK
// client's Get/List path.
func TestTerraform_CloudfrontAndRoute53(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "cloudfront-and-route53",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				return map[string]any{}
			},
			verify: func(t *testing.T, ctx context.Context, _ map[string]any) {
				t.Helper()
				cfg := megaConfig(t)

				cf := cloudfrontsvc.NewFromConfig(cfg, func(o *cloudfrontsvc.Options) {
					o.BaseEndpoint = aws.String(endpoint)
				})
				r53 := route53svc.NewFromConfig(cfg, func(o *route53svc.Options) {
					o.BaseEndpoint = aws.String(endpoint)
				})

				verifyCloudfrontAndRoute53CloudFront(ctx, t, cf)
				verifyCloudfrontAndRoute53Route53(ctx, t, r53)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTFTest(t, tc)
		})
	}
}

func verifyCloudfrontAndRoute53CloudFront(ctx context.Context, t *testing.T, cf *cloudfrontsvc.Client) {
	t.Helper()

	pkOut, err := cf.ListPublicKeys(ctx, &cloudfrontsvc.ListPublicKeysInput{})
	require.NoError(t, err, "ListPublicKeys should succeed")

	var publicKeyID string

	for _, pk := range pkOut.PublicKeyList.Items {
		if aws.ToString(pk.Name) == "cfr5-public-key" {
			publicKeyID = aws.ToString(pk.Id)
		}
	}

	require.NotEmpty(t, publicKeyID, "public key cfr5-public-key should be listed")

	kgOut, err := cf.ListKeyGroups(ctx, &cloudfrontsvc.ListKeyGroupsInput{})
	require.NoError(t, err, "ListKeyGroups should succeed")

	var foundKeyGroup bool

	for _, kg := range kgOut.KeyGroupList.Items {
		if aws.ToString(kg.KeyGroup.KeyGroupConfig.Name) == "cfr5-key-group" {
			foundKeyGroup = true

			require.Contains(t, kg.KeyGroup.KeyGroupConfig.Items, publicKeyID)
		}
	}

	assert.True(t, foundKeyGroup, "key group cfr5-key-group should be listed")

	fleProfileOut, err := cf.ListFieldLevelEncryptionProfiles(
		ctx,
		&cloudfrontsvc.ListFieldLevelEncryptionProfilesInput{},
	)
	require.NoError(t, err, "ListFieldLevelEncryptionProfiles should succeed")

	var fleProfileID string

	for _, p := range fleProfileOut.FieldLevelEncryptionProfileList.Items {
		if aws.ToString(p.Name) == "cfr5-fle-profile" {
			fleProfileID = aws.ToString(p.Id)
		}
	}

	require.NotEmpty(t, fleProfileID, "field-level encryption profile should be listed")

	fleConfigOut, err := cf.ListFieldLevelEncryptionConfigs(ctx, &cloudfrontsvc.ListFieldLevelEncryptionConfigsInput{})
	require.NoError(t, err, "ListFieldLevelEncryptionConfigs should succeed")

	var foundFLEConfig bool

	for _, c := range fleConfigOut.FieldLevelEncryptionList.Items {
		if aws.ToString(c.Comment) == "cfr5 field level encryption config" {
			foundFLEConfig = true
		}
	}

	assert.True(t, foundFLEConfig, "field-level encryption config should be listed")

	oacOut, err := cf.ListOriginAccessControls(ctx, &cloudfrontsvc.ListOriginAccessControlsInput{})
	require.NoError(t, err, "ListOriginAccessControls should succeed")

	var foundOAC bool

	for _, o := range oacOut.OriginAccessControlList.Items {
		if aws.ToString(o.Name) == "cfr5-oac" {
			foundOAC = true
		}
	}

	assert.True(t, foundOAC, "origin access control should be listed")

	orpOut, err := cf.ListOriginRequestPolicies(ctx, &cloudfrontsvc.ListOriginRequestPoliciesInput{
		Type: cftypes.OriginRequestPolicyTypeCustom,
	})
	require.NoError(t, err, "ListOriginRequestPolicies should succeed")

	var foundORP bool

	for _, o := range orpOut.OriginRequestPolicyList.Items {
		if aws.ToString(o.OriginRequestPolicy.OriginRequestPolicyConfig.Name) == "cfr5-orp" {
			foundORP = true
		}
	}

	assert.True(t, foundORP, "origin request policy should be listed")

	rhpOut, err := cf.ListResponseHeadersPolicies(ctx, &cloudfrontsvc.ListResponseHeadersPoliciesInput{
		Type: cftypes.ResponseHeadersPolicyTypeCustom,
	})
	require.NoError(t, err, "ListResponseHeadersPolicies should succeed")

	var foundRHP bool

	for _, r := range rhpOut.ResponseHeadersPolicyList.Items {
		if aws.ToString(r.ResponseHeadersPolicy.ResponseHeadersPolicyConfig.Name) == "cfr5-rhp" {
			foundRHP = true
		}
	}

	assert.True(t, foundRHP, "response headers policy should be listed")

	cpOut, err := cf.ListCachePolicies(ctx, &cloudfrontsvc.ListCachePoliciesInput{
		Type: cftypes.CachePolicyTypeCustom,
	})
	require.NoError(t, err, "ListCachePolicies should succeed")

	var foundCachePolicy bool

	for _, c := range cpOut.CachePolicyList.Items {
		if aws.ToString(c.CachePolicy.CachePolicyConfig.Name) == "cfr5-cache-policy" {
			foundCachePolicy = true
		}
	}

	assert.True(t, foundCachePolicy, "cache policy should be listed")

	fnOut, err := cf.GetFunction(ctx, &cloudfrontsvc.GetFunctionInput{
		Name: aws.String("cfr5-function"),
	})
	require.NoError(t, err, "GetFunction should succeed")
	assert.Contains(t, string(fnOut.FunctionCode), "return event.request")

	rtlOut, err := cf.GetRealtimeLogConfig(ctx, &cloudfrontsvc.GetRealtimeLogConfigInput{
		Name: aws.String("cfr5-realtime-log-config"),
	})
	require.NoError(t, err, "GetRealtimeLogConfig should succeed")
	assert.EqualValues(t, 75, aws.ToInt64(rtlOut.RealtimeLogConfig.SamplingRate))

	distOut, err := cf.ListDistributions(ctx, &cloudfrontsvc.ListDistributionsInput{})
	require.NoError(t, err, "ListDistributions should succeed")

	var distributionID string

	for _, d := range distOut.DistributionList.Items {
		if aws.ToString(d.Comment) == "cfr5 staging distribution" {
			distributionID = aws.ToString(d.Id)
		}
	}

	require.NotEmpty(t, distributionID, "staging distribution should be listed")

	monOut, err := cf.GetMonitoringSubscription(ctx, &cloudfrontsvc.GetMonitoringSubscriptionInput{
		DistributionId: aws.String(distributionID),
	})
	require.NoError(t, err, "GetMonitoringSubscription should succeed")
	require.NotNil(t, monOut.MonitoringSubscription.RealtimeMetricsSubscriptionConfig)
	assert.Equal(
		t,
		cftypes.RealtimeMetricsSubscriptionStatusEnabled,
		monOut.MonitoringSubscription.RealtimeMetricsSubscriptionConfig.RealtimeMetricsSubscriptionStatus,
	)

	cdpOut, err := cf.ListContinuousDeploymentPolicies(ctx, &cloudfrontsvc.ListContinuousDeploymentPoliciesInput{})
	require.NoError(t, err, "ListContinuousDeploymentPolicies should succeed")

	var foundCDP bool

	for _, p := range cdpOut.ContinuousDeploymentPolicyList.Items {
		for _, dns := range p.ContinuousDeploymentPolicy.ContinuousDeploymentPolicyConfig.StagingDistributionDnsNames.Items {
			if dns != "" {
				foundCDP = true
			}
		}
	}

	assert.True(t, foundCDP, "continuous deployment policy should be listed")

	vpcOriginOut, err := cf.ListVpcOrigins(ctx, &cloudfrontsvc.ListVpcOriginsInput{})
	require.NoError(t, err, "ListVpcOrigins should succeed")

	var foundVPCOrigin bool

	for _, v := range vpcOriginOut.VpcOriginList.Items {
		if aws.ToString(v.Name) == "cfr5-vpc-origin" {
			foundVPCOrigin = true
		}
	}

	assert.True(t, foundVPCOrigin, "VPC origin should be listed")
}

func verifyCloudfrontAndRoute53Route53(ctx context.Context, t *testing.T, r53 *route53svc.Client) {
	t.Helper()

	dsOut, err := r53.ListReusableDelegationSets(ctx, &route53svc.ListReusableDelegationSetsInput{})
	require.NoError(t, err, "ListReusableDelegationSets should succeed")

	var foundDelegationSet bool

	for _, ds := range dsOut.DelegationSets {
		if len(ds.NameServers) > 0 {
			foundDelegationSet = true
		}
	}

	assert.True(t, foundDelegationSet, "a reusable delegation set with name servers should be listed")

	hcOut, err := r53.ListHealthChecks(ctx, &route53svc.ListHealthChecksInput{})
	require.NoError(t, err, "ListHealthChecks should succeed")

	var foundHealthCheck bool

	for _, hc := range hcOut.HealthChecks {
		if aws.ToString(hc.HealthCheckConfig.FullyQualifiedDomainName) == "cfr5.example.com" {
			foundHealthCheck = true
		}
	}

	assert.True(t, foundHealthCheck, "health check cfr5.example.com should be listed")

	collOut, err := r53.ListCidrCollections(ctx, &route53svc.ListCidrCollectionsInput{})
	require.NoError(t, err, "ListCidrCollections should succeed")

	var collectionID string

	for _, c := range collOut.CidrCollections {
		if aws.ToString(c.Name) == "cfr5-cidr-collection" {
			collectionID = aws.ToString(c.Id)
		}
	}

	require.NotEmpty(t, collectionID, "CIDR collection should be listed")

	blocksOut, err := r53.ListCidrBlocks(ctx, &route53svc.ListCidrBlocksInput{
		CollectionId: aws.String(collectionID),
	})
	require.NoError(t, err, "ListCidrBlocks should succeed")

	var foundCidrBlock bool

	for _, b := range blocksOut.CidrBlocks {
		if aws.ToString(b.LocationName) == "cfr5-location" && aws.ToString(b.CidrBlock) == "10.114.32.0/24" {
			foundCidrBlock = true
		}
	}

	assert.True(t, foundCidrBlock, "CIDR location cfr5-location should be listed")

	zonesOut, err := r53.ListHostedZonesByName(ctx, &route53svc.ListHostedZonesByNameInput{
		DNSName: aws.String("cfr5-dnssec.example.com"),
	})
	require.NoError(t, err, "ListHostedZonesByName should succeed")
	require.NotEmpty(t, zonesOut.HostedZones, "dnssec hosted zone should exist")

	dnssecZoneID := aws.ToString(zonesOut.HostedZones[0].Id)

	dnssecOut, err := r53.GetDNSSEC(ctx, &route53svc.GetDNSSECInput{
		HostedZoneId: aws.String(dnssecZoneID),
	})
	require.NoError(t, err, "GetDNSSEC should succeed")
	require.NotNil(t, dnssecOut.Status)
	assert.Equal(t, "SIGNING", aws.ToString(dnssecOut.Status.ServeSignature))

	var foundKSK bool

	for _, ksk := range dnssecOut.KeySigningKeys {
		if aws.ToString(ksk.Name) == "cfr5_ksk" {
			foundKSK = true
		}
	}

	assert.True(t, foundKSK, "key signing key cfr5_ksk should be listed")

	qlOut, err := r53.ListQueryLoggingConfigs(ctx, &route53svc.ListQueryLoggingConfigsInput{
		HostedZoneId: aws.String(dnssecZoneID),
	})
	require.NoError(t, err, "ListQueryLoggingConfigs should succeed")
	require.NotEmpty(t, qlOut.QueryLoggingConfigs, "query logging config should be listed")

	tpOut, err := r53.ListTrafficPolicies(ctx, &route53svc.ListTrafficPoliciesInput{})
	require.NoError(t, err, "ListTrafficPolicies should succeed")

	var foundTrafficPolicy bool

	for _, tp := range tpOut.TrafficPolicySummaries {
		if aws.ToString(tp.Name) == "cfr5-traffic-policy" {
			foundTrafficPolicy = true
		}
	}

	assert.True(t, foundTrafficPolicy, "traffic policy should be listed")

	tpiOut, err := r53.ListTrafficPolicyInstances(ctx, &route53svc.ListTrafficPolicyInstancesInput{})
	require.NoError(t, err, "ListTrafficPolicyInstances should succeed")

	var foundTrafficPolicyInstance bool

	for _, tpi := range tpiOut.TrafficPolicyInstances {
		if aws.ToString(tpi.Name) == "tp.cfr5-dnssec.example.com." {
			foundTrafficPolicyInstance = true
		}
	}

	assert.True(t, foundTrafficPolicyInstance, "traffic policy instance should be listed")

	exclZonesOut, err := r53.ListHostedZonesByName(ctx, &route53svc.ListHostedZonesByNameInput{
		DNSName: aws.String("cfr5-excl.example.com"),
	})
	require.NoError(t, err, "ListHostedZonesByName should succeed")
	require.NotEmpty(t, exclZonesOut.HostedZones, "excl hosted zone should exist")

	rrOut, err := r53.ListResourceRecordSets(ctx, &route53svc.ListResourceRecordSetsInput{
		HostedZoneId: exclZonesOut.HostedZones[0].Id,
	})
	require.NoError(t, err, "ListResourceRecordSets should succeed")

	var foundExclusiveRecord bool

	for _, rr := range rrOut.ResourceRecordSets {
		if aws.ToString(rr.Name) == "sub.cfr5-excl.example.com." {
			foundExclusiveRecord = true
		}
	}

	assert.True(t, foundExclusiveRecord, "record created by records_exclusive should be listed")

	privZonesOut, err := r53.ListHostedZonesByName(ctx, &route53svc.ListHostedZonesByNameInput{
		DNSName: aws.String("cfr5-private.internal"),
	})
	require.NoError(t, err, "ListHostedZonesByName should succeed")
	require.NotEmpty(t, privZonesOut.HostedZones, "private hosted zone should exist")

	privZoneOut, err := r53.GetHostedZone(ctx, &route53svc.GetHostedZoneInput{
		Id: privZonesOut.HostedZones[0].Id,
	})
	require.NoError(t, err, "GetHostedZone should succeed")
	assert.Len(t, privZoneOut.VPCs, 2, "both VPCs should be associated with the private hosted zone")
}
