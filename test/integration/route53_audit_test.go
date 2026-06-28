package integration_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	route53sdk "github.com/aws/aws-sdk-go-v2/service/route53"
	route53types "github.com/aws/aws-sdk-go-v2/service/route53/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_Route53Audit_GetAccountLimit verifies that
// GetAccountLimit(MAX_HOSTED_ZONES_BY_OWNER) reports a real usage count that
// reflects the hosted zones actually present in the account, rather than a
// hardcoded zero.
func TestIntegration_Route53Audit_GetAccountLimit(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createRoute53Client(t)
	ctx := t.Context()

	const numZones = 3
	suffix := time.Now().Format("20060102150405.000000")

	for i := range numZones {
		ref := "audit-acctlimit-" + suffix + "-" + string(rune('a'+i))
		out, err := client.CreateHostedZone(ctx, &route53sdk.CreateHostedZoneInput{
			Name:            aws.String("audit-acctlimit-" + suffix + "-" + string(rune('a'+i)) + ".example.com"),
			CallerReference: aws.String(ref),
		})
		require.NoError(t, err)
		require.NotNil(t, out.HostedZone)

		zoneID := aws.ToString(out.HostedZone.Id)
		t.Cleanup(func() {
			_, _ = client.DeleteHostedZone(ctx, &route53sdk.DeleteHostedZoneInput{Id: aws.String(zoneID)})
		})
	}

	limitOut, err := client.GetAccountLimit(ctx, &route53sdk.GetAccountLimitInput{
		Type: route53types.AccountLimitTypeMaxHostedZonesByOwner,
	})
	require.NoError(t, err)
	require.NotNil(t, limitOut.Limit)

	assert.GreaterOrEqual(t, limitOut.Count, int64(numZones),
		"GetAccountLimit count should reflect the number of hosted zones in the account")
	assert.Equal(t, route53types.AccountLimitTypeMaxHostedZonesByOwner, limitOut.Limit.Type)
	assert.Positive(t, limitOut.Limit.Value, "documented limit value should be populated")
}

// TestIntegration_Route53Audit_GetHostedZoneLimit verifies that
// GetHostedZoneLimit(MAX_RRSETS_BY_ZONE) returns the actual resource record set
// count for the referenced zone, rather than a hardcoded zero.
func TestIntegration_Route53Audit_GetHostedZoneLimit(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createRoute53Client(t)
	ctx := t.Context()

	suffix := time.Now().Format("20060102150405.000000")
	zoneName := "audit-hzlimit-" + suffix + ".example.com"

	createOut, err := client.CreateHostedZone(ctx, &route53sdk.CreateHostedZoneInput{
		Name:            aws.String(zoneName),
		CallerReference: aws.String("audit-hzlimit-" + suffix),
	})
	require.NoError(t, err)
	require.NotNil(t, createOut.HostedZone)

	zoneID := aws.ToString(createOut.HostedZone.Id)
	t.Cleanup(func() {
		_, _ = client.DeleteHostedZone(ctx, &route53sdk.DeleteHostedZoneInput{Id: aws.String(zoneID)})
	})

	// Baseline RRSet count (a new zone has its default SOA + NS records).
	baseOut, err := client.GetHostedZoneLimit(ctx, &route53sdk.GetHostedZoneLimitInput{
		HostedZoneId: aws.String(zoneID),
		Type:         route53types.HostedZoneLimitTypeMaxRrsetsByZone,
	})
	require.NoError(t, err)
	require.NotNil(t, baseOut.Limit)
	baseCount := baseOut.Count

	// Add two additional record sets.
	added := []string{"www." + zoneName, "api." + zoneName}
	changes := make([]route53types.Change, 0, len(added))
	for i, name := range added {
		changes = append(changes, route53types.Change{
			Action: route53types.ChangeActionCreate,
			ResourceRecordSet: &route53types.ResourceRecordSet{
				Name: aws.String(name),
				Type: route53types.RRTypeA,
				TTL:  aws.Int64(300),
				ResourceRecords: []route53types.ResourceRecord{
					{Value: aws.String("1.2.3." + string(rune('1'+i)))},
				},
			},
		})
	}

	_, err = client.ChangeResourceRecordSets(ctx, &route53sdk.ChangeResourceRecordSetsInput{
		HostedZoneId: aws.String(zoneID),
		ChangeBatch:  &route53types.ChangeBatch{Changes: changes},
	})
	require.NoError(t, err)

	afterOut, err := client.GetHostedZoneLimit(ctx, &route53sdk.GetHostedZoneLimitInput{
		HostedZoneId: aws.String(zoneID),
		Type:         route53types.HostedZoneLimitTypeMaxRrsetsByZone,
	})
	require.NoError(t, err)
	require.NotNil(t, afterOut.Limit)

	assert.Equal(t, route53types.HostedZoneLimitTypeMaxRrsetsByZone, afterOut.Limit.Type)
	assert.Positive(t, afterOut.Count,
		"GetHostedZoneLimit count should reflect the real RRSet count in the zone")
	assert.Equal(t, baseCount+int64(len(added)), afterOut.Count,
		"RRSet count should increase by the number of records added")
}

// TestIntegration_Route53Audit_GetHostedZoneLimit_NotFound verifies that
// GetHostedZoneLimit returns NoSuchHostedZone for a non-existent zone.
func TestIntegration_Route53Audit_GetHostedZoneLimit_NotFound(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createRoute53Client(t)
	ctx := t.Context()

	_, err := client.GetHostedZoneLimit(ctx, &route53sdk.GetHostedZoneLimitInput{
		HostedZoneId: aws.String("Z0000000000000NONEXIST"),
		Type:         route53types.HostedZoneLimitTypeMaxRrsetsByZone,
	})
	require.Error(t, err, "GetHostedZoneLimit on a missing zone should return an error")

	var nshz *route53types.NoSuchHostedZone
	assert.ErrorAs(t, err, &nshz, "expected NoSuchHostedZone error")
}
