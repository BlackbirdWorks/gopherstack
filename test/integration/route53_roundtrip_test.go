package integration_test

import (
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
