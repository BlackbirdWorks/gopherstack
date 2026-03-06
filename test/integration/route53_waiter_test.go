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

// TestIntegration_Route53_ResourceRecordSetsChangedWaiter verifies that
// ResourceRecordSetsChangedWaiter succeeds immediately because ChangeResourceRecordSets
// returns INSYNC status and GetChange also returns INSYNC.
func TestIntegration_Route53_ResourceRecordSetsChangedWaiter(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createRoute53Client(t)
	ctx := t.Context()

	// Create a hosted zone
	createZoneOut, err := client.CreateHostedZone(ctx, &route53sdk.CreateHostedZoneInput{
		Name:            aws.String("waiter-test.example.com"),
		CallerReference: aws.String("waiter-ref-" + time.Now().Format("20060102150405")),
	})
	require.NoError(t, err)
	require.NotNil(t, createZoneOut.HostedZone)

	zoneID := aws.ToString(createZoneOut.HostedZone.Id)

	t.Cleanup(func() {
		_, _ = client.DeleteHostedZone(ctx, &route53sdk.DeleteHostedZoneInput{Id: aws.String(zoneID)})
	})

	// Apply a record change
	changeOut, err := client.ChangeResourceRecordSets(ctx, &route53sdk.ChangeResourceRecordSetsInput{
		HostedZoneId: aws.String(zoneID),
		ChangeBatch: &route53types.ChangeBatch{
			Changes: []route53types.Change{
				{
					Action: route53types.ChangeActionCreate,
					ResourceRecordSet: &route53types.ResourceRecordSet{
						Name: aws.String("www.waiter-test.example.com"),
						Type: route53types.RRTypeA,
						TTL:  aws.Int64(300),
						ResourceRecords: []route53types.ResourceRecord{
							{Value: aws.String("1.2.3.4")},
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, changeOut.ChangeInfo)
	assert.Equal(t, route53types.ChangeStatusInsync, changeOut.ChangeInfo.Status)

	changeID := aws.ToString(changeOut.ChangeInfo.Id)

	waiter := route53sdk.NewResourceRecordSetsChangedWaiter(client)
	start := time.Now()
	err = waiter.Wait(ctx, &route53sdk.GetChangeInput{
		Id: aws.String(changeID),
	}, 30*time.Second)
	elapsed := time.Since(start)

	require.NoError(t, err, "ResourceRecordSetsChangedWaiter should succeed because change returns INSYNC")
	assert.Less(t, elapsed, 2*time.Second, "ResourceRecordSetsChangedWaiter should complete quickly, took %v", elapsed)
}
