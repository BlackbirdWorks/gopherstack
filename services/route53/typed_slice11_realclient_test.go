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

// TestTypedSlice11RealClient drives route53's typed-coverage-blind ops
// (gopherstack-n3zi slice 11) through the real aws-sdk-go-v2 client.
func TestTypedSlice11RealClient(t *testing.T) {
	t.Parallel()

	t.Run("hosted zone count and comment", func(t *testing.T) {
		t.Parallel()

		backend := route53.NewInMemoryBackend()
		client := newTestRoute53Client(t, route53.NewHandler(backend))
		ctx := t.Context()

		before, err := client.GetHostedZoneCount(ctx, &route53sdk.GetHostedZoneCountInput{})
		require.NoError(t, err)

		createOut, err := client.CreateHostedZone(ctx, &route53sdk.CreateHostedZoneInput{
			Name:            aws.String("s11-example.com."),
			CallerReference: aws.String("s11-caller-ref"),
		})
		require.NoError(t, err)
		zoneID := aws.ToString(createOut.HostedZone.Id)

		afterOut, err := client.GetHostedZoneCount(ctx, &route53sdk.GetHostedZoneCountInput{})
		require.NoError(t, err)
		assert.Equal(t, aws.ToInt64(before.HostedZoneCount)+1, aws.ToInt64(afterOut.HostedZoneCount))

		updOut, err := client.UpdateHostedZoneComment(ctx, &route53sdk.UpdateHostedZoneCommentInput{
			Id:      aws.String(zoneID),
			Comment: aws.String("s11 comment"),
		})
		require.NoError(t, err)
		assert.Equal(t, "s11 comment", aws.ToString(updOut.HostedZone.Config.Comment))
	})

	t.Run("get change", func(t *testing.T) {
		t.Parallel()

		client := newTestRoute53Client(t, newHandler(t))
		ctx := t.Context()

		createOut, err := client.CreateHostedZone(ctx, &route53sdk.CreateHostedZoneInput{
			Name:            aws.String("s11-change.com."),
			CallerReference: aws.String("s11-change-ref"),
		})
		require.NoError(t, err)
		zoneID := aws.ToString(createOut.HostedZone.Id)

		changeOut, err := client.ChangeResourceRecordSets(ctx, &route53sdk.ChangeResourceRecordSetsInput{
			HostedZoneId: aws.String(zoneID),
			ChangeBatch: &types.ChangeBatch{
				Changes: []types.Change{
					{
						Action: types.ChangeActionUpsert,
						ResourceRecordSet: &types.ResourceRecordSet{
							Name: aws.String("www.s11-change.com."),
							Type: types.RRTypeA,
							TTL:  aws.Int64(300),
							ResourceRecords: []types.ResourceRecord{
								{Value: aws.String("1.2.3.4")},
							},
						},
					},
				},
			},
		})
		require.NoError(t, err)
		changeID := aws.ToString(changeOut.ChangeInfo.Id)
		require.NotEmpty(t, changeID)

		getOut, err := client.GetChange(ctx, &route53sdk.GetChangeInput{
			Id: aws.String(changeID),
		})
		require.NoError(t, err)
		assert.Equal(t, types.ChangeStatusInsync, getOut.ChangeInfo.Status)
	})

	t.Run("health check status and last failure reason", func(t *testing.T) {
		t.Parallel()

		backend := route53.NewInMemoryBackend()
		client := newTestRoute53Client(t, route53.NewHandler(backend))
		ctx := t.Context()

		createOut, err := client.CreateHealthCheck(ctx, &route53sdk.CreateHealthCheckInput{
			CallerReference: aws.String("s11-hc-ref"),
			HealthCheckConfig: &types.HealthCheckConfig{
				Type:                     types.HealthCheckTypeHttp,
				FullyQualifiedDomainName: aws.String("s11.example.com"),
				Port:                     aws.Int32(80),
			},
		})
		require.NoError(t, err)
		hcID := aws.ToString(createOut.HealthCheck.Id)

		require.NoError(t, backend.SetHealthCheckStatus(hcID, "Failure: DNS resolution failed"))

		statusOut, err := client.GetHealthCheckStatus(ctx, &route53sdk.GetHealthCheckStatusInput{
			HealthCheckId: aws.String(hcID),
		})
		require.NoError(t, err)
		require.NotEmpty(t, statusOut.HealthCheckObservations)
		assert.Contains(t, aws.ToString(statusOut.HealthCheckObservations[0].StatusReport.Status), "Failure")

		reasonOut, err := client.GetHealthCheckLastFailureReason(ctx, &route53sdk.GetHealthCheckLastFailureReasonInput{
			HealthCheckId: aws.String(hcID),
		})
		require.NoError(t, err)
		require.NotEmpty(t, reasonOut.HealthCheckObservations)
		assert.Contains(t, aws.ToString(reasonOut.HealthCheckObservations[0].StatusReport.Status), "Failure")

		updOut, err := client.UpdateHealthCheck(ctx, &route53sdk.UpdateHealthCheckInput{
			HealthCheckId:            aws.String(hcID),
			FullyQualifiedDomainName: aws.String("s11-updated.example.com"),
		})
		require.NoError(t, err)
		assert.Equal(t, "s11-updated.example.com",
			aws.ToString(updOut.HealthCheck.HealthCheckConfig.FullyQualifiedDomainName))
	})

	t.Run("traffic policy lifecycle", func(t *testing.T) {
		t.Parallel()

		client := newTestRoute53Client(t, newHandler(t))
		ctx := t.Context()

		const doc = `{"AWSPolicyFormatVersion":"2015-10-01","RecordType":"A",` +
			`"Endpoints":{"e1":{"Type":"value","Value":"1.2.3.4"}},"StartEndpoint":"e1"}`

		createOut, err := client.CreateTrafficPolicy(ctx, &route53sdk.CreateTrafficPolicyInput{
			Name:     aws.String("s11-traffic-policy"),
			Document: aws.String(doc),
			Comment:  aws.String("s11 initial comment"),
		})
		require.NoError(t, err)
		tpID := aws.ToString(createOut.TrafficPolicy.Id)
		tpVersion := aws.ToInt32(createOut.TrafficPolicy.Version)

		getOut, err := client.GetTrafficPolicy(ctx, &route53sdk.GetTrafficPolicyInput{
			Id:      aws.String(tpID),
			Version: aws.Int32(tpVersion),
		})
		require.NoError(t, err)
		assert.Equal(t, "s11-traffic-policy", aws.ToString(getOut.TrafficPolicy.Name))

		updOut, err := client.UpdateTrafficPolicyComment(ctx, &route53sdk.UpdateTrafficPolicyCommentInput{
			Id:      aws.String(tpID),
			Version: aws.Int32(tpVersion),
			Comment: aws.String("s11 updated comment"),
		})
		require.NoError(t, err)
		assert.Equal(t, "s11 updated comment", aws.ToString(updOut.TrafficPolicy.Comment))

		_, err = client.DeleteTrafficPolicy(ctx, &route53sdk.DeleteTrafficPolicyInput{
			Id:      aws.String(tpID),
			Version: aws.Int32(tpVersion),
		})
		require.NoError(t, err)

		_, err = client.GetTrafficPolicy(ctx, &route53sdk.GetTrafficPolicyInput{
			Id:      aws.String(tpID),
			Version: aws.Int32(tpVersion),
		})
		assert.Error(t, err, "GetTrafficPolicy on a deleted policy must fail")
	})

	t.Run("checker ip ranges health check count delegation set limit", func(t *testing.T) {
		t.Parallel()

		client := newTestRoute53Client(t, newHandler(t))
		ctx := t.Context()

		ipRangesOut, err := client.GetCheckerIpRanges(ctx, &route53sdk.GetCheckerIpRangesInput{})
		require.NoError(t, err)
		assert.NotEmpty(t, ipRangesOut.CheckerIpRanges)

		countBefore, err := client.GetHealthCheckCount(ctx, &route53sdk.GetHealthCheckCountInput{})
		require.NoError(t, err)

		_, err = client.CreateHealthCheck(ctx, &route53sdk.CreateHealthCheckInput{
			CallerReference: aws.String("s11-hc-count-ref"),
			HealthCheckConfig: &types.HealthCheckConfig{
				Type: types.HealthCheckTypeTcp,
				Port: aws.Int32(80),
			},
		})
		require.NoError(t, err)

		countAfter, err := client.GetHealthCheckCount(ctx, &route53sdk.GetHealthCheckCountInput{})
		require.NoError(t, err)
		assert.Equal(t, aws.ToInt64(countBefore.HealthCheckCount)+1, aws.ToInt64(countAfter.HealthCheckCount))

		dsOut, err := client.CreateReusableDelegationSet(ctx, &route53sdk.CreateReusableDelegationSetInput{
			CallerReference: aws.String("s11-ds-ref"),
		})
		require.NoError(t, err)
		dsID := aws.ToString(dsOut.DelegationSet.Id)

		limitOut, err := client.GetReusableDelegationSetLimit(ctx, &route53sdk.GetReusableDelegationSetLimitInput{
			DelegationSetId: aws.String(dsID),
			Type:            types.ReusableDelegationSetLimitTypeMaxZonesByReusableDelegationSet,
		})
		require.NoError(t, err)
		assert.Positive(t, aws.ToInt64(limitOut.Limit.Value))
		assert.Equal(t, int64(0), limitOut.Count)
	})

	t.Run("test dns answer", func(t *testing.T) {
		t.Parallel()

		client := newTestRoute53Client(t, newHandler(t))
		ctx := t.Context()

		createOut, err := client.CreateHostedZone(ctx, &route53sdk.CreateHostedZoneInput{
			Name:            aws.String("s11-dns.com."),
			CallerReference: aws.String("s11-dns-ref"),
		})
		require.NoError(t, err)
		zoneID := aws.ToString(createOut.HostedZone.Id)

		_, err = client.ChangeResourceRecordSets(ctx, &route53sdk.ChangeResourceRecordSetsInput{
			HostedZoneId: aws.String(zoneID),
			ChangeBatch: &types.ChangeBatch{
				Changes: []types.Change{
					{
						Action: types.ChangeActionUpsert,
						ResourceRecordSet: &types.ResourceRecordSet{
							Name: aws.String("www.s11-dns.com."),
							Type: types.RRTypeA,
							TTL:  aws.Int64(300),
							ResourceRecords: []types.ResourceRecord{
								{Value: aws.String("5.6.7.8")},
							},
						},
					},
				},
			},
		})
		require.NoError(t, err)

		out, err := client.TestDNSAnswer(ctx, &route53sdk.TestDNSAnswerInput{
			HostedZoneId: aws.String(zoneID),
			RecordName:   aws.String("www.s11-dns.com."),
			RecordType:   types.RRTypeA,
		})
		require.NoError(t, err)
		assert.Equal(t, "NOERROR", aws.ToString(out.ResponseCode))
		assert.Contains(t, out.RecordData, "5.6.7.8")
	})
}
