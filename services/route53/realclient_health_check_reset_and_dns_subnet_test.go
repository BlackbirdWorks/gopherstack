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

func TestRealClient_UpdateHealthCheckResetElements(t *testing.T) {
	t.Parallel()

	client := newTestRoute53Client(t, newHandler(t))
	ctx := t.Context()

	createOut, err := client.CreateHealthCheck(ctx, &route53sdk.CreateHealthCheckInput{
		CallerReference: aws.String("s11-reset-ref"),
		HealthCheckConfig: &types.HealthCheckConfig{
			Type:                     types.HealthCheckTypeHttp,
			FullyQualifiedDomainName: aws.String("s11-reset.example.com"),
			Regions: []types.HealthCheckRegion{
				types.HealthCheckRegionUsEast1,
				types.HealthCheckRegionEuWest1,
			},
			Port: aws.Int32(80),
		},
	})
	require.NoError(t, err)
	hcID := aws.ToString(createOut.HealthCheck.Id)
	require.NotEmpty(t, createOut.HealthCheck.HealthCheckConfig.FullyQualifiedDomainName)
	require.NotEmpty(t, createOut.HealthCheck.HealthCheckConfig.Regions)

	updOut, err := client.UpdateHealthCheck(ctx, &route53sdk.UpdateHealthCheckInput{
		HealthCheckId: aws.String(hcID),
		ResetElements: []types.ResettableElementName{
			types.ResettableElementNameFullyQualifiedDomainName,
			types.ResettableElementNameRegions,
		},
	})
	require.NoError(t, err)
	assert.Empty(t, aws.ToString(updOut.HealthCheck.HealthCheckConfig.FullyQualifiedDomainName))
	assert.Empty(t, updOut.HealthCheck.HealthCheckConfig.Regions)

	getOut, err := client.GetHealthCheck(ctx, &route53sdk.GetHealthCheckInput{HealthCheckId: aws.String(hcID)})
	require.NoError(t, err)
	assert.Empty(t, aws.ToString(getOut.HealthCheck.HealthCheckConfig.FullyQualifiedDomainName))
	assert.Empty(t, getOut.HealthCheck.HealthCheckConfig.Regions)
}

func TestRealClient_TestDNSAnswerEDNS0ClientSubnetMask(t *testing.T) {
	t.Parallel()

	backend := route53.NewInMemoryBackend()
	client := newTestRoute53Client(t, route53.NewHandler(backend))
	ctx := t.Context()

	createOut, err := client.CreateHostedZone(ctx, &route53sdk.CreateHostedZoneInput{
		Name:            aws.String("s11-subnet.com."),
		CallerReference: aws.String("s11-subnet-ref"),
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
						Name:          aws.String("geo.s11-subnet.com."),
						Type:          types.RRTypeA,
						TTL:           aws.Int64(60),
						SetIdentifier: aws.String("ca"),
						GeoLocation: &types.GeoLocation{
							CountryCode:     aws.String("US"),
							SubdivisionCode: aws.String("CA"),
						},
						ResourceRecords: []types.ResourceRecord{
							{Value: aws.String("10.0.0.1")},
						},
					},
				},
				{
					Action: types.ChangeActionUpsert,
					ResourceRecordSet: &types.ResourceRecordSet{
						Name:          aws.String("geo.s11-subnet.com."),
						Type:          types.RRTypeA,
						TTL:           aws.Int64(60),
						SetIdentifier: aws.String("us"),
						GeoLocation:   &types.GeoLocation{CountryCode: aws.String("US")},
						ResourceRecords: []types.ResourceRecord{
							{Value: aws.String("10.0.0.2")},
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	// 8.8.8.200/24 (edns0clientsubnetmask=24, matching the geoIPTable's
	// 8.8.8.0/24 -> US/CA entry) resolves the California-specific record.
	narrowOut, err := client.TestDNSAnswer(ctx, &route53sdk.TestDNSAnswerInput{
		HostedZoneId:        aws.String(zoneID),
		RecordName:          aws.String("geo.s11-subnet.com."),
		RecordType:          types.RRTypeA,
		EDNS0ClientSubnetIP: aws.String("8.8.8.200"),
	})
	require.NoError(t, err)
	assert.Contains(t, narrowOut.RecordData, "10.0.0.1")

	// The same IP masked to /16 (network 8.8.0.0) falls outside the /24
	// entry entirely, so it falls back to the country-only US record.
	wideOut, err := client.TestDNSAnswer(ctx, &route53sdk.TestDNSAnswerInput{
		HostedZoneId:          aws.String(zoneID),
		RecordName:            aws.String("geo.s11-subnet.com."),
		RecordType:            types.RRTypeA,
		EDNS0ClientSubnetIP:   aws.String("8.8.8.200"),
		EDNS0ClientSubnetMask: aws.String("16"),
	})
	require.NoError(t, err)
	assert.Contains(t, wideOut.RecordData, "10.0.0.2")
}
