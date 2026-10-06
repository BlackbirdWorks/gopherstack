package route53_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	route53sdk "github.com/aws/aws-sdk-go-v2/service/route53"
	"github.com/aws/aws-sdk-go-v2/service/route53/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHostedZone_NamesStoredLowercase(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name, zone, record, wantZone, wantRecord string
	}{
		{
			name:       "mixed case",
			zone:       "Example.COM",
			record:     "WWW.Example.com",
			wantZone:   "example.com.",
			wantRecord: "www.example.com.",
		},
		{
			name:       "already normal",
			zone:       "example.org.",
			record:     "a.example.org.",
			wantZone:   "example.org.",
			wantRecord: "a.example.org.",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newTestRoute53Client(t, newHandler(t))
			ctx := t.Context()

			z, err := c.CreateHostedZone(ctx, &route53sdk.CreateHostedZoneInput{
				Name: aws.String(tc.zone), CallerReference: aws.String("r1"),
			})
			require.NoError(t, err)
			assert.Equal(t, tc.wantZone, aws.ToString(z.HostedZone.Name))

			_, err = c.ChangeResourceRecordSets(ctx, &route53sdk.ChangeResourceRecordSetsInput{
				HostedZoneId: z.HostedZone.Id,
				ChangeBatch: &types.ChangeBatch{Changes: []types.Change{{
					Action: types.ChangeActionCreate,
					ResourceRecordSet: &types.ResourceRecordSet{
						Name: aws.String(tc.record), Type: types.RRTypeA, TTL: aws.Int64(60),
						ResourceRecords: []types.ResourceRecord{{Value: aws.String("1.2.3.4")}},
					},
				}}},
			})
			require.NoError(t, err)

			l, err := c.ListResourceRecordSets(
				ctx,
				&route53sdk.ListResourceRecordSetsInput{HostedZoneId: z.HostedZone.Id},
			)
			require.NoError(t, err)

			var names []string
			for _, r := range l.ResourceRecordSets {
				names = append(names, aws.ToString(r.Name))
			}

			assert.Contains(t, names, tc.wantRecord)
		})
	}
}

func TestHealthCheck_CreateAppliesDocumentedDefaults(t *testing.T) {
	t.Parallel()

	cases := []struct {
		cfg          *types.HealthCheckConfig
		name         string
		wantInterval int32
		wantFailure  int32
	}{
		{
			name:         "http defaults",
			cfg:          &types.HealthCheckConfig{Type: types.HealthCheckTypeHttp, IPAddress: aws.String("1.2.3.4")},
			wantInterval: 30, wantFailure: 3,
		},
		{
			name: "explicit values kept",
			cfg: &types.HealthCheckConfig{
				Type: types.HealthCheckTypeTcp, IPAddress: aws.String("1.2.3.4"), Port: aws.Int32(22),
				RequestInterval: aws.Int32(10), FailureThreshold: aws.Int32(5),
			},
			wantInterval: 10, wantFailure: 5,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newTestRoute53Client(t, newHandler(t))

			created, err := c.CreateHealthCheck(t.Context(), &route53sdk.CreateHealthCheckInput{
				CallerReference: aws.String("h1"), HealthCheckConfig: tc.cfg,
			})
			require.NoError(t, err)

			got, err := c.GetHealthCheck(
				t.Context(),
				&route53sdk.GetHealthCheckInput{HealthCheckId: created.HealthCheck.Id},
			)
			require.NoError(t, err)
			assert.Equal(t, tc.wantInterval, aws.ToInt32(got.HealthCheck.HealthCheckConfig.RequestInterval))
			assert.Equal(t, tc.wantFailure, aws.ToInt32(got.HealthCheck.HealthCheckConfig.FailureThreshold))
		})
	}
}
