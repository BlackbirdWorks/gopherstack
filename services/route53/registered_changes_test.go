package route53_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	route53sdk "github.com/aws/aws-sdk-go-v2/service/route53"
	route53types "github.com/aws/aws-sdk-go-v2/service/route53/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/route53"
)

func TestChangeIDs_AreRegisteredAndDistinct(t *testing.T) {
	t.Parallel()

	tests := []struct {
		op   func(t *testing.T, c *route53sdk.Client, zoneID string) string
		name string
	}{
		{name: "create_ksk", op: func(t *testing.T, c *route53sdk.Client, zoneID string) string {
			t.Helper()

			out, err := c.CreateKeySigningKey(t.Context(), &route53sdk.CreateKeySigningKeyInput{
				HostedZoneId: aws.String(zoneID), CallerReference: aws.String("k1"), Name: aws.String("k"),
				KeyManagementServiceArn: aws.String("arn:aws:kms:us-east-1:123456789012:key/k"),
				Status:                  aws.String("INACTIVE"),
			})
			require.NoError(t, err)

			return aws.ToString(out.ChangeInfo.Id)
		}},
		{name: "activate_deactivate_delete_ksk", op: func(t *testing.T, c *route53sdk.Client, zoneID string) string {
			t.Helper()

			_, err := c.CreateKeySigningKey(t.Context(), &route53sdk.CreateKeySigningKeyInput{
				HostedZoneId: aws.String(zoneID), CallerReference: aws.String("k2"), Name: aws.String("k"),
				KeyManagementServiceArn: aws.String("arn:aws:kms:us-east-1:123456789012:key/k"),
				Status:                  aws.String("INACTIVE"),
			})
			require.NoError(t, err)

			in := func(name string) (*string, *string) { return aws.String(zoneID), aws.String(name) }
			zid, name := in("k")

			act, err := c.ActivateKeySigningKey(
				t.Context(), &route53sdk.ActivateKeySigningKeyInput{HostedZoneId: zid, Name: name},
			)
			require.NoError(t, err)

			deact, err := c.DeactivateKeySigningKey(
				t.Context(), &route53sdk.DeactivateKeySigningKeyInput{HostedZoneId: zid, Name: name},
			)
			require.NoError(t, err)

			del, err := c.DeleteKeySigningKey(
				t.Context(), &route53sdk.DeleteKeySigningKeyInput{HostedZoneId: zid, Name: name},
			)
			require.NoError(t, err)

			ids := []string{
				aws.ToString(act.ChangeInfo.Id), aws.ToString(deact.ChangeInfo.Id), aws.ToString(del.ChangeInfo.Id),
			}
			assert.Len(t, map[string]struct{}{ids[0]: {}, ids[1]: {}, ids[2]: {}}, len(ids))

			for _, id := range ids[:2] {
				_, err = c.GetChange(t.Context(), &route53sdk.GetChangeInput{Id: aws.String(id)})
				require.NoError(t, err)
			}

			return ids[2]
		}},
		{name: "delete_hosted_zone", op: func(t *testing.T, c *route53sdk.Client, zoneID string) string {
			t.Helper()

			out, err := c.DeleteHostedZone(t.Context(), &route53sdk.DeleteHostedZoneInput{Id: aws.String(zoneID)})
			require.NoError(t, err)

			return aws.ToString(out.ChangeInfo.Id)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newTestRoute53Client(t, route53.NewHandler(route53.NewInMemoryBackend()))

			zone, err := c.CreateHostedZone(t.Context(), &route53sdk.CreateHostedZoneInput{
				Name: aws.String("chg.example.com."), CallerReference: aws.String("chg"),
			})
			require.NoError(t, err)

			id := tt.op(t, c, aws.ToString(zone.HostedZone.Id))
			assert.NotEqual(t, aws.ToString(zone.ChangeInfo.Id), id, "a distinct change per operation")

			got, err := c.GetChange(t.Context(), &route53sdk.GetChangeInput{Id: aws.String(id)})
			require.NoError(t, err)
			assert.Equal(t, route53types.ChangeStatusInsync, got.ChangeInfo.Status)
		})
	}
}

func TestUpdateHostedZoneFeatures_AcceleratedRecovery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want   route53types.AcceleratedRecoveryStatus
		name   string
		enable []bool
	}{
		{name: "never_set"},
		{name: "enabled", enable: []bool{true}, want: route53types.AcceleratedRecoveryStatusEnabled},
		{name: "disabled", enable: []bool{true, false}, want: route53types.AcceleratedRecoveryStatusDisabled},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newTestRoute53Client(t, route53.NewHandler(route53.NewInMemoryBackend()))

			zone, err := c.CreateHostedZone(t.Context(), &route53sdk.CreateHostedZoneInput{
				Name: aws.String("feat.example.com."), CallerReference: aws.String("feat"),
			})
			require.NoError(t, err)

			zoneID := aws.ToString(zone.HostedZone.Id)

			for _, e := range tt.enable {
				_, err = c.UpdateHostedZoneFeatures(t.Context(), &route53sdk.UpdateHostedZoneFeaturesInput{
					HostedZoneId: aws.String(zoneID), EnableAcceleratedRecovery: aws.Bool(e),
				})
				require.NoError(t, err)
			}

			got, err := c.GetHostedZone(t.Context(), &route53sdk.GetHostedZoneInput{Id: aws.String(zoneID)})
			require.NoError(t, err)

			if tt.want == "" {
				assert.Nil(t, got.HostedZone.Features)

				return
			}

			require.NotNil(t, got.HostedZone.Features)
			assert.Equal(t, tt.want, got.HostedZone.Features.AcceleratedRecoveryStatus)
		})
	}
}
