package lightsail_test

import (
	"testing"
	"time"

	lightsailsdk "github.com/aws/aws-sdk-go-v2/service/lightsail"
	"github.com/aws/aws-sdk-go-v2/service/lightsail/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lightsail"
)

func TestSDKRoundTrip_GetProfile(t *testing.T) {
	t.Parallel()

	enrolled := time.Date(2025, 3, 4, 5, 6, 7, 0, time.UTC)

	tests := []struct {
		setup       func(b *lightsail.InMemoryBackend)
		wantPartner *types.PartnerInfo
		name        string
		wantType    types.ProfileType
	}{
		{
			name:     "default_account",
			wantType: types.ProfileTypeLightsailor,
		},
		{
			name: "partner_account",
			setup: func(b *lightsail.InMemoryBackend) {
				b.SetPartnerProfile("Growth", "Suspended", enrolled)
			},
			wantType:    types.ProfileTypeLightsailPartner,
			wantPartner: &types.PartnerInfo{TierName: types.TierNameGrowth, Status: types.PartnerStatusSuspended},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := lightsail.NewInMemoryBackend(t.Context(), rtTestAccountID, rtTestRegion)
			t.Cleanup(b.Close)

			if tt.setup != nil {
				tt.setup(b)
			}

			out, err := newRoundTripClient(t, lightsail.NewHandler(b)).GetProfile(
				t.Context(), &lightsailsdk.GetProfileInput{},
			)
			require.NoError(t, err)
			assert.Equal(t, tt.wantType, out.ProfileType)

			if tt.wantPartner == nil {
				assert.Nil(t, out.Partner)

				return
			}

			require.NotNil(t, out.Partner)
			assert.Equal(t, tt.wantPartner.TierName, out.Partner.TierName)
			assert.Equal(t, tt.wantPartner.Status, out.Partner.Status)
			require.NotNil(t, out.Partner.EnrolledAt)
			assert.True(t, enrolled.Equal(*out.Partner.EnrolledAt))
		})
	}
}

func TestGetProfile_Persistence(t *testing.T) {
	t.Parallel()

	src := lightsail.NewInMemoryBackend(t.Context(), rtTestAccountID, rtTestRegion)
	t.Cleanup(src.Close)
	src.SetPartnerProfile("Premier", "Active", time.Unix(1700000000, 0))

	dst := lightsail.NewInMemoryBackend(t.Context(), rtTestAccountID, rtTestRegion)
	t.Cleanup(dst.Close)
	require.NoError(t, dst.Restore(t.Context(), src.Snapshot(t.Context())))

	out, err := newRoundTripClient(t, lightsail.NewHandler(dst)).GetProfile(
		t.Context(), &lightsailsdk.GetProfileInput{},
	)
	require.NoError(t, err)
	require.NotNil(t, out.Partner)
	assert.Equal(t, types.TierNamePremier, out.Partner.TierName)

	dst.Reset()

	out, err = newRoundTripClient(t, lightsail.NewHandler(dst)).GetProfile(
		t.Context(), &lightsailsdk.GetProfileInput{},
	)
	require.NoError(t, err)
	assert.Nil(t, out.Partner)
}
