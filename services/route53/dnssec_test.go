package route53_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/route53"
)

// TestRoute53_EnableDisableGetDNSSEC covers EnableHostedZoneDNSSEC,
// DisableHostedZoneDNSSEC, and GetDNSSEC.
func TestRoute53_EnableDisableGetDNSSEC(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *route53.Handler) string
		name         string
		method       string
		pathSuffix   string
		wantContains []string
		wantCode     int
	}{
		{
			name: "enable_dnssec_success",
			setup: func(t *testing.T, h *route53.Handler) string {
				t.Helper()

				zoneID := createZoneForOpsTest(t, h)
				createKSKForOpsTest(t, h, zoneID, "main-key")

				return zoneID
			},
			method:       http.MethodPost,
			pathSuffix:   "/enable-dnssec",
			wantCode:     http.StatusOK,
			wantContains: []string{"EnableHostedZoneDNSSECResponse", "INSYNC"},
		},
		{
			name: "enable_dnssec_not_found",
			setup: func(t *testing.T, _ *route53.Handler) string {
				t.Helper()

				return "ZNONEXISTENT"
			},
			method:     http.MethodPost,
			pathSuffix: "/enable-dnssec",
			wantCode:   http.StatusNotFound,
		},
		{
			name: "disable_dnssec_success",
			setup: func(t *testing.T, h *route53.Handler) string {
				t.Helper()

				zoneID := createZoneForOpsTest(t, h)
				createKSKForOpsTest(t, h, zoneID, "disable-key")
				rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/enable-dnssec", "")
				require.Equal(t, http.StatusOK, rec.Code)

				return zoneID
			},
			method:       http.MethodPost,
			pathSuffix:   "/disable-dnssec",
			wantCode:     http.StatusOK,
			wantContains: []string{"DisableHostedZoneDNSSECResponse", "INSYNC"},
		},
		{
			name: "get_dnssec_not_signed",
			setup: func(t *testing.T, h *route53.Handler) string {
				t.Helper()

				return createZoneForOpsTest(t, h)
			},
			method:       http.MethodGet,
			pathSuffix:   "/dnssec",
			wantCode:     http.StatusOK,
			wantContains: []string{"GetDNSSECResponse", "NOT_SIGNING"},
		},
		{
			name: "get_dnssec_signing",
			setup: func(t *testing.T, h *route53.Handler) string {
				t.Helper()

				zoneID := createZoneForOpsTest(t, h)
				createKSKForOpsTest(t, h, zoneID, "signing-key")
				rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/enable-dnssec", "")
				require.Equal(t, http.StatusOK, rec.Code)

				return zoneID
			},
			method:       http.MethodGet,
			pathSuffix:   "/dnssec",
			wantCode:     http.StatusOK,
			wantContains: []string{"GetDNSSECResponse", "SIGNING"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)
			zoneID := tt.setup(t, h)
			path := "/2013-04-01/hostedzone/" + zoneID + tt.pathSuffix
			rec := send(t, h, tt.method, path, "")

			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

func TestEnableDNSSEC_RequiresActiveKSK(t *testing.T) {
	t.Parallel()

	b := route53.NewInMemoryBackend()
	hz, err := b.CreateHostedZone("example.com", "ref", "", false, "", "", "")
	require.NoError(t, err)

	// No KSK — should fail.
	err = b.EnableHostedZoneDNSSEC(hz.ID)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "KeySigningKeyWithActiveStatusNotFound")

	// Create inactive KSK — should still fail.
	_, err = b.CreateKeySigningKey(
		hz.ID, "ref", "my-ksk", "arn:aws:kms:us-east-1:123456789012:key/test-ksk", "INACTIVE",
	)
	require.NoError(t, err)

	err = b.EnableHostedZoneDNSSEC(hz.ID)
	require.Error(t, err)

	// Activate KSK — now should succeed.
	_, err = b.ActivateKeySigningKey(hz.ID, "my-ksk")
	require.NoError(t, err)

	err = b.EnableHostedZoneDNSSEC(hz.ID)
	require.NoError(t, err)
}
