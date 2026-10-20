package azurekeyvault_test

import (
	"crypto"
	"crypto/x509"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azcertificates"
	"github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azkeys"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azurekeyvault"
)

const day = 24 * time.Hour

// fakeClock is a settable clock safe to share with the server goroutines.
type fakeClock struct{ unixNano atomic.Int64 }

func newFakeClock() *fakeClock {
	c := &fakeClock{}
	c.unixNano.Store(time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC).UnixNano())

	return c
}

func (c *fakeClock) now() time.Time          { return time.Unix(0, c.unixNano.Load()).UTC() }
func (c *fakeClock) advance(d time.Duration) { c.unixNano.Add(int64(d)) }

func clockedServer(t *testing.T) (*testServer, *fakeClock) {
	t.Helper()

	s := defaultServer(t)
	clk := newFakeClock()
	azurekeyvault.SetClock(s.handler.Backend, clk.now)

	return s, clk
}

func countKeyVersions(t *testing.T, c *azkeys.Client, name string) int {
	t.Helper()

	pager := c.NewListKeyPropertiesVersionsPager(name, nil)
	n := 0

	for pager.More() {
		pg, err := pager.NextPage(t.Context())
		require.NoError(t, err)

		n += len(pg.Value)
	}

	return n
}

func countCertVersions(t *testing.T, c *azcertificates.Client, name string) int {
	t.Helper()

	pager := c.NewListCertificatePropertiesVersionsPager(name, nil)
	n := 0

	for pager.More() {
		pg, err := pager.NextPage(t.Context())
		require.NoError(t, err)

		n += len(pg.Value)
	}

	return n
}

func TestKeyAutoRotation_SDK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		action      azkeys.KeyRotationPolicyAction
		trigger     azkeys.LifetimeActionTrigger
		expiryTime  string
		keyExpires  time.Duration
		advance     time.Duration
		wantVersion int
		wantExpiry  time.Duration
	}{
		{
			name: "after_create_due", action: azkeys.KeyRotationPolicyActionRotate,
			trigger: azkeys.LifetimeActionTrigger{TimeAfterCreate: new("P30D")}, expiryTime: "P1Y",
			advance: 31 * day, wantVersion: 2, wantExpiry: 365 * day,
		},
		{
			name: "after_create_not_due", action: azkeys.KeyRotationPolicyActionRotate,
			trigger: azkeys.LifetimeActionTrigger{TimeAfterCreate: new("P30D")}, expiryTime: "P1Y",
			advance: 10 * day, wantVersion: 1,
		},
		{
			name: "before_expiry_due", action: azkeys.KeyRotationPolicyActionRotate,
			trigger: azkeys.LifetimeActionTrigger{TimeBeforeExpiry: new("P10D")}, expiryTime: "P90D",
			keyExpires: 60 * day, advance: 51 * day, wantVersion: 2, wantExpiry: 90 * day,
		},
		{
			name: "before_expiry_not_due", action: azkeys.KeyRotationPolicyActionRotate,
			trigger:    azkeys.LifetimeActionTrigger{TimeBeforeExpiry: new("P10D")},
			keyExpires: 60 * day, advance: 40 * day, wantVersion: 1,
		},
		{
			name: "notify_never_rotates", action: azkeys.KeyRotationPolicyActionNotify,
			trigger: azkeys.LifetimeActionTrigger{TimeAfterCreate: new("P1D")},
			advance: 400 * day, wantVersion: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s, clk := clockedServer(t)
			c := newKeysClient(t, s, "rot")
			ctx := t.Context()

			params := azkeys.CreateKeyParameters{Kty: new(azkeys.KeyTypeRSA)}
			if tt.keyExpires > 0 {
				params.KeyAttributes = &azkeys.KeyAttributes{Expires: new(clk.now().Add(tt.keyExpires))}
			}

			_, err := c.CreateKey(ctx, "k", params, nil)
			require.NoError(t, err)

			policy := azkeys.KeyRotationPolicy{LifetimeActions: []*azkeys.LifetimeAction{{
				Action:  &azkeys.LifetimeActionType{Type: &tt.action},
				Trigger: &tt.trigger,
			}}}
			if tt.expiryTime != "" {
				policy.Attributes = &azkeys.KeyRotationPolicyAttributes{ExpiryTime: &tt.expiryTime}
			}

			_, err = c.UpdateKeyRotationPolicy(ctx, "k", policy, nil)
			require.NoError(t, err)

			clk.advance(tt.advance)
			rotated, _ := azurekeyvault.SweepLifetimeActions(s.handler.Backend)

			assert.Equal(t, tt.wantVersion-1, rotated)
			assert.Equal(t, tt.wantVersion, countKeyVersions(t, c, "k"))

			// a second sweep must not rotate again
			again, _ := azurekeyvault.SweepLifetimeActions(s.handler.Backend)
			assert.Zero(t, again)

			if tt.wantExpiry > 0 {
				got, gErr := c.GetKey(ctx, "k", "", nil)
				require.NoError(t, gErr)
				require.NotNil(t, got.Attributes.Expires)
				assert.WithinDuration(t, clk.now().Add(tt.wantExpiry), *got.Attributes.Expires, time.Second)
			}
		})
	}
}

func TestRotationPolicy_RejectsBadDuration_SDK(t *testing.T) {
	t.Parallel()

	s := defaultServer(t)
	c := newKeysClient(t, s, "bad")

	_, err := c.CreateKey(t.Context(), "k", azkeys.CreateKeyParameters{Kty: new(azkeys.KeyTypeRSA)}, nil)
	require.NoError(t, err)

	_, err = c.UpdateKeyRotationPolicy(t.Context(), "k", azkeys.KeyRotationPolicy{
		LifetimeActions: []*azkeys.LifetimeAction{{
			Action:  &azkeys.LifetimeActionType{Type: new(azkeys.KeyRotationPolicyActionRotate)},
			Trigger: &azkeys.LifetimeActionTrigger{TimeAfterCreate: new("30 days")},
		}},
	}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "400")
}

func TestCertAutoRenew_SDK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		trigger   azcertificates.LifetimeActionTrigger
		action    azcertificates.CertificatePolicyAction
		reuseKey  bool
		advance   time.Duration
		wantCount int
	}{
		{
			name: "percentage_due", action: azcertificates.CertificatePolicyActionAutoRenew,
			trigger: azcertificates.LifetimeActionTrigger{LifetimePercentage: new(int32(80))},
			advance: 150 * day, wantCount: 2,
		},
		{
			name: "percentage_not_due", action: azcertificates.CertificatePolicyActionAutoRenew,
			trigger: azcertificates.LifetimeActionTrigger{LifetimePercentage: new(int32(80))},
			advance: 100 * day, wantCount: 1,
		},
		{
			name: "days_before_expiry_due", action: azcertificates.CertificatePolicyActionAutoRenew,
			trigger: azcertificates.LifetimeActionTrigger{DaysBeforeExpiry: new(int32(30))},
			advance: 160 * day, wantCount: 2,
		},
		{
			name: "reuse_key", action: azcertificates.CertificatePolicyActionAutoRenew,
			trigger:  azcertificates.LifetimeActionTrigger{LifetimePercentage: new(int32(50))},
			reuseKey: true, advance: 100 * day, wantCount: 2,
		},
		{
			name: "email_contacts_only", action: azcertificates.CertificatePolicyActionEmailContacts,
			trigger: azcertificates.LifetimeActionTrigger{LifetimePercentage: new(int32(10))},
			advance: 170 * day, wantCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s, clk := clockedServer(t)
			c := newCertsClient(t, s, "ren")
			ctx := t.Context()

			params := selfSignedPolicy("CN=renew.example.com", azcertificates.KeyTypeEC)
			params.CertificatePolicy.KeyProperties.ReuseKey = &tt.reuseKey
			params.CertificatePolicy.LifetimeActions = []*azcertificates.LifetimeAction{{
				Action:  &azcertificates.LifetimeActionType{ActionType: &tt.action},
				Trigger: &tt.trigger,
			}}

			_, err := c.CreateCertificate(ctx, "c", params, nil)
			require.NoError(t, err)

			first, err := c.GetCertificate(ctx, "c", "", nil)
			require.NoError(t, err)

			clk.advance(tt.advance)
			_, renewed := azurekeyvault.SweepLifetimeActions(s.handler.Backend)

			assert.Equal(t, tt.wantCount-1, renewed)
			assert.Equal(t, tt.wantCount, countCertVersions(t, c, "c"))

			again, _ := azurekeyvault.SweepLifetimeActions(s.handler.Backend)
			assert.Zero(t, again, "second sweep must not renew again")

			if tt.wantCount == 1 {
				return
			}

			latest, err := c.GetCertificate(ctx, "c", "", nil)
			require.NoError(t, err)
			assert.NotEqual(t, first.ID.Version(), latest.ID.Version())

			oldCert, err := x509.ParseCertificate(first.CER)
			require.NoError(t, err)

			newCert, err := x509.ParseCertificate(latest.CER)
			require.NoError(t, err)
			assert.Equal(t, oldCert.Subject.CommonName, newCert.Subject.CommonName)
			assert.True(t, newCert.NotBefore.After(oldCert.NotBefore))
			assert.Equal(t, tt.reuseKey, samePub(oldCert, newCert))
		})
	}
}

func samePub(a, b *x509.Certificate) bool {
	k, ok := a.PublicKey.(interface{ Equal(x crypto.PublicKey) bool })

	return ok && k.Equal(b.PublicKey)
}
