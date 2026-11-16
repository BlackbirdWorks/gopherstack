package azurekeyvault_test

import (
	"crypto"
	"crypto/x509"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azcertificates"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azurekeyvault"
)

func TestCertAutoRenew_PendingIssuer_SDK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		provider string
		reuseKey bool
	}{
		{name: "unknown", reuseKey: false},
		{name: "unknown_reuse_key", reuseKey: true},
		{name: "digicert", provider: "DigiCert"},
		{name: "globalsign", provider: "GlobalSign", reuseKey: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s, clk := clockedServer(t)
			c := newCertsClient(t, s, "renp")
			ctx := t.Context()
			ca := newTestCA(t)

			issuerName := "Unknown"

			if tt.provider != "" {
				issuerName = "myissuer"
				_, err := c.SetIssuer(ctx, issuerName, azcertificates.SetIssuerParameters{Provider: &tt.provider}, nil)
				require.NoError(t, err)
			}

			params := unknownIssuerParams("CN=ren.example.com")
			params.CertificatePolicy.IssuerParameters.Name = &issuerName
			params.CertificatePolicy.KeyProperties.ReuseKey = &tt.reuseKey
			renew := azcertificates.CertificatePolicyActionAutoRenew
			params.CertificatePolicy.LifetimeActions = []*azcertificates.LifetimeAction{{
				Action:  &azcertificates.LifetimeActionType{ActionType: &renew},
				Trigger: &azcertificates.LifetimeActionTrigger{LifetimePercentage: new(int32(80))},
			}}

			op, err := c.CreateCertificate(ctx, "c", params, nil)
			require.NoError(t, err)
			assert.Equal(t, "inProgress", *op.Status)

			first := ca.sign(t, op.CSR)
			_, err = c.MergeCertificate(ctx, "c", azcertificates.MergeCertificateParameters{
				X509Certificates: [][]byte{first},
			}, nil)
			require.NoError(t, err)

			clk.advance(600 * day)

			_, renewed := azurekeyvault.SweepLifetimeActions(s.handler.Backend)
			assert.Equal(t, 1, renewed)

			pending, err := c.GetCertificateOperation(ctx, "c", nil)
			require.NoError(t, err)
			assert.Equal(t, "inProgress", *pending.Status)
			require.NotEmpty(t, pending.CSR)
			assert.Equal(t, issuerName, *pending.IssuerParameters.Name)

			oldCert, err := x509.ParseCertificate(first)
			require.NoError(t, err)

			csr, err := x509.ParseCertificateRequest(pending.CSR)
			require.NoError(t, err)
			assert.Equal(t, tt.reuseKey, samePub2(oldCert.PublicKey, csr.PublicKey))

			again, _ := azurekeyvault.SweepLifetimeActions(s.handler.Backend)
			assert.Zero(t, again, "an open pending request must not be reopened")

			_, err = c.MergeCertificate(ctx, "c", azcertificates.MergeCertificateParameters{
				X509Certificates: [][]byte{ca.sign(t, pending.CSR)},
			}, nil)
			require.NoError(t, err)
			assert.Equal(t, 2, countCertVersions(t, c, "c"))
		})
	}
}

func samePub2(a, b crypto.PublicKey) bool {
	k, ok := a.(interface{ Equal(x crypto.PublicKey) bool })

	return ok && k.Equal(b)
}

func TestCertificates_PartnerIssuerConfig_SDK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		provider string
		disable  bool
		wantErr  bool
	}{
		{name: "digicert", provider: "DigiCert"},
		{name: "globalsign_case", provider: "globalsign"},
		{name: "disabled", provider: "DigiCert", disable: true, wantErr: true},
		{name: "other_provider", provider: "Contoso", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := defaultServer(t)
			c := newCertsClient(t, s, "partner")
			ctx := t.Context()

			in := azcertificates.SetIssuerParameters{Provider: &tt.provider}
			if tt.disable {
				in.Attributes = &azcertificates.IssuerAttributes{Enabled: new(false)}
			}

			_, err := c.SetIssuer(ctx, "iss", in, nil)
			require.NoError(t, err)

			p := unknownIssuerParams("CN=p.example.com")
			p.CertificatePolicy.IssuerParameters.Name = new("iss")

			op, err := c.CreateCertificate(ctx, "p", p, nil)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "400")

				return
			}

			require.NoError(t, err)
			assert.Equal(t, "inProgress", *op.Status)
			assert.NotEmpty(t, op.CSR)
		})
	}
}
