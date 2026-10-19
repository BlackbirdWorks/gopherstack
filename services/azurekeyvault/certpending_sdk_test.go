package azurekeyvault_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"encoding/pem"
	"math/big"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azcertificates"
	"github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azkeys"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
}

func newTestCA(t *testing.T) *testCA {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "Test CA"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().AddDate(10, 0, 0),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)

	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)

	return &testCA{cert: cert, key: key}
}

// sign issues a certificate for csr, copying its requested extensions.
func (ca *testCA) sign(t *testing.T, csrDER []byte) []byte {
	t.Helper()

	csr, err := x509.ParseCertificateRequest(csrDER)
	require.NoError(t, err)
	require.NoError(t, csr.CheckSignature())

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: csr.Subject,
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().AddDate(1, 0, 0),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtraExtensions: csr.Extensions,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, csr.PublicKey, ca.key)
	require.NoError(t, err)

	return der
}

func pemOf(ders ...[]byte) []byte {
	out := make([]byte, 0, len(ders)*512)

	for _, d := range ders {
		out = append(out, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: d})...)
	}

	return out
}

func unknownIssuerParams(subject string, upns ...string) azcertificates.CreateCertificateParameters {
	p := selfSignedPolicy(subject, azcertificates.KeyTypeEC)
	p.CertificatePolicy.IssuerParameters = &azcertificates.IssuerParameters{Name: new("Unknown")}

	for _, u := range upns {
		p.CertificatePolicy.X509CertificateProperties.SubjectAlternativeNames.UserPrincipalNames = append(
			p.CertificatePolicy.X509CertificateProperties.SubjectAlternativeNames.UserPrincipalNames, new(u))
	}

	return p
}

// sanUPNs extracts UPN otherName values from a certificate's or request's SAN extension.
func sanUPNs(t *testing.T, exts []pkix.Extension) []string {
	t.Helper()

	var out []string

	for _, e := range exts {
		if !e.Id.Equal(asn1.ObjectIdentifier{2, 5, 29, 17}) {
			continue
		}

		var names []asn1.RawValue

		_, err := asn1.Unmarshal(e.Value, &names)
		require.NoError(t, err)

		for _, n := range names {
			if n.Tag != 0 || n.Class != asn1.ClassContextSpecific {
				continue
			}

			var oid asn1.ObjectIdentifier

			rest, oErr := asn1.Unmarshal(n.Bytes, &oid)
			require.NoError(t, oErr)
			require.True(t, oid.Equal(asn1.ObjectIdentifier{1, 3, 6, 1, 4, 1, 311, 20, 2, 3}))

			var explicit asn1.RawValue

			_, eErr := asn1.Unmarshal(rest, &explicit)
			require.NoError(t, eErr)

			var upn string

			_, uErr := asn1.UnmarshalWithParams(explicit.Bytes, &upn, "utf8")
			require.NoError(t, uErr)

			out = append(out, upn)
		}
	}

	return out
}

func TestCertificates_UnknownIssuerMerge_SDK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		merge func(leaf []byte, ca *testCA) [][]byte
		name  string
	}{
		{func(leaf []byte, ca *testCA) [][]byte { return [][]byte{leaf, ca.cert.Raw} }, "der_chain"},
		{func(leaf []byte, _ *testCA) [][]byte { return [][]byte{leaf} }, "leaf_only"},
		{func(leaf []byte, ca *testCA) [][]byte { return [][]byte{pemOf(leaf, ca.cert.Raw)} }, "pem_bundle"},
		{func(leaf []byte, ca *testCA) [][]byte { return [][]byte{ca.cert.Raw, leaf} }, "chain_first"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := defaultServer(t)
			c := newCertsClient(t, s, "csr")
			ctx := t.Context()
			ca := newTestCA(t)

			params := unknownIssuerParams("CN=ext.example.com, O=Acme", "svc@example.com")
			op, err := c.CreateCertificate(ctx, "ext", params, nil)
			require.NoError(t, err)
			assert.Equal(t, "inProgress", *op.Status)
			assert.Equal(t, "Unknown", *op.IssuerParameters.Name)
			require.NotEmpty(t, op.CSR)

			csr, err := x509.ParseCertificateRequest(op.CSR)
			require.NoError(t, err)
			require.NoError(t, csr.CheckSignature())
			assert.Equal(t, "ext.example.com", csr.Subject.CommonName)
			assert.Equal(t, []string{"example.com", "www.example.com"}, csr.DNSNames)
			assert.Equal(t, []string{"svc@example.com"}, sanUPNs(t, csr.Extensions))

			got, err := c.GetCertificateOperation(ctx, "ext", nil)
			require.NoError(t, err)
			assert.Equal(t, "inProgress", *got.Status)
			assert.Equal(t, op.CSR, got.CSR)

			pendingBundle, err := c.GetCertificate(ctx, "ext", "", nil)
			require.NoError(t, err)
			assert.Empty(t, pendingBundle.CER)

			leaf := ca.sign(t, op.CSR)
			merged, err := c.MergeCertificate(ctx, "ext", azcertificates.MergeCertificateParameters{
				X509Certificates: tt.merge(leaf, ca),
			}, nil)
			require.NoError(t, err)
			assert.Equal(t, leaf, merged.CER)
			assert.Equal(t, pendingBundle.ID.Version(), merged.ID.Version())

			done, err := c.GetCertificateOperation(ctx, "ext", nil)
			require.NoError(t, err)
			assert.Equal(t, "completed", *done.Status)

			final, err := c.GetCertificate(ctx, "ext", "", nil)
			require.NoError(t, err)
			assert.Equal(t, leaf, final.CER)
			assert.Equal(t, "Unknown", *final.Policy.IssuerParameters.Name)
		})
	}
}

func TestCertificates_UnknownIssuerLifecycle_SDK(t *testing.T) {
	t.Parallel()

	mergeParams := func(der []byte) azcertificates.MergeCertificateParameters {
		return azcertificates.MergeCertificateParameters{X509Certificates: [][]byte{der}}
	}

	tests := []struct {
		run  func(t *testing.T, c *azcertificates.Client, ca *testCA, csr []byte)
		name string
	}{
		{func(t *testing.T, c *azcertificates.Client, _ *testCA, _ []byte) {
			t.Helper()

			_, err := c.CreateCertificate(t.Context(), "p", unknownIssuerParams("CN=p.example.com"), nil)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "409")
		}, "duplicate_create_conflicts"},
		{func(t *testing.T, c *azcertificates.Client, ca *testCA, _ []byte) {
			t.Helper()

			other, err := c.CreateCertificate(t.Context(), "other", unknownIssuerParams("CN=other.example.com"), nil)
			require.NoError(t, err)

			_, err = c.MergeCertificate(t.Context(), "p", mergeParams(ca.sign(t, other.CSR)), nil)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "400")
		}, "merge_wrong_key_rejected"},
		{func(t *testing.T, c *azcertificates.Client, ca *testCA, csr []byte) {
			t.Helper()

			upd, err := c.UpdateCertificateOperation(t.Context(), "p",
				azcertificates.UpdateCertificateOperationParameter{CancellationRequested: new(true)}, nil)
			require.NoError(t, err)
			assert.True(t, *upd.CancellationRequested)

			_, err = c.MergeCertificate(t.Context(), "p", mergeParams(ca.sign(t, csr)), nil)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "409")
		}, "cancel_blocks_merge"},
		{func(t *testing.T, c *azcertificates.Client, ca *testCA, csr []byte) {
			t.Helper()

			_, err := c.DeleteCertificateOperation(t.Context(), "p", nil)
			require.NoError(t, err)

			_, err = c.GetCertificateOperation(t.Context(), "p", nil)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "404")

			_, err = c.MergeCertificate(t.Context(), "p", mergeParams(ca.sign(t, csr)), nil)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "404")
		}, "delete_operation"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := defaultServer(t)
			c := newCertsClient(t, s, "life")

			op, err := c.CreateCertificate(t.Context(), "p", unknownIssuerParams("CN=p.example.com"), nil)
			require.NoError(t, err)

			tt.run(t, c, newTestCA(t), op.CSR)
		})
	}
}

func TestCertificates_ThirdPartyIssuerRejected_SDK(t *testing.T) {
	t.Parallel()

	s := defaultServer(t)
	c := newCertsClient(t, s, "tp")

	for _, issuer := range []string{"DigiCert", "GlobalSign"} {
		p := selfSignedPolicy("CN=x.example.com", azcertificates.KeyTypeRSA)
		p.CertificatePolicy.IssuerParameters.Name = new(issuer)

		_, err := c.CreateCertificate(t.Context(), "x", p, nil)
		require.Error(t, err, issuer)
		assert.Contains(t, err.Error(), "400")
		assert.Contains(t, err.Error(), "not supported")
	}
}

func TestCertificates_UPNSelfSigned_SDK(t *testing.T) {
	t.Parallel()

	s := defaultServer(t)
	c := newCertsClient(t, s, "upn")

	p := selfSignedPolicy("CN=upn.example.com", azcertificates.KeyTypeEC)
	p.CertificatePolicy.X509CertificateProperties.SubjectAlternativeNames.UserPrincipalNames = []*string{
		new("alice@corp.example.com"), new("bob@corp.example.com"),
	}

	_, err := c.CreateCertificate(t.Context(), "u", p, nil)
	require.NoError(t, err)

	got, err := c.GetCertificate(t.Context(), "u", "", nil)
	require.NoError(t, err)

	cert, err := x509.ParseCertificate(got.CER)
	require.NoError(t, err)
	assert.Equal(t, []string{"example.com", "www.example.com"}, cert.DNSNames)
	assert.Equal(t, []string{"alice@corp.example.com", "bob@corp.example.com"}, sanUPNs(t, cert.Extensions))
}

func TestKeys_SecP256K1Unsupported_SDK(t *testing.T) {
	t.Parallel()

	s := defaultServer(t)
	c := newKeysClient(t, s, "k1")

	_, err := c.CreateKey(t.Context(), "k", azkeys.CreateKeyParameters{
		Kty: new(azkeys.KeyTypeEC), Curve: new(azkeys.CurveNameP256K),
	}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "400")
}
