package azurekeyvault_test

import (
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"math/big"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azcertificates"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newCertsClient(t *testing.T, s *testServer, vault string) *azcertificates.Client {
	t.Helper()

	c, err := azcertificates.NewClient(s.vaultURL(vault), fakeCredential{}, &azcertificates.ClientOptions{
		ClientOptions:                        s.clientOptions(),
		DisableChallengeResourceVerification: true,
	})
	require.NoError(t, err)

	return c
}

func selfSignedPolicy(subject string, kty azcertificates.KeyType) azcertificates.CreateCertificateParameters {
	p := azcertificates.CreateCertificateParameters{CertificatePolicy: &azcertificates.CertificatePolicy{
		IssuerParameters: &azcertificates.IssuerParameters{Name: new("Self")},
		X509CertificateProperties: &azcertificates.X509CertificateProperties{
			Subject:          new(subject),
			ValidityInMonths: new(int32(6)),
			SubjectAlternativeNames: &azcertificates.SubjectAlternativeNames{
				DNSNames: []*string{new("example.com"), new("www.example.com")},
			},
		},
		KeyProperties: &azcertificates.KeyProperties{Exportable: new(true), KeyType: new(kty)},
	}}
	if kty == azcertificates.KeyTypeEC {
		p.CertificatePolicy.KeyProperties.Curve = new(azcertificates.CurveNameP256)
	}

	return p
}

func TestCertificates_SelfSigned_SDK(t *testing.T) {
	t.Parallel()

	s := defaultServer(t)
	ctx := t.Context()

	tests := []struct {
		name string
		kty  azcertificates.KeyType
	}{
		{"rsa", azcertificates.KeyTypeRSA},
		{"ec", azcertificates.KeyTypeEC},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newCertsClient(t, s, tt.name)
			sc := newSecretsClient(t, s, tt.name)

			op, err := c.CreateCertificate(ctx, "web", selfSignedPolicy("CN=example.com, O=Acme", tt.kty), nil)
			require.NoError(t, err)
			assert.Equal(t, "completed", *op.Status)

			pending, err := c.GetCertificateOperation(ctx, "web", nil)
			require.NoError(t, err)
			assert.Equal(t, "completed", *pending.Status)
			assert.Contains(t, *pending.Target, "/certificates/web")

			got, err := c.GetCertificate(ctx, "web", "", nil)
			require.NoError(t, err)

			cert, err := x509.ParseCertificate(got.CER)
			require.NoError(t, err)
			assert.Equal(t, "example.com", cert.Subject.CommonName)
			assert.Equal(t, []string{"Acme"}, cert.Subject.Organization)
			assert.Equal(t, []string{"example.com", "www.example.com"}, cert.DNSNames)
			assert.WithinDuration(t, time.Now().AddDate(0, 6, 0), cert.NotAfter, 48*time.Hour)
			require.NoError(
				t,
				cert.CheckSignature(cert.SignatureAlgorithm, cert.RawTBSCertificate, cert.Signature),
				"self-signed",
			)
			assert.Equal(t, *got.Policy.KeyProperties.KeyType, tt.kty)
			assert.NotEmpty(t, got.X509Thumbprint)

			// the backing secret holds a usable certificate + private key
			sec, err := sc.GetSecret(ctx, "web", got.ID.Version(), nil)
			require.NoError(t, err)

			var gotCert, gotKey bool

			rest := []byte(*sec.Value)
			for {
				var blk *pem.Block

				blk, rest = pem.Decode(rest)
				if blk == nil {
					break
				}

				switch blk.Type {
				case "CERTIFICATE":
					gotCert = true
				case "PRIVATE KEY":
					k, kErr := x509.ParsePKCS8PrivateKey(blk.Bytes)
					require.NoError(t, kErr)

					switch k.(type) {
					case *rsa.PrivateKey, *ecdsa.PrivateKey:
						gotKey = true
					}
				}
			}

			assert.True(t, gotCert && gotKey)

			// a second create adds a version
			_, err = c.CreateCertificate(ctx, "web", selfSignedPolicy("CN=example.com", tt.kty), nil)
			require.NoError(t, err)

			pager := c.NewListCertificatePropertiesVersionsPager("web", nil)
			versions := 0

			for pager.More() {
				pg, pErr := pager.NextPage(ctx)
				require.NoError(t, pErr)

				versions += len(pg.Value)
			}

			assert.Equal(t, 2, versions)
		})
	}
}

func TestCertificates_Lifecycle_SDK(t *testing.T) {
	t.Parallel()

	s := defaultServer(t)
	c := newCertsClient(t, s, "life")
	ctx := t.Context()

	t.Run("delete_recover_purge_backup", func(t *testing.T) {
		t.Parallel()

		_, err := c.CreateCertificate(ctx, "x", selfSignedPolicy("CN=x", azcertificates.KeyTypeRSA), nil)
		require.NoError(t, err)

		bk, err := c.BackupCertificate(ctx, "x", nil)
		require.NoError(t, err)

		del, err := c.DeleteCertificate(ctx, "x", nil)
		require.NoError(t, err)
		assert.NotNil(t, del.DeletedDate)

		_, err = c.GetCertificate(ctx, "x", "", nil)
		require.Error(t, err)

		_, err = c.RecoverDeletedCertificate(ctx, "x", nil)
		require.NoError(t, err)

		_, err = c.GetCertificate(ctx, "x", "", nil)
		require.NoError(t, err)

		_, err = c.DeleteCertificate(ctx, "x", nil)
		require.NoError(t, err)
		_, err = c.PurgeDeletedCertificate(ctx, "x", nil)
		require.NoError(t, err)

		_, err = c.RestoreCertificate(
			ctx,
			azcertificates.RestoreCertificateParameters{CertificateBackup: bk.Value},
			nil,
		)
		require.NoError(t, err)
	})

	t.Run("import_pem", func(t *testing.T) {
		t.Parallel()

		key, err := rsa.GenerateKey(rand.Reader, 2048)
		require.NoError(t, err)

		tmpl := &x509.Certificate{
			SerialNumber: big.NewInt(42), Subject: pkix.Name{CommonName: "imported.test"},
			NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		}
		der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
		require.NoError(t, err)

		keyDER, err := x509.MarshalPKCS8PrivateKey(key)
		require.NoError(t, err)

		bundle := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})) +
			string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}))

		imp, err := c.ImportCertificate(ctx, "imp", azcertificates.ImportCertificateParameters{
			Base64EncodedCertificate: new(base64.StdEncoding.EncodeToString([]byte(bundle))),
		}, nil)
		require.NoError(t, err)
		assert.Equal(t, der, imp.CER)

		_, err = c.ImportCertificate(ctx, "bad", azcertificates.ImportCertificateParameters{
			Base64EncodedCertificate: new(base64.StdEncoding.EncodeToString([]byte("not a cert"))),
		}, nil)
		require.Error(t, err)
	})

	t.Run("update_and_policy", func(t *testing.T) {
		t.Parallel()

		created, err := c.CreateCertificate(ctx, "upd", selfSignedPolicy("CN=upd", azcertificates.KeyTypeRSA), nil)
		require.NoError(t, err)
		require.NotNil(t, created.ID)

		got, err := c.GetCertificate(ctx, "upd", "", nil)
		require.NoError(t, err)

		upd, err := c.UpdateCertificate(ctx, "upd", got.ID.Version(), azcertificates.UpdateCertificateParameters{
			Tags: map[string]*string{"k": new("v")},
		}, nil)
		require.NoError(t, err)
		assert.Equal(t, "v", *upd.Tags["k"])

		pol, err := c.GetCertificatePolicy(ctx, "upd", nil)
		require.NoError(t, err)
		assert.Equal(t, "CN=upd", *pol.X509CertificateProperties.Subject)
	})

	t.Run("unsupported_issuer", func(t *testing.T) {
		t.Parallel()

		p := selfSignedPolicy("CN=z", azcertificates.KeyTypeRSA)
		p.CertificatePolicy.IssuerParameters.Name = new("DigiCert")
		_, err := c.CreateCertificate(ctx, "z", p, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "BadParameter")
	})

	t.Run("issuers_and_contacts", func(t *testing.T) {
		t.Parallel()

		_, err := c.SetIssuer(ctx, "acme", azcertificates.SetIssuerParameters{
			Provider:    new("DigiCert"),
			Credentials: &azcertificates.IssuerCredentials{AccountID: new("acct"), Password: new("pw")},
		}, nil)
		require.NoError(t, err)

		iss, err := c.GetIssuer(ctx, "acme", nil)
		require.NoError(t, err)
		assert.Equal(t, "DigiCert", *iss.Provider)
		assert.Equal(t, "acct", *iss.Credentials.AccountID)
		assert.Nil(t, iss.Credentials.Password, "password is write-only")

		pager := c.NewListIssuerPropertiesPager(nil)
		pg, err := pager.NextPage(ctx)
		require.NoError(t, err)
		require.Len(t, pg.Value, 1)

		_, err = c.DeleteIssuer(ctx, "acme", nil)
		require.NoError(t, err)
		_, err = c.GetIssuer(ctx, "acme", nil)
		require.Error(t, err)

		_, err = c.GetContacts(ctx, nil)
		require.Error(t, err)

		_, err = c.SetContacts(ctx, azcertificates.Contacts{ContactList: []*azcertificates.Contact{
			{Email: new("a@example.com"), Name: new("A")},
		}}, nil)
		require.NoError(t, err)

		ct, err := c.GetContacts(ctx, nil)
		require.NoError(t, err)
		require.Len(t, ct.ContactList, 1)
		assert.Equal(t, "a@example.com", *ct.ContactList[0].Email)

		_, err = c.DeleteContacts(ctx, nil)
		require.NoError(t, err)
	})
}
