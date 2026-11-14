package azurearm_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/appcontainers/armappcontainers/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pkcs12 "software.sslmate.com/src/go-pkcs12"
)

func testPFX(t *testing.T, cn, password string) []byte {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(7), Subject: pkix.Name{CommonName: cn}, DNSNames: []string{cn, "alt." + cn},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)

	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)

	pfx, err := pkcs12.Modern.Encode(key, cert, nil, password)
	require.NoError(t, err)

	return pfx
}

func TestACADaprComponents(t *testing.T) {
	t.Parallel()

	tests := []struct {
		props   *armappcontainers.DaprComponentProperties
		name    string
		wantErr bool
	}{
		{name: "valid", props: &armappcontainers.DaprComponentProperties{
			ComponentType: new("state.azure.blobstorage"), Version: new("v1"),
			Secrets: []*armappcontainers.Secret{{Name: new("key"), Value: new("s3cret")}},
			Metadata: []*armappcontainers.DaprMetadata{
				{Name: new("account"), Value: new("acct")},
				{Name: new("accountKey"), SecretRef: new("key")},
			},
			Scopes: []*string{new("app1")},
		}},
		{
			name:    "no component type",
			wantErr: true,
			props:   &armappcontainers.DaprComponentProperties{Version: new("v1")},
		},
		{
			name:    "no version",
			wantErr: true,
			props:   &armappcontainers.DaprComponentProperties{ComponentType: new("pubsub.redis")},
		},
		{name: "dangling secret ref", wantErr: true, props: &armappcontainers.DaprComponentProperties{
			ComponentType: new("pubsub.redis"), Version: new("v1"),
			Metadata: []*armappcontainers.DaprMetadata{{Name: new("pw"), SecretRef: new("nope")}},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newACAStack(t)
			s.mustEnv(t, "rg", "env1")
			c := s.factory.NewDaprComponentsClient()

			got, err := c.CreateOrUpdate(t.Context(), "rg", "env1", "comp1",
				armappcontainers.DaprComponent{Properties: tt.props}, nil)
			if tt.wantErr {
				require.Error(t, err)
				assert.Equal(t, "InvalidRequestParameterWithDetails", sdkErrCode(err))

				return
			}

			require.NoError(t, err)
			assert.Nil(t, got.Properties.Secrets[0].Value, "secret values are write-only")

			secrets, err := c.ListSecrets(t.Context(), "rg", "env1", "comp1", nil)
			require.NoError(t, err)
			require.Len(t, secrets.Value, 1)
			assert.Equal(t, "s3cret", *secrets.Value[0].Value)

			pager := c.NewListPager("rg", "env1", nil)
			page, err := pager.NextPage(t.Context())
			require.NoError(t, err)
			assert.Len(t, page.Value, 1)

			_, err = c.Delete(t.Context(), "rg", "env1", "comp1", nil)
			require.NoError(t, err)

			_, err = c.Get(t.Context(), "rg", "env1", "comp1", nil)
			require.Error(t, err)

			_, err = c.CreateOrUpdate(t.Context(), "rg", "missing", "comp1",
				armappcontainers.DaprComponent{Properties: tt.props}, nil)
			require.Error(t, err, "environment must exist")
		})
	}
}

func TestACACertificates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		password string
		value    []byte
		wantErr  bool
	}{
		{name: "pfx", value: testPFX(t, "app.example.com", "pw"), password: "pw"},
		{name: "wrong password", value: testPFX(t, "app.example.com", "pw"), password: "bad", wantErr: true},
		{name: "garbage", value: []byte("not a cert"), wantErr: true},
		{name: "empty", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newACAStack(t)
			s.mustEnv(t, "rg", "env1")
			c := s.factory.NewCertificatesClient()

			got, err := c.CreateOrUpdate(
				t.Context(),
				"rg",
				"env1",
				"cert1",
				&armappcontainers.CertificatesClientCreateOrUpdateOptions{
					CertificateEnvelope: &armappcontainers.Certificate{
						Location: new("local"),
						Properties: &armappcontainers.CertificateProperties{
							Value: tt.value, Password: new(tt.password),
						},
					},
				},
			)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, "app.example.com", *got.Properties.SubjectName)
			assert.Len(t, *got.Properties.Thumbprint, 40)
			assert.True(t, *got.Properties.Valid)
			assert.ElementsMatch(t, []string{"app.example.com", "alt.app.example.com"},
				[]string{*got.Properties.SubjectAlternativeNames[0], *got.Properties.SubjectAlternativeNames[1]})
			assert.Nil(t, got.Properties.Password)

			patched, err := c.Update(t.Context(), "rg", "env1", "cert1", armappcontainers.CertificatePatch{
				Tags: map[string]*string{"k": new("v")},
			}, nil)
			require.NoError(t, err)
			assert.Equal(t, "v", *patched.Tags["k"])
			assert.Equal(t, *got.Properties.Thumbprint, *patched.Properties.Thumbprint)

			_, err = c.Delete(t.Context(), "rg", "env1", "cert1", nil)
			require.NoError(t, err)
		})
	}
}

func TestACAJavaComponents(t *testing.T) {
	t.Parallel()

	s := newACAStack(t)
	s.mustEnv(t, "rg-java", "env1")
	c := s.factory.NewJavaComponentsClient()

	put := func(name string, props armappcontainers.JavaComponentPropertiesClassification) error {
		p, err := c.BeginCreateOrUpdate(
			t.Context(),
			"rg-java",
			"env1",
			name,
			armappcontainers.JavaComponent{Properties: props},
			nil,
		)
		if err != nil {
			return err
		}

		_, err = p.PollUntilDone(t.Context(), pollFast())

		return err
	}

	eureka := armappcontainers.JavaComponentTypeSpringCloudEureka
	require.NoError(t, put("eureka", &armappcontainers.SpringCloudEurekaComponent{ComponentType: &eureka}))

	got, err := c.Get(t.Context(), "rg-java", "env1", "eureka", nil)
	require.NoError(t, err)
	assert.Equal(t, armappcontainers.JavaComponentProvisioningStateSucceeded,
		*got.Properties.GetJavaComponentProperties().ProvisioningState)

	bad := armappcontainers.JavaComponentType("Zookeeper")
	require.Error(t, put("zk", &armappcontainers.JavaComponentProperties{ComponentType: &bad}))

	admin := armappcontainers.JavaComponentTypeSpringBootAdmin
	require.Error(t, put("admin", &armappcontainers.SpringBootAdminComponent{
		ComponentType: &admin, ServiceBinds: []*armappcontainers.JavaComponentServiceBind{
			{Name: new("b"), ServiceID: new(s.envID("rg-java", "env1") + "/javaComponents/missing")},
		},
	}), "bind target must exist")

	require.NoError(t, put("admin", &armappcontainers.SpringBootAdminComponent{
		ComponentType: &admin, ServiceBinds: []*armappcontainers.JavaComponentServiceBind{
			{Name: new("b"), ServiceID: new(s.envID("rg-java", "env1") + "/javaComponents/eureka")},
		},
	}))

	del, err := c.BeginDelete(t.Context(), "rg-java", "env1", "eureka", nil)
	if err == nil {
		_, err = del.PollUntilDone(t.Context(), pollFast())
	}

	require.Error(t, err, "a bound component cannot be deleted")

	pager := c.NewListPager("rg-java", "env1", nil)
	page, err := pager.NextPage(t.Context())
	require.NoError(t, err)
	assert.Len(t, page.Value, 2)
}

func TestACAChildrenSurviveRestore(t *testing.T) {
	t.Parallel()

	s := newACAStack(t)
	s.mustEnv(t, "rg", "env1")

	_, err := s.factory.NewDaprComponentsClient().CreateOrUpdate(t.Context(), "rg", "env1", "comp1",
		armappcontainers.DaprComponent{Properties: &armappcontainers.DaprComponentProperties{
			ComponentType: new("pubsub.redis"), Version: new("v1"),
		}}, nil)
	require.NoError(t, err)

	other := newACAStack(t)
	require.NoError(t, other.handler.Restore(t.Context(), s.handler.Snapshot(t.Context())))

	got, err := other.factory.NewDaprComponentsClient().Get(t.Context(), "rg", "env1", "comp1", nil)
	require.NoError(t, err)
	assert.Equal(t, "pubsub.redis", *got.Properties.ComponentType)
}
