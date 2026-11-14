package app

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json/v2"
	"math/big"
	"net/http"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/cloud"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/aadauth"
	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/services/azuregraph"
)

const graphScope = "https://graph.microsoft.com/.default"

func (s *graphStack) clientOptions() azcore.ClientOptions {
	return azcore.ClientOptions{
		Cloud:     cloud.Configuration{ActiveDirectoryAuthorityHost: s.arm.URL},
		Transport: s.arm.Client(),
	}
}

func (s *graphStack) bootstrapToken(t *testing.T) string {
	t.Helper()

	status, boot := s.token(t, "", "")
	require.Equal(t, http.StatusOK, status)

	tok, _ := boot["access_token"].(string)

	return tok
}

// registerApp creates an application with a service principal through Graph and returns both IDs.
func (s *graphStack) registerApp(t *testing.T, admin, name string) (string, string, string) {
	t.Helper()

	status, app := s.call(t, admin, http.MethodPost, "/v1.0/applications", `{"displayName":"`+name+`"}`)
	require.Equal(t, http.StatusCreated, status)

	appID, _ := app["appId"].(string)
	objectID, _ := app["id"].(string)

	status, sp := s.call(t, admin, http.MethodPost, "/v1.0/servicePrincipals", `{"appId":"`+appID+`"}`)
	require.Equal(t, http.StatusCreated, status)

	spID, _ := sp["id"].(string)

	return appID, objectID, spID
}

func selfSignedCert(t *testing.T) (*x509.Certificate, *rsa.PrivateKey) {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: "sdk-test"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature,
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)

	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)

	return cert, key
}

func tokenClaims(t *testing.T, token string) jwt.MapClaims {
	t.Helper()

	claims := jwt.MapClaims{}
	_, _, err := jwt.NewParser().ParseUnverified(token, claims)
	require.NoError(t, err)

	return claims
}

func TestAzureIdentityClientCertificateCredential(t *testing.T) {
	t.Parallel()

	s := newGraphStack(t)
	admin := s.bootstrapToken(t)
	appID, objectID, spID := s.registerApp(t, admin, "cert-daemon")
	cert, key := selfSignedCert(t)

	keyBody, err := json.Marshal(map[string]any{"keyCredential": map[string]any{
		"type": "AsymmetricX509Cert", "usage": "Verify", "key": cert.Raw,
	}}, jsonx.V1())
	require.NoError(t, err)

	status, added := s.call(t, admin, http.MethodPost, "/v1.0/applications/"+objectID+"/addKey", string(keyBody))
	require.Equal(t, http.StatusOK, status, added)

	newCred := func(c *x509.Certificate, k *rsa.PrivateKey) *azidentity.ClientCertificateCredential {
		cred, cerr := azidentity.NewClientCertificateCredential(azuregraph.DefaultTenantID, appID,
			[]*x509.Certificate{c}, k, &azidentity.ClientCertificateCredentialOptions{
				ClientOptions: s.clientOptions(), DisableInstanceDiscovery: true,
			})
		require.NoError(t, cerr)

		return cred
	}

	t.Run("registered certificate obtains a token usable on graph", func(t *testing.T) {
		t.Parallel()

		tok, gerr := newCred(
			cert,
			key,
		).GetToken(context.Background(), policy.TokenRequestOptions{Scopes: []string{graphScope}})
		require.NoError(t, gerr)

		claims := tokenClaims(t, tok.Token)
		assert.Equal(t, spID, claims["oid"])
		assert.Equal(t, appID, claims["appid"])

		code, got := s.call(t, tok.Token, http.MethodGet, "/v1.0/servicePrincipals(appId='"+appID+"')", "")
		require.Equal(t, http.StatusOK, code)
		assert.Equal(t, spID, got["id"])
	})

	t.Run("unregistered certificate is rejected", func(t *testing.T) {
		t.Parallel()

		otherCert, otherKey := selfSignedCert(t)
		_, gerr := newCred(
			otherCert,
			otherKey,
		).GetToken(context.Background(), policy.TokenRequestOptions{Scopes: []string{graphScope}})
		require.Error(t, gerr)
		assert.Contains(t, gerr.Error(), "AADSTS700027")
	})
}

func TestAzureIdentityClientAssertionCredentialFederated(t *testing.T) {
	t.Parallel()

	s := newGraphStack(t)
	admin := s.bootstrapToken(t)
	appID, objectID, spID := s.registerApp(t, admin, "federated-daemon")

	const (
		issuer   = "https://issuer.example.test"
		subject  = "workload-1"
		audience = "api://AzureADTokenExchange"
	)

	status, fic := s.call(t, admin, http.MethodPost, "/v1.0/applications/"+objectID+"/federatedIdentityCredentials",
		`{"name":"workload","issuer":"`+issuer+`","subject":"`+subject+`","audiences":["`+audience+`"]}`)
	require.Equal(t, http.StatusCreated, status, fic)

	emulatorAssertion := func(sub string) func(context.Context) (string, error) {
		return func(context.Context) (string, error) {
			return s.armH.Issuer.IssueClientCredentialsToken(aadauth.ClientCredentialsClaims{
				Issuer: issuer, Audience: audience, TenantID: azuregraph.DefaultTenantID, ClientID: sub, ObjectID: sub,
			}, 0)
		}
	}

	foreign, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	forged := func(context.Context) (string, error) {
		now := time.Now()
		tok := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
			"iss": issuer, "sub": subject, "aud": audience, "nbf": now.Unix(), "exp": now.Add(time.Hour).Unix(),
		})
		tok.Header["kid"] = s.armH.Issuer.KeyID()

		return tok.SignedString(foreign)
	}

	tests := []struct {
		assertion func(context.Context) (string, error)
		name      string
		wantErr   string
	}{
		{emulatorAssertion(subject), "assertion from the emulator issuer", ""},
		{emulatorAssertion("other-workload"), "subject without a credential", "AADSTS70021"},
		{forged, "forged signature under the emulator key id", "AADSTS700027"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cred, cerr := azidentity.NewClientAssertionCredential(
				azuregraph.DefaultTenantID,
				appID,
				tt.assertion,
				&azidentity.ClientAssertionCredentialOptions{
					ClientOptions:            s.clientOptions(),
					DisableInstanceDiscovery: true,
				},
			)
			require.NoError(t, cerr)

			tok, gerr := cred.GetToken(context.Background(), policy.TokenRequestOptions{Scopes: []string{graphScope}})
			if tt.wantErr != "" {
				require.Error(t, gerr)
				assert.Contains(t, gerr.Error(), tt.wantErr)

				return
			}

			require.NoError(t, gerr)
			assert.Equal(t, spID, tokenClaims(t, tok.Token)["oid"])

			code, body := s.call(t, tok.Token, http.MethodGet, "/v1.0/applications/"+objectID, "")
			assert.Equal(t, http.StatusOK, code, body)
		})
	}
}
