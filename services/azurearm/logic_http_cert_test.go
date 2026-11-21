package azurearm_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/logic/armlogic"
	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pkcs12 "software.sslmate.com/src/go-pkcs12"
)

const certPassword = "pfx-pass-w0rd"

type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pool *x509.CertPool
}

func newTestCA(t *testing.T, name string) *testCA {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: name}, IsCA: true, BasicConstraintsValid: true,
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)

	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)

	pool := x509.NewCertPool()
	pool.AddCert(cert)

	return &testCA{cert: cert, key: key, pool: pool}
}

func (ca *testCA) issue(t *testing.T, cn string, usage x509.ExtKeyUsage) (*x509.Certificate, *rsa.PrivateKey) {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: cn},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{usage},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	require.NoError(t, err)

	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)

	return cert, key
}

func (ca *testCA) pfx(t *testing.T, cn string) (string, *x509.Certificate) {
	t.Helper()

	cert, key := ca.issue(t, cn, x509.ExtKeyUsageClientAuth)
	raw, err := pkcs12.Modern2023.Encode(key, cert, nil, certPassword)
	require.NoError(t, err)

	return base64.StdEncoding.EncodeToString(raw), cert
}

// mtlsBackend is a TLS server that requires a client certificate issued by clientCA.
func mtlsBackend(t *testing.T, serverCA, clientCA *testCA) *httptest.Server {
	t.Helper()

	cert, key := serverCA.issue(t, "backend", x509.ExtKeyUsageServerAuth)
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(r.TLS.PeerCertificates[0].Subject.CommonName + "|" + tls.VersionName(r.TLS.Version)))
	}))
	srv.TLS = &tls.Config{
		Certificates: []tls.Certificate{{Certificate: [][]byte{cert.Raw}, PrivateKey: key}},
		ClientAuth:   tls.RequireAndVerifyClientCert, ClientCAs: clientCA.pool, MinVersion: tls.VersionTLS12,
	}
	srv.StartTLS()
	t.Cleanup(srv.Close)

	return srv
}

func TestLogicHTTPClientCertificate(t *testing.T) {
	t.Parallel()

	serverCA, clientCA, otherCA := newTestCA(t, "server-ca"), newTestCA(t, "client-ca"), newTestCA(t, "other-ca")
	good, _ := clientCA.pfx(t, "logic-client")
	foreign, _ := otherCA.pfx(t, "intruder")
	backend := mtlsBackend(t, serverCA, clientCA)

	tests := []struct {
		auth       la
		name       string
		wantStatus armlogic.WorkflowStatus
		wantCode   string
		wantBody   string
	}{
		{
			name: "valid certificate", wantStatus: armlogic.WorkflowStatusSucceeded,
			auth:     la{"type": "ClientCertificate", "pfx": good, "password": certPassword},
			wantBody: "logic-client|TLS 1.3",
		},
		{
			name: "wrong password", wantStatus: armlogic.WorkflowStatusFailed, wantCode: "BadRequest",
			auth: la{"type": "ClientCertificate", "pfx": good, "password": "nope"},
		},
		{
			name: "not base64", wantStatus: armlogic.WorkflowStatusFailed, wantCode: "BadRequest",
			auth: la{"type": "ClientCertificate", "pfx": "%%%", "password": certPassword},
		},
		{
			name: "certificate from an untrusted issuer", wantStatus: armlogic.WorkflowStatusFailed,
			wantCode: "ActionFailed", auth: la{"type": "ClientCertificate", "pfx": foreign, "password": certPassword},
		},
		{
			name: "no certificate presented", wantStatus: armlogic.WorkflowStatusFailed, wantCode: "ActionFailed",
			auth: la{"type": "None"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newLogicStack(t)
			s.prov.SetOutboundRootCAs(serverCA.pool)
			s.create(t, "mtls", logicDef(requestTrigger(nil), la{"Call": la{
				"type": "Http", "runAfter": la{},
				"inputs": la{
					"method": "GET", "uri": backend.URL, "authentication": tc.auth,
					"retryPolicy": la{"type": "none"},
				},
			}}))

			call := s.invoke(t, "mtls", `{}`).actions["Call"].Properties
			assert.Equal(t, tc.wantStatus, *call.Status)

			if tc.wantCode != "" {
				assert.Equal(t, tc.wantCode, *call.Code)
				assert.NotContains(t, toJSON(t, call.Error), certPassword)
			}

			if tc.wantBody != "" {
				out, ok := s.content(t, call.OutputsLink).(map[string]any)
				require.True(t, ok)
				assert.True(t, strings.HasPrefix(out["body"].(string), "logic-client|TLS 1."), "%v", out["body"])
			}
		})
	}
}

func TestLogicHTTPClientCertificateSecretsNotRecorded(t *testing.T) {
	t.Parallel()

	serverCA, clientCA := newTestCA(t, "server-ca"), newTestCA(t, "client-ca")
	good, _ := clientCA.pfx(t, "logic-client")
	backend := mtlsBackend(t, serverCA, clientCA)

	s := newLogicStack(t)
	s.prov.SetOutboundRootCAs(serverCA.pool)

	def := logicDef(requestTrigger(nil), la{"Call": la{
		"type": "Http", "runAfter": la{},
		"inputs": la{
			"method": "GET", "uri": backend.URL, "retryPolicy": la{"type": "none"},
			"authentication": la{
				"type": "ClientCertificate", "pfx": "@parameters('pfxParam')", "password": "@parameters('passParam')",
			},
		},
	}})
	def["parameters"] = la{
		"pfxParam":  la{"type": "String", "defaultValue": good},
		"passParam": la{"type": "String", "defaultValue": certPassword},
	}
	s.create(t, "secrets", def)

	out := s.invoke(t, "secrets", `{}`)
	require.Equal(t, armlogic.WorkflowStatusSucceeded, *out.actions["Call"].Properties.Status)

	run := *out.run.Name
	tr, err := s.factory.NewWorkflowRunActionsClient().
		NewListExpressionTracesPager(logicRG, "secrets", run, "Call", nil).NextPage(t.Context())
	require.NoError(t, err)
	require.NotEmpty(t, tr.Inputs)

	for _, r := range tr.Inputs {
		if strings.HasPrefix(*r.Path, "inputs.authentication") {
			assert.Nil(t, r.Value)
		}
	}

	assert.NotContains(t, toJSON(t, tr), certPassword)
	assert.NotContains(t, toJSON(t, tr), good)
	assert.NotContains(t, toJSON(t, s.content(t, out.actions["Call"].Properties.InputsLink)), "authentication")

	for _, h := range s.requestHistories(t, "secrets", "Call") {
		assert.NotContains(t, toJSON(t, h), certPassword)
	}
}

func TestLogicHTTPOAuthCertificate(t *testing.T) {
	t.Parallel()

	ca := newTestCA(t, "client-ca")
	pfx, cert := ca.pfx(t, "app")
	otherPFX, _ := newTestCA(t, "other").pfx(t, "app")
	sum := sha256.Sum256(cert.Raw)

	tests := []struct {
		auth       la
		name       string
		wantStatus armlogic.WorkflowStatus
		wantCode   string
	}{
		{
			name: "certificate credential", wantStatus: armlogic.WorkflowStatusSucceeded,
			auth: la{"credentialType": "Certificate", "pfx": pfx, "password": certPassword},
		},
		{
			name: "assertion signed by another key is rejected", wantStatus: armlogic.WorkflowStatusFailed,
			wantCode: "ActionFailed",
			auth:     la{"credentialType": "Certificate", "pfx": otherPFX, "password": certPassword},
		},
		{
			name: "bad pfx", wantStatus: armlogic.WorkflowStatusFailed, wantCode: "BadRequest",
			auth: la{"credentialType": "Certificate", "pfx": pfx, "password": "wrong"},
		},
		{
			name: "pfx missing", wantStatus: armlogic.WorkflowStatusFailed, wantCode: "InvalidTemplate",
			auth: la{"credentialType": "Certificate"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			gotAuth := make(chan string, 1)
			backend := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
				gotAuth <- r.Header.Get("Authorization")
			}))
			t.Cleanup(backend.Close)

			var authority *httptest.Server

			authority = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.NoError(t, r.ParseForm())
				assert.Equal(t, "/t1/oauth2/token", r.URL.Path)
				assert.Equal(t, "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
					r.PostForm.Get("client_assertion_type"))
				assert.Empty(t, r.PostForm.Get("client_secret"))

				claims := jwt.MapClaims{}
				_, err := jwt.NewParser(jwt.WithValidMethods([]string{"RS256"}), jwt.WithoutClaimsValidation()).
					ParseWithClaims(
						r.PostForm.Get("client_assertion"), claims,
						func(tok *jwt.Token) (any, error) {
							assert.Len(t, tok.Header["x5t"], 27, "SHA-1 thumbprint, base64url")
							if tc.wantCode == "" {
								assert.Equal(t, base64.RawURLEncoding.EncodeToString(sum[:]), tok.Header["x5t#S256"])
							}

							return cert.PublicKey, nil
						})
				if err != nil {
					http.Error(w, `{"error":"invalid_client"}`, http.StatusUnauthorized)

					return
				}

				assert.Equal(t, "cid", claims["iss"])
				assert.Equal(t, "cid", claims["sub"])
				assert.Equal(t, authority.URL+"/t1/oauth2/token", claims["aud"])

				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"token_type":"Bearer","access_token":"cert-tok"}`))
			}))
			t.Cleanup(authority.Close)

			auth := tc.auth
			auth["type"], auth["authority"] = "ActiveDirectoryOAuth", authority.URL+"/"
			auth["tenant"], auth["audience"], auth["clientId"] = "t1", "https://aud/", "cid"

			s := newLogicStack(t)
			s.create(t, "oauthcert", logicDef(requestTrigger(nil), la{"Call": la{
				"type": "Http", "runAfter": la{},
				"inputs": la{
					"method": "GET", "uri": backend.URL, "authentication": auth, "retryPolicy": la{"type": "none"},
				},
			}}))

			call := s.invoke(t, "oauthcert", `{}`).actions["Call"].Properties
			require.Equal(t, tc.wantStatus, *call.Status, "%s", toJSON(t, call.Error))

			if tc.wantCode != "" {
				assert.Equal(t, tc.wantCode, *call.Code)

				return
			}

			assert.Equal(t, "Bearer cert-tok", <-gotAuth)
		})
	}
}
