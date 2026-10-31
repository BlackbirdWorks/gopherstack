package app

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"log/slog"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	vpsdk "github.com/aws/aws-sdk-go-v2/service/verifiedpermissions"
	vptypes "github.com/aws/aws-sdk-go-v2/service/verifiedpermissions/types"
	"github.com/aws/smithy-go"
	"github.com/golang-jwt/jwt/v5"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	cognitoidpbackend "github.com/blackbirdworks/gopherstack/services/cognitoidp"
	verifiedpermissionsbackend "github.com/blackbirdworks/gopherstack/services/verifiedpermissions"
)

type vpJWTEnv struct {
	client  *vpsdk.Client
	vp      *verifiedpermissionsbackend.Handler
	idp     *cognitoidpbackend.Handler
	storeID string
}

func newVPJWTEnv(t *testing.T, disableVerification bool) *vpJWTEnv {
	t.Helper()

	cli := &CLI{AccountID: "000000000000", Region: "us-east-1"}
	cli.VerifiedPermissions.DisableJWTVerification = disableVerification
	cli.faultStore = chaos.NewFaultStore()

	services, err := initializeServicesT(t,
		&service.AppContext{Logger: slog.Default(), Config: cli, JanitorCtx: t.Context()},
	)
	require.NoError(t, err)

	byName := serviceByName(services)
	vpH, ok := byName["VerifiedPermissions"].(*verifiedpermissionsbackend.Handler)
	require.True(t, ok)
	idpH, ok := byName["CognitoIDP"].(*cognitoidpbackend.Handler)
	require.True(t, ok)

	registry := service.NewRegistry()
	require.NoError(t, registry.Register(vpH))

	e := echo.New()
	e.Use(service.NewServiceRouter(registry).RouteHandler())
	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	client := vpsdk.NewFromConfig(wiredSDKConfig(t, "test", "test"), func(o *vpsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})

	store, err := client.CreatePolicyStore(t.Context(), &vpsdk.CreatePolicyStoreInput{
		ValidationSettings: &vptypes.ValidationSettings{Mode: vptypes.ValidationModeOff},
	})
	require.NoError(t, err)

	return &vpJWTEnv{client: client, vp: vpH, idp: idpH, storeID: aws.ToString(store.PolicyStoreId)}
}

func (e *vpJWTEnv) addPolicy(t *testing.T, statement string) {
	t.Helper()

	_, err := e.client.CreatePolicy(t.Context(), &vpsdk.CreatePolicyInput{
		PolicyStoreId: aws.String(e.storeID),
		Definition: &vptypes.PolicyDefinitionMemberStatic{
			Value: vptypes.StaticPolicyDefinition{Statement: aws.String(statement)},
		},
	})
	require.NoError(t, err)
}

func (e *vpJWTEnv) isAuthorized(t *testing.T, action, idToken, accessToken string) (*vpsdk.IsAuthorizedWithTokenOutput, error) {
	t.Helper()

	in := &vpsdk.IsAuthorizedWithTokenInput{
		PolicyStoreId: aws.String(e.storeID),
		Action:        &vptypes.ActionIdentifier{ActionType: aws.String("App::Action"), ActionId: aws.String(action)},
		Resource:      &vptypes.EntityIdentifier{EntityType: aws.String("App::Doc"), EntityId: aws.String("d1")},
	}
	if idToken != "" {
		in.IdentityToken = aws.String(idToken)
	}

	if accessToken != "" {
		in.AccessToken = aws.String(accessToken)
	}

	return e.client.IsAuthorizedWithToken(t.Context(), in)
}

type cognitoFixture struct {
	poolID, poolARN, clientID, idToken, accessToken string
}

func (e *vpJWTEnv) cognitoSource(t *testing.T, configuredClient string) cognitoFixture {
	t.Helper()

	pool, err := e.idp.Backend.CreateUserPool("vp-users")
	require.NoError(t, err)
	client, err := e.idp.Backend.CreateUserPoolClient(pool.ID, "app")
	require.NoError(t, err)
	_, err = e.idp.Backend.CreateGroup(pool.ID, "admins", "", 1)
	require.NoError(t, err)
	_, err = e.idp.Backend.AdminCreateUser(pool.ID, "alice", "Temp#Pass1", nil)
	require.NoError(t, err)
	require.NoError(t, e.idp.Backend.AdminSetUserPassword(pool.ID, "alice", "Perm#Pass1x", true))
	require.NoError(t, e.idp.Backend.AdminAddUserToGroup(pool.ID, "alice", "admins"))

	auth, err := e.idp.Backend.InitiateAuth(client.ClientID, "USER_PASSWORD_AUTH", "alice", "Perm#Pass1x")
	require.NoError(t, err)

	if configuredClient == "" {
		configuredClient = client.ClientID
	}

	_, err = e.client.CreateIdentitySource(t.Context(), &vpsdk.CreateIdentitySourceInput{
		PolicyStoreId:       aws.String(e.storeID),
		PrincipalEntityType: aws.String("App::User"),
		Configuration: &vptypes.ConfigurationMemberCognitoUserPoolConfiguration{
			Value: vptypes.CognitoUserPoolConfiguration{
				UserPoolArn: aws.String(pool.ARN),
				ClientIds:   []string{configuredClient},
				GroupConfiguration: &vptypes.CognitoGroupConfiguration{
					GroupEntityType: aws.String("App::Group"),
				},
			},
		},
	})
	require.NoError(t, err)

	e.addPolicy(t, `permit(principal is App::User, action == App::Action::"read", resource);`)
	e.addPolicy(t, `permit(principal in App::Group::"`+pool.ID+`|admins", action == App::Action::"write", resource);`)
	e.addPolicy(t, `permit(principal in App::Group::"`+pool.ID+`|nobody", action == App::Action::"delete", resource);`)

	return cognitoFixture{
		poolID: pool.ID, poolARN: pool.ARN, clientID: client.ClientID,
		idToken: auth.Tokens.IDToken, accessToken: auth.Tokens.AccessToken,
	}
}

func tamperSignature(tok string) string {
	i := strings.LastIndex(tok, ".")
	sig := []byte(tok[i+1:])
	mid := len(sig) / 2

	if sig[mid] == 'A' {
		sig[mid] = 'B'
	} else {
		sig[mid] = 'A'
	}

	return tok[:i+1] + string(sig)
}

func errorCode(err error) string {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		return ae.ErrorCode()
	}

	return ""
}

func TestVerifiedPermissions_CognitoTokenVerification(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		clock      func() time.Time
		useAccess  bool
		tamper     bool
		wrongApp   bool
		disable    bool
		action     string
		wantDeny   bool
		wantErrors bool
	}{
		{name: "id_token_allow", action: "read"},
		{name: "access_token_allow", action: "read", useAccess: true},
		{name: "group_permit", action: "write"},
		{name: "group_not_member_deny", action: "delete", wantDeny: true},
		{name: "tampered_signature", action: "read", tamper: true, wantErrors: true},
		{name: "tampered_access_signature", action: "read", tamper: true, useAccess: true, wantErrors: true},
		{
			name: "expired", action: "read", wantErrors: true,
			clock: func() time.Time { return time.Now().Add(2 * time.Hour) },
		},
		{name: "wrong_client", action: "read", wrongApp: true, wantErrors: true},
		{name: "disable_flag_skips_tamper", action: "read", tamper: true, disable: true},
		{
			name: "disable_flag_skips_expiry", action: "read", disable: true,
			clock: func() time.Time { return time.Now().Add(2 * time.Hour) },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newVPJWTEnv(t, tt.disable)

			configured := ""
			if tt.wrongApp {
				configured = "someotherclient"
			}

			fx := env.cognitoSource(t, configured)

			if tt.clock != nil {
				env.vp.TokenVerifier().SetClock(tt.clock)
			}

			idTok, accTok := fx.idToken, ""
			if tt.useAccess {
				idTok, accTok = "", fx.accessToken
			}

			if tt.tamper {
				idTok, accTok = tamperIf(idTok), tamperIf(accTok)
			}

			out, err := env.isAuthorized(t, tt.action, idTok, accTok)
			if tt.wantErrors {
				require.Error(t, err)
				assert.Equal(t, "ValidationException", errorCode(err))

				return
			}

			require.NoError(t, err)

			want := vptypes.DecisionAllow
			if tt.wantDeny {
				want = vptypes.DecisionDeny
			}

			assert.Equal(t, want, out.Decision)
			assert.Equal(t, "App::User", aws.ToString(out.Principal.EntityType))
			assert.True(t, strings.HasPrefix(aws.ToString(out.Principal.EntityId), fx.poolID+"|"))
		})
	}
}

func tamperIf(tok string) string {
	if tok == "" {
		return ""
	}

	return tamperSignature(tok)
}

func TestVerifiedPermissions_CognitoTokenUseMismatch(t *testing.T) {
	t.Parallel()

	env := newVPJWTEnv(t, false)
	fx := env.cognitoSource(t, "")

	_, err := env.isAuthorized(t, "read", "", fx.idToken)
	require.Error(t, err)
	assert.Equal(t, "ValidationException", errorCode(err))

	_, err = env.isAuthorized(t, "read", fx.accessToken, "")
	require.Error(t, err)
}

type oidcServer struct {
	srv      *httptest.Server
	key      *rsa.PrivateKey
	jwksHits atomic.Int32
}

func newOIDCServer(t *testing.T) *oidcServer {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	o := &oidcServer{key: key}
	mux := http.NewServeMux()
	o.srv = httptest.NewTLSServer(mux)
	t.Cleanup(o.srv.Close)

	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"issuer": o.srv.URL, "jwks_uri": o.srv.URL + "/keys"})
	})
	mux.HandleFunc("/keys", func(w http.ResponseWriter, _ *http.Request) {
		o.jwksHits.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": []map[string]string{{
			"kty": "RSA", "use": "sig", "alg": "RS256", "kid": "k1",
			"n": base64.RawURLEncoding.EncodeToString(key.N.Bytes()),
			"e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.E)).Bytes()),
		}}})
	})

	return o
}

func (o *oidcServer) sign(t *testing.T, signer *rsa.PrivateKey, claims jwt.MapClaims) string {
	t.Helper()

	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	tok.Header["kid"] = "k1"
	s, err := tok.SignedString(signer)
	require.NoError(t, err)

	return s
}

func TestVerifiedPermissions_OIDCTokenVerification(t *testing.T) {
	t.Parallel()

	otherKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	tests := []struct {
		name     string
		action   string
		useOther bool
		audience string
		exp      time.Duration
		nbf      time.Duration
		asID     bool
		wantErr  bool
		wantDeny bool
	}{
		{name: "allow", action: "read", audience: "api", exp: time.Hour},
		{name: "group_permit", action: "write", audience: "api", exp: time.Hour},
		{name: "wrong_signing_key", action: "read", audience: "api", exp: time.Hour, useOther: true, wantErr: true},
		{name: "wrong_audience", action: "read", audience: "other", exp: time.Hour, wantErr: true},
		{name: "expired", action: "read", audience: "api", exp: -time.Hour, wantErr: true},
		{name: "not_yet_valid", action: "read", audience: "api", exp: 2 * time.Hour, nbf: time.Hour, wantErr: true},
		{name: "identity_token_for_access_source", action: "read", audience: "api", exp: time.Hour, asID: true, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newVPJWTEnv(t, false)
			o := newOIDCServer(t)
			trustOIDCServer(env, o)

			_, err := env.client.CreateIdentitySource(t.Context(), &vpsdk.CreateIdentitySourceInput{
				PolicyStoreId:       aws.String(env.storeID),
				PrincipalEntityType: aws.String("App::User"),
				Configuration: &vptypes.ConfigurationMemberOpenIdConnectConfiguration{
					Value: vptypes.OpenIdConnectConfiguration{
						Issuer:         aws.String(o.srv.URL),
						EntityIdPrefix: aws.String("corp"),
						GroupConfiguration: &vptypes.OpenIdConnectGroupConfiguration{
							GroupClaim:      aws.String("roles"),
							GroupEntityType: aws.String("App::Group"),
						},
						TokenSelection: &vptypes.OpenIdConnectTokenSelectionMemberAccessTokenOnly{
							Value: vptypes.OpenIdConnectAccessTokenConfiguration{
								Audiences:        []string{"api"},
								PrincipalIdClaim: aws.String("sub"),
							},
						},
					},
				},
			})
			require.NoError(t, err)

			env.addPolicy(t, `permit(principal == App::User::"corp|bob", action == App::Action::"read", resource);`)
			env.addPolicy(t, `permit(principal in App::Group::"corp|editors", action == App::Action::"write", resource);`)

			signer := o.key
			if tt.useOther {
				signer = otherKey
			}

			tok := o.sign(t, signer, jwt.MapClaims{
				"iss": o.srv.URL, "sub": "bob", "aud": tt.audience, "roles": []string{"editors"},
				"exp": time.Now().Add(tt.exp).Unix(), "nbf": time.Now().Add(tt.nbf).Unix(),
			})

			idTok, accTok := "", tok
			if tt.asID {
				idTok, accTok = tok, ""
			}

			out, err := env.isAuthorized(t, tt.action, idTok, accTok)
			if tt.wantErr {
				require.Error(t, err)
				assert.Equal(t, "ValidationException", errorCode(err))

				return
			}

			require.NoError(t, err)
			assert.Equal(t, vptypes.DecisionAllow, out.Decision)
			assert.Equal(t, "corp|bob", aws.ToString(out.Principal.EntityId))
		})
	}
}

func trustOIDCServer(env *vpJWTEnv, o *oidcServer) {
	pool := x509.NewCertPool()
	pool.AddCert(o.srv.Certificate())

	c := httpclient.New(5 * time.Second)
	tr := httpclient.NewTransport()
	tr.TLSClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	c.Transport = tr
	env.vp.TokenVerifier().SetHTTPClient(c)
}

func TestVerifiedPermissions_OIDCJWKSCacheTTL(t *testing.T) {
	t.Parallel()

	env := newVPJWTEnv(t, false)
	o := newOIDCServer(t)
	trustOIDCServer(env, o)

	_, err := env.client.CreateIdentitySource(t.Context(), &vpsdk.CreateIdentitySourceInput{
		PolicyStoreId:       aws.String(env.storeID),
		PrincipalEntityType: aws.String("App::User"),
		Configuration: &vptypes.ConfigurationMemberOpenIdConnectConfiguration{
			Value: vptypes.OpenIdConnectConfiguration{
				Issuer: aws.String(o.srv.URL),
				TokenSelection: &vptypes.OpenIdConnectTokenSelectionMemberAccessTokenOnly{
					Value: vptypes.OpenIdConnectAccessTokenConfiguration{Audiences: []string{"api"}},
				},
			},
		},
	})
	require.NoError(t, err)
	env.addPolicy(t, `permit(principal is App::User, action == App::Action::"read", resource);`)

	tok := o.sign(t, o.key, jwt.MapClaims{
		"iss": o.srv.URL, "sub": "bob", "aud": "api", "exp": time.Now().Add(48 * time.Hour).Unix(),
	})

	var offset atomic.Int64

	env.vp.TokenVerifier().SetClock(func() time.Time { return time.Now().Add(time.Duration(offset.Load())) })

	for range 3 {
		_, err = env.isAuthorized(t, "read", "", tok)
		require.NoError(t, err)
	}

	assert.EqualValues(t, 1, o.jwksHits.Load(), "JWKS cached within TTL")

	offset.Store(int64(2 * time.Hour))

	_, err = env.isAuthorized(t, "read", "", tok)
	require.NoError(t, err)
	assert.EqualValues(t, 2, o.jwksHits.Load(), "JWKS refetched after TTL")
}
