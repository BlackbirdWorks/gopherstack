package eks

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

const (
	testAccount    = "000000000000"
	testAdminToken = "admin-token-0123456789"
	roleARN        = "arn:aws:iam::" + testAccount + ":role/dev"
	sessionARN     = "arn:aws:sts::" + testAccount + ":assumed-role/dev/alice@example.com"
	userARN        = "arn:aws:iam::" + testAccount + ":user/bob"
	rootARN        = "arn:aws:iam::" + testAccount + ":root"
)

var errUnknownKey = errors.New("unknown access key")

type fakeCreds map[string]CredentialIdentity

func (f fakeCreds) ResolveCredential(accessKeyID, _ string) (CredentialIdentity, error) {
	if id, ok := f[accessKeyID]; ok {
		return id, nil
	}

	return CredentialIdentity{}, errUnknownKey
}

// makeToken builds a bearer exactly as `aws eks get-token` does: a presigned STS GetCallerIdentity URL.
func makeToken(t *testing.T, accessKey, secret, cluster, host string, signedAt time.Time) string {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet,
		"https://"+host+"/?Action=GetCallerIdentity&Version=2011-06-15&X-Amz-Expires=60", nil)
	require.NoError(t, err)

	req.Header.Set(clusterIDHeader, cluster)

	signed, _, err := v4.NewSigner().PresignHTTP(t.Context(),
		aws.Credentials{AccessKeyID: accessKey, SecretAccessKey: secret}, req, emptyPayloadHash,
		sigService, "us-east-1", signedAt)
	require.NoError(t, err)

	return IAMTokenPrefix + base64.RawURLEncoding.EncodeToString([]byte(signed))
}

func TestAuthenticateIAMToken(t *testing.T) {
	t.Parallel()

	res := fakeCreds{
		"AKIAREAL":  {PrincipalARN: userARN, Secret: "s3cret"},
		"AKIAROOT":  {PrincipalARN: rootARN},
		"AKIAOTHER": {PrincipalARN: roleARN, Secret: "other"},
	}
	now := time.Now().UTC()

	tests := []struct {
		name    string
		token   func(t *testing.T) string
		wantARN string
		wantErr bool
	}{
		{
			name: "valid",
			token: func(t *testing.T) string {
				t.Helper()

				return makeToken(t, "AKIAREAL", "s3cret", "c1", "sts.us-east-1.amazonaws.com", now)
			},
			wantARN: userARN,
		},
		{
			name: "emulator_endpoint_host",
			token: func(t *testing.T) string {
				t.Helper()

				return makeToken(t, "AKIAREAL", "s3cret", "c1", "localhost:4566", now)
			},
			wantARN: userARN,
		},
		{
			name: "unverifiable_root_key",
			token: func(t *testing.T) string {
				t.Helper()

				return makeToken(t, "AKIAROOT", "anything", "c1", "sts.us-east-1.amazonaws.com", now)
			},
			wantARN: rootARN,
		},
		{
			name: "wrong_secret",
			token: func(t *testing.T) string {
				t.Helper()

				return makeToken(t, "AKIAREAL", "wrong", "c1", "sts.us-east-1.amazonaws.com", now)
			},
			wantErr: true,
		},
		{
			name: "other_cluster",
			token: func(t *testing.T) string {
				t.Helper()

				return makeToken(t, "AKIAREAL", "s3cret", "c2", "sts.us-east-1.amazonaws.com", now)
			},
			wantErr: true,
		},
		{
			name: "expired",
			token: func(t *testing.T) string {
				t.Helper()

				return makeToken(t, "AKIAREAL", "s3cret", "c1", "sts.us-east-1.amazonaws.com", now.Add(-20*time.Minute))
			},
			wantErr: true,
		},
		{
			name: "future",
			token: func(t *testing.T) string {
				t.Helper()

				return makeToken(t, "AKIAREAL", "s3cret", "c1", "sts.us-east-1.amazonaws.com", now.Add(time.Hour))
			},
			wantErr: true,
		},
		{
			name: "unknown_key",
			token: func(t *testing.T) string {
				t.Helper()

				return makeToken(t, "AKIANOPE", "s3cret", "c1", "sts.us-east-1.amazonaws.com", now)
			},
			wantErr: true,
		},
		{name: "garbage", token: func(*testing.T) string { return IAMTokenPrefix + "!!!" }, wantErr: true},
		{name: "no_prefix", token: func(*testing.T) string { return "abc" }, wantErr: true},
		{
			name: "wrong_action",
			token: func(*testing.T) string {
				u := "https://sts.amazonaws.com/?Action=AssumeRole&X-Amz-Algorithm=AWS4-HMAC-SHA256"

				return IAMTokenPrefix + base64.RawURLEncoding.EncodeToString([]byte(u))
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			id, _, err := authenticateIAMToken(res, tt.token(t), "c1", time.Now())
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantARN, id.PrincipalARN)
		})
	}
}

func TestAuthenticateIAMToken_NoResolver(t *testing.T) {
	t.Parallel()

	tok := makeToken(t, "AKIAREAL", "s3cret", "c1", "sts.us-east-1.amazonaws.com", time.Now().UTC())
	_, _, err := authenticateIAMToken(nil, tok, "c1", time.Now())
	require.Error(t, err)
}

func newIdentityBackend(t *testing.T, ac *AccessConfig, creator string) *InMemoryBackend {
	t.Helper()

	b := NewInMemoryBackend(t.Context(), testAccount, "us-east-1")
	t.Cleanup(b.Close)

	_, err := b.CreateCluster(
		"c1",
		"1.32",
		"",
		nil,
		nil,
		nil,
		ClusterOptionalConfig{AccessConfig: ac, CreatorARN: creator},
	)
	require.NoError(t, err)

	return b
}

func TestK8sIdentityMapping(t *testing.T) {
	t.Parallel()

	awsAuth := parseAwsAuth(map[string]string{
		"mapRoles": "- rolearn: " + roleARN + "\n  username: cm-{{SessionName}}\n  groups: [cm-devs]\n",
		"mapUsers": "- userarn: " + userARN + "\n  username: bob-cm\n  groups: [cm-ops]\n",
	})

	tests := []struct {
		name       string
		access     *AccessConfig
		principal  string
		entryUser  string
		entryARN   string
		wantUser   string
		wantGroups []string
		wantOK     bool
	}{
		{
			name:      "creator_is_master",
			access:    &AccessConfig{BootstrapClusterCreatorAdminPermissions: true, AuthenticationMode: authModeAPI},
			principal: rootARN, wantUser: rootARN, wantGroups: []string{mastersGroup}, wantOK: true,
		},
		{
			name:      "creator_default_when_no_access_config",
			principal: rootARN, wantUser: rootARN, wantGroups: []string{mastersGroup}, wantOK: true,
		},
		{
			name:      "creator_bootstrap_off",
			access:    &AccessConfig{AuthenticationMode: authModeAPI},
			principal: rootARN, wantOK: false,
		},
		{
			name:   "entry_role_session",
			access: &AccessConfig{AuthenticationMode: authModeAPI}, principal: sessionARN, entryARN: roleARN,
			entryUser: "ent-{{SessionName}}", wantUser: "ent-alice-example.com", wantOK: true,
		},
		{
			name:   "entry_default_username",
			access: &AccessConfig{AuthenticationMode: authModeAPI}, principal: sessionARN, entryARN: roleARN,
			wantUser: "arn:aws:sts::" + testAccount + ":assumed-role/dev/alice-example.com", wantOK: true,
		},
		{
			name:   "entry_ignored_in_config_map_mode",
			access: &AccessConfig{AuthenticationMode: authModeConfigMap}, principal: userARN, entryARN: userARN,
			wantUser: "bob-cm", wantGroups: []string{"cm-ops"}, wantOK: true,
		},
		{
			name:   "aws_auth_ignored_in_api_mode",
			access: &AccessConfig{AuthenticationMode: authModeAPI}, principal: userARN, wantOK: false,
		},
		{
			name:      "aws_auth_role_template",
			access:    &AccessConfig{AuthenticationMode: authModeConfigMap},
			principal: sessionARN, wantUser: "cm-alice-example.com", wantGroups: []string{"cm-devs"}, wantOK: true,
		},
		{
			name:   "entry_wins_over_aws_auth",
			access: &AccessConfig{AuthenticationMode: authModeAPIAndConfig}, principal: userARN, entryARN: userARN,
			entryUser: "from-entry", wantUser: "from-entry", wantOK: true,
		},
		{
			name:      "unknown_denied",
			access:    &AccessConfig{AuthenticationMode: authModeAPI},
			principal: userARN,
			wantOK:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newIdentityBackend(t, tt.access, "")
			if tt.entryARN != "" {
				_, err := b.CreateAccessEntry("c1", tt.entryARN, "STANDARD", tt.entryUser, nil, nil)
				require.NoError(t, err)
			}

			p, ok := parsePrincipal(tt.principal)
			require.True(t, ok)

			b.mu.RLock("test")
			defer b.mu.RUnlock()

			cl, _ := b.clusters.Get("c1")
			id, granted := b.k8sIdentityForLocked(cl, p, "AKIAX", awsAuth)

			require.Equal(t, tt.wantOK, granted)

			if !tt.wantOK {
				return
			}

			assert.Equal(t, tt.wantUser, id.Username)

			for _, g := range tt.wantGroups {
				assert.Contains(t, id.Groups, g)
			}

			if tt.entryARN != "" && authModeAllows(accessMode(cl), authModeAPI) {
				assert.Contains(t, id.Groups, accessGroupName("c1", p))
			}
		})
	}
}

func TestDesiredBindings(t *testing.T) {
	t.Parallel()

	b := newIdentityBackend(t, &AccessConfig{AuthenticationMode: authModeAPI}, "")

	_, err := b.CreateAccessEntry("c1", roleARN, "STANDARD", "", nil, nil)
	require.NoError(t, err)

	pol := func(n string) string { return "arn:aws:eks::aws:cluster-access-policy/" + n }

	_, err = b.AssociateAccessPolicy(
		"c1",
		roleARN,
		pol("AmazonEKSClusterAdminPolicy"),
		map[string]any{"type": "cluster"},
	)
	require.NoError(t, err)
	_, err = b.AssociateAccessPolicy("c1", roleARN, pol("AmazonEKSViewPolicy"),
		map[string]any{"type": "namespace", "namespaces": []any{"dev", "qa"}})
	require.NoError(t, err)
	_, err = b.AssociateAccessPolicy("c1", roleARN, pol("AmazonEKSAutoNodePolicy"), map[string]any{"type": "cluster"})
	require.NoError(t, err)

	b.mu.RLock("test")
	got := b.desiredBindingsLocked("c1")
	b.mu.RUnlock()

	p, _ := parsePrincipal(roleARN)
	group := accessGroupName("c1", p)

	type brief struct{ kind, ns, role, group string }

	have := make([]brief, 0, len(got))
	for _, d := range got {
		have = append(have, brief{d.kind, d.namespace, d.role, d.group})
	}

	assert.ElementsMatch(t, []brief{
		{kindCRB, "", "cluster-admin", group},
		{kindRB, "dev", "view", group},
		{kindRB, "qa", "view", group},
	}, have, "unmapped AutoNode policy must not produce a binding")
}

// upstreamRecorder is a fake k3s API that records the headers it was called with.
type upstreamRecorder struct {
	srv  *httptest.Server
	seen sync.Map
}

func newUpstream(t *testing.T) *upstreamRecorder {
	t.Helper()

	u := &upstreamRecorder{}
	u.srv = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/namespaces/kube-system/configmaps/aws-auth" {
			w.WriteHeader(http.StatusNotFound)

			return
		}

		u.seen.Store(r.URL.Query().Get("case"), r.Header.Clone())

		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(u.srv.Close)

	return u
}

func TestK8sProxy(t *testing.T) {
	t.Parallel()

	up := newUpstream(t)
	upPEM := certPEM(up.srv.Certificate())

	b := newIdentityBackend(
		t,
		&AccessConfig{AuthenticationMode: authModeAPI, BootstrapClusterCreatorAdminPermissions: true},
		"",
	)
	require.NoError(t, b.EnableClusters(ClusterEngineConfig{
		Runtime: noopRuntime{}, Token: testAdminToken, Host: "127.0.0.1",
		Credentials: fakeCreds{
			"AKIAROOT": {PrincipalARN: rootARN},
			"AKIABOB":  {PrincipalARN: userARN, Secret: "bobsecret"},
		},
	}))

	_, err := b.CreateAccessEntry("c1", userARN, "STANDARD", "bob", []string{"devs"}, nil)
	require.NoError(t, err)

	ca, err := newClusterCA("c1")
	require.NoError(t, err)

	api, err := newK8sAPI(up.srv.Listener.Addr().String(), testAdminToken, upPEM)
	require.NoError(t, err)

	proxy, err := b.startProxy("c1", ca, api)
	require.NoError(t, err)

	t.Cleanup(func() { proxy.close(); api.close() })

	pool := x509.NewCertPool()
	require.True(t, pool.AppendCertsFromPEM(ca.certPEM))

	client := &http.Client{Transport: &http.Transport{
		TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}, DisableKeepAlives: true,
	}}
	bobPrincipal, _ := parsePrincipal(userARN)
	bobGroup := accessGroupName("c1", bobPrincipal)
	endpoint := "https://127.0.0.1:" + strconv.Itoa(proxy.port)
	now := time.Now().UTC()

	tests := []struct {
		extra      map[string]string
		name       string
		authz      string
		wantUser   string
		wantGroups []string
		wantCode   int
	}{
		{name: "anonymous", wantCode: 200, wantUser: anonymousUser, wantGroups: []string{unauthenticatedGroup}},
		{name: "static_token", authz: "Bearer " + testAdminToken, wantCode: 200},
		{name: "wrong_static", authz: "Bearer nope", wantCode: 401},
		{
			name:     "creator_root",
			authz:    "Bearer " + makeToken(t, "AKIAROOT", "x", "c1", "sts.us-east-1.amazonaws.com", now),
			wantCode: 200, wantUser: rootARN, wantGroups: []string{mastersGroup},
		},
		{
			name:     "entry_user_spoof_stripped",
			authz:    "Bearer " + makeToken(t, "AKIABOB", "bobsecret", "c1", "sts.us-east-1.amazonaws.com", now),
			extra:    map[string]string{"Impersonate-Group": "system:masters", "Impersonate-User": "admin"},
			wantCode: 200, wantUser: "bob", wantGroups: []string{"devs", bobGroup},
		},
		{
			name:     "bad_signature",
			authz:    "Bearer " + makeToken(t, "AKIABOB", "wrong", "c1", "sts.us-east-1.amazonaws.com", now),
			wantCode: 401,
		},
		{
			name:     "wrong_cluster",
			authz:    "Bearer " + makeToken(t, "AKIABOB", "bobsecret", "c9", "sts.us-east-1.amazonaws.com", now),
			wantCode: 401,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req, rerr := http.NewRequestWithContext(
				t.Context(),
				http.MethodGet,
				endpoint+"/api/v1/namespaces?case="+tt.name,
				nil,
			)
			require.NoError(t, rerr)

			if tt.authz != "" {
				req.Header.Set("Authorization", tt.authz)
			}

			for k, v := range tt.extra {
				req.Header.Set(k, v)
			}

			resp, derr := client.Do(req)
			require.NoError(t, derr)

			_ = resp.Body.Close()

			require.Equal(t, tt.wantCode, resp.StatusCode)

			if tt.wantCode != 200 {
				return
			}

			raw, _ := up.seen.Load(tt.name)
			h, _ := raw.(http.Header)

			assert.Equal(t, "Bearer "+testAdminToken, h.Get("Authorization"))
			assert.Equal(t, tt.wantUser, h.Get("Impersonate-User"))
			assert.ElementsMatch(t, tt.wantGroups, h.Values("Impersonate-Group"))
		})
	}
}

type noopRuntime struct{}

func (noopRuntime) CreateAndStart(context.Context, container.Spec) (string, error) { return "id", nil }
func (noopRuntime) StopAndRemove(context.Context, string) error                    { return nil }

func certPEM(c *x509.Certificate) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: c.Raw})
}
