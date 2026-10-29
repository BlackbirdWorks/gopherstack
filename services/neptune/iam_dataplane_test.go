package neptune_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/bolt"
	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
	"github.com/blackbirdworks/gopherstack/services/iam"
	"github.com/blackbirdworks/gopherstack/services/neptune"
)

const (
	dpAccount = "000000000000"
	dpRegion  = "us-east-1"
	dpSecret  = "dp-secret-key-0123456789abcdefghijklmnop"
)

var errDPNoUser = errors.New("no such user")

func dpPolicy(actions ...string) string {
	quoted := make([]string, len(actions))
	for i, a := range actions {
		quoted[i] = strconv.Quote(a)
	}

	return `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":[` +
		strings.Join(quoted, ",") + `],"Resource":"*"}]}`
}

// dpIdentities serves the access keys AKIAFULL, AKIAREAD and AKIANONE.
type dpIdentities struct{ policies map[string][]string }

func newDPIdentities() *dpIdentities {
	return &dpIdentities{policies: map[string][]string{
		"full": {dpPolicy("neptune-db:ReadDataViaQuery", "neptune-db:WriteDataViaQuery",
			"neptune-db:DeleteDataViaQuery", "neptune-db:GetQueryStatus", "neptune-db:CancelQuery")},
		"read": {dpPolicy("neptune-db:ReadDataViaQuery")},
		"none": nil,
	}}
}

func (d *dpIdentities) ListAllAccessKeys() []iam.AccessKey {
	return []iam.AccessKey{
		{AccessKeyID: "AKIAFULL", SecretAccessKey: dpSecret, UserName: "full", Status: "Active"},
		{AccessKeyID: "AKIAREAD", SecretAccessKey: dpSecret, UserName: "read", Status: "Active"},
		{AccessKeyID: "AKIANONE", SecretAccessKey: dpSecret, UserName: "none", Status: "Active"},
	}
}

func (d *dpIdentities) GetUserByAccessKeyID(id string) (*iam.User, error) {
	name := map[string]string{"AKIAFULL": "full", "AKIAREAD": "read", "AKIANONE": "none"}[id]
	if name == "" {
		return nil, errDPNoUser
	}

	return &iam.User{UserName: name, Arn: "arn:aws:iam::" + dpAccount + ":user/" + name}, nil
}

func (d *dpIdentities) GetPoliciesForUser(name string) ([]string, error) {
	return d.policies[name], nil
}

func (d *dpIdentities) GetPoliciesForRole(string) ([]string, error) { return nil, nil }

func newIAMCluster(t *testing.T, iamEnabled, enforce bool) neptune.DBCluster {
	t.Helper()

	b := neptune.NewInMemoryBackend(dpAccount, dpRegion)
	b.EnableEngine(neptune.EngineConfig{InProcessGremlin: true, OpenCypher: true})
	b.SetIAMAuth(func() neptune.IAMAuthConfig {
		return neptune.IAMAuthConfig{Identities: newDPIdentities(), Enforce: enforce}
	})
	t.Cleanup(b.Close)

	_, err := b.CreateDBCluster(t.Context(), "c1", "", 0, neptune.DBClusterCreateOptions{
		EngineVersion: "1.3.0.0", EnableIAMDatabaseAuthentication: iamEnabled,
	})
	require.NoError(t, err)

	return waitCluster(t, b, "c1", "available")
}

func dpSign(t *testing.T, req *http.Request, body, accessKey, secret, service string) {
	t.Helper()

	sum := sha256.Sum256([]byte(body))
	creds := aws.Credentials{AccessKeyID: accessKey, SecretAccessKey: secret}

	require.NoError(t, v4.NewSigner().SignHTTP(t.Context(), creds, req, hex.EncodeToString(sum[:]),
		service, dpRegion, time.Now()))
}

func dpDo(t *testing.T, c neptune.DBCluster, method, path, body, accessKey, secret, service string) (int, string) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), method, "http://"+clusterAddr(c)+path, strings.NewReader(body))
	require.NoError(t, err)

	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}

	if accessKey != "" {
		dpSign(t, req, body, accessKey, secret, service)
	}

	resp, err := testclient.New(t).Do(req)
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return resp.StatusCode, string(raw)
}

func TestDataPlaneIAMHTTP(t *testing.T) {
	t.Parallel()

	countQ := "/openCypher?" + url.Values{"query": {"MATCH (n) RETURN count(n) AS c"}}.Encode()
	createQ := "/openCypher?" + url.Values{"query": {"CREATE (n:T) RETURN n"}}.Encode()
	gremlinCount := `{"gremlin":"g.V().count()"}`
	gremlinDrop := `{"gremlin":"g.V().drop()"}`

	tests := []struct {
		name       string
		method     string
		path       string
		body       string
		accessKey  string
		secret     string
		service    string
		wantBody   string
		wantStatus int
		iamEnabled bool
		enforce    bool
	}{
		{name: "unsigned_denied", method: "GET", path: countQ, iamEnabled: true, enforce: true,
			wantStatus: 403, wantBody: "AccessDeniedException"},
		{name: "signed_allowed", method: "GET", path: countQ, accessKey: "AKIAREAD", secret: dpSecret,
			service: "neptune-db", iamEnabled: true, enforce: true, wantStatus: 200},
		{name: "signed_without_policy_denied", method: "GET", path: countQ, accessKey: "AKIANONE", secret: dpSecret,
			service: "neptune-db", iamEnabled: true, enforce: true, wantStatus: 403,
			wantBody: "neptune-db:ReadDataViaQuery"},
		{name: "wrong_service_denied", method: "GET", path: countQ, accessKey: "AKIAFULL", secret: dpSecret,
			service: "neptune", iamEnabled: true, enforce: true, wantStatus: 403, wantBody: "AccessDeniedException"},
		{name: "wrong_secret_denied", method: "GET", path: countQ, accessKey: "AKIAFULL", secret: "wrong",
			service: "neptune-db", iamEnabled: true, enforce: true, wantStatus: 403, wantBody: "signature"},
		{name: "unknown_key_denied", method: "GET", path: countQ, accessKey: "AKIAUNKNOWN", secret: dpSecret,
			service: "neptune-db", iamEnabled: true, enforce: true, wantStatus: 403},
		{name: "write_needs_write_action", method: "GET", path: createQ, accessKey: "AKIAREAD", secret: dpSecret,
			service: "neptune-db", iamEnabled: true, enforce: true, wantStatus: 403,
			wantBody: "neptune-db:WriteDataViaQuery"},
		{name: "write_allowed_with_policy", method: "GET", path: createQ, accessKey: "AKIAFULL", secret: dpSecret,
			service: "neptune-db", iamEnabled: true, enforce: true, wantStatus: 200},
		{name: "status_needs_action", method: "GET", path: "/openCypher/status", accessKey: "AKIAREAD",
			secret: dpSecret, service: "neptune-db", iamEnabled: true, enforce: true, wantStatus: 403,
			wantBody: "neptune-db:GetQueryStatus"},
		{name: "status_allowed", method: "GET", path: "/openCypher/status", accessKey: "AKIAFULL",
			secret: dpSecret, service: "neptune-db", iamEnabled: true, enforce: true, wantStatus: 200},
		{name: "gremlin_read_allowed", method: "POST", path: "/gremlin", body: gremlinCount, accessKey: "AKIAREAD",
			secret: dpSecret, service: "neptune-db", iamEnabled: true, enforce: true, wantStatus: 200},
		{name: "gremlin_drop_denied", method: "POST", path: "/gremlin", body: gremlinDrop, accessKey: "AKIAREAD",
			secret: dpSecret, service: "neptune-db", iamEnabled: true, enforce: true, wantStatus: 403,
			wantBody: "neptune-db:DeleteDataViaQuery"},
		{name: "gremlin_unsigned_denied", method: "POST", path: "/gremlin", body: gremlinCount,
			iamEnabled: true, enforce: true, wantStatus: 403},
		{name: "enforce_off_admits_signed_caller", method: "GET", path: createQ, accessKey: "AKIANONE",
			secret: dpSecret, service: "neptune-db", iamEnabled: true, enforce: false, wantStatus: 200},
		{name: "enforce_off_still_requires_signature", method: "GET", path: countQ, iamEnabled: true,
			enforce: false, wantStatus: 403},
		{name: "iam_disabled_unsigned_ok", method: "GET", path: createQ, iamEnabled: false, enforce: true,
			wantStatus: 200},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newIAMCluster(t, tt.iamEnabled, tt.enforce)
			code, body := dpDo(t, c, tt.method, tt.path, tt.body, tt.accessKey, tt.secret, tt.service)

			assert.Equal(t, tt.wantStatus, code, body)
			assert.Contains(t, body, tt.wantBody)
		})
	}
}

func dpGremlinWS(t *testing.T, c neptune.DBCluster, accessKey, script string) (map[string]any, error) {
	t.Helper()

	hdr := http.Header{}

	if accessKey != "" {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+clusterAddr(c)+"/gremlin", nil)
		require.NoError(t, err)
		dpSign(t, req, "", accessKey, dpSecret, "neptune-db")

		for _, k := range []string{"Authorization", "X-Amz-Date"} {
			hdr.Set(k, req.Header.Get(k))
		}
	}

	conn, resp, err := websocket.DefaultDialer.DialContext(t.Context(), gremlinWS(c), hdr)
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}

	if err != nil {
		return nil, err
	}

	defer func() { _ = conn.Close() }()

	frame, err := json.Marshal(map[string]any{
		"requestId": "7f1a8f4c-0b0e-4a8a-9d3a-1c2d3e4f5a6b", "op": "eval", "processor": "",
		"args": map[string]any{"gremlin": script},
	})
	require.NoError(t, err)
	require.NoError(t, conn.WriteMessage(websocket.TextMessage, frame))

	_, raw, err := conn.ReadMessage()
	require.NoError(t, err)

	var out map[string]any
	require.NoError(t, json.Unmarshal(raw, &out))

	return out, nil
}

func TestDataPlaneIAMGremlinWebSocket(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		accessKey  string
		script     string
		wantDialOK bool
		wantCode   float64
	}{
		{name: "unsigned_handshake_rejected", script: "g.V().count()"},
		{name: "signed_read_allowed", accessKey: "AKIAREAD", script: "g.V().count()", wantDialOK: true, wantCode: 200},
		{name: "signed_drop_denied", accessKey: "AKIAREAD", script: "g.V().drop()", wantDialOK: true, wantCode: 403},
		{name: "signed_drop_allowed", accessKey: "AKIAFULL", script: "g.V().drop()", wantDialOK: true, wantCode: 204},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newIAMCluster(t, true, true)
			out, err := dpGremlinWS(t, c, tt.accessKey, tt.script)

			if !tt.wantDialOK {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			status, ok := out["status"].(map[string]any)
			require.True(t, ok, out)
			assert.InDelta(t, tt.wantCode, status["code"], 0)
		})
	}
}

func dpBoltToken(t *testing.T, c neptune.DBCluster, accessKey, secret, service string) string {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+clusterAddr(c)+"/opencypher", nil)
	require.NoError(t, err)
	dpSign(t, req, "", accessKey, secret, service)

	raw, err := json.Marshal(map[string]string{
		"Authorization": req.Header.Get("Authorization"), "HttpMethod": http.MethodGet,
		"X-Amz-Date": req.Header.Get("X-Amz-Date"), "Host": req.Host,
	})
	require.NoError(t, err)

	return string(raw)
}

func dpBoltConnect(t *testing.T, c neptune.DBCluster, creds string) (*bolt.Client, bolt.Response) {
	t.Helper()

	nc, err := (&net.Dialer{}).DialContext(t.Context(), "tcp", net.JoinHostPort(c.Endpoint, strconv.Itoa(c.Port)))
	require.NoError(t, err)
	t.Cleanup(func() { _ = nc.Close() })

	cl, err := bolt.Dial(nc, bolt.Version{Major: 4, Minor: 4})
	require.NoError(t, err)

	auth := map[string]any{"user_agent": "t", "scheme": "basic", "principal": "username", "credentials": creds}
	r, err := cl.Call(bolt.TagHello, auth)
	require.NoError(t, err)

	return cl, r
}

func TestDataPlaneIAMBolt(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		token    func(t *testing.T, c neptune.DBCluster) string
		query    string
		wantAuth bool
		wantRun  bool
	}{
		{name: "signed_read", wantAuth: true, wantRun: true, query: "MATCH (n) RETURN count(n)",
			token: func(t *testing.T, c neptune.DBCluster) string {
				t.Helper()

				return dpBoltToken(t, c, "AKIAREAD", dpSecret, "neptune-db")
			}},
		{name: "signed_write_denied", wantAuth: true, query: "CREATE (n:T) RETURN n",
			token: func(t *testing.T, c neptune.DBCluster) string {
				t.Helper()

				return dpBoltToken(t, c, "AKIAREAD", dpSecret, "neptune-db")
			}},
		{name: "signed_write_allowed", wantAuth: true, wantRun: true, query: "CREATE (n:T) RETURN n",
			token: func(t *testing.T, c neptune.DBCluster) string {
				t.Helper()

				return dpBoltToken(t, c, "AKIAFULL", dpSecret, "neptune-db")
			}},
		{name: "wrong_service_rejected", query: "RETURN 1",
			token: func(t *testing.T, c neptune.DBCluster) string {
				t.Helper()

				return dpBoltToken(t, c, "AKIAFULL", dpSecret, "neptune")
			}},
		{name: "wrong_secret_rejected", query: "RETURN 1",
			token: func(t *testing.T, c neptune.DBCluster) string {
				t.Helper()

				return dpBoltToken(t, c, "AKIAFULL", "wrong", "neptune-db")
			}},
		{name: "bogus_token_rejected", query: "RETURN 1",
			token: func(*testing.T, neptune.DBCluster) string { return "not-json" }},
		{name: "empty_password_rejected", query: "RETURN 1",
			token: func(*testing.T, neptune.DBCluster) string { return "" }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newIAMCluster(t, true, true)
			cl, r := dpBoltConnect(t, c, tt.token(t, c))

			if !tt.wantAuth {
				assert.Equal(t, bolt.TagFailure, r.Tag)
				assert.Equal(t, bolt.CodeUnauthorized, r.Meta()["code"])

				return
			}

			require.Equal(t, bolt.TagSuccess, r.Tag, r.Meta())

			_, sum, err := cl.Query(tt.query, nil)
			require.NoError(t, err)

			if tt.wantRun {
				assert.Equal(t, bolt.TagSuccess, sum.Tag, sum.Meta())

				return
			}

			assert.Equal(t, bolt.TagFailure, sum.Tag)
			assert.Equal(t, bolt.CodeForbidden, sum.Meta()["code"])
			assert.Contains(t, sum.Meta()["message"], "neptune-db:WriteDataViaQuery")
		})
	}
}

func TestDataPlaneIAMBoltDisabledAcceptsNone(t *testing.T) {
	t.Parallel()

	c := newIAMCluster(t, false, true)
	cl := boltDial(t, c, bolt.Version{Major: 4, Minor: 4})

	_, sum, err := cl.Query("RETURN 1", nil)
	require.NoError(t, err)
	assert.Equal(t, bolt.TagSuccess, sum.Tag)
}
