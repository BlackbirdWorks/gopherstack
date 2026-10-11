package rds

import (
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iam"
	"github.com/blackbirdworks/gopherstack/services/sts"
)

const (
	authTestEndpoint = "127.0.0.1:15432"
	authTestRegion   = "us-east-1"
	authTestResource = "db-ABCDEFG"
	authTestAccount  = "123456789012"
)

type fakeSessions struct{ s *sts.SessionInfo }

func (f fakeSessions) LookupSession(string, string) *sts.SessionInfo { return f.s }

// buildTestToken mirrors feature/rds/auth.BuildAuthToken: a SigV4 presigned GET for service rds-db.
func buildTestToken(
	t *testing.T, endpoint, region, dbUser string, creds aws.Credentials, signedAt time.Time, expires string,
) string {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://"+endpoint, nil)
	require.NoError(t, err)

	q := req.URL.Query()
	q.Set("Action", "connect")
	q.Set("DBUser", dbUser)
	q.Set("X-Amz-Expires", expires)
	req.URL.RawQuery = q.Encode()

	signed, _, err := v4.NewSigner().PresignHTTP(t.Context(), creds, req, iamAuthEmptyHash, "rds-db", region, signedAt)
	require.NoError(t, err)

	return strings.TrimPrefix(signed, "https://")
}

type authFixture struct {
	iam   *iam.InMemoryBackend
	creds aws.Credentials
}

func newAuthFixture(t *testing.T, policy string) *authFixture {
	t.Helper()

	b := iam.NewInMemoryBackend()
	_, err := b.CreateUser("alice", "/", "")
	require.NoError(t, err)

	ak, err := b.CreateAccessKey("alice")
	require.NoError(t, err)

	if policy != "" {
		require.NoError(t, b.PutUserPolicy("alice", "p", policy))
	}

	return &authFixture{
		iam:   b,
		creds: aws.Credentials{AccessKeyID: ak.AccessKeyID, SecretAccessKey: ak.SecretAccessKey},
	}
}

func allowPolicy(resource string) string {
	return `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
		`"Action":"rds-db:connect","Resource":"` + resource + `"}]}`
}

func TestAuthenticateToken(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	allowAll := allowPolicy("arn:aws:rds-db:us-east-1:" + authTestAccount + ":dbuser:" + authTestResource + "/*")

	tests := []struct {
		signed  time.Time
		wantErr error
		mutate  func(f *authFixture, tok *string, tg *iamAuthTarget, cfg *IAMAuthConfig)
		name    string
		policy  string
		region  string
		expires string
		enforce bool
	}{
		{name: "valid", policy: allowAll, enforce: true},
		{name: "valid unenforced no policy", enforce: false},
		{
			name:    "expired",
			policy:  allowAll,
			enforce: true,
			signed:  now.Add(-20 * time.Minute),
			wantErr: errTokenExpired,
		},
		{name: "wrong region", policy: allowAll, enforce: true, region: "eu-west-1", wantErr: errTokenRegion},
		{name: "expires too long", policy: allowAll, enforce: true, expires: "901", wantErr: errTokenMalformed},
		{
			name: "tampered signature", policy: allowAll, enforce: true, wantErr: errTokenSignature,
			mutate: func(_ *authFixture, tok *string, _ *iamAuthTarget, _ *IAMAuthConfig) {
				*tok = strings.Replace(*tok, "X-Amz-Signature=", "X-Amz-Signature=0", 1)
				*tok = strings.TrimSuffix(*tok, (*tok)[len(*tok)-1:])
			},
		},
		{
			name: "tampered user", policy: allowAll, enforce: true, wantErr: errTokenSignature,
			mutate: func(_ *authFixture, tok *string, _ *iamAuthTarget, _ *IAMAuthConfig) {
				*tok = strings.Replace(*tok, "DBUser=bob", "DBUser=root", 1)
			},
		},
		{
			name: "wrong action", policy: allowAll, enforce: true, wantErr: errTokenMalformed,
			mutate: func(_ *authFixture, tok *string, _ *iamAuthTarget, _ *IAMAuthConfig) {
				*tok = strings.Replace(*tok, "Action=connect", "Action=other", 1)
			},
		},
		{
			name: "wrong secret", policy: allowAll, enforce: true, wantErr: errTokenSignature,
			mutate: func(f *authFixture, _ *string, _ *iamAuthTarget, cfg *IAMAuthConfig) {
				cfg.Identities = &secretSwap{InMemoryBackend: f.iam}
			},
		},
		{
			name: "unknown access key", policy: allowAll, enforce: true, wantErr: errTokenUnknownID,
			mutate: func(f *authFixture, tok *string, _ *iamAuthTarget, _ *IAMAuthConfig) {
				f.creds.AccessKeyID = "AKIAUNKNOWN000000000"
				*tok = buildTestToken(t, authTestEndpoint, authTestRegion, "bob", f.creds, now, "900")
			},
		},
		{
			name: "wrong endpoint", policy: allowAll, enforce: true, wantErr: errTokenEndpoint,
			mutate: func(_ *authFixture, _ *string, tg *iamAuthTarget, _ *IAMAuthConfig) {
				tg.endpoint = "127.0.0.1:19999"
			},
		},
		{name: "no allow policy", enforce: true, wantErr: errTokenDenied},
		{
			name:    "other db user arn",
			enforce: true,
			wantErr: errTokenDenied,
			policy: allowPolicy(
				"arn:aws:rds-db:us-east-1:" + authTestAccount + ":dbuser:" + authTestResource + "/carol",
			),
		},
		{
			name: "explicit deny", enforce: true, wantErr: errTokenDenied,
			policy: `{"Version":"2012-10-17","Statement":[` +
				`{"Effect":"Allow","Action":"rds-db:connect","Resource":"*"},` +
				`{"Effect":"Deny","Action":"rds-db:connect","Resource":"*"}]}`,
		},
		{
			name: "iam auth disabled", policy: allowAll, enforce: true, wantErr: errTokenDisabled,
			mutate: func(_ *authFixture, _ *string, tg *iamAuthTarget, _ *IAMAuthConfig) { tg.enabled = false },
		},
		{
			name: "missing resource id", policy: allowAll, enforce: true, wantErr: errTokenNoResID,
			mutate: func(_ *authFixture, _ *string, tg *iamAuthTarget, _ *IAMAuthConfig) { tg.resourceID = "" },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newAuthFixture(t, tt.policy)
			region, expires, signed := tt.region, tt.expires, tt.signed

			if region == "" {
				region = authTestRegion
			}

			if expires == "" {
				expires = "900"
			}

			if signed.IsZero() {
				signed = now
			}

			tok := buildTestToken(t, authTestEndpoint, region, "bob", f.creds, signed, expires)
			target := iamAuthTarget{
				endpoint: authTestEndpoint, region: authTestRegion, account: authTestAccount, partition: "aws",
				resourceID: authTestResource, enabled: true,
			}
			cfg := IAMAuthConfig{Identities: f.iam, Enforce: tt.enforce}

			if tt.mutate != nil {
				tt.mutate(f, &tok, &target, &cfg)
			}

			user, err := authenticateToken(cfg, target, tok, "127.0.0.1", now)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, "bob", user)
		})
	}
}

type secretSwap struct{ *iam.InMemoryBackend }

func (s *secretSwap) ListAllAccessKeys() []iam.AccessKey {
	keys := s.InMemoryBackend.ListAllAccessKeys()
	for i := range keys {
		keys[i].SecretAccessKey = "not-the-secret"
	}

	return keys
}

func TestAuthenticateTokenSessionCredentials(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	creds := aws.Credentials{AccessKeyID: "ASIATEST", SecretAccessKey: "sessionsecret", SessionToken: "tok123"}
	session := &sts.SessionInfo{
		AccessKeyID: "ASIATEST", SecretAccessKey: "sessionsecret", SessionToken: "tok123",
		AssumedRoleArn: "arn:aws:sts::" + authTestAccount + ":assumed-role/dbrole/s1", AccountID: authTestAccount,
		IsAssumedRole: true,
	}
	target := iamAuthTarget{
		endpoint: authTestEndpoint, region: authTestRegion, account: authTestAccount, partition: "aws",
		resourceID: authTestResource, enabled: true,
	}

	tests := []struct {
		wantErr error
		session *sts.SessionInfo
		name    string
	}{
		{name: "valid session", session: session},
		{name: "unknown session", session: nil, wantErr: errTokenUnknownID},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tok := buildTestToken(t, authTestEndpoint, authTestRegion, "bob", creds, now, "900")
			require.Contains(t, tok, "X-Amz-Security-Token=")

			cfg := IAMAuthConfig{Sessions: fakeSessions{tt.session}}
			user, err := authenticateToken(cfg, target, tok, "", now)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, "bob", user)
		})
	}
}

func TestParseAuthToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		token string
		ok    bool
	}{
		{name: "empty", token: ""},
		{name: "not a token", token: "hunter2"},
		{name: "missing params", token: authTestEndpoint + "/?Action=connect&DBUser=bob"},
		{name: "bad path", token: authTestEndpoint + "/x?" + url.Values{"Action": {"connect"}}.Encode()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := parseAuthToken(tt.token)
			assert.Equal(t, tt.ok, err == nil)
		})
	}
}
