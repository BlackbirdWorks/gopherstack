package amp_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	ampsdk "github.com/aws/aws-sdk-go-v2/service/amp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/amp"
)

type fakeKeys map[string]bool

func (k fakeKeys) KMSKeyState(_ context.Context, arn string) (bool, bool) {
	enabled, ok := k[arn]

	return ok, enabled
}

type logLine struct{ region, group, stream, msg string }

type fakeLogs struct {
	groups map[string]bool
	lines  []logLine
	mu     sync.Mutex
}

func (l *fakeLogs) LogGroupExists(_ context.Context, _, group string) bool { return l.groups[group] }

func (l *fakeLogs) PutLogLine(_ context.Context, region, group, stream, msg string, _ time.Time) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.lines = append(l.lines, logLine{region, group, stream, msg})

	return nil
}

func TestCreateWorkspaceKMSKey(t *testing.T) {
	t.Parallel()

	const (
		ok       = "arn:aws:kms:us-east-1:123456789012:key/11111111-1111-1111-1111-111111111111"
		disabled = "arn:aws:kms:us-east-1:123456789012:key/22222222-2222-2222-2222-222222222222"
		missing  = "arn:aws:kms:us-east-1:123456789012:key/33333333-3333-3333-3333-333333333333"
	)

	tests := []struct {
		name string
		key  string
		code string
	}{
		{name: "enabled", key: ok},
		{name: "disabled", key: disabled, code: "ValidationException"},
		{name: "missing", key: missing, code: "ValidationException"},
		{name: "none", key: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			f.backend.SetKeyChecker(fakeKeys{ok: true, disabled: false})

			in := &ampsdk.CreateWorkspaceInput{Alias: aws.String("w")}
			if tt.key != "" {
				in.KmsKeyArn = aws.String(tt.key)
			}

			_, err := f.client.CreateWorkspace(t.Context(), in)
			if tt.code == "" {
				require.NoError(t, err)

				return
			}

			assert.Equal(t, tt.code, apiCode(err))
		})
	}
}

func TestLoggingConfigurationLogGroup(t *testing.T) {
	t.Parallel()

	const (
		have = "arn:aws:logs:us-east-1:123456789012:log-group:/aws/vendedlogs/amp"
		gone = "arn:aws:logs:us-east-1:123456789012:log-group:/nope"
	)

	f := newFixture(t)
	ws := f.workspace(t)
	f.backend.SetLogSink(&fakeLogs{groups: map[string]bool{"/aws/vendedlogs/amp": true}})

	_, err := f.client.CreateLoggingConfiguration(t.Context(),
		&ampsdk.CreateLoggingConfigurationInput{WorkspaceId: aws.String(ws), LogGroupArn: aws.String(gone)})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	_, err = f.client.CreateLoggingConfiguration(t.Context(),
		&ampsdk.CreateLoggingConfigurationInput{WorkspaceId: aws.String(ws), LogGroupArn: aws.String(have)})
	require.NoError(t, err)
	f.settle()

	_, err = f.client.UpdateLoggingConfiguration(t.Context(),
		&ampsdk.UpdateLoggingConfigurationInput{WorkspaceId: aws.String(ws), LogGroupArn: aws.String(gone)})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}

func TestRuleFailureLogged(t *testing.T) {
	t.Parallel()

	const group = "arn:aws:logs:us-east-1:123456789012:log-group:/amp"

	tests := []struct {
		name    string
		logging bool
		want    int
	}{
		{name: "delivered", logging: true, want: 1},
		{name: "no logging configured", want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			sink := &fakeLogs{groups: map[string]bool{"/amp": true}}
			f.backend.SetLogSink(sink)
			ws := f.workspace(t)

			if tt.logging {
				_, err := f.client.CreateLoggingConfiguration(
					t.Context(),
					&ampsdk.CreateLoggingConfigurationInput{
						WorkspaceId: aws.String(ws),
						LogGroupArn: aws.String(group),
					},
				)
				require.NoError(t, err)
				f.settle()
			}

			f.putRules(t, ws, "groups:\n  - name: g\n    rules:\n      - record: bad\n        expr: '\"x\"'\n")
			f.clock.advance(2 * time.Minute)
			f.backend.Tick(t.Context())

			sink.mu.Lock()
			defer sink.mu.Unlock()

			require.GreaterOrEqual(t, len(sink.lines), tt.want)

			if tt.want == 0 {
				assert.Empty(t, sink.lines)

				return
			}

			assert.Equal(t, "/amp", sink.lines[0].group)
			assert.Equal(t, "us-east-1", sink.lines[0].region)
			assert.Contains(t, sink.lines[0].msg, `"component":"ruler"`)
			assert.Contains(t, sink.lines[0].msg, `"level":"error"`)
		})
	}
}

func (f *fixture) signedRequest(t *testing.T, method, path, svc, secret, key string) (*http.Request, httpResp) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), method, f.url+path, strings.NewReader(""))
	require.NoError(t, err)

	sum := sha256.Sum256(nil)
	err = v4.NewSigner().SignHTTP(t.Context(),
		aws.Credentials{AccessKeyID: key, SecretAccessKey: secret}, req,
		hex.EncodeToString(sum[:]), svc, "us-east-1", time.Now())
	require.NoError(t, err)

	res, err := f.http.Do(req)
	require.NoError(t, err)

	defer res.Body.Close()

	b, err := io.ReadAll(res.Body)
	require.NoError(t, err)

	return req, httpResp{status: res.StatusCode, body: b}
}

func TestDataPlaneAuth(t *testing.T) {
	t.Parallel()

	denyAll := func(string, string, string, string) (bool, bool) { return false, true }
	allowAll := func(string, string, string, string) (bool, bool) { return true, false }

	tests := []struct {
		eval   amp.PolicyEvaluator
		name   string
		svc    string
		secret string
		key    string
		code   string
		want   int
		policy bool
		sign   bool
	}{
		{name: "valid", sign: true, svc: "aps", secret: "s3cret", want: http.StatusOK},
		{name: "unsigned", want: http.StatusForbidden, code: "Missing Authentication Token"},
		{name: "bad secret", sign: true, svc: "aps", secret: "other", want: http.StatusForbidden,
			code: "signature we calculated does not match"},
		{name: "policy denies", sign: true, svc: "aps", secret: "s3cret", eval: denyAll, policy: true,
			want: http.StatusForbidden, code: "not authorized to perform aps:QueryMetrics"},
		{name: "cross account without allow", sign: true, svc: "aps", secret: "s3cret", key: "999999999999",
			eval: func(string, string, string, string) (bool, bool) { return false, false }, policy: true,
			want: http.StatusForbidden, code: "not authorized"},
		{name: "policy allows", sign: true, svc: "aps", secret: "s3cret", eval: allowAll, policy: true,
			want: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			ws := f.workspace(t)
			f.backend.SetDataPlaneAuth(&amp.DataPlaneAuth{Secret: "s3cret", Evaluate: tt.eval})

			if tt.policy {
				_, err := f.client.PutResourcePolicy(t.Context(), &ampsdk.PutResourcePolicyInput{
					WorkspaceId: aws.String(ws),
					PolicyDocument: aws.String(
						`{"Version":"2012-10-17","Statement":[` +
							`{"Effect":"Allow","Principal":"*","Action":"aps:*","Resource":"*"}]}`,
					),
				})
				require.NoError(t, err)
			}

			path := "/workspaces/" + ws + "/api/v1/query?query=vector(1)&time=60"

			var r httpResp
			if tt.sign {
				key := tt.key
				if key == "" {
					key = "test"
				}

				_, r = f.signedRequest(t, http.MethodGet, path, tt.svc, tt.secret, key)
			} else {
				r = f.do(t, http.MethodGet, path, nil, nil)
			}

			assert.Equal(t, tt.want, r.status, string(r.body))

			if tt.code != "" {
				assert.Contains(t, string(r.body), tt.code)
			}
		})
	}
}
