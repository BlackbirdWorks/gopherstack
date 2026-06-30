package sts_test

// Tests for parity gap fixes introduced in parity/emr:
//   1. Role-chaining duration cap (ASIA caller → max 3600 s)
//   2. Transitive tag propagation across role chains
//
// JWT-expiry validation is already covered in handler_accuracy_test.go.

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"encoding/base64"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/services/sts"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// newEchoServer builds a minimal echo server wired to handler h.
func newEchoServer(h *sts.Handler) *httptest.Server {
	e := echo.New()
	e.Use(func(_ echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			ctx := logger.Save(c.Request().Context(), logger.NewTestLogger())

			return h.Handler()(echo.NewContext(c.Request().WithContext(ctx), c.Response()))
		}
	})

	return httptest.NewServer(e)
}

// postSTS sends a form POST with STS Action and optional auth headers.
func postSTS(t *testing.T, serverURL string, form map[string]string, authHeader, secToken string) *http.Response {
	t.Helper()

	params := make([]string, 0, len(form))
	for k, v := range form {
		params = append(params, k+"="+v)
	}
	body := strings.NewReader(strings.Join(params, "&"))

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, serverURL+"/", body)
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if authHeader != "" {
		req.Header.Set("Authorization", authHeader)
	}
	if secToken != "" {
		req.Header.Set("X-Amz-Security-Token", secToken)
	}

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })

	return resp
}

func sigV4Auth(accessKeyID string) string {
	return fmt.Sprintf(
		"AWS4-HMAC-SHA256 Credential=%s/20260101/us-east-1/sts/aws4_request, "+
			"SignedHeaders=host;x-amz-date, Signature=deadbeef",
		accessKeyID,
	)
}

// ---------------------------------------------------------------------------
// 1. Role-chaining duration cap
// ---------------------------------------------------------------------------

func TestRoleChaining_DurationCap(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		durationSecs string
		wantStatus   int
		callerTemp   bool
	}{
		{
			name:         "AKIA caller — 7200s accepted",
			callerTemp:   false,
			durationSecs: "7200",
			wantStatus:   http.StatusOK,
		},
		{
			name:         "ASIA caller — exactly 3600s accepted",
			callerTemp:   true,
			durationSecs: "3600",
			wantStatus:   http.StatusOK,
		},
		{
			name:         "ASIA caller — 3601s rejected",
			callerTemp:   true,
			durationSecs: "3601",
			wantStatus:   http.StatusBadRequest,
		},
		{
			name:         "ASIA caller — 1800s accepted",
			callerTemp:   true,
			durationSecs: "1800",
			wantStatus:   http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := sts.NewInMemoryBackend()
			h := sts.NewHandler(backend)
			srv := newEchoServer(h)
			defer srv.Close()

			var callerKey, secToken string
			if tc.callerTemp {
				callerKey = "ASIATESTROLEID000001"
				secToken = "parent-session-token"
				backend.AddSessionInternal(&sts.SessionInfo{
					AccessKeyID:    callerKey,
					SessionToken:   secToken,
					AssumedRoleArn: "arn:aws:sts::123456789012:assumed-role/Parent/s",
					AccountID:      "123456789012",
					SessionName:    "s",
					AssumedRoleID:  "AROATESTPARENT:s",
					Expiration:     time.Now().Add(1 * time.Hour),
				})
			} else {
				callerKey = "AKIATESTLONGTERM0001"
			}

			resp := postSTS(t, srv.URL, map[string]string{
				"Action":          "AssumeRole",
				"Version":         "2011-06-15",
				"RoleArn":         "arn:aws:iam::123456789012:role/Child",
				"RoleSessionName": "child-session",
				"DurationSeconds": tc.durationSecs,
			}, sigV4Auth(callerKey), secToken)

			assert.Equal(t, tc.wantStatus, resp.StatusCode)
		})
	}
}

// ---------------------------------------------------------------------------
// 2. Transitive tag propagation
// ---------------------------------------------------------------------------

func TestTransitiveTagPropagation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantTags         map[string]string
		name             string
		parentTags       []sts.Tag
		parentTransitive []string
		childTags        []sts.Tag
	}{
		{
			name:      "no parent — child tags pass through",
			wantTags:  map[string]string{"env": "prod"},
			childTags: []sts.Tag{{Key: "env", Value: "prod"}},
		},
		{
			name:             "parent transitive tag inherited",
			parentTags:       []sts.Tag{{Key: "cost-center", Value: "42"}, {Key: "team", Value: "eng"}},
			parentTransitive: []string{"cost-center"},
			childTags:        []sts.Tag{{Key: "env", Value: "dev"}},
			wantTags:         map[string]string{"cost-center": "42", "env": "dev"},
		},
		{
			name:             "non-transitive parent tag not inherited",
			parentTags:       []sts.Tag{{Key: "cost-center", Value: "42"}, {Key: "team", Value: "eng"}},
			parentTransitive: []string{"cost-center"},
			childTags:        nil,
			wantTags:         map[string]string{"cost-center": "42"},
		},
		{
			name:             "child override wins on key conflict",
			parentTags:       []sts.Tag{{Key: "env", Value: "prod"}},
			parentTransitive: []string{"env"},
			childTags:        []sts.Tag{{Key: "env", Value: "staging"}},
			wantTags:         map[string]string{"env": "staging"},
		},
		{
			name:             "multiple transitive tags propagate",
			parentTags:       []sts.Tag{{Key: "a", Value: "1"}, {Key: "b", Value: "2"}, {Key: "c", Value: "3"}},
			parentTransitive: []string{"a", "b"},
			childTags:        nil,
			wantTags:         map[string]string{"a": "1", "b": "2"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := sts.NewInMemoryBackend()

			var parentSession *sts.SessionInfo
			if len(tc.parentTags) > 0 || len(tc.parentTransitive) > 0 {
				parentSession = &sts.SessionInfo{
					AccessKeyID:       "ASIATESTPARENTKEY001",
					SessionToken:      "parent-token",
					AssumedRoleArn:    "arn:aws:sts::123456789012:assumed-role/Parent/s",
					AccountID:         "123456789012",
					SessionName:       "s",
					AssumedRoleID:     "AROAPARENT:s",
					Tags:              tc.parentTags,
					TransitiveTagKeys: tc.parentTransitive,
					Expiration:        time.Now().Add(1 * time.Hour),
				}
				backend.AddSessionInternal(parentSession)
			}

			input := &sts.AssumeRoleInput{
				RoleArn:         "arn:aws:iam::123456789012:role/Child",
				RoleSessionName: "child",
				Tags:            tc.childTags,
				CallerSession:   parentSession,
			}
			if parentSession != nil {
				input.CallerAccessKeyID = parentSession.AccessKeyID
			}

			resp, err := backend.AssumeRole(input)
			require.NoError(t, err)
			require.NotNil(t, resp)

			childSession := backend.LookupSession(resp.AssumeRoleResult.Credentials.AccessKeyID, "")
			require.NotNil(t, childSession)

			got := make(map[string]string, len(childSession.Tags))
			for _, tag := range childSession.Tags {
				got[tag.Key] = tag.Value
			}
			assert.Equal(t, tc.wantTags, got)
		})
	}
}

// ---------------------------------------------------------------------------
// JWT helpers used only to confirm the existing accuracy-test coverage works
// (no new JWT tests here — already in handler_accuracy_test.go).
// Kept as a compile-time reference for the JWT encoding path.
// ---------------------------------------------------------------------------

func buildParityJWT(t *testing.T, expUnix int64) string {
	t.Helper()

	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none","typ":"JWT"}`))
	payload, err := json.Marshal(map[string]any{"sub": "test", "exp": expUnix})
	require.NoError(t, err)

	return header + "." + base64.RawURLEncoding.EncodeToString(payload) + "."
}

// TestJWTExpiry_Wire confirms that AssumeRoleWithWebIdentity rejects expired tokens.
func TestJWTExpiry_Wire(t *testing.T) {
	t.Parallel()

	backend := sts.NewInMemoryBackend()

	expired := buildParityJWT(t, time.Now().Add(-10*time.Minute).Unix())
	_, err := backend.AssumeRoleWithWebIdentity(&sts.AssumeRoleWithWebIdentityInput{
		RoleArn:          "arn:aws:iam::123456789012:role/WebRole",
		RoleSessionName:  "web-session",
		WebIdentityToken: expired,
	})
	require.ErrorIs(t, err, sts.ErrExpiredToken)

	fresh := buildParityJWT(t, time.Now().Add(1*time.Hour).Unix())
	resp, err := backend.AssumeRoleWithWebIdentity(&sts.AssumeRoleWithWebIdentityInput{
		RoleArn:          "arn:aws:iam::123456789012:role/WebRole",
		RoleSessionName:  "web-session",
		WebIdentityToken: fresh,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, resp.AssumeRoleWithWebIdentityResult.Credentials.AccessKeyID)
}
