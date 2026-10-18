package iam_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/iampolicystream"
	"github.com/blackbirdworks/gopherstack/services/iam"
)

func TestEnforcementPolicyStream(t *testing.T) {
	t.Parallel()

	allowSid := `{"Statement":[{"Sid":"ReadAll","Effect":"Allow","Action":"s3:Get*","Resource":"*",` +
		`"Condition":{"StringEquals":{"aws:username":"alice"}}}]}`
	denySid := `{"Statement":[{"Sid":"NoRead","Effect":"Deny","Action":"s3:GetObject","Resource":"*"}]}`
	allowIAM := `{"Statement":[{"Effect":"Allow","Action":"iam:*","Resource":"*"}]}`

	tests := []struct {
		name         string
		policy       string
		wantDecision string
		wantReason   string
		wantSid      string
		wantStatus   int
		audit        bool
		wantSuggest  bool
	}{
		{
			name: "allow_records_sid", policy: allowSid, wantStatus: http.StatusOK,
			wantDecision: iampolicystream.DecisionAllow, wantReason: iampolicystream.ReasonIdentityPolicy,
			wantSid: "ReadAll",
		},
		{
			name: "explicit_deny_has_sid_no_suggestion", policy: denySid, wantStatus: http.StatusForbidden,
			wantDecision: iampolicystream.DecisionDeny, wantReason: iampolicystream.ReasonIdentityPolicy,
			wantSid: "NoRead",
		},
		{
			name: "implicit_deny_suggests_statement", policy: allowIAM, wantStatus: http.StatusForbidden,
			wantDecision: iampolicystream.DecisionDeny, wantReason: iampolicystream.ReasonImplicitDeny,
			wantSuggest: true,
		},
		{
			name: "audit_mode_records_but_allows", policy: allowIAM, audit: true, wantStatus: http.StatusOK,
			wantDecision: iampolicystream.DecisionDeny, wantReason: iampolicystream.ReasonImplicitDeny,
			wantSuggest: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := newMockEnforcementBackend()
			backend.users["alice"] = &iam.User{UserName: "alice", Arn: "arn:aws:iam::000000000000:user/alice"}
			backend.keyMap["AKIAPS"] = "alice"
			backend.policies["alice"] = []string{tt.policy}

			stream := iampolicystream.New(10)
			e := echo.New()
			e.Use(iam.EnforcementMiddleware(backend, iam.EnforcementConfig{Recorder: stream, Audit: tt.audit}))
			e.Any("/*", func(c *echo.Context) error { return c.String(http.StatusOK, "ok") })

			req := httptest.NewRequest(http.MethodGet, "/my-bucket/key", nil)
			req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKIAPS/20230101/us-east-1/s3/aws4_request")
			rec := httptest.NewRecorder()
			e.ServeHTTP(rec, req)

			assert.Equal(t, tt.wantStatus, rec.Code)

			events := stream.Snapshot(nil)
			require.Len(t, events, 1)

			ev := events[0]
			assert.Equal(t, "s3:GetObject", ev.Action)
			assert.Equal(t, "s3", ev.Service)
			assert.Equal(t, "GetObject", ev.Operation)
			assert.Equal(t, "arn:aws:iam::000000000000:user/alice", ev.Principal)
			assert.Equal(t, tt.wantDecision, ev.Decision)
			assert.Equal(t, tt.wantReason, ev.Reason)
			assert.Equal(t, tt.wantSid, ev.MatchedSid)
			assert.Equal(t, !tt.audit, ev.Enforced)
			assert.Equal(t, tt.wantSuggest, ev.Suggested != nil)

			if tt.wantSuggest {
				assert.Equal(t, []string{"s3:GetObject"}, ev.Suggested.Statement[0].Action)
			}
		})
	}
}

func TestExplainPolicies(t *testing.T) {
	t.Parallel()

	docs := []string{
		`{"Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*","Sid":"A"}]}`,
		`{"Statement":[{"Effect":"Allow","Action":"s3:Get*","Resource":"*","Sid":"B",` +
			`"Condition":{"StringEquals":{"aws:RequestedRegion":"eu-west-1"}}}]}`,
	}

	res, m := iam.ExplainPolicies(docs, "s3:GetObject", "*", iam.ConditionContext{})
	assert.Equal(t, iam.EvalAllow, res)
	assert.Equal(t, "A", m.Sid)
	assert.Equal(t, 0, m.PolicyIndex)
	assert.Equal(t, []string{"aws:RequestedRegion"}, m.ConditionKeys)

	res, m = iam.ExplainPolicies(docs, "sqs:SendMessage", "*", iam.ConditionContext{})
	assert.Equal(t, iam.EvalImplicitDeny, res)
	assert.False(t, m.Matched)
}
