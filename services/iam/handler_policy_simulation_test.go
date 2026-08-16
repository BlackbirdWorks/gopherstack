package iam_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iam"
)

func TestSimulatePrincipalPolicy_ConditionContext_SourceIP(t *testing.T) {
	t.Parallel()

	b := iam.NewInMemoryBackend()

	_, err := b.CreateUser("ctx-user", "/", "")
	require.NoError(t, err)

	allowFromIPDoc := `{
		"Version": "2012-10-17",
		"Statement": [{
			"Effect": "Allow",
			"Action": "s3:GetObject",
			"Resource": "*",
			"Condition": {"IpAddress": {"aws:SourceIp": "192.0.2.0/24"}}
		}]
	}`

	pol, err := b.CreatePolicy("AllowFromIP", "/", allowFromIPDoc)
	require.NoError(t, err)
	require.NoError(t, b.AttachUserPolicy("ctx-user", pol.Arn))

	userArn := "arn:aws:iam::000000000000:user/ctx-user"

	tests := []struct {
		name    string
		ctx     iam.ConditionContext
		wantDec string
	}{
		{
			name:    "no context → implicit deny",
			ctx:     iam.ConditionContext{},
			wantDec: "implicitDeny",
		},
		{
			name:    "matching IP → allowed",
			ctx:     iam.ConditionContext{SourceIP: "192.0.2.42"},
			wantDec: "allowed",
		},
		{
			name:    "non-matching IP → implicit deny",
			ctx:     iam.ConditionContext{SourceIP: "10.0.0.1"},
			wantDec: "implicitDeny",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			results, simErr := b.SimulatePrincipalPolicy(
				userArn, "", "", nil, []string{"s3:GetObject"}, []string{"*"}, tt.ctx,
			)
			require.NoError(t, simErr)
			require.Len(t, results, 1)
			assert.Equal(t, tt.wantDec, results[0].Decision)
		})
	}
}

func TestSimulateCustomPolicy_ConditionContext_ExtraKey(t *testing.T) {
	t.Parallel()

	b := iam.NewInMemoryBackend()

	policyDoc := `{
		"Version": "2012-10-17",
		"Statement": [{
			"Effect": "Allow",
			"Action": "s3:GetObject",
			"Resource": "*",
			"Condition": {"StringEquals": {"aws:RequestedRegion": "us-east-1"}}
		}]
	}`

	tests := []struct {
		name    string
		ctx     iam.ConditionContext
		wantDec string
	}{
		{
			name:    "no extra key → implicit deny",
			ctx:     iam.ConditionContext{},
			wantDec: "implicitDeny",
		},
		{
			name:    "matching extra key → allowed",
			ctx:     iam.ConditionContext{Extra: map[string]string{"aws:requestedregion": "us-east-1"}},
			wantDec: "allowed",
		},
		{
			name:    "wrong region → implicit deny",
			ctx:     iam.ConditionContext{Extra: map[string]string{"aws:requestedregion": "eu-west-1"}},
			wantDec: "implicitDeny",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			results, err := b.SimulateCustomPolicy(
				[]string{policyDoc}, nil, []string{"s3:GetObject"}, []string{"*"}, tt.ctx,
			)
			require.NoError(t, err)
			require.Len(t, results, 1)
			assert.Equal(t, tt.wantDec, results[0].Decision)
		})
	}
}

func TestHandler_SimulatePrincipalPolicy_ContextEntries(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)

	_, err := b.CreateUser("ip-user", "/", "")
	require.NoError(t, err)

	ipDoc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject",` +
		`"Resource":"*","Condition":{"IpAddress":{"aws:SourceIp":"10.0.0.0/8"}}}]}`

	pol, err := b.CreatePolicy("IPPolicy", "/", ipDoc)
	require.NoError(t, err)
	require.NoError(t, b.AttachUserPolicy("ip-user", pol.Arn))

	userArn := "arn:aws:iam::000000000000:user/ip-user"

	// Without ContextEntries: implicitDeny.
	noCtxRec := iamCall(t, e, h, "SimulatePrincipalPolicy", map[string]string{
		"PolicySourceArn":       userArn,
		"ActionNames.member.1":  "s3:GetObject",
		"ResourceArns.member.1": "*",
	})
	require.Equal(t, http.StatusOK, noCtxRec.Code)
	assert.Contains(t, noCtxRec.Body.String(), "implicitDeny",
		"no ContextEntries → IP condition fails → implicitDeny")

	// With matching ContextEntries: allowed.
	ctxRec := iamCall(t, e, h, "SimulatePrincipalPolicy", map[string]string{
		"PolicySourceArn":                                   userArn,
		"ActionNames.member.1":                              "s3:GetObject",
		"ResourceArns.member.1":                             "*",
		"ContextEntries.member.1.ContextKeyName":            "aws:SourceIp",
		"ContextEntries.member.1.ContextKeyType":            "ip",
		"ContextEntries.member.1.ContextKeyValues.member.1": "10.1.2.3",
	})
	require.Equal(t, http.StatusOK, ctxRec.Code)
	assert.Contains(t, ctxRec.Body.String(), "allowed",
		"matching ContextEntries → IP condition satisfied → allowed")
}

// Test_PolicyDocumentEncoding verifies that policy-document response fields
// are percent-encoded (RFC 3986) on the wire, matching real AWS IAM. Every
// per-operation reference page (GetRole, GetRolePolicy, GetUserPolicy,
// GetGroupPolicy, GetPolicyVersion) documents:
// "Policies returned by this operation are URL-encoded compliant with RFC 3986."
// e.g. https://docs.aws.amazon.com/IAM/latest/APIReference/API_GetRole.html
//
// Before this fix, gopherstack returned the raw JSON document unencoded,
// which breaks any client that (correctly, per AWS's documented contract)
// URL-decodes the field it receives.

func TestHandler_SimulatePrincipalPolicy_AllowDenyDecisions(t *testing.T) {
	t.Parallel()

	const allowS3GetPolicy = `{
		"Version":"2012-10-17",
		"Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]
	}`
	const denyS3Policy = `{
		"Version":"2012-10-17",
		"Statement":[{"Effect":"Deny","Action":"s3:GetObject","Resource":"*"}]
	}`

	tests := []struct {
		setup         func(b *iam.InMemoryBackend, userArn *string)
		wantDecisions map[string]string // action → decision
		name          string
		actions       []string
		resources     []string
	}{
		{
			name: "no_policies_all_implicit_deny",
			setup: func(b *iam.InMemoryBackend, userArn *string) {
				u, err := b.CreateUser("alice", "/", "")
				require.NoError(t, err)
				*userArn = u.Arn
			},
			actions:   []string{"s3:GetObject", "ec2:DescribeInstances"},
			resources: []string{"*"},
			wantDecisions: map[string]string{
				"s3:GetObject":          "implicitDeny",
				"ec2:DescribeInstances": "implicitDeny",
			},
		},
		{
			name: "attached_allow_policy_grants_access",
			setup: func(b *iam.InMemoryBackend, userArn *string) {
				u, err := b.CreateUser("bob", "/", "")
				require.NoError(t, err)
				*userArn = u.Arn

				pol, err := b.CreatePolicy("AllowS3", "/", allowS3GetPolicy)
				require.NoError(t, err)

				require.NoError(t, b.AttachUserPolicy("bob", pol.Arn))
			},
			actions:   []string{"s3:GetObject", "ec2:DescribeInstances"},
			resources: []string{"*"},
			wantDecisions: map[string]string{
				"s3:GetObject":          "allowed",
				"ec2:DescribeInstances": "implicitDeny",
			},
		},
		{
			name: "inline_allow_policy_grants_access",
			setup: func(b *iam.InMemoryBackend, userArn *string) {
				u, err := b.CreateUser("carol", "/", "")
				require.NoError(t, err)
				*userArn = u.Arn

				require.NoError(t, b.PutUserPolicy("carol", "S3Access", allowS3GetPolicy))
			},
			actions:   []string{"s3:GetObject"},
			resources: []string{"*"},
			wantDecisions: map[string]string{
				"s3:GetObject": "allowed",
			},
		},
		{
			name: "explicit_deny_overrides_allow",
			setup: func(b *iam.InMemoryBackend, userArn *string) {
				u, err := b.CreateUser("dave", "/", "")
				require.NoError(t, err)
				*userArn = u.Arn

				allowPol, err := b.CreatePolicy("AllowS3b", "/", allowS3GetPolicy)
				require.NoError(t, err)

				denyPol, err := b.CreatePolicy("DenyS3", "/", denyS3Policy)
				require.NoError(t, err)

				require.NoError(t, b.AttachUserPolicy("dave", allowPol.Arn))
				require.NoError(t, b.AttachUserPolicy("dave", denyPol.Arn))
			},
			actions:   []string{"s3:GetObject"},
			resources: []string{"*"},
			wantDecisions: map[string]string{
				"s3:GetObject": "explicitDeny",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := iam.NewInMemoryBackend()
			var principalArn string
			tt.setup(b, &principalArn)

			results, err := b.SimulatePrincipalPolicy(
				principalArn,
				"",
				"",
				nil,
				tt.actions,
				tt.resources,
				iam.ConditionContext{},
			)
			require.NoError(t, err)
			require.Len(t, results, len(tt.actions)*len(tt.resources))

			for _, r := range results {
				wantDecision, ok := tt.wantDecisions[r.ActionName]
				if !ok {
					continue
				}

				assert.Equal(t, wantDecision, r.Decision,
					"action %q on resource %q: want decision %q, got %q",
					r.ActionName, r.ResourceName, wantDecision, r.Decision)
			}
		})
	}
}

func TestHandler_SimulatePrincipalPolicy_EvalDecisionPresent(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	_, _ = b.CreateUser("sim-user", "/", "")

	pol, _ := b.CreatePolicy("AllowS3", "/",
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`)
	_ = b.AttachUserPolicy("sim-user", pol.Arn)

	req := iamRequest("SimulatePrincipalPolicy", map[string]string{
		"PolicySourceArn":       "arn:aws:iam::000000000000:user/sim-user",
		"ActionNames.member.1":  "s3:GetObject",
		"ResourceArns.member.1": "*",
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, "EvalDecision")
	assert.Contains(t, body, "allowed")
	assert.Contains(t, body, "EvalDecisionDetails")
}

func TestHandler_SimulatePrincipalPolicy_MultipleActions(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	_, _ = b.CreateUser("multi-action-user", "/", "")

	pol, _ := b.CreatePolicy("AllowS3Read", "/",
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`)
	_ = b.AttachUserPolicy("multi-action-user", pol.Arn)

	req := iamRequest("SimulatePrincipalPolicy", map[string]string{
		"PolicySourceArn":       "arn:aws:iam::000000000000:user/multi-action-user",
		"ActionNames.member.1":  "s3:GetObject",
		"ActionNames.member.2":  "s3:DeleteObject",
		"ResourceArns.member.1": "*",
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, "s3:GetObject")
	assert.Contains(t, body, "s3:DeleteObject")
	assert.Contains(t, body, "allowed")
	assert.Contains(t, body, "implicitDeny")
}

func TestHandler_SimulateCustomPolicy_EvalDecisionDetails(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, _ := newTestHandler(t)

	allowDoc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`
	denyDoc := `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"s3:GetObject","Resource":"*"}]}`

	req := iamRequest("SimulateCustomPolicy", map[string]string{
		"PolicyInputList.member.1": allowDoc,
		"PolicyInputList.member.2": denyDoc,
		"ActionNames.member.1":     "s3:GetObject",
		"ResourceArns.member.1":    "*",
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, "EvalDecisionDetails")
	assert.Contains(t, body, "explicitDeny", "combined decision is explicitDeny")
}

func TestHandler_SimulatePrincipalPolicy_UserNotFound(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, _ := newTestHandler(t)

	req := iamRequest("SimulatePrincipalPolicy", map[string]string{
		"PolicySourceArn":      "arn:aws:iam::000000000000:user/nonexistent",
		"ActionNames.member.1": "s3:GetObject",
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	// NoSuchEntity is 404 on real AWS IAM.
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_SimulateCustomPolicy_IndexedParams(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, _ := newTestHandler(t)

	policyDoc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"iam:*","Resource":"*"}]}`
	action := "iam:CreateUser"

	params := map[string]string{
		"PolicyInputList.member.1": policyDoc,
		"ActionNames.member.1":     action,
	}

	req := iamRequest("SimulateCustomPolicy", params)
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, "allowed")
	assert.Contains(t, body, action)
}

func TestHandler_SimulatePrincipalPolicy_MultipleResources(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	_, _ = b.CreateUser("multi-res-user", "/", "")

	s3Policy := `{"Version":"2012-10-17","Statement":[` +
		`{"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket-a/*"}]}`
	pol, _ := b.CreatePolicy("AllowS3", "/", s3Policy)
	_ = b.AttachUserPolicy("multi-res-user", pol.Arn)

	req := iamRequestWithMembers(
		"SimulatePrincipalPolicy",
		map[string]string{
			"PolicySourceArn": "arn:aws:iam::000000000000:user/multi-res-user",
		},
		map[string][]string{
			"ActionNames.member":  {"s3:GetObject"},
			"ResourceArns.member": {"arn:aws:s3:::bucket-a/file.txt", "arn:aws:s3:::bucket-b/other.txt"},
		},
	)
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, "bucket-a")
	assert.Contains(t, body, "bucket-b")
	// bucket-a should be allowed, bucket-b implicitly denied.
	assert.Contains(t, body, "allowed")
	assert.Contains(t, body, "implicitDeny")
}

func TestHandler_SimulatePrincipalPolicy(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	_, _ = b.CreateUser("alice", "/", "")
	u, _ := b.GetUser("alice")

	req := iamRequest("SimulatePrincipalPolicy", map[string]string{
		"PolicySourceArn":       u.Arn,
		"ActionNames.member.1":  "s3:GetObject",
		"ResourceArns.member.1": "arn:aws:s3:::my-bucket/*",
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "EvaluationResults")
}
