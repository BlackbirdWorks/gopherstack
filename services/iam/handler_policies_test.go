package iam_test

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/iam"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
func Test_PolicyDocumentEncoding(t *testing.T) {
	t.Parallel()

	const rawDoc = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}`
	// url.QueryEscape("s3:*") == "s3%3A%2A"; the whole document is escaped the
	// same way once it reaches the wire.
	const encodedFragment = "s3%3A%2A"

	tests := []struct {
		setup    func(t *testing.T, b *iam.InMemoryBackend) map[string]string
		name     string
		action   string
		wantElem string
	}{
		{
			name:   "GetRole_AssumeRolePolicyDocument",
			action: "GetRole",
			setup: func(t *testing.T, b *iam.InMemoryBackend) map[string]string {
				t.Helper()
				_, err := b.CreateRole("MyRole", "/", rawDoc, "")
				require.NoError(t, err)

				return map[string]string{"RoleName": "MyRole"}
			},
			wantElem: "AssumeRolePolicyDocument",
		},
		{
			name:   "GetPolicyVersion_Document",
			action: "GetPolicyVersion",
			setup: func(t *testing.T, b *iam.InMemoryBackend) map[string]string {
				t.Helper()
				pol, err := b.CreatePolicy("MyPolicy", "/", rawDoc)
				require.NoError(t, err)

				return map[string]string{"PolicyArn": pol.Arn, "VersionId": "v1"}
			},
			wantElem: "Document",
		},
		{
			name:   "GetUserPolicy_PolicyDocument",
			action: "GetUserPolicy",
			setup: func(t *testing.T, b *iam.InMemoryBackend) map[string]string {
				t.Helper()
				_, err := b.CreateUser("bob", "/", "")
				require.NoError(t, err)
				require.NoError(t, b.PutUserPolicy("bob", "Inline", rawDoc))

				return map[string]string{"UserName": "bob", "PolicyName": "Inline"}
			},
			wantElem: "PolicyDocument",
		},
		{
			name:   "GetRolePolicy_PolicyDocument",
			action: "GetRolePolicy",
			setup: func(t *testing.T, b *iam.InMemoryBackend) map[string]string {
				t.Helper()
				_, err := b.CreateRole("PolRole", "/", "{}", "")
				require.NoError(t, err)
				require.NoError(t, b.PutRolePolicy("PolRole", "Inline", rawDoc))

				return map[string]string{"RoleName": "PolRole", "PolicyName": "Inline"}
			},
			wantElem: "PolicyDocument",
		},
		{
			name:   "GetGroupPolicy_PolicyDocument",
			action: "GetGroupPolicy",
			setup: func(t *testing.T, b *iam.InMemoryBackend) map[string]string {
				t.Helper()
				_, err := b.CreateGroup("PolGroup", "/")
				require.NoError(t, err)
				require.NoError(t, b.PutGroupPolicy("PolGroup", "Inline", rawDoc))

				return map[string]string{"GroupName": "PolGroup", "PolicyName": "Inline"}
			},
			wantElem: "PolicyDocument",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			h, b := newTestHandler(t)
			params := tt.setup(t, b)

			req := iamRequest(tt.action, params)
			rec := httptest.NewRecorder()
			require.NoError(t, h.Handler()(e.NewContext(req, rec)))
			require.Equal(t, http.StatusOK, rec.Code)

			body := rec.Body.String()
			assert.Contains(t, body, encodedFragment,
				"%s must be percent-encoded on the wire", tt.wantElem)
			assert.NotContains(t, body, rawDoc,
				"%s must not contain the raw (unencoded) policy document", tt.wantElem)
		})
	}
}

func TestAttachRolePolicy_Idempotent(t *testing.T) {
	t.Parallel()

	b := iam.NewInMemoryBackend()
	_, err := b.CreateRole("myrole", "/", "{}", "")
	require.NoError(t, err)

	policyArn := "arn:aws:iam::000000000000:policy/MyPolicy"

	// First attach
	require.NoError(t, b.AttachRolePolicy("myrole", policyArn))

	// Second attach to same role with same ARN – should be a no-op (idempotent)
	require.NoError(t, b.AttachRolePolicy("myrole", policyArn))

	policies, err := b.ListAttachedRolePolicies("myrole")
	require.NoError(t, err)
	assert.Len(t, policies, 1, "policy should appear only once")
}

func TestPolicyNameFromARN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		policyArn string
		wantName  string
	}{
		{
			name:      "arn_with_policy_prefix",
			policyArn: "arn:aws:iam::000000000000:policy/MyManagedPolicy",
			wantName:  "MyManagedPolicy",
		},
		{
			name:      "arn_without_policy_prefix_returns_full_string",
			policyArn: "SomeArbitraryString",
			wantName:  "SomeArbitraryString",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()

			// Create a policy then look it up by ARN to trigger policyNameFromARN
			_, err := b.CreatePolicy(
				tt.wantName,
				"/",
				`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
			)
			require.NoError(t, err)

			pol, err := b.GetPolicy(tt.policyArn)
			if tt.policyArn == "SomeArbitraryString" {
				// Won't be found, but function is exercised
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.wantName, pol.PolicyName)
			}
		})
	}
}

func TestIAMHandler_PolicyDispatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(b *iam.InMemoryBackend)
		params      map[string]string
		name        string
		action      string
		wantContain string
		wantCode    int
	}{
		{
			name:   "GetPolicy_success",
			action: "GetPolicy",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreatePolicy(
					"ReadOnlyPolicy",
					"/",
					`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
				)
			},
			params:      map[string]string{"PolicyArn": "arn:aws:iam::000000000000:policy/ReadOnlyPolicy"},
			wantCode:    http.StatusOK,
			wantContain: "GetPolicyResponse",
		},
		{
			name:   "GetPolicy_not_found",
			action: "GetPolicy",
			params: map[string]string{"PolicyArn": "arn:aws:iam::000000000000:policy/Ghost"},
			// NoSuchEntity is 404 on real AWS IAM.
			wantCode: http.StatusNotFound,
		},
		{
			name:   "GetPolicyVersion_success",
			action: "GetPolicyVersion",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreatePolicy(
					"VersionedPolicy",
					"/",
					`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
				)
			},
			params: map[string]string{
				"PolicyArn": "arn:aws:iam::000000000000:policy/VersionedPolicy",
				"VersionId": "v1",
			},
			wantCode:    http.StatusOK,
			wantContain: "GetPolicyVersionResponse",
		},
		{
			name:   "ListPolicyVersions_success",
			action: "ListPolicyVersions",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreatePolicy(
					"AnyPolicy",
					"/",
					`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
				)
			},
			params:      map[string]string{"PolicyArn": "arn:aws:iam::000000000000:policy/AnyPolicy"},
			wantCode:    http.StatusOK,
			wantContain: "ListPolicyVersionsResponse",
		},
		{
			name:   "AttachRolePolicy_success",
			action: "AttachRolePolicy",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateRole("svc-role", "/", "{}", "")
			},
			params: map[string]string{
				"RoleName":  "svc-role",
				"PolicyArn": "arn:aws:iam::aws:policy/ReadOnlyAccess",
			},
			wantCode:    http.StatusOK,
			wantContain: "AttachRolePolicyResponse",
		},
		{
			name:   "DetachRolePolicy_success",
			action: "DetachRolePolicy",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateRole("detach-role", "/", "{}", "")
				_ = b.AttachRolePolicy("detach-role", "arn:aws:iam::aws:policy/Policy1")
			},
			params: map[string]string{
				"RoleName":  "detach-role",
				"PolicyArn": "arn:aws:iam::aws:policy/Policy1",
			},
			wantCode:    http.StatusOK,
			wantContain: "DetachRolePolicyResponse",
		},
		{
			name:   "ListAttachedRolePolicies_success",
			action: "ListAttachedRolePolicies",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateRole("list-role", "/", "{}", "")
				_ = b.AttachRolePolicy("list-role", "arn:aws:iam::aws:policy/Policy1")
			},
			params:      map[string]string{"RoleName": "list-role"},
			wantCode:    http.StatusOK,
			wantContain: "ListAttachedRolePoliciesResponse",
		},
		{
			name:   "ListRolePolicies_success",
			action: "ListRolePolicies",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateRole("any-role", "/", "{}", "")
			},
			params:      map[string]string{"RoleName": "any-role"},
			wantCode:    http.StatusOK,
			wantContain: "ListRolePoliciesResponse",
		},
		{
			name:   "ListInstanceProfilesForRole_success",
			action: "ListInstanceProfilesForRole",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateRole(
					"any-role",
					"/",
					`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
					"",
				)
			},
			params:      map[string]string{"RoleName": "any-role"},
			wantCode:    http.StatusOK,
			wantContain: "ListInstanceProfilesForRoleResponse",
		},
		{
			name:   "ListAttachedUserPolicies_success",
			action: "ListAttachedUserPolicies",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("policy-user", "/", "")
				_ = b.AttachUserPolicy("policy-user", "arn:aws:iam::aws:policy/ReadOnly")
			},
			params:      map[string]string{"UserName": "policy-user"},
			wantCode:    http.StatusOK,
			wantContain: "ListAttachedUserPoliciesResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			h, b := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(b)
			}

			req := iamRequest(tt.action, tt.params)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			err := h.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, tt.wantCode, rec.Code)
			if tt.wantContain != "" {
				assert.Contains(t, rec.Body.String(), tt.wantContain)
			}
		})
	}
}

func TestIAMHandler_ListPolicies(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)
	e := echo.New()

	_, _ = b.CreatePolicy(
		"APolicy",
		"/",
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
	)
	_, _ = b.CreatePolicy(
		"BPolicy",
		"/",
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
	)

	req := iamRequest("ListPolicies", nil)
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		XMLName xml.Name `xml:"ListPoliciesResponse"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
}

func TestIAMHandler_AttachUserPolicy_Success(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)
	e := echo.New()

	_, _ = b.CreateUser("policy-attach-user", "/", "")

	req := iamRequest("AttachUserPolicy", map[string]string{
		"UserName":  "policy-attach-user",
		"PolicyArn": "arn:aws:iam::aws:policy/ReadOnlyAccess",
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "AttachUserPolicyResponse")
}

// TestHandler_SimulatePrincipalPolicy_AllowDenyDecisions verifies that SimulatePrincipalPolicy
// evaluates actual attached policies rather than returning canned results.
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

func TestHandler_GetPolicyVersion_VersionIdNotHardcoded(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		requestedVersion string
		wantVersionID    string
		setAsDefault     bool
	}{
		{"v1", "v1", "v1", false},
		{"v2 non-default", "v2", "v2", false},
		{"v2 set as default", "v2", "v2", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			h, b := newTestHandler(t)

			doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
			p, err := b.CreatePolicy("GPVPolicy-"+tc.name, "/", doc)
			require.NoError(t, err)

			doc2 := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}`
			_, err = b.CreatePolicyVersion(p.Arn, doc2, false)
			require.NoError(t, err)

			if tc.setAsDefault {
				require.NoError(t, b.SetDefaultPolicyVersion(p.Arn, "v2"))
			}

			req := iamRequest("GetPolicyVersion", map[string]string{
				"PolicyArn": p.Arn,
				"VersionId": tc.requestedVersion,
			})
			rec := httptest.NewRecorder()
			require.NoError(t, h.Handler()(e.NewContext(req, rec)))
			assert.Equal(t, http.StatusOK, rec.Code)

			body := rec.Body.String()
			assert.Contains(t, body, "<VersionId>"+tc.wantVersionID+"</VersionId>",
				"GetPolicyVersion response VersionId must match requested version, not hardcoded v1")
		})
	}
}

func TestHandler_GetPolicy_ReturnsAWSFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		setup            func(b *iam.InMemoryBackend, policyArn string)
		wantDefaultVer   string
		wantAttachCount  string
		wantIsAttachable string
	}{
		{
			name:             "fresh policy",
			setup:            func(_ *iam.InMemoryBackend, _ string) {},
			wantDefaultVer:   "v1",
			wantAttachCount:  "0",
			wantIsAttachable: "true",
		},
		{
			name: "after attach to user",
			setup: func(b *iam.InMemoryBackend, policyArn string) {
				_, _ = b.CreateUser("gp-fields-user", "/", "")
				_ = b.AttachUserPolicy("gp-fields-user", policyArn)
			},
			wantDefaultVer:   "v1",
			wantAttachCount:  "1",
			wantIsAttachable: "true",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			h, b := newTestHandler(t)

			doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
			p, err := b.CreatePolicy("GPFieldsPolicy-"+tc.name, "/", doc)
			require.NoError(t, err)

			tc.setup(b, p.Arn)

			req := iamRequest("GetPolicy", map[string]string{"PolicyArn": p.Arn})
			rec := httptest.NewRecorder()
			require.NoError(t, h.Handler()(e.NewContext(req, rec)))
			assert.Equal(t, http.StatusOK, rec.Code)

			body := rec.Body.String()
			assert.Contains(t, body, "<DefaultVersionId>"+tc.wantDefaultVer+"</DefaultVersionId>",
				"GetPolicy must include DefaultVersionId")
			assert.Contains(t, body, "<AttachmentCount>"+tc.wantAttachCount+"</AttachmentCount>",
				"GetPolicy must include AttachmentCount")
			assert.Contains(t, body, "<IsAttachable>"+tc.wantIsAttachable+"</IsAttachable>",
				"GetPolicy must include IsAttachable")
			assert.Contains(t, body, "<UpdateDate>",
				"GetPolicy must include UpdateDate")
		})
	}
}

func TestHandler_GetPolicy_DefaultVersionIdUpdatesAfterNewDefault(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)

	doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
	p, err := b.CreatePolicy("GPDefVer", "/", doc)
	require.NoError(t, err)

	doc2 := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}`
	_, err = b.CreatePolicyVersion(p.Arn, doc2, false)
	require.NoError(t, err)

	require.NoError(t, b.SetDefaultPolicyVersion(p.Arn, "v2"))

	req := iamRequest("GetPolicy", map[string]string{"PolicyArn": p.Arn})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, "<DefaultVersionId>v2</DefaultVersionId>",
		"DefaultVersionId must reflect v2 after SetDefaultPolicyVersion")
	assert.NotContains(t, body, "<DefaultVersionId>v1</DefaultVersionId>",
		"DefaultVersionId must not still say v1 after v2 was set as default")
}

func TestCreatePolicyVersion_Backend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup         func(b *iam.InMemoryBackend) string
		name          string
		policyDoc     string
		wantVersionID string
		setAsDefault  bool
		wantErr       bool
	}{
		{
			name: "create_version_success",
			setup: func(b *iam.InMemoryBackend) string {
				p, _ := b.CreatePolicy(
					"ReadOnly",
					"/",
					`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
				)

				return p.Arn
			},
			policyDoc:     `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
			wantVersionID: "v2",
		},
		{
			name: "create_version_set_as_default",
			setup: func(b *iam.InMemoryBackend) string {
				p, _ := b.CreatePolicy(
					"WritePolicy",
					"/",
					`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
				)

				return p.Arn
			},
			policyDoc:     `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
			setAsDefault:  true,
			wantVersionID: "v2",
		},
		{
			name: "policy_not_found_returns_error",
			setup: func(_ *iam.InMemoryBackend) string {
				return "arn:aws:iam::000000000000:policy/NonExistent"
			},
			policyDoc: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
			wantErr:   true,
		},
		{
			name: "empty_document_returns_error",
			setup: func(_ *iam.InMemoryBackend) string {
				return "arn:aws:iam::000000000000:policy/SomePolicy"
			},
			policyDoc: "",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			policyArn := tt.setup(b)

			pv, err := b.CreatePolicyVersion(policyArn, tt.policyDoc, tt.setAsDefault)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, pv)
			assert.Equal(t, tt.wantVersionID, pv.VersionID)
			assert.Equal(t, tt.setAsDefault, pv.IsDefaultVersion)
		})
	}
}

func TestIAMHandler_ListPolicyVersions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		params           map[string]string
		name             string
		wantBodyContains []string
		wantCode         int
		setupData        bool
	}{
		{
			name:      "returns_multiple_versions",
			setupData: true,
			params: map[string]string{
				"PolicyArn": "arn:aws:iam::000000000000:policy/VersionListPolicy",
			},
			wantCode: http.StatusOK,
			wantBodyContains: []string{
				"<VersionId>v1</VersionId>",
				"<VersionId>v2</VersionId>",
				"<IsDefaultVersion>false</IsDefaultVersion>",
				"<IsDefaultVersion>true</IsDefaultVersion>",
			},
		},
		{
			name: "policy_not_found_returns_404",
			params: map[string]string{
				"PolicyArn": "arn:aws:iam::000000000000:policy/VersionListPolicy",
			},
			// NoSuchEntity is 404 on real AWS IAM.
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			h, b := newTestHandler(t)
			if tt.setupData {
				pol, setupErr := b.CreatePolicy(
					"VersionListPolicy",
					"/",
					`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
				)
				require.NoError(t, setupErr)
				_, setupErr = b.CreatePolicyVersion(
					pol.Arn,
					`{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"*","Resource":"*"}]}`,
					true,
				)
				require.NoError(t, setupErr)
			}

			req := iamRequest("ListPolicyVersions", tt.params)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			err := h.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, expected := range tt.wantBodyContains {
				assert.Contains(t, rec.Body.String(), expected)
			}
		})
	}
}

func TestHandler_AttachDetachUserPolicy_RoundTrip(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	_, _ = b.CreateUser("alice", "/", "")
	doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
	p, _ := b.CreatePolicy("P", "/", doc)

	// Attach.
	req := iamRequest("AttachUserPolicy", map[string]string{
		"UserName":  "alice",
		"PolicyArn": p.Arn,
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify via ListAttachedUserPolicies.
	req2 := iamRequest("ListAttachedUserPolicies", map[string]string{"UserName": "alice"})
	rec2 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req2, rec2)))
	assert.Equal(t, http.StatusOK, rec2.Code)
	assert.Contains(t, rec2.Body.String(), p.Arn)

	// Detach.
	req3 := iamRequest("DetachUserPolicy", map[string]string{
		"UserName":  "alice",
		"PolicyArn": p.Arn,
	})
	rec3 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req3, rec3)))
	assert.Equal(t, http.StatusOK, rec3.Code)

	// Verify detached.
	req4 := iamRequest("ListAttachedUserPolicies", map[string]string{"UserName": "alice"})
	rec4 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req4, rec4)))
	assert.Equal(t, http.StatusOK, rec4.Code)
	assert.NotContains(t, rec4.Body.String(), p.Arn)
}

func TestHandler_AttachRolePolicy_RoundTrip(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
	_, _ = b.CreateRole("MyRole", "/", doc, "")
	p, _ := b.CreatePolicy("P", "/", doc)

	req := iamRequest("AttachRolePolicy", map[string]string{
		"RoleName":  "MyRole",
		"PolicyArn": p.Arn,
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	req2 := iamRequest("ListAttachedRolePolicies", map[string]string{"RoleName": "MyRole"})
	rec2 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req2, rec2)))
	assert.Contains(t, rec2.Body.String(), p.Arn)

	req3 := iamRequest("DetachRolePolicy", map[string]string{
		"RoleName":  "MyRole",
		"PolicyArn": p.Arn,
	})
	rec3 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req3, rec3)))
	assert.Equal(t, http.StatusOK, rec3.Code)
}

func TestHandler_PolicyVersion_CRUD(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
	p, _ := b.CreatePolicy("VersionedPolicy", "/", doc)

	// CreatePolicyVersion.
	doc2 := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}`
	req := iamRequest("CreatePolicyVersion", map[string]string{
		"PolicyArn":      p.Arn,
		"PolicyDocument": doc2,
		"SetAsDefault":   "false",
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code, "CreatePolicyVersion must succeed")

	// ListPolicyVersions.
	req2 := iamRequest("ListPolicyVersions", map[string]string{"PolicyArn": p.Arn})
	rec2 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req2, rec2)))
	assert.Equal(t, http.StatusOK, rec2.Code)
	assert.Contains(t, rec2.Body.String(), "v1")
	assert.Contains(t, rec2.Body.String(), "v2")

	// GetPolicyVersion.
	req3 := iamRequest("GetPolicyVersion", map[string]string{
		"PolicyArn": p.Arn,
		"VersionId": "v2",
	})
	rec3 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req3, rec3)))
	assert.Equal(t, http.StatusOK, rec3.Code)
	// Real AWS IAM returns PolicyVersion.Document URL-encoded (RFC 3986); "s3:*"
	// becomes "s3%3A%2A" on the wire.
	assert.Contains(t, rec3.Body.String(), "s3%3A%2A")

	// SetDefaultPolicyVersion.
	req4 := iamRequest("SetDefaultPolicyVersion", map[string]string{
		"PolicyArn": p.Arn,
		"VersionId": "v2",
	})
	rec4 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req4, rec4)))
	assert.Equal(t, http.StatusOK, rec4.Code)

	// DeletePolicyVersion (non-default).
	req5 := iamRequest("CreatePolicyVersion", map[string]string{
		"PolicyArn":      p.Arn,
		"PolicyDocument": doc,
		"SetAsDefault":   "false",
	})
	rec5 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req5, rec5)))
	require.Equal(t, http.StatusOK, rec5.Code)

	req6 := iamRequest("DeletePolicyVersion", map[string]string{
		"PolicyArn": p.Arn,
		"VersionId": "v1",
	})
	rec6 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req6, rec6)))
	assert.Equal(t, http.StatusOK, rec6.Code)
}

func TestHandler_PolicyVersion_LimitExceeded(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
	p, _ := b.CreatePolicy("LP", "/", doc)

	// Create 4 more versions to hit the cap (v1 + v2 + v3 + v4 + v5 = 5).
	for range 4 {
		req := iamRequest("CreatePolicyVersion", map[string]string{
			"PolicyArn":      p.Arn,
			"PolicyDocument": doc,
			"SetAsDefault":   "false",
		})
		rec := httptest.NewRecorder()
		require.NoError(t, h.Handler()(e.NewContext(req, rec)))
		require.Equal(t, http.StatusOK, rec.Code, "must succeed under limit")
	}

	// 6th version must fail.
	req := iamRequest("CreatePolicyVersion", map[string]string{
		"PolicyArn":      p.Arn,
		"PolicyDocument": doc,
		"SetAsDefault":   "false",
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	// Real AWS IAM returns 409 for LimitExceeded, not 400.
	assert.Equal(t, http.StatusConflict, rec.Code)

	var errResp iam.ErrorResponse
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "LimitExceeded", errResp.Error.Code)
}

func TestHandler_ListEntitiesForPolicy(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
	_, _ = b.CreateUser("alice", "/", "")
	p, _ := b.CreatePolicy("P", "/", doc)
	_ = b.AttachUserPolicy("alice", p.Arn)

	req := iamRequest("ListEntitiesForPolicy", map[string]string{"PolicyArn": p.Arn})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "alice")
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

func TestIAMHandler_Policies(t *testing.T) {
	t.Parallel()

	t.Run("CreatePolicy", func(t *testing.T) {
		t.Parallel()
		e := echo.New()
		h, _ := newTestHandler(t)

		req := iamRequest("CreatePolicy", map[string]string{
			"PolicyName":     "MyPolicy",
			"PolicyDocument": `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
		})
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := h.Handler()(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, rec.Code)

		var resp iam.CreatePolicyResponse
		require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
		assert.Equal(t, "MyPolicy", resp.CreatePolicyResult.Policy.PolicyName)
	})

	t.Run("DeletePolicy", func(t *testing.T) {
		t.Parallel()
		e := echo.New()
		h, b := newTestHandler(t)
		pol, _ := b.CreatePolicy("MyPolicy", "/", "")

		req := iamRequest("DeletePolicy", map[string]string{"PolicyArn": pol.Arn})
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := h.Handler()(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("ListPolicies", func(t *testing.T) {
		t.Parallel()
		e := echo.New()
		h, b := newTestHandler(t)
		_, _ = b.CreatePolicy("PolicyA", "/", "")

		req := iamRequest("ListPolicies", nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := h.Handler()(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, rec.Code)

		var resp iam.ListPoliciesResponse
		require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
		assert.Len(t, resp.ListPoliciesResult.Policies, 1)
	})

	t.Run("AttachUserPolicy", func(t *testing.T) {
		t.Parallel()
		e := echo.New()
		h, b := newTestHandler(t)
		_, _ = b.CreateUser("alice", "/", "")

		req := iamRequest("AttachUserPolicy", map[string]string{
			"UserName":  "alice",
			"PolicyArn": "arn:aws:iam::000000000000:policy/SomePolicy",
		})
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := h.Handler()(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("AttachRolePolicy", func(t *testing.T) {
		t.Parallel()
		e := echo.New()
		h, b := newTestHandler(t)
		_, _ = b.CreateRole("MyRole", "/", "", "")

		req := iamRequest("AttachRolePolicy", map[string]string{
			"RoleName":  "MyRole",
			"PolicyArn": "arn:aws:iam::000000000000:policy/SomePolicy",
		})
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := h.Handler()(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, rec.Code)
	})
}

func TestIAMHandler_DetachRolePolicy(t *testing.T) {
	t.Parallel()

	t.Run("DetachRolePolicy", func(t *testing.T) {
		t.Parallel()
		e := echo.New()
		h, b := newTestHandler(t)
		_, _ = b.CreateRole("MyRole", "/", "", "")
		_ = b.AttachRolePolicy("MyRole", "arn:aws:iam::000000000000:policy/SomePolicy")

		req := iamRequest("DetachRolePolicy", map[string]string{
			"RoleName":  "MyRole",
			"PolicyArn": "arn:aws:iam::000000000000:policy/SomePolicy",
		})
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := h.Handler()(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("DetachRolePolicyRoleNotFound", func(t *testing.T) {
		t.Parallel()
		e := echo.New()
		h, _ := newTestHandler(t)

		req := iamRequest("DetachRolePolicy", map[string]string{
			"RoleName":  "nonexistent",
			"PolicyArn": "arn:aws:iam::000000000000:policy/P",
		})
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := h.Handler()(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusNotFound, rec.Code)

		var errResp iam.ErrorResponse
		require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
		assert.Equal(t, "NoSuchEntity", errResp.Error.Code)
	})
}
