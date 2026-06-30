package iam_test

// IAM audit fixes — table-driven tests for:
//   1. GetAccountAuthorizationDetails GroupList per user
//   2. DeleteServiceLinkedRole actually removes the role from the backend
//   3. SimulatePrincipalPolicy / SimulateCustomPolicy honour ConditionContext

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	iam "github.com/blackbirdworks/gopherstack/services/iam"
)

// iamCall wraps the IAM test pattern: create context, run handler, return recorder.
func iamCall(
	t *testing.T, e *echo.Echo, h *iam.Handler, action string, params map[string]string,
) *httptest.ResponseRecorder {
	t.Helper()
	req := iamRequest(action, params)
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))

	return rec
}

// ---- GetAccountAuthorizationDetails GroupList ----

func TestGetAccountAuthorizationDetails_GroupList(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)

	// Create users, groups, memberships via backend to avoid ordering dependencies.
	_, err := b.CreateUser("alice", "/", "")
	require.NoError(t, err)
	_, err = b.CreateUser("bob", "/", "")
	require.NoError(t, err)
	_, err = b.CreateGroup("admins", "/")
	require.NoError(t, err)
	_, err = b.CreateGroup("devs", "/")
	require.NoError(t, err)
	require.NoError(t, b.AddUserToGroup("admins", "alice"))
	require.NoError(t, b.AddUserToGroup("devs", "alice"))
	require.NoError(t, b.AddUserToGroup("devs", "bob"))

	rec := iamCall(t, e, h, "GetAccountAuthorizationDetails", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	type userEntry struct {
		UserName  string   `xml:"UserName"`
		GroupList []string `xml:"GroupList>member"`
	}
	var resp struct {
		XMLName        xml.Name    `xml:"GetAccountAuthorizationDetailsResponse"`
		UserDetailList []userEntry `xml:"GetAccountAuthorizationDetailsResult>UserDetailList>member"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))

	userGroups := make(map[string][]string)
	for _, u := range resp.UserDetailList {
		userGroups[u.UserName] = u.GroupList
	}

	assert.ElementsMatch(t, []string{"admins", "devs"}, userGroups["alice"],
		"alice should be in both groups")
	assert.ElementsMatch(t, []string{"devs"}, userGroups["bob"],
		"bob should be in devs only")
}

func TestGetAccountAuthorizationDetails_GroupList_Empty(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)

	_, err := b.CreateUser("solo", "/", "")
	require.NoError(t, err)

	rec := iamCall(t, e, h, "GetAccountAuthorizationDetails", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	type userEntry2 struct {
		UserName  string   `xml:"UserName"`
		GroupList []string `xml:"GroupList>member"`
	}
	var resp struct {
		XMLName        xml.Name     `xml:"GetAccountAuthorizationDetailsResponse"`
		UserDetailList []userEntry2 `xml:"GetAccountAuthorizationDetailsResult>UserDetailList>member"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))

	for _, u := range resp.UserDetailList {
		if u.UserName == "solo" {
			assert.Empty(t, u.GroupList, "user with no memberships should have empty GroupList")
		}
	}
}

// ---- DeleteServiceLinkedRole ----

func TestDeleteServiceLinkedRole_RemovesRole(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, _ := newTestHandler(t)

	// Create a service-linked role.
	iamCall(t, e, h, "CreateServiceLinkedRole", map[string]string{
		"AWSServiceName": "elasticloadbalancing.amazonaws.com",
	})

	// Discover the role name from ListRoles.
	listBefore := iamCall(t, e, h, "ListRoles", nil)
	require.Equal(t, http.StatusOK, listBefore.Code)
	roleName := extractFirstXMLTag(listBefore.Body.String(), "RoleName")
	require.NotEmpty(t, roleName, "service-linked role should appear in ListRoles")

	// Delete it.
	delRec := iamCall(t, e, h, "DeleteServiceLinkedRole", map[string]string{"RoleName": roleName})
	require.Equal(t, http.StatusOK, delRec.Code)
	assert.Contains(t, delRec.Body.String(), "DeletionTaskId",
		"response must include a DeletionTaskId")

	// Verify role is gone.
	listAfter := iamCall(t, e, h, "ListRoles", nil)
	require.Equal(t, http.StatusOK, listAfter.Code)
	assert.NotContains(t, listAfter.Body.String(), roleName,
		"service-linked role must be removed from the backend")
}

func TestDeleteServiceLinkedRole_Idempotent(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, _ := newTestHandler(t)

	// Deleting a non-existent role must not error (AWS async semantics).
	rec := iamCall(t, e, h, "DeleteServiceLinkedRole", map[string]string{"RoleName": "NonExistentRole"})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DeletionTaskId")
}

// extractFirstXMLTag extracts the text of the first occurrence of <tag>text</tag>.
func extractFirstXMLTag(body, tag string) string {
	openTag := "<" + tag + ">"
	closeTag := "</" + tag + ">"
	idx := strings.Index(body, openTag)
	if idx < 0 {
		return ""
	}
	start := idx + len(openTag)
	end := strings.Index(body[start:], closeTag)
	if end < 0 {
		return ""
	}

	return body[start : start+end]
}

// ---- SimulatePrincipalPolicy with ConditionContext (backend) ----

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

// ---- SimulatePrincipalPolicy ContextEntries via HTTP handler ----

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
