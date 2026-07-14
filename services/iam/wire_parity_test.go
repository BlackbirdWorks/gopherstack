package iam_test

import (
	"context"
	"encoding/xml"
	"maps"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iam"
)

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

// Test_ListInstanceProfilesForRole verifies ListInstanceProfilesForRole
// returns the real instance profiles containing the given role, instead of
// the previous hardcoded-empty stub response (which silently discarded the
// RoleName parameter and never touched backend state).
func Test_ListInstanceProfilesForRole(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(t *testing.T, b *iam.InMemoryBackend)
		name        string
		roleName    string
		wantProfile string
		wantCode    int
		wantEmpty   bool
	}{
		{
			name: "role_with_one_profile",
			setup: func(t *testing.T, b *iam.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateRole("MyRole", "/", "{}", "")
				require.NoError(t, err)
				_, err = b.CreateInstanceProfile("MyProfile", "/")
				require.NoError(t, err)
				require.NoError(t, b.AddRoleToInstanceProfile("MyProfile", "MyRole"))
			},
			roleName:    "MyRole",
			wantCode:    http.StatusOK,
			wantProfile: "MyProfile",
		},
		{
			name: "role_with_no_profiles",
			setup: func(t *testing.T, b *iam.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateRole("LonelyRole", "/", "{}", "")
				require.NoError(t, err)
			},
			roleName:  "LonelyRole",
			wantCode:  http.StatusOK,
			wantEmpty: true,
		},
		{
			name:     "role_not_found",
			setup:    func(_ *testing.T, _ *iam.InMemoryBackend) {},
			roleName: "ghost",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			h, b := newTestHandler(t)
			tt.setup(t, b)

			req := iamRequest("ListInstanceProfilesForRole", map[string]string{"RoleName": tt.roleName})
			rec := httptest.NewRecorder()
			require.NoError(t, h.Handler()(e.NewContext(req, rec)))
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantProfile != "" {
				assert.Contains(t, rec.Body.String(), tt.wantProfile)
			}

			if tt.wantEmpty {
				assert.Contains(t, rec.Body.String(), "<InstanceProfiles></InstanceProfiles>")
			}
		})
	}
}

// Test_GetAccountAuthorizationDetails_PolicyVersions verifies that
// GetAccountAuthorizationDetails reports every real stored version of a
// managed policy (via StorageBackend.ListPolicyVersions), not a single
// hardcoded fake "v1" entry that ignored CreatePolicyVersion/SetDefaultPolicyVersion
// state entirely.
func Test_GetAccountAuthorizationDetails_PolicyVersions(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)

	pol, err := b.CreatePolicy(
		"MultiVersionPolicy", "/",
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
	)
	require.NoError(t, err)

	v2, err := b.CreatePolicyVersion(
		pol.Arn,
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}`,
		true, // set as default
	)
	require.NoError(t, err)

	req := iamRequest("GetAccountAuthorizationDetails", nil)
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	require.Equal(t, http.StatusOK, rec.Code)

	var resp iam.GetAccountAuthorizationDetailsResponse
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))

	require.Len(t, resp.GetAccountAuthorizationDetailsResult.Policies, 1)
	managed := resp.GetAccountAuthorizationDetailsResult.Policies[0]

	require.Len(t, managed.PolicyVersionList, 2, "must report both v1 and v2, not a fake single version")

	byVersion := map[string]iam.PolicyVersionXML{}
	for _, v := range managed.PolicyVersionList {
		byVersion[v.VersionID] = v
	}

	v1XML, ok := byVersion["v1"]
	require.True(t, ok, "v1 must be present")
	assert.False(t, v1XML.IsDefaultVersion, "v1 is no longer default after SetAsDefault on v2")

	v2XML, ok := byVersion[v2.VersionID]
	require.True(t, ok, "%s must be present", v2.VersionID)
	assert.True(t, v2XML.IsDefaultVersion)
	// Document must be percent-encoded, matching GetPolicyVersion's documented behavior.
	assert.Contains(t, v2XML.Document, "s3%3A%2A")
}

// Test_GetAccountAuthorizationDetails_InstanceProfileList verifies that
// RoleDetail.InstanceProfileList reports every instance profile containing the
// role, matching the real IAM RoleDetail shape
// (https://docs.aws.amazon.com/IAM/latest/APIReference/API_RoleDetail.html).
// Previously RoleDetailXML had no InstanceProfileList field at all, so this
// element was always absent from the wire response regardless of backend state.
func Test_GetAccountAuthorizationDetails_InstanceProfileList(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup             func(t *testing.T, b *iam.InMemoryBackend)
		name              string
		roleName          string
		wantProfileNames  []string
		wantProfileRoleID bool
	}{
		{
			name: "role_with_one_profile",
			setup: func(t *testing.T, b *iam.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateRole("MyRole", "/", "{}", "")
				require.NoError(t, err)
				_, err = b.CreateInstanceProfile("MyProfile", "/")
				require.NoError(t, err)
				require.NoError(t, b.AddRoleToInstanceProfile("MyProfile", "MyRole"))
			},
			roleName:          "MyRole",
			wantProfileNames:  []string{"MyProfile"},
			wantProfileRoleID: true,
		},
		{
			name: "role_with_two_profiles_sorted",
			setup: func(t *testing.T, b *iam.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateRole("SharedRole", "/", "{}", "")
				require.NoError(t, err)
				_, err = b.CreateInstanceProfile("ZProfile", "/")
				require.NoError(t, err)
				_, err = b.CreateInstanceProfile("AProfile", "/")
				require.NoError(t, err)
				require.NoError(t, b.AddRoleToInstanceProfile("ZProfile", "SharedRole"))
				require.NoError(t, b.AddRoleToInstanceProfile("AProfile", "SharedRole"))
			},
			roleName:         "SharedRole",
			wantProfileNames: []string{"AProfile", "ZProfile"},
		},
		{
			name: "role_with_no_profiles",
			setup: func(t *testing.T, b *iam.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateRole("LonelyRole", "/", "{}", "")
				require.NoError(t, err)
			},
			roleName:         "LonelyRole",
			wantProfileNames: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			h, b := newTestHandler(t)
			tt.setup(t, b)

			req := iamRequest("GetAccountAuthorizationDetails", nil)
			rec := httptest.NewRecorder()
			require.NoError(t, h.Handler()(e.NewContext(req, rec)))
			require.Equal(t, http.StatusOK, rec.Code)

			var resp iam.GetAccountAuthorizationDetailsResponse
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))

			var role *iam.RoleDetailXML
			for i := range resp.GetAccountAuthorizationDetailsResult.RoleDetailList {
				if resp.GetAccountAuthorizationDetailsResult.RoleDetailList[i].RoleName == tt.roleName {
					role = &resp.GetAccountAuthorizationDetailsResult.RoleDetailList[i]
				}
			}
			require.NotNil(t, role, "role %q must be present in RoleDetailList", tt.roleName)

			gotNames := make([]string, 0, len(role.InstanceProfileList))
			for _, ip := range role.InstanceProfileList {
				gotNames = append(gotNames, ip.InstanceProfileName)
			}
			assert.Equal(t, tt.wantProfileNames, gotNames)

			if tt.wantProfileRoleID {
				require.Len(t, role.InstanceProfileList, 1)
				require.Len(t, role.InstanceProfileList[0].Roles, 1,
					"instance profile must report the full nested role, not just its name")
				assert.Equal(t, tt.roleName, role.InstanceProfileList[0].Roles[0].RoleName)
				assert.NotEmpty(t, role.InstanceProfileList[0].Roles[0].RoleID,
					"nested role must be resolved to the full RoleXML, not a bare-name placeholder")
			}
		})
	}
}

// Test_HandlerPersistence_RoundTrip verifies that a Handler.Snapshot/Restore
// cycle preserves state that previously lived outside backendSnapshot:
// Handler-level resource tags (instance profiles, MFA devices, SAML/OIDC
// providers, server certificates) and comprehensiveBackend state (SSH public
// keys). Both were silently dropped by every persistence restore before this
// fix.
func Test_HandlerPersistence_RoundTrip(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := echo.New()
	h1, b1 := newTestHandler(t)

	_, err := b1.CreateInstanceProfile("PersistedProfile", "/")
	require.NoError(t, err)

	tagReq := iamRequest("TagInstanceProfile", map[string]string{
		"InstanceProfileName": "PersistedProfile",
		"Tags.member.1.Key":   "Team",
		"Tags.member.1.Value": "Platform",
	})
	tagRec := httptest.NewRecorder()
	require.NoError(t, h1.Handler()(e.NewContext(tagReq, tagRec)))
	require.Equal(t, http.StatusOK, tagRec.Code)

	_, err = b1.CreateUser("keyholder", "/", "")
	require.NoError(t, err)
	_, err = b1.UploadSSHPublicKey("keyholder", "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQDPersistedKeyBody")
	require.NoError(t, err)

	blob := h1.Snapshot(ctx)
	require.NotEmpty(t, blob)

	// Restore into a brand-new handler/backend pair.
	h2, _ := newTestHandler(t)
	require.NoError(t, h2.Restore(ctx, blob))

	// Instance profile tags must have round-tripped.
	listTagsReq := iamRequest("ListInstanceProfileTags", map[string]string{
		"InstanceProfileName": "PersistedProfile",
	})
	listTagsRec := httptest.NewRecorder()
	require.NoError(t, h2.Handler()(e.NewContext(listTagsReq, listTagsRec)))
	assert.Equal(t, http.StatusOK, listTagsRec.Code)
	assert.Contains(t, listTagsRec.Body.String(), "Platform")

	// SSH public keys (comprehensiveBackend state) must have round-tripped.
	listKeysReq := iamRequest("ListSSHPublicKeys", map[string]string{"UserName": "keyholder"})
	listKeysRec := httptest.NewRecorder()
	require.NoError(t, h2.Handler()(e.NewContext(listKeysReq, listKeysRec)))
	assert.Equal(t, http.StatusOK, listKeysRec.Code)
	assert.Contains(t, listKeysRec.Body.String(), "keyholder")
}

// Test_ErrorHTTPStatusCodes verifies each IAM error sentinel maps to the
// exact HTTP status code AWS documents per-operation (e.g. NoSuchEntity=404,
// EntityAlreadyExists=409), not a blanket 400. See e.g.
// https://docs.aws.amazon.com/IAM/latest/APIReference/API_CreateUser.html#API_CreateUser_Errors
// and https://docs.aws.amazon.com/IAM/latest/APIReference/API_DeleteUser.html#API_DeleteUser_Errors.
func Test_ErrorHTTPStatusCodes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, b *iam.InMemoryBackend)
		params     map[string]string
		name       string
		action     string
		wantCode   string
		wantStatus int
	}{
		{
			name:       "NoSuchEntity_is_404",
			action:     "GetUser",
			params:     map[string]string{"UserName": "ghost"},
			setup:      func(_ *testing.T, _ *iam.InMemoryBackend) {},
			wantCode:   "NoSuchEntity",
			wantStatus: http.StatusNotFound,
		},
		{
			name:   "EntityAlreadyExists_is_409",
			action: "CreateUser",
			params: map[string]string{"UserName": "dup"},
			setup: func(t *testing.T, b *iam.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateUser("dup", "/", "")
				require.NoError(t, err)
			},
			wantCode:   "EntityAlreadyExists",
			wantStatus: http.StatusConflict,
		},
		{
			name:   "DeleteConflict_is_409",
			action: "DeleteUser",
			params: map[string]string{"UserName": "attached"},
			setup: func(t *testing.T, b *iam.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateUser("attached", "/", "")
				require.NoError(t, err)
				pol, err := b.CreatePolicy(
					"StuckPolicy", "/",
					`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
				)
				require.NoError(t, err)
				require.NoError(t, b.AttachUserPolicy("attached", pol.Arn))
			},
			wantCode:   "DeleteConflict",
			wantStatus: http.StatusConflict,
		},
		{
			name:   "LimitExceeded_is_409",
			action: "CreateAccessKey",
			params: map[string]string{"UserName": "keyed"},
			setup: func(t *testing.T, b *iam.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateUser("keyed", "/", "")
				require.NoError(t, err)
				_, err = b.CreateAccessKey("keyed")
				require.NoError(t, err)
				_, err = b.CreateAccessKey("keyed")
				require.NoError(t, err)
			},
			wantCode:   "LimitExceeded",
			wantStatus: http.StatusConflict,
		},
		{
			name:       "MalformedPolicyDocument_is_400",
			action:     "CreatePolicy",
			params:     map[string]string{"PolicyName": "Bad", "PolicyDocument": "not-json"},
			setup:      func(_ *testing.T, _ *iam.InMemoryBackend) {},
			wantCode:   "MalformedPolicyDocument",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			h, b := newTestHandler(t)
			tt.setup(t, b)

			req := iamRequest(tt.action, tt.params)
			rec := httptest.NewRecorder()
			require.NoError(t, h.Handler()(e.NewContext(req, rec)))
			assert.Equal(t, tt.wantStatus, rec.Code)

			var errResp iam.ErrorResponse
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
			assert.Equal(t, tt.wantCode, errResp.Error.Code)
		})
	}
}

// Test_CreateAcceptsTagsAtCreation verifies that CreateUser, CreateRole, and
// CreatePolicy honor an optional Tags.member.N parameter at creation time, per
// real AWS IAM (e.g. CreateRole: "A list of tags that you want to attach to
// the new role"). Previously the parameter was silently accepted and dropped
// — the entity was created untagged and callers had to issue a separate
// Tag* call. CreateGroup deliberately does NOT support Tags at creation
// (real AWS CreateGroup has no Tags parameter), so it is not covered here.
func Test_CreateAcceptsTagsAtCreation(t *testing.T) {
	t.Parallel()

	tagParams := map[string]string{
		"Tags.member.1.Key":   "Team",
		"Tags.member.1.Value": "Platform",
	}

	tests := []struct {
		params      map[string]string
		name        string
		action      string
		wantContain string
	}{
		{
			name:   "CreateUser_with_tags",
			action: "CreateUser",
			params: mergeParams(map[string]string{"UserName": "tagged-user"}, tagParams),
		},
		{
			name:   "CreateRole_with_tags",
			action: "CreateRole",
			params: mergeParams(map[string]string{
				"RoleName":                 "tagged-role",
				"AssumeRolePolicyDocument": "{}",
			}, tagParams),
		},
		{
			name:   "CreatePolicy_with_tags",
			action: "CreatePolicy",
			params: mergeParams(map[string]string{
				"PolicyName": "tagged-policy",
				"PolicyDocument": `{"Version":"2012-10-17",` +
					`"Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
			}, tagParams),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			h, _ := newTestHandler(t)

			req := iamRequest(tt.action, tt.params)
			rec := httptest.NewRecorder()
			require.NoError(t, h.Handler()(e.NewContext(req, rec)))
			require.Equal(t, http.StatusOK, rec.Code)

			body := rec.Body.String()
			assert.Contains(t, body, "<Key>Team</Key>")
			assert.Contains(t, body, "<Value>Platform</Value>")
		})
	}
}

// mergeParams returns a new map containing all entries of base and extra.
func mergeParams(base, extra map[string]string) map[string]string {
	out := make(map[string]string, len(base)+len(extra))
	maps.Copy(out, base)

	maps.Copy(out, extra)

	return out
}
