package iam_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/iam"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

func TestHandler_GetTags_WithExistingTags(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)

	_, err := b.CreateRole("tagged-role", "/", "{}", "")
	require.NoError(t, err)

	// Tag the role via HTTP
	rec := httptest.NewRecorder()
	params := map[string]string{
		"RoleName":            "tagged-role",
		"Tags.member.1.Key":   "env",
		"Tags.member.1.Value": "prod",
	}
	req := iamRequest("TagRole", params)
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	// ListRoleTags should return the tag (exercises getTags non-nil branch)
	rec2 := httptest.NewRecorder()
	req2 := iamRequest("ListRoleTags", map[string]string{"RoleName": "tagged-role"})
	require.NoError(t, h.Handler()(e.NewContext(req2, rec2)))
	assert.Equal(t, http.StatusOK, rec2.Code)
	assert.Contains(t, rec2.Body.String(), "env")
}

func TestIAMHandler_TagAndList(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		setup            func(*iam.InMemoryBackend) string
		tagAction        string
		tagParams        func(id string) map[string]string
		wantTagResp      string
		listAction       string
		listParams       func(id string) map[string]string
		wantListContains []string
	}{
		{
			name: "role",
			setup: func(b *iam.InMemoryBackend) string {
				_, _ = b.CreateRole(
					"MyRole",
					"/",
					`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
					"",
				)

				return "MyRole"
			},
			tagAction: "TagRole",
			tagParams: func(id string) map[string]string {
				return map[string]string{
					"RoleName":            id,
					"Tags.member.1.Key":   "env",
					"Tags.member.1.Value": "prod",
					"Tags.member.2.Key":   "team",
					"Tags.member.2.Value": "platform",
				}
			},
			wantTagResp:      "TagRoleResponse",
			listAction:       "ListRoleTags",
			listParams:       func(id string) map[string]string { return map[string]string{"RoleName": id} },
			wantListContains: []string{"env", "prod"},
		},
		{
			name: "user",
			setup: func(b *iam.InMemoryBackend) string {
				_, _ = b.CreateUser("alice", "/", "")

				return "alice"
			},
			tagAction: "TagUser",
			tagParams: func(id string) map[string]string {
				return map[string]string{
					"UserName":            id,
					"Tags.member.1.Key":   "dept",
					"Tags.member.1.Value": "engineering",
				}
			},
			wantTagResp:      "TagUserResponse",
			listAction:       "ListUserTags",
			listParams:       func(id string) map[string]string { return map[string]string{"UserName": id} },
			wantListContains: []string{"dept", "engineering"},
		},
		{
			name: "policy",
			setup: func(b *iam.InMemoryBackend) string {
				pol, _ := b.CreatePolicy(
					"MyPolicy",
					"/",
					`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
				)

				return pol.Arn
			},
			tagAction: "TagPolicy",
			tagParams: func(id string) map[string]string {
				return map[string]string{
					"PolicyArn":           id,
					"Tags.member.1.Key":   "env",
					"Tags.member.1.Value": "staging",
					"Tags.member.2.Key":   "owner",
					"Tags.member.2.Value": "platform",
				}
			},
			wantTagResp:      "TagPolicyResponse",
			listAction:       "ListPolicyTags",
			listParams:       func(id string) map[string]string { return map[string]string{"PolicyArn": id} },
			wantListContains: []string{"ListPolicyTagsResponse", "env", "staging"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e := echo.New()
			h, b := newTestHandler(t)
			id := tt.setup(b)

			req := iamRequest(tt.tagAction, tt.tagParams(id))
			rec := httptest.NewRecorder()
			err := h.Handler()(e.NewContext(req, rec))
			require.NoError(t, err)
			assert.Equal(t, http.StatusOK, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantTagResp)

			req = iamRequest(tt.listAction, tt.listParams(id))
			rec = httptest.NewRecorder()
			err = h.Handler()(e.NewContext(req, rec))
			require.NoError(t, err)
			assert.Equal(t, http.StatusOK, rec.Code)
			for _, want := range tt.wantListContains {
				assert.Contains(t, rec.Body.String(), want)
			}
		})
	}
}

func TestIAMHandler_UntagAndVerify(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		setup          func(*iam.InMemoryBackend) string
		tagAction      string
		tagParams      func(id string) map[string]string
		untagAction    string
		untagParams    func(id string) map[string]string
		wantUntagResp  string
		listAction     string
		listParams     func(id string) map[string]string
		wantListAbsent []string
	}{
		{
			name: "role",
			setup: func(b *iam.InMemoryBackend) string {
				_, _ = b.CreateRole(
					"MyRole",
					"/",
					`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
					"",
				)

				return "MyRole"
			},
			tagAction: "TagRole",
			tagParams: func(id string) map[string]string {
				return map[string]string{
					"RoleName":            id,
					"Tags.member.1.Key":   "env",
					"Tags.member.1.Value": "prod",
				}
			},
			untagAction: "UntagRole",
			untagParams: func(id string) map[string]string {
				return map[string]string{"RoleName": id, "TagKeys.member.1": "env"}
			},
			wantUntagResp:  "UntagRoleResponse",
			listAction:     "ListRoleTags",
			listParams:     func(id string) map[string]string { return map[string]string{"RoleName": id} },
			wantListAbsent: []string{"env"},
		},
		{
			name: "user",
			setup: func(b *iam.InMemoryBackend) string {
				_, _ = b.CreateUser("alice", "/", "")

				return "alice"
			},
			tagAction: "TagUser",
			tagParams: func(id string) map[string]string {
				return map[string]string{
					"UserName":            id,
					"Tags.member.1.Key":   "dept",
					"Tags.member.1.Value": "engineering",
				}
			},
			untagAction: "UntagUser",
			untagParams: func(id string) map[string]string {
				return map[string]string{"UserName": id, "TagKeys.member.1": "dept"}
			},
			wantUntagResp:  "UntagUserResponse",
			listAction:     "ListUserTags",
			listParams:     func(id string) map[string]string { return map[string]string{"UserName": id} },
			wantListAbsent: []string{"dept"},
		},
		{
			name: "policy",
			setup: func(b *iam.InMemoryBackend) string {
				pol, _ := b.CreatePolicy(
					"MyPolicy",
					"/",
					`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
				)

				return pol.Arn
			},
			tagAction: "TagPolicy",
			tagParams: func(id string) map[string]string {
				return map[string]string{
					"PolicyArn":           id,
					"Tags.member.1.Key":   "env",
					"Tags.member.1.Value": "prod",
				}
			},
			untagAction: "UntagPolicy",
			untagParams: func(id string) map[string]string {
				return map[string]string{"PolicyArn": id, "TagKeys.member.1": "env"}
			},
			wantUntagResp:  "UntagPolicyResponse",
			listAction:     "ListPolicyTags",
			listParams:     func(id string) map[string]string { return map[string]string{"PolicyArn": id} },
			wantListAbsent: []string{"env"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e := echo.New()
			h, b := newTestHandler(t)
			id := tt.setup(b)

			req := iamRequest(tt.tagAction, tt.tagParams(id))
			rec := httptest.NewRecorder()
			err := h.Handler()(e.NewContext(req, rec))
			require.NoError(t, err)

			req = iamRequest(tt.untagAction, tt.untagParams(id))
			rec = httptest.NewRecorder()
			err = h.Handler()(e.NewContext(req, rec))
			require.NoError(t, err)
			assert.Equal(t, http.StatusOK, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantUntagResp)

			req = iamRequest(tt.listAction, tt.listParams(id))
			rec = httptest.NewRecorder()
			err = h.Handler()(e.NewContext(req, rec))
			require.NoError(t, err)
			for _, absent := range tt.wantListAbsent {
				assert.NotContains(t, rec.Body.String(), absent)
			}
		})
	}
}

// TestHandler_DeleteResource_ClearsHandlerLevelTags verifies that deleting a
// taggable resource whose tags live only on the Handler (instance profiles,
// MFA devices, SAML/OIDC providers, server certificates — see
// resourceTagDispatch) also clears its tag entry. Without this, re-creating a
// resource with the same name/ID after deletion would resurrect the old
// resource's tags, since the Handler-level tag map is keyed by name/ARN and
// outlives the backend entity.
func TestHandler_DeleteResource_ClearsHandlerLevelTags(t *testing.T) {
	t.Parallel()

	t.Run("instance_profile", func(t *testing.T) {
		t.Parallel()

		h, b := newTestHandler(t)
		_, err := b.CreateInstanceProfile("MyProfile", "/")
		require.NoError(t, err)

		callIAM(t, h, "TagInstanceProfile", map[string]string{
			"InstanceProfileName": "MyProfile",
			"Tags.member.1.Key":   "k",
			"Tags.member.1.Value": "v",
		})

		rec := callIAM(t, h, "DeleteInstanceProfile", map[string]string{"InstanceProfileName": "MyProfile"})
		require.Equal(t, http.StatusOK, rec.Code)

		_, err = b.CreateInstanceProfile("MyProfile", "/")
		require.NoError(t, err)

		rec = callIAM(t, h, "ListInstanceProfileTags", map[string]string{"InstanceProfileName": "MyProfile"})
		require.Equal(t, http.StatusOK, rec.Code)
		assert.NotContains(
			t, rec.Body.String(), "<Key>k</Key>",
			"recreated resource must not inherit deleted resource's tags",
		)
	})

	t.Run("virtual_mfa_device", func(t *testing.T) {
		t.Parallel()

		h, b := newTestHandler(t)
		dev, err := b.CreateVirtualMFADevice("MyDevice", "/")
		require.NoError(t, err)

		callIAM(t, h, "TagMFADevice", map[string]string{
			"SerialNumber":        dev.SerialNumber,
			"Tags.member.1.Key":   "k",
			"Tags.member.1.Value": "v",
		})

		rec := callIAM(t, h, "DeleteVirtualMFADevice", map[string]string{"SerialNumber": dev.SerialNumber})
		require.Equal(t, http.StatusOK, rec.Code)

		_, err = b.CreateVirtualMFADevice("MyDevice", "/")
		require.NoError(t, err)

		rec = callIAM(t, h, "ListMFADeviceTags", map[string]string{"SerialNumber": dev.SerialNumber})
		require.Equal(t, http.StatusOK, rec.Code)
		assert.NotContains(
			t, rec.Body.String(), "<Key>k</Key>",
			"recreated device must not inherit deleted device's tags",
		)
	})
}

// TestHandler_UpdateServerCertificate_MovesHandlerLevelTags verifies that
// renaming a server certificate (NewServerCertificateName) carries its
// Handler-level tags forward under the new name instead of orphaning them.
func TestHandler_UpdateServerCertificate_MovesHandlerLevelTags(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)
	_, err := b.UploadServerCertificate("OldCert", "/", "body", "")
	require.NoError(t, err)

	callIAM(t, h, "TagServerCertificate", map[string]string{
		"ServerCertificateName": "OldCert",
		"Tags.member.1.Key":     "k",
		"Tags.member.1.Value":   "v",
	})

	rec := callIAM(t, h, "UpdateServerCertificate", map[string]string{
		"ServerCertificateName":    "OldCert",
		"NewServerCertificateName": "NewCert",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = callIAM(t, h, "ListServerCertificateTags", map[string]string{"ServerCertificateName": "NewCert"})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "<Key>k</Key>", "tags must follow the certificate under its new name")
}
