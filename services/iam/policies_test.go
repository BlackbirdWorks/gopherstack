package iam_test

import (
	"net/http"
	"strings"
	"testing"
	"unicode"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iam"
)

// TestDetachUserPolicy covers DetachUserPolicy.
func TestDetachUserPolicy(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	_, err := b.CreateUser("detach-policy-user", "/", "")
	require.NoError(t, err)

	policyARN := "arn:aws:iam::000000000000:policy/MyPolicy"
	require.NoError(t, b.AttachUserPolicy("detach-policy-user", policyARN))

	// Detach it.
	rec := callIAM(t, h, "DetachUserPolicy", map[string]string{
		"UserName":  "detach-policy-user",
		"PolicyArn": policyARN,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DetachUserPolicyResponse")
}

// TestPolicyVersionManagement tests SetDefaultPolicyVersion and DeletePolicyVersion.
func TestPolicyVersionManagement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		action    string // "set_default", "delete"
		versionID string
		wantErr   bool
	}{
		{
			name:      "set_default_v2",
			action:    "set_default",
			versionID: "v2",
		},
		{
			name:      "delete_non_default_v2",
			action:    "delete",
			versionID: "v2",
		},
		{
			name:      "delete_default_v1_fails",
			action:    "delete",
			versionID: "v1",
			wantErr:   true,
		},
		{
			name:      "set_default_nonexistent",
			action:    "set_default",
			versionID: "v99",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			pol, err := b.CreatePolicy(
				"MyPolicy",
				"/",
				`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
			)
			require.NoError(t, err)

			_, err = b.CreatePolicyVersion(
				pol.Arn,
				`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
				false,
			)
			require.NoError(t, err)

			switch tt.action {
			case "set_default":
				err = b.SetDefaultPolicyVersion(pol.Arn, tt.versionID)
			case "delete":
				err = b.DeletePolicyVersion(pol.Arn, tt.versionID)
			}

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestListEntitiesForPolicy_Backend tests ListEntitiesForPolicy.
func TestListEntitiesForPolicy_Backend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(*iam.InMemoryBackend, string)
		filter     string
		wantUsers  int
		wantGroups int
		wantRoles  int
		wantErr    bool
	}{
		{
			name: "all_entities",
			setup: func(b *iam.InMemoryBackend, policyArn string) {
				_, _ = b.CreateUser("alice", "/", "")
				_, _ = b.CreateGroup("Admins", "/")
				_, _ = b.CreateRole(
					"DevRole",
					"/",
					`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
					"",
				)
				_ = b.AttachUserPolicy("alice", policyArn)
				_ = b.AttachGroupPolicy("Admins", policyArn)
				_ = b.AttachRolePolicy("DevRole", policyArn)
			},
			wantUsers:  1,
			wantGroups: 1,
			wantRoles:  1,
		},
		{
			name: "filter_user",
			setup: func(b *iam.InMemoryBackend, policyArn string) {
				_, _ = b.CreateUser("alice", "/", "")
				_ = b.AttachUserPolicy("alice", policyArn)
			},
			filter:    "User",
			wantUsers: 1,
		},
		{
			name:    "nonexistent_policy",
			setup:   func(_ *iam.InMemoryBackend, _ string) {},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			var policyArn string

			if !tt.wantErr {
				pol, err := b.CreatePolicy(
					"TestPolicy",
					"/",
					`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
				)
				require.NoError(t, err)
				policyArn = pol.Arn
			} else {
				policyArn = "arn:aws:iam::000000000000:policy/ghost"
			}

			tt.setup(b, policyArn)

			entities, err := b.ListEntitiesForPolicy(policyArn, tt.filter)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Len(t, entities.PolicyUsers, tt.wantUsers)
			assert.Len(t, entities.PolicyGroups, tt.wantGroups)
			assert.Len(t, entities.PolicyRoles, tt.wantRoles)
		})
	}
}

func TestTagPolicy_StoresOnModel(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	pol, err := b.CreatePolicy(
		"MyPol",
		"/",
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
	)
	require.NoError(t, err)

	require.NoError(t, b.TagPolicy(pol.Arn, map[string]string{"owner": "infra"}))

	p, err := b.GetPolicy(pol.Arn)
	require.NoError(t, err)
	assert.Equal(t, "infra", p.Tags["owner"])
}

func TestTagPolicy_NotFoundError(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	err := b.TagPolicy("arn:aws:iam::000000000000:policy/Ghost", map[string]string{"k": "v"})
	require.Error(t, err)
	assert.ErrorIs(t, err, iam.ErrPolicyNotFound)
}

func TestUntagPolicy_RemovesKeys(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	pol, _ := b.CreatePolicy(
		"P1",
		"/",
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
	)
	require.NoError(t, b.TagPolicy(pol.Arn, map[string]string{"a": "1", "b": "2"}))
	require.NoError(t, b.UntagPolicy(pol.Arn, []string{"a"}))

	p, err := b.GetPolicy(pol.Arn)
	require.NoError(t, err)
	assert.Empty(t, p.Tags["a"])
	assert.Equal(t, "2", p.Tags["b"])
}

const allowS3GetDoc = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`

func TestGetPolicyVersion_VersionIdMatchesRequested(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		requestedVersion string
		wantVersionID    string
		wantIsDefault    bool
	}{
		{"v1 is default", "v1", "v1", true},
		{"v2 non-default", "v2", "v2", false},
		{"v2 set as default", "v2", "v2", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
			p, err := b.CreatePolicy("VersionIdPolicy-"+tc.name, "/", doc)
			require.NoError(t, err)

			doc2 := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}`
			_, err = b.CreatePolicyVersion(p.Arn, doc2, false)
			require.NoError(t, err)

			if tc.name == "v2 set as default" {
				require.NoError(t, b.SetDefaultPolicyVersion(p.Arn, "v2"))
			}

			pv, err := b.GetPolicyVersion(p.Arn, tc.requestedVersion)
			require.NoError(t, err)

			assert.Equal(t, tc.wantVersionID, pv.VersionID,
				"GetPolicyVersion must return VersionID matching the requested version, not hardcoded v1")
			assert.Equal(t, tc.wantIsDefault, pv.IsDefaultVersion,
				"IsDefaultVersion must reflect actual default state")
		})
	}
}

func TestPolicy_UpdateDateSetOnCreate(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
	p, err := b.CreatePolicy("UpdateDatePolicy", "/", doc)
	require.NoError(t, err)

	assert.False(t, p.UpdateDate.IsZero(), "UpdateDate must be set on CreatePolicy")
	assert.Equal(t, p.CreateDate, p.UpdateDate, "UpdateDate equals CreateDate on fresh policy")
}

func TestPolicy_DefaultVersionIdSetOnCreate(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
	p, err := b.CreatePolicy("DefaultVerPolicy", "/", doc)
	require.NoError(t, err)

	assert.Equal(t, "v1", p.DefaultVersionID,
		"DefaultVersionId must be v1 on freshly created policy")
}

func TestPolicy_IsAttachableTrueOnCreate(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
	p, err := b.CreatePolicy("IsAttachablePolicy", "/", doc)
	require.NoError(t, err)

	assert.True(t, p.IsAttachable, "customer-managed policy must be IsAttachable=true")
}

func TestPolicy_AttachmentCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(b *iam.InMemoryBackend, policyArn string)
		name  string
		want  int
	}{
		{
			name:  "zero on create",
			setup: func(_ *iam.InMemoryBackend, _ string) {},
			want:  0,
		},
		{
			name: "one after attach to user",
			setup: func(b *iam.InMemoryBackend, policyArn string) {
				_, _ = b.CreateUser("att-count-user", "/", "")
				_ = b.AttachUserPolicy("att-count-user", policyArn)
			},
			want: 1,
		},
		{
			name: "decrements after detach",
			setup: func(b *iam.InMemoryBackend, policyArn string) {
				_, _ = b.CreateUser("att-count-user2", "/", "")
				_ = b.AttachUserPolicy("att-count-user2", policyArn)
				_ = b.DetachUserPolicy("att-count-user2", policyArn)
			},
			want: 0,
		},
		{
			name: "counts user role and group",
			setup: func(b *iam.InMemoryBackend, policyArn string) {
				_, _ = b.CreateUser("att-count-u3", "/", "")
				_ = b.AttachUserPolicy("att-count-u3", policyArn)
				_, _ = b.CreateGroup("att-count-g3", "/")
				_ = b.AttachGroupPolicy("att-count-g3", policyArn)
				trust := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
					`"Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}`
				_, _ = b.CreateRole("att-count-r3", "/", trust, "")
				_ = b.AttachRolePolicy("att-count-r3", policyArn)
			},
			want: 3,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
			p, err := b.CreatePolicy("AttCountPolicy-"+tc.name, "/", doc)
			require.NoError(t, err)

			tc.setup(b, p.Arn)

			got, err := b.GetPolicy(p.Arn)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got.AttachmentCount,
				"AttachmentCount must reflect actual number of entities with this policy attached")
		})
	}
}

func TestPolicy_UpdateDateAdvancesOnNewDefault(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		via  string // "create" or "set"
	}{
		{"via CreatePolicyVersion setAsDefault", "create"},
		{"via SetDefaultPolicyVersion", "set"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
			p, err := b.CreatePolicy("UpdateDateAdv-"+tc.name, "/", doc)
			require.NoError(t, err)

			originalUpdateDate := p.UpdateDate

			doc2 := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}`

			switch tc.via {
			case "create":
				_, err = b.CreatePolicyVersion(p.Arn, doc2, true)
				require.NoError(t, err)
			case "set":
				_, err = b.CreatePolicyVersion(p.Arn, doc2, false)
				require.NoError(t, err)
				require.NoError(t, b.SetDefaultPolicyVersion(p.Arn, "v2"))
			}

			got, err := b.GetPolicy(p.Arn)
			require.NoError(t, err)

			assert.True(t, got.UpdateDate.After(originalUpdateDate) || got.UpdateDate.Equal(originalUpdateDate),
				"UpdateDate must advance when a new default version is set")
			assert.Equal(t, "v2", got.DefaultVersionID,
				"DefaultVersionId must reflect the new default version")
		})
	}
}

func TestPolicy_DefaultVersionIdAfterSetDefault(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
	p, err := b.CreatePolicy("DefVerAfterSet", "/", doc)
	require.NoError(t, err)

	doc2 := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}`
	_, err = b.CreatePolicyVersion(p.Arn, doc2, false)
	require.NoError(t, err)

	require.NoError(t, b.SetDefaultPolicyVersion(p.Arn, "v2"))

	got, err := b.GetPolicy(p.Arn)
	require.NoError(t, err)
	assert.Equal(t, "v2", got.DefaultVersionID)

	// Revert to v1.
	require.NoError(t, b.SetDefaultPolicyVersion(p.Arn, "v1"))

	got2, err := b.GetPolicy(p.Arn)
	require.NoError(t, err)
	assert.Equal(t, "v1", got2.DefaultVersionID, "DefaultVersionId reverts to v1")
}

func TestPolicyID_Format(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	p, err := b.CreatePolicy(
		"MyPolicy",
		"/",
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
	)
	require.NoError(t, err)

	// Policy IDs are ANPA + 16 uppercase alphanumeric chars.
	assert.True(t, strings.HasPrefix(p.PolicyID, "ANPA"), "policy ID must start with ANPA")
	assert.Len(t, p.PolicyID, 20, "policy ID must be 20 chars")
	assert.NotContains(t, p.PolicyID, "-", "policy ID must not contain dashes")

	for _, ch := range p.PolicyID {
		assert.True(t, unicode.IsUpper(ch) || unicode.IsDigit(ch),
			"policy ID char %q must be uppercase alphanumeric", ch)
	}
}

func TestCreatePolicyVersion_LimitExceededError(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
	p, err := b.CreatePolicy("P", "/", doc)
	require.NoError(t, err)

	// v1 is implicit; create v2–v5 to fill the 5-version limit.
	for range 4 {
		_, err = b.CreatePolicyVersion(p.Arn, doc, false)
		require.NoError(t, err)
	}

	// 6th version must fail with LimitExceeded (not DeleteConflict).
	_, err = b.CreatePolicyVersion(p.Arn, doc, false)
	require.Error(t, err)
	require.ErrorIs(t, err, iam.ErrLimitExceeded,
		"exceeding 5 policy versions must return LimitExceeded")
	assert.NotErrorIs(t, err, iam.ErrDeleteConflict,
		"policy version limit must NOT return DeleteConflict")
}

func TestCreatePolicyVersion_MonotonicVersionID(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
	p, err := b.CreatePolicy("Mono", "/", doc)
	require.NoError(t, err)

	v2, err := b.CreatePolicyVersion(p.Arn, doc, false)
	require.NoError(t, err)
	assert.Equal(t, "v2", v2.VersionID)

	v3, err := b.CreatePolicyVersion(p.Arn, doc, false)
	require.NoError(t, err)
	assert.Equal(t, "v3", v3.VersionID)

	// Delete v2; next version must be v4 (monotonic), not v3 (collision).
	require.NoError(t, b.DeletePolicyVersion(p.Arn, "v2"))

	v4, err := b.CreatePolicyVersion(p.Arn, doc, false)
	require.NoError(t, err)
	assert.Equal(t, "v4", v4.VersionID,
		"version ID must be monotonically increasing even after deletes")
}

func TestCreatePolicyVersion_V1CountsTowardLimit(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
	p, err := b.CreatePolicy("LimitTest", "/", doc)
	require.NoError(t, err)

	// v1 is implicit; only 4 more should succeed.
	for i := range 4 {
		_, err = b.CreatePolicyVersion(p.Arn, doc, false)
		require.NoError(t, err, "version %d should succeed", i+2)
	}

	// 5th new version (6th total) must fail.
	_, err = b.CreatePolicyVersion(p.Arn, doc, false)
	require.ErrorIs(t, err, iam.ErrLimitExceeded)
}

func TestSetDefaultPolicyVersion_UpdatesDefault(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	doc1 := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
	doc2 := `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"*","Resource":"*"}]}`

	p, err := b.CreatePolicy("DefaultTest", "/", doc1)
	require.NoError(t, err)

	v2, err := b.CreatePolicyVersion(p.Arn, doc2, false)
	require.NoError(t, err)

	require.NoError(t, b.SetDefaultPolicyVersion(p.Arn, v2.VersionID))

	versions, err := b.ListPolicyVersions(p.Arn)
	require.NoError(t, err)

	var defaultCount int
	for _, v := range versions {
		if v.IsDefaultVersion {
			defaultCount++
			assert.Equal(t, "v2", v.VersionID, "v2 must be the new default")
		}
	}

	assert.Equal(t, 1, defaultCount, "exactly one version must be default")
}

func TestDeletePolicyVersion_CannotDeleteDefault(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
	p, err := b.CreatePolicy("DelDefault", "/", doc)
	require.NoError(t, err)

	// v1 is the default; deleting it must fail.
	err = b.DeletePolicyVersion(p.Arn, "v1")
	require.Error(t, err, "deleting the default version must fail")
}

func TestPolicyARN_Format(t *testing.T) {
	t.Parallel()

	b := iam.NewInMemoryBackendWithConfig("123456789012")
	p, err := b.CreatePolicy(
		"MyPolicy",
		"/",
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
	)
	require.NoError(t, err)

	assert.Equal(t, "arn:aws:iam::123456789012:policy/MyPolicy", p.Arn)
}

func TestDeletePolicy_FailsWhenAttachedToUser(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
	_, _ = b.CreateUser("alice", "/", "")
	p, err := b.CreatePolicy("P", "/", doc)
	require.NoError(t, err)
	require.NoError(t, b.AttachUserPolicy("alice", p.Arn))

	err = b.DeletePolicy(p.Arn)
	require.Error(t, err)
	assert.ErrorIs(t, err, iam.ErrDeleteConflict)
}

func TestDeletePolicy_FailsWhenAttachedToRole(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
	_, _ = b.CreateRole("R", "/", doc, "")
	p, err := b.CreatePolicy("P", "/", doc)
	require.NoError(t, err)
	require.NoError(t, b.AttachRolePolicy("R", p.Arn))

	err = b.DeletePolicy(p.Arn)
	require.Error(t, err)
	assert.ErrorIs(t, err, iam.ErrDeleteConflict)
}

func TestDeletePolicy_SucceedsWhenDetached(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
	_, _ = b.CreateUser("alice", "/", "")
	p, err := b.CreatePolicy("P", "/", doc)
	require.NoError(t, err)
	require.NoError(t, b.AttachUserPolicy("alice", p.Arn))
	require.NoError(t, b.DetachUserPolicy("alice", p.Arn))
	require.NoError(t, b.DeletePolicy(p.Arn))
}

func TestCreatePolicyVersion_MonotonicAfterRestore(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
	p, err := b.CreatePolicy("SnapTest", "/", doc)
	require.NoError(t, err)

	v2, err := b.CreatePolicyVersion(p.Arn, doc, false)
	require.NoError(t, err)
	assert.Equal(t, "v2", v2.VersionID)

	// Snapshot and restore.
	snap := b.Snapshot(t.Context())
	b2 := iam.NewInMemoryBackend()
	require.NoError(t, b2.Restore(t.Context(), snap))

	// Next version must be v3, not v2.
	v3, err := b2.CreatePolicyVersion(p.Arn, doc, false)
	require.NoError(t, err)
	assert.Equal(t, "v3", v3.VersionID,
		"after restore, version counter must continue from where it left off")
}

func TestPermissionsBoundary_UserRoundTrip(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
	_, _ = b.CreateUser("alice", "/", "")
	p, _ := b.CreatePolicy("Boundary", "/", doc)

	require.NoError(t, b.PutUserPermissionsBoundary("alice", p.Arn))
	u, _ := b.GetUser("alice")
	assert.Equal(t, p.Arn, u.PermissionsBoundary)

	require.NoError(t, b.DeleteUserPermissionsBoundary("alice"))
	u2, _ := b.GetUser("alice")
	assert.Empty(t, u2.PermissionsBoundary)
}

func TestPermissionsBoundary_RoleRoundTrip(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
	_, _ = b.CreateRole("MyRole", "/", doc, "")
	p, _ := b.CreatePolicy("Boundary", "/", doc)

	require.NoError(t, b.PutRolePermissionsBoundary("MyRole", p.Arn))
	r, _ := b.GetRole("MyRole")
	assert.Equal(t, p.Arn, r.PermissionsBoundary)

	require.NoError(t, b.DeleteRolePermissionsBoundary("MyRole"))
	r2, _ := b.GetRole("MyRole")
	assert.Empty(t, r2.PermissionsBoundary)
}

func TestListEntitiesForPolicy_AllEntityTypes(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
	_, _ = b.CreateUser("alice", "/", "")
	_, _ = b.CreateRole("R", "/", doc, "")
	_, _ = b.CreateGroup("G", "/")
	p, _ := b.CreatePolicy("P", "/", doc)

	require.NoError(t, b.AttachUserPolicy("alice", p.Arn))
	require.NoError(t, b.AttachRolePolicy("R", p.Arn))
	require.NoError(t, b.AttachGroupPolicy("G", p.Arn))

	entities, err := b.ListEntitiesForPolicy(p.Arn, "")
	require.NoError(t, err)
	assert.Len(t, entities.PolicyUsers, 1)
	assert.Len(t, entities.PolicyRoles, 1)
	assert.Len(t, entities.PolicyGroups, 1)
}

func TestListEntitiesForPolicy_FilterByType(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
	_, _ = b.CreateUser("alice", "/", "")
	_, _ = b.CreateRole("R", "/", doc, "")
	p, _ := b.CreatePolicy("P", "/", doc)
	require.NoError(t, b.AttachUserPolicy("alice", p.Arn))
	require.NoError(t, b.AttachRolePolicy("R", p.Arn))

	users, err := b.ListEntitiesForPolicy(p.Arn, "User")
	require.NoError(t, err)
	assert.Len(t, users.PolicyUsers, 1)
	assert.Empty(t, users.PolicyRoles)

	roles, err := b.ListEntitiesForPolicy(p.Arn, "Role")
	require.NoError(t, err)
	assert.Len(t, roles.PolicyRoles, 1)
	assert.Empty(t, roles.PolicyUsers)
}

func TestInMemoryBackend_Policies(t *testing.T) {
	t.Parallel()

	t.Run("CreateAndListPolicy", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		pol, err := b.CreatePolicy(
			"MyPolicy",
			"/",
			`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
		)
		require.NoError(t, err)
		assert.Equal(t, "MyPolicy", pol.PolicyName)
		assert.NotEmpty(t, pol.Arn)

		policies, err := b.ListPolicies("", 0)
		require.NoError(t, err)
		require.Len(t, policies.Data, 1)
	})

	t.Run("CreatePolicyAlreadyExists", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, err := b.CreatePolicy("MyPolicy", "/", "")
		require.NoError(t, err)
		_, err = b.CreatePolicy("MyPolicy", "/", "")
		require.ErrorIs(t, err, iam.ErrPolicyAlreadyExists)
	})

	t.Run("DeletePolicy", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		pol, err := b.CreatePolicy("MyPolicy", "/", "")
		require.NoError(t, err)
		err = b.DeletePolicy(pol.Arn)
		require.NoError(t, err)
		policies, _ := b.ListPolicies("", 0)
		assert.Empty(t, policies.Data)
	})

	t.Run("DeletePolicyNotFound", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		err := b.DeletePolicy("arn:aws:iam::000000000000:policy/nonexistent")
		require.ErrorIs(t, err, iam.ErrPolicyNotFound)
	})

	t.Run("AttachUserPolicy", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, _ = b.CreateUser("alice", "/", "")
		err := b.AttachUserPolicy("alice", "arn:aws:iam::000000000000:policy/SomePolicy")
		require.NoError(t, err)
	})

	t.Run("AttachUserPolicyUserNotFound", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		err := b.AttachUserPolicy("nonexistent", "arn:aws:iam::000000000000:policy/SomePolicy")
		require.ErrorIs(t, err, iam.ErrUserNotFound)
	})

	t.Run("AttachRolePolicy", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, _ = b.CreateRole("MyRole", "/", "", "")
		err := b.AttachRolePolicy("MyRole", "arn:aws:iam::000000000000:policy/SomePolicy")
		require.NoError(t, err)
	})

	t.Run("AttachRolePolicyRoleNotFound", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		err := b.AttachRolePolicy("nonexistent", "arn:aws:iam::000000000000:policy/SomePolicy")
		require.ErrorIs(t, err, iam.ErrRoleNotFound)
	})

	t.Run("ListAllPolicies", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, _ = b.CreatePolicy("ZPolicy", "/", "")
		_, _ = b.CreatePolicy("APolicy", "/", "")
		policies := b.ListAllPolicies()
		require.Len(t, policies, 2)
		assert.Equal(t, "APolicy", policies[0].PolicyName)
	})
}

func TestInMemoryBackend_DetachRolePolicy(t *testing.T) {
	t.Parallel()

	t.Run("DetachExistingPolicy", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, _ = b.CreateRole("MyRole", "/", "", "")
		_ = b.AttachRolePolicy("MyRole", "arn:aws:iam::000000000000:policy/SomePolicy")
		err := b.DetachRolePolicy("MyRole", "arn:aws:iam::000000000000:policy/SomePolicy")
		require.NoError(t, err)

		policies, err := b.ListAttachedRolePolicies("MyRole")
		require.NoError(t, err)
		assert.Empty(t, policies)
	})

	t.Run("DetachNonExistentPolicy", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, _ = b.CreateRole("MyRole", "/", "", "")
		err := b.DetachRolePolicy("MyRole", "arn:aws:iam::000000000000:policy/NoSuchPolicy")
		require.NoError(t, err)
	})

	t.Run("DetachRolePolicyRoleNotFound", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		err := b.DetachRolePolicy("nonexistent", "arn:aws:iam::000000000000:policy/P")
		require.ErrorIs(t, err, iam.ErrRoleNotFound)
	})
}

const (
	// allowS3GetObjectPolicy is a minimal allow-S3-GetObject IAM policy for use in tests.
	allowS3GetObjectPolicy = `{"Version":"2012-10-17","Statement":[` +
		`{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`

	// allowS3WildcardPolicy is a minimal allow-all-S3 IAM policy for use in tests.
	allowS3WildcardPolicy = `{"Version":"2012-10-17","Statement":[` +
		`{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}`

	// denyAllPolicy is a minimal deny-all IAM policy for use in tests.
	denyAllPolicy = `{"Version":"2012-10-17","Statement":[` +
		`{"Effect":"Deny","Action":"*","Resource":"*"}]}`
)

func TestInMemoryBackend_PermissionsBoundaries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*iam.InMemoryBackend)
		name         string
		wantBoundary string
		wantErr      error
		action       string
	}{
		{
			name: "PutUserPermissionsBoundary",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("alice", "/", "")
			},
			action:       "put_user",
			wantBoundary: "arn:aws:iam::000000000000:policy/Boundary",
		},
		{
			name:    "PutUserPermissionsBoundary_UserNotFound",
			setup:   func(_ *iam.InMemoryBackend) {},
			action:  "put_user_notfound",
			wantErr: iam.ErrUserNotFound,
		},
		{
			name: "DeleteUserPermissionsBoundary",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("alice", "/", "arn:aws:iam::000000000000:policy/Boundary")
			},
			action: "delete_user",
		},
		{
			name: "PutRolePermissionsBoundary",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateRole("MyRole", "/", "{}", "")
			},
			action:       "put_role",
			wantBoundary: "arn:aws:iam::000000000000:policy/Boundary",
		},
		{
			name:    "PutRolePermissionsBoundary_RoleNotFound",
			setup:   func(_ *iam.InMemoryBackend) {},
			action:  "put_role_notfound",
			wantErr: iam.ErrRoleNotFound,
		},
		{
			name: "DeleteRolePermissionsBoundary",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateRole("MyRole", "/", "{}", "arn:aws:iam::000000000000:policy/Boundary")
			},
			action: "delete_role",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			tt.setup(b)

			switch tt.action {
			case "put_user":
				err := b.PutUserPermissionsBoundary("alice", tt.wantBoundary)
				require.NoError(t, err)

				u, err := b.GetUser("alice")
				require.NoError(t, err)
				assert.Equal(t, tt.wantBoundary, u.PermissionsBoundary)

			case "put_user_notfound":
				err := b.PutUserPermissionsBoundary("nobody", "arn:aws:iam::000000000000:policy/Boundary")
				require.ErrorIs(t, err, tt.wantErr)

			case "delete_user":
				err := b.DeleteUserPermissionsBoundary("alice")
				require.NoError(t, err)

				u, err := b.GetUser("alice")
				require.NoError(t, err)
				assert.Empty(t, u.PermissionsBoundary)

			case "put_role":
				err := b.PutRolePermissionsBoundary("MyRole", tt.wantBoundary)
				require.NoError(t, err)

				r, err := b.GetRole("MyRole")
				require.NoError(t, err)
				assert.Equal(t, tt.wantBoundary, r.PermissionsBoundary)

			case "put_role_notfound":
				err := b.PutRolePermissionsBoundary("Ghost", "arn:aws:iam::000000000000:policy/Boundary")
				require.ErrorIs(t, err, tt.wantErr)

			case "delete_role":
				err := b.DeleteRolePermissionsBoundary("MyRole")
				require.NoError(t, err)

				r, err := b.GetRole("MyRole")
				require.NoError(t, err)
				assert.Empty(t, r.PermissionsBoundary)
			}
		})
	}
}
