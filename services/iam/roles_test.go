package iam_test

import (
	"encoding/xml"
	"net/http"
	"strings"
	"testing"
	"unicode"

	"github.com/blackbirdworks/gopherstack/services/iam"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGetServiceLinkedRoleDeletionStatus covers GetServiceLinkedRoleDeletionStatus.
func TestGetServiceLinkedRoleDeletionStatus(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)

	rec := callIAM(t, h, "GetServiceLinkedRoleDeletionStatus", map[string]string{
		"DeletionTaskId": "task-123",
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var resp iam.GetServiceLinkedRoleDeletionStatusResponse
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "SUCCEEDED", resp.GetServiceLinkedRoleDeletionStatusResult.Status)
}

// TestUpdateRole_Backend tests UpdateRole.
func TestUpdateRole_Backend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		roleName    string
		description string
		wantErr     bool
	}{
		{
			name:        "update_description",
			roleName:    "MyRole",
			description: "A new description",
		},
		{
			name:     "role_not_found",
			roleName: "ghost",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			_, _ = b.CreateRole(
				"MyRole",
				"/",
				`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
				"",
			)

			err := b.UpdateRole(tt.roleName, tt.description)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			r, err := b.GetRole("MyRole")
			require.NoError(t, err)
			assert.Equal(t, tt.description, r.Description)
		})
	}
}

func TestTagRole_StoresOnModel(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, err := b.CreateRole(
		"MyRole",
		"/",
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
		"",
	)
	require.NoError(t, err)

	require.NoError(t, b.TagRole("MyRole", map[string]string{"dept": "eng"}))

	r, err := b.GetRole("MyRole")
	require.NoError(t, err)
	assert.Equal(t, "eng", r.Tags["dept"])
}

func TestTagRole_NotFoundError(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	err := b.TagRole("NoRole", map[string]string{"k": "v"})
	require.Error(t, err)
	assert.ErrorIs(t, err, iam.ErrRoleNotFound)
}

func TestUntagRole_RemovesKeys(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, err := b.CreateRole(
		"R2",
		"/",
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
		"",
	)
	require.NoError(t, err)
	require.NoError(t, b.TagRole("R2", map[string]string{"x": "1", "y": "2"}))
	require.NoError(t, b.UntagRole("R2", []string{"x"}))

	r, err := b.GetRole("R2")
	require.NoError(t, err)
	assert.Empty(t, r.Tags["x"])
	assert.Equal(t, "2", r.Tags["y"])
}

func TestEvaluateAssumeRoleTrustPolicy_AllowsMatchingPrincipal(t *testing.T) {
	t.Parallel()

	policy := `{
		"Version":"2012-10-17",
		"Statement":[{
			"Effect":"Allow",
			"Principal":{"AWS":"arn:aws:iam::111111111111:root"},
			"Action":"sts:AssumeRole"
		}]
	}`

	result := iam.EvaluateAssumeRoleTrustPolicy(
		policy, "arn:aws:iam::111111111111:root", "", false, iam.ConditionContext{},
	)
	assert.True(t, result)
}

func TestEvaluateAssumeRoleTrustPolicy_DeniesNonMatchingPrincipal(t *testing.T) {
	t.Parallel()

	policy := `{
		"Version":"2012-10-17",
		"Statement":[{
			"Effect":"Allow",
			"Principal":{"AWS":"arn:aws:iam::111111111111:root"},
			"Action":"sts:AssumeRole"
		}]
	}`

	result := iam.EvaluateAssumeRoleTrustPolicy(
		policy, "arn:aws:iam::999999999999:root", "", false, iam.ConditionContext{},
	)
	assert.False(t, result)
}

func TestEvaluateAssumeRoleTrustPolicy_WildcardPrincipalAllowsAll(t *testing.T) {
	t.Parallel()

	policy := `{
		"Version":"2012-10-17",
		"Statement":[{
			"Effect":"Allow",
			"Principal":"*",
			"Action":"sts:AssumeRole"
		}]
	}`

	result := iam.EvaluateAssumeRoleTrustPolicy(
		policy, "arn:aws:iam::anything:root", "", false, iam.ConditionContext{},
	)
	assert.True(t, result)
}

func TestEvaluateAssumeRoleTrustPolicy_ExternalIDRequired(t *testing.T) {
	t.Parallel()

	policy := `{
		"Version":"2012-10-17",
		"Statement":[{
			"Effect":"Allow",
			"Principal":{"AWS":"arn:aws:iam::111111111111:root"},
			"Action":"sts:AssumeRole",
			"Condition":{"StringEquals":{"sts:ExternalId":"secret-id"}}
		}]
	}`

	// Correct external ID.
	assert.True(t, iam.EvaluateAssumeRoleTrustPolicy(
		policy, "arn:aws:iam::111111111111:root", "secret-id", false, iam.ConditionContext{},
	))

	// Wrong external ID.
	assert.False(t, iam.EvaluateAssumeRoleTrustPolicy(
		policy, "arn:aws:iam::111111111111:root", "wrong-id", false, iam.ConditionContext{},
	))

	// Missing external ID.
	assert.False(t, iam.EvaluateAssumeRoleTrustPolicy(
		policy, "arn:aws:iam::111111111111:root", "", false, iam.ConditionContext{},
	))
}

func TestEvaluateAssumeRoleTrustPolicy_MFARequired(t *testing.T) {
	t.Parallel()

	policy := `{
		"Version":"2012-10-17",
		"Statement":[{
			"Effect":"Allow",
			"Principal":{"AWS":"arn:aws:iam::111111111111:user/alice"},
			"Action":"sts:AssumeRole",
			"Condition":{"Bool":{"aws:MultiFactorAuthPresent":"true"}}
		}]
	}`

	// MFA present → allowed.
	assert.True(t, iam.EvaluateAssumeRoleTrustPolicy(
		policy, "arn:aws:iam::111111111111:user/alice", "", true, iam.ConditionContext{},
	))

	// MFA absent → denied.
	assert.False(t, iam.EvaluateAssumeRoleTrustPolicy(
		policy, "arn:aws:iam::111111111111:user/alice", "", false, iam.ConditionContext{},
	))
}

func TestEvaluateAssumeRoleTrustPolicy_ServicePrincipal(t *testing.T) {
	t.Parallel()

	policy := `{
		"Version":"2012-10-17",
		"Statement":[{
			"Effect":"Allow",
			"Principal":{"Service":"lambda.amazonaws.com"},
			"Action":"sts:AssumeRole"
		}]
	}`

	assert.True(t, iam.EvaluateAssumeRoleTrustPolicy(
		policy, "lambda.amazonaws.com", "", false, iam.ConditionContext{},
	))
	assert.False(t, iam.EvaluateAssumeRoleTrustPolicy(
		policy, "ec2.amazonaws.com", "", false, iam.ConditionContext{},
	))
}

func TestEvaluateAssumeRoleTrustPolicy_EmptyPolicy(t *testing.T) {
	t.Parallel()

	result := iam.EvaluateAssumeRoleTrustPolicy("", "anything", "", false, iam.ConditionContext{})
	assert.False(t, result)
}

func TestEvaluateAssumeRoleTrustPolicy_MultipleStatements(t *testing.T) {
	t.Parallel()

	// Second statement allows a different principal.
	policy := `{
		"Version":"2012-10-17",
		"Statement":[
			{
				"Effect":"Allow",
				"Principal":{"AWS":"arn:aws:iam::111111111111:root"},
				"Action":"sts:AssumeRole"
			},
			{
				"Effect":"Allow",
				"Principal":{"Service":"ec2.amazonaws.com"},
				"Action":"sts:AssumeRole"
			}
		]
	}`

	assert.True(t, iam.EvaluateAssumeRoleTrustPolicy(
		policy, "arn:aws:iam::111111111111:root", "", false, iam.ConditionContext{},
	))
	assert.True(t, iam.EvaluateAssumeRoleTrustPolicy(
		policy, "ec2.amazonaws.com", "", false, iam.ConditionContext{},
	))
	assert.False(t, iam.EvaluateAssumeRoleTrustPolicy(
		policy, "arn:aws:iam::999999999999:root", "", false, iam.ConditionContext{},
	))
}

func TestCreateRole_RejectsBadPrincipalARN(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	// Non-ARN AWS principal.
	badPolicy := `{
		"Version":"2012-10-17",
		"Statement":[{
			"Effect":"Allow",
			"Principal":{"AWS":"not-an-arn"},
			"Action":"sts:AssumeRole"
		}]
	}`

	_, err := b.CreateRole("BadRole", "/", badPolicy, "")
	require.Error(t, err)
	assert.ErrorIs(t, err, iam.ErrMalformedPolicyDocument)
}

func TestCreateRole_AllowsWildcardPrincipal(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	// Wildcard principal is allowed.
	policy := `{
		"Version":"2012-10-17",
		"Statement":[{
			"Effect":"Allow",
			"Principal":"*",
			"Action":"sts:AssumeRole"
		}]
	}`

	_, err := b.CreateRole("WildRole", "/", policy, "")
	require.NoError(t, err)
}

func TestCreateRole_AllowsServicePrincipal(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	policy := `{
		"Version":"2012-10-17",
		"Statement":[{
			"Effect":"Allow",
			"Principal":{"Service":"lambda.amazonaws.com"},
			"Action":"sts:AssumeRole"
		}]
	}`

	_, err := b.CreateRole("LambdaRole", "/", policy, "")
	require.NoError(t, err)
}

func TestUpdateAssumeRolePolicy_RejectsBadPrincipal(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	validPolicy := `{"Version":"2012-10-17","Statement":[` +
		`{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}`
	_, err := b.CreateRole("TestRole2", "/", validPolicy, "")
	require.NoError(t, err)

	badPolicy := `{"Version":"2012-10-17","Statement":[` +
		`{"Effect":"Allow","Principal":{"AWS":"invalid-not-arn"},"Action":"sts:AssumeRole"}]}`
	err = b.UpdateAssumeRolePolicy("TestRole2", badPolicy)
	require.Error(t, err)
	assert.ErrorIs(t, err, iam.ErrMalformedPolicyDocument)
}

func TestUpdateRole_RejectsPathChange(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	validPolicy := `{"Version":"2012-10-17","Statement":[` +
		`{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}`
	_, err := b.CreateRole("PathTestRole", "/", validPolicy, "")
	require.NoError(t, err)

	// Backend UpdateRole itself only changes description, not path. The path-rejection
	// is enforced in the handler layer. Test the handler via HTTP in handler tests.
	// Here we test that UpdateRole only updates description.
	err = b.UpdateRole("PathTestRole", "new description")
	require.NoError(t, err)

	r, err := b.GetRole("PathTestRole")
	require.NoError(t, err)
	assert.Equal(t, "new description", r.Description)
	assert.Equal(t, "/", r.Path) // path unchanged
}

func TestEvaluateAssumeRoleTrustPolicy_MultiValueAWSPrincipal(t *testing.T) {
	t.Parallel()

	policy := `{
		"Version":"2012-10-17",
		"Statement":[{
			"Effect":"Allow",
			"Principal":{"AWS":["arn:aws:iam::111111111111:root","arn:aws:iam::222222222222:root"]},
			"Action":"sts:AssumeRole"
		}]
	}`

	assert.True(t, iam.EvaluateAssumeRoleTrustPolicy(
		policy, "arn:aws:iam::111111111111:root", "", false, iam.ConditionContext{},
	))
	assert.True(t, iam.EvaluateAssumeRoleTrustPolicy(
		policy, "arn:aws:iam::222222222222:root", "", false, iam.ConditionContext{},
	))
	assert.False(t, iam.EvaluateAssumeRoleTrustPolicy(
		policy, "arn:aws:iam::333333333333:root", "", false, iam.ConditionContext{},
	))
}

func TestRoleID_Format(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	r, err := b.CreateRole(
		"MyRole",
		"/",
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
		"",
	)
	require.NoError(t, err)

	// Role IDs are AROA + 16 uppercase alphanumeric chars.
	assert.True(t, strings.HasPrefix(r.RoleID, "AROA"), "role ID must start with AROA")
	assert.Len(t, r.RoleID, 20, "role ID must be 20 chars")
	assert.NotContains(t, r.RoleID, "-", "role ID must not contain dashes")

	for _, ch := range r.RoleID {
		assert.True(t, unicode.IsUpper(ch) || unicode.IsDigit(ch),
			"role ID char %q must be uppercase alphanumeric", ch)
	}
}

func TestRoleARN_Format(t *testing.T) {
	t.Parallel()

	b := iam.NewInMemoryBackendWithConfig("123456789012")
	r, err := b.CreateRole(
		"MyRole",
		"/",
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
		"",
	)
	require.NoError(t, err)

	assert.Equal(t, "arn:aws:iam::123456789012:role/MyRole", r.Arn)
}

func TestUpdateAssumeRolePolicy_Valid(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
	_, _ = b.CreateRole("MyRole", "/", doc, "")

	newDoc := `{"Version":"2012-10-17","Statement":[` +
		`{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},` +
		`"Action":"sts:AssumeRole"}]}`
	require.NoError(t, b.UpdateAssumeRolePolicy("MyRole", newDoc))

	r, err := b.GetRole("MyRole")
	require.NoError(t, err)
	assert.Equal(t, newDoc, r.AssumeRolePolicyDocument)
}

func TestInMemoryBackend_Roles(t *testing.T) {
	t.Parallel()

	t.Run("CreateAndGetRole", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
		r, err := b.CreateRole("MyRole", "/", doc, "")
		require.NoError(t, err)
		assert.Equal(t, "MyRole", r.RoleName)
		assert.Equal(t, doc, r.AssumeRolePolicyDocument)

		got, err := b.GetRole("MyRole")
		require.NoError(t, err)
		assert.Equal(t, "MyRole", got.RoleName)
	})

	t.Run("CreateRoleAlreadyExists", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, err := b.CreateRole("MyRole", "/", "", "")
		require.NoError(t, err)
		_, err = b.CreateRole("MyRole", "/", "", "")
		require.ErrorIs(t, err, iam.ErrRoleAlreadyExists)
	})

	t.Run("GetRoleNotFound", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, err := b.GetRole("nonexistent")
		require.ErrorIs(t, err, iam.ErrRoleNotFound)
	})

	t.Run("DeleteRole", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, _ = b.CreateRole("MyRole", "/", "", "")
		err := b.DeleteRole("MyRole")
		require.NoError(t, err)
	})

	t.Run("DeleteRoleNotFound", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		err := b.DeleteRole("nonexistent")
		require.ErrorIs(t, err, iam.ErrRoleNotFound)
	})

	t.Run("ListRoles", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, _ = b.CreateRole("ZRole", "/", "", "")
		_, _ = b.CreateRole("ARole", "/", "", "")
		roles, err := b.ListRoles("", 0)
		require.NoError(t, err)
		require.Len(t, roles.Data, 2)
		assert.Equal(t, "ARole", roles.Data[0].RoleName) // sorted
	})

	t.Run("ListAllRoles", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, _ = b.CreateRole("RoleB", "/", "", "")
		_, _ = b.CreateRole("RoleA", "/", "", "")
		roles := b.ListAllRoles()
		require.Len(t, roles, 2)
		assert.Equal(t, "RoleA", roles[0].RoleName)
	})

	t.Run("GetRoleByArn_Found", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		r, err := b.CreateRole("MyRole", "/", "", "")
		require.NoError(t, err)

		got, err := b.GetRoleByArn(r.Arn)
		require.NoError(t, err)
		assert.Equal(t, "MyRole", got.RoleName)
		assert.Equal(t, r.Arn, got.Arn)
	})

	t.Run("GetRoleByArn_NotFound", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()

		_, err := b.GetRoleByArn("arn:aws:iam::000000000000:role/nonexistent")
		require.ErrorIs(t, err, iam.ErrRoleNotFound)
	})

	t.Run("UpdateRoleMaxSessionDuration_Success", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, err := b.CreateRole("MyRole", "/", "", "")
		require.NoError(t, err)

		err = b.UpdateRoleMaxSessionDuration("MyRole", 7200)
		require.NoError(t, err)

		got, err := b.GetRole("MyRole")
		require.NoError(t, err)
		assert.Equal(t, int32(7200), got.MaxSessionDuration)
	})

	t.Run("UpdateRoleMaxSessionDuration_NotFound", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()

		err := b.UpdateRoleMaxSessionDuration("nonexistent", 3600)
		require.ErrorIs(t, err, iam.ErrRoleNotFound)
	})
}

func TestInMemoryBackend_RoleInlinePolicies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(*iam.InMemoryBackend)
		name    string
		wantDoc string
		wantErr error
		action  string
	}{
		{
			name: "PutAndGetRolePolicy",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateRole("MyRole", "/", "{}", "")
			},
			action:  "put_get",
			wantDoc: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
		},
		{
			name:    "PutRolePolicy_RoleNotFound",
			setup:   func(_ *iam.InMemoryBackend) {},
			action:  "put_notfound",
			wantErr: iam.ErrRoleNotFound,
		},
		{
			name: "GetRolePolicy_PolicyNotFound",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateRole("MyRole", "/", "{}", "")
			},
			action:  "get_notfound",
			wantErr: iam.ErrInlinePolicyNotFound,
		},
		{
			name: "DeleteRolePolicy",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateRole("MyRole", "/", "{}", "")
				_ = b.PutRolePolicy(
					"MyRole",
					"InlinePolicy",
					`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
				)
			},
			action: "delete",
		},
		{
			name: "ListRolePolicies_Sorted",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateRole("MyRole", "/", "{}", "")
				_ = b.PutRolePolicy(
					"MyRole",
					"ZPolicy",
					`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
				)
				_ = b.PutRolePolicy(
					"MyRole",
					"APolicy",
					`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
				)
			},
			action: "list",
		},
		{
			name: "DeleteRole_WithInlinePolicy_Conflict",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateRole("MyRole", "/", "{}", "")
				_ = b.PutRolePolicy(
					"MyRole",
					"InlinePolicy",
					`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
				)
			},
			action:  "delete_role_conflict",
			wantErr: iam.ErrDeleteConflict,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			tt.setup(b)

			switch tt.action {
			case "put_get":
				err := b.PutRolePolicy("MyRole", "MyPolicy", tt.wantDoc)
				require.NoError(t, err)

				doc, err := b.GetRolePolicy("MyRole", "MyPolicy")
				require.NoError(t, err)
				assert.Equal(t, tt.wantDoc, doc)

			case "put_notfound":
				err := b.PutRolePolicy(
					"Ghost",
					"MyPolicy",
					`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
				)
				require.ErrorIs(t, err, tt.wantErr)

			case "get_notfound":
				_, err := b.GetRolePolicy("MyRole", "Ghost")
				require.ErrorIs(t, err, tt.wantErr)

			case "delete":
				err := b.DeleteRolePolicy("MyRole", "InlinePolicy")
				require.NoError(t, err)

				names, err := b.ListRolePolicies("MyRole")
				require.NoError(t, err)
				assert.Empty(t, names)

			case "list":
				names, err := b.ListRolePolicies("MyRole")
				require.NoError(t, err)
				require.Len(t, names, 2)
				assert.Equal(t, "APolicy", names[0])
				assert.Equal(t, "ZPolicy", names[1])

			case "delete_role_conflict":
				err := b.DeleteRole("MyRole")
				require.ErrorIs(t, err, tt.wantErr)
			}
		})
	}
}

func TestInMemoryBackend_UpdateAssumeRolePolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		setup   func(*iam.InMemoryBackend)
		name    string
		doc     string
	}{
		{
			name: "UpdateAssumeRolePolicy_Success",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateRole("MyRole", "/", "{}", "")
			},
			doc: `{"Version":"2012-10-17","Statement":[` +
				`{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}`,
		},
		{
			name:    "UpdateAssumeRolePolicy_RoleNotFound",
			setup:   func(_ *iam.InMemoryBackend) {},
			doc:     "{}",
			wantErr: iam.ErrRoleNotFound,
		},
		{
			name: "UpdateAssumeRolePolicy_InvalidJSON",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateRole("MyRole", "/", "{}", "")
			},
			doc:     "not-json",
			wantErr: iam.ErrMalformedPolicyDocument,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			tt.setup(b)

			err := b.UpdateAssumeRolePolicy("MyRole", tt.doc)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)

			r, err := b.GetRole("MyRole")
			require.NoError(t, err)
			assert.Equal(t, tt.doc, r.AssumeRolePolicyDocument)
		})
	}
}

// Real AWS DeleteRole is rejected with DeleteConflictException while the
// role is attached to an instance profile — the caller must call
// RemoveRoleFromInstanceProfile first. See
// https://docs.aws.amazon.com/IAM/latest/APIReference/API_DeleteRole.html.
func TestDeleteRole_FailsWhenAttachedToInstanceProfile(t *testing.T) {
	t.Parallel()

	b := iam.NewInMemoryBackend()
	_, err := b.CreateRole("MyRole", "/", "", "")
	require.NoError(t, err)
	_, err = b.CreateInstanceProfile("MyProfile", "/")
	require.NoError(t, err)
	require.NoError(t, b.AddRoleToInstanceProfile("MyProfile", "MyRole"))

	err = b.DeleteRole("MyRole")
	require.Error(t, err)
	require.ErrorIs(t, err, iam.ErrDeleteConflict)

	// The role must still exist since the delete was rejected.
	_, getErr := b.GetRole("MyRole")
	require.NoError(t, getErr)
}

// TestDeleteRole_SucceedsAfterRemovedFromInstanceProfile verifies the
// documented AWS workflow: RemoveRoleFromInstanceProfile, then DeleteRole
// succeeds and does not leave a ghost role reference on the instance profile.
func TestDeleteRole_SucceedsAfterRemovedFromInstanceProfile(t *testing.T) {
	t.Parallel()

	b := iam.NewInMemoryBackend()
	_, err := b.CreateRole("MyRole", "/", "", "")
	require.NoError(t, err)
	_, err = b.CreateInstanceProfile("MyProfile", "/")
	require.NoError(t, err)
	require.NoError(t, b.AddRoleToInstanceProfile("MyProfile", "MyRole"))

	require.NoError(t, b.RemoveRoleFromInstanceProfile("MyProfile", "MyRole"))
	require.NoError(t, b.DeleteRole("MyRole"))

	ip, err := b.GetInstanceProfile("MyProfile")
	require.NoError(t, err)
	assert.Empty(t, ip.Roles, "instance profile must not retain a ghost role reference")
}
