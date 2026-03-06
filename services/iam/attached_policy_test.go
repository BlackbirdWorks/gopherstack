package iam_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iam"
)

func newIAMBackend(t *testing.T) *iam.InMemoryBackend {
	t.Helper()

	return iam.NewInMemoryBackend()
}

func TestListAttachedUserPolicies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		setupUser      string
		setupPolicy    string
		policyDoc      string
		userName       string
		wantPolicyName string
		wantCount      int
		wantErr        bool
	}{
		{
			name:           "success",
			setupUser:      "alice",
			setupPolicy:    "MyPolicy",
			policyDoc:      `{"Version":"2012-10-17","Statement":[]}`,
			userName:       "alice",
			wantCount:      1,
			wantPolicyName: "MyPolicy",
		},
		{
			name:     "user_not_found",
			userName: "nobody",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newIAMBackend(t)

			var polArn string
			if tt.setupUser != "" {
				_, err := b.CreateUser(tt.setupUser, "/")
				require.NoError(t, err)
			}
			if tt.setupPolicy != "" {
				pol, err := b.CreatePolicy(tt.setupPolicy, "/", tt.policyDoc)
				require.NoError(t, err)
				polArn = pol.Arn
				require.NoError(t, b.AttachUserPolicy(tt.setupUser, polArn))
			}

			policies, err := b.ListAttachedUserPolicies(tt.userName)
			if tt.wantErr {
				require.Error(t, err)

				return
			}
			require.NoError(t, err)
			require.Len(t, policies, tt.wantCount)
			if tt.wantCount > 0 {
				assert.Equal(t, polArn, policies[0].PolicyArn)
				assert.Equal(t, tt.wantPolicyName, policies[0].PolicyName)
			}
		})
	}
}

func TestListAttachedRolePolicies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		setupRole      string
		setupPolicy    string
		policyDoc      string
		roleName       string
		wantPolicyName string
		wantCount      int
		wantErr        bool
	}{
		{
			name:           "success",
			setupRole:      "MyRole",
			setupPolicy:    "RolePolicy",
			policyDoc:      `{"Version":"2012-10-17","Statement":[]}`,
			roleName:       "MyRole",
			wantCount:      1,
			wantPolicyName: "RolePolicy",
		},
		{
			name:     "role_not_found",
			roleName: "NoRole",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newIAMBackend(t)

			var polArn string
			if tt.setupRole != "" {
				_, err := b.CreateRole(tt.setupRole, "/", `{}`)
				require.NoError(t, err)
			}
			if tt.setupPolicy != "" {
				pol, err := b.CreatePolicy(tt.setupPolicy, "/", tt.policyDoc)
				require.NoError(t, err)
				polArn = pol.Arn
				require.NoError(t, b.AttachRolePolicy(tt.setupRole, polArn))
			}

			policies, err := b.ListAttachedRolePolicies(tt.roleName)
			if tt.wantErr {
				require.Error(t, err)

				return
			}
			require.NoError(t, err)
			require.Len(t, policies, tt.wantCount)
			if tt.wantCount > 0 {
				assert.Equal(t, polArn, policies[0].PolicyArn)
				assert.Equal(t, tt.wantPolicyName, policies[0].PolicyName)
			}
		})
	}
}

func TestAttachUserPolicyIdempotent(t *testing.T) {
	t.Parallel()

	b := newIAMBackend(t)

	_, err := b.CreateUser("bob", "/")
	require.NoError(t, err)

	pol, err := b.CreatePolicy("Pol", "/", `{}`)
	require.NoError(t, err)

	// Attach twice — should not duplicate.
	require.NoError(t, b.AttachUserPolicy("bob", pol.Arn))
	require.NoError(t, b.AttachUserPolicy("bob", pol.Arn))

	policies, err := b.ListAttachedUserPolicies("bob")
	require.NoError(t, err)
	assert.Len(t, policies, 1)
}

func TestGetPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		setupPolicy    string
		queryArn       string
		wantPolicyName string
		wantErr        bool
	}{
		{
			name:           "success",
			setupPolicy:    "GetMe",
			wantPolicyName: "GetMe",
		},
		{
			name:     "not_found",
			queryArn: "arn:aws:iam::000000000000:policy/nosuchpolicy",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newIAMBackend(t)

			var polArn string
			if tt.setupPolicy != "" {
				pol, err := b.CreatePolicy(tt.setupPolicy, "/", `{"Version":"2012-10-17"}`)
				require.NoError(t, err)
				polArn = pol.Arn
			}

			queryArn := tt.queryArn
			if queryArn == "" {
				queryArn = polArn
			}

			got, err := b.GetPolicy(queryArn)
			if tt.wantErr {
				require.Error(t, err)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantPolicyName, got.PolicyName)
			assert.Equal(t, polArn, got.Arn)
		})
	}
}

func TestGetPolicyVersion(t *testing.T) {
	t.Parallel()

	b := newIAMBackend(t)

	pol, err := b.CreatePolicy("VersionedPol", "/", `{"Version":"2012-10-17","Statement":[]}`)
	require.NoError(t, err)

	// "v1" is always the default version in Gopherstack.
	got, err := b.GetPolicyVersion(pol.Arn, "v1")
	require.NoError(t, err)
	assert.JSONEq(t, `{"Version":"2012-10-17","Statement":[]}`, got.PolicyDocument)
}

func TestDeleteConflict_UserWithAttachedPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr      error
		name         string
		attachPolicy bool
	}{
		{
			name:         "user_with_attached_policy",
			attachPolicy: true,
			wantErr:      iam.ErrDeleteConflict,
		},
		{
			name:         "user_without_policies",
			attachPolicy: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newIAMBackend(t)

			_, err := b.CreateUser("alice", "/")
			require.NoError(t, err)

			if tt.attachPolicy {
				pol, pErr := b.CreatePolicy("MyPolicy", "/", `{"Version":"2012-10-17","Statement":[]}`)
				require.NoError(t, pErr)
				require.NoError(t, b.AttachUserPolicy("alice", pol.Arn))
			}

			err = b.DeleteUser("alice")

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestDeleteConflict_RoleWithAttachedPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr      error
		name         string
		attachPolicy bool
	}{
		{
			name:         "role_with_attached_policy",
			attachPolicy: true,
			wantErr:      iam.ErrDeleteConflict,
		},
		{
			name:         "role_without_policies",
			attachPolicy: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newIAMBackend(t)

			_, err := b.CreateRole("MyRole", "/", `{"Version":"2012-10-17","Statement":[]}`)
			require.NoError(t, err)

			if tt.attachPolicy {
				pol, pErr := b.CreatePolicy("RolePolicy", "/", `{"Version":"2012-10-17","Statement":[]}`)
				require.NoError(t, pErr)
				require.NoError(t, b.AttachRolePolicy("MyRole", pol.Arn))
			}

			err = b.DeleteRole("MyRole")

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestDeleteConflict_PolicyAttachedToEntity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr      error
		name         string
		attachToUser bool
	}{
		{
			name:         "policy_attached_to_user",
			attachToUser: true,
			wantErr:      iam.ErrDeleteConflict,
		},
		{
			name:         "policy_not_attached",
			attachToUser: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newIAMBackend(t)

			pol, err := b.CreatePolicy("StuckPolicy", "/", `{"Version":"2012-10-17","Statement":[]}`)
			require.NoError(t, err)

			if tt.attachToUser {
				_, uErr := b.CreateUser("bob", "/")
				require.NoError(t, uErr)
				require.NoError(t, b.AttachUserPolicy("bob", pol.Arn))
			}

			err = b.DeletePolicy(pol.Arn)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestMalformedPolicyDocument(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		name    string
		doc     string
	}{
		{
			name:    "invalid_json_create_policy",
			doc:     "not-json",
			wantErr: iam.ErrMalformedPolicyDocument,
		},
		{
			name: "valid_json_create_policy",
			doc:  `{"Version":"2012-10-17","Statement":[]}`,
		},
		{
			name: "empty_doc_allowed",
			doc:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newIAMBackend(t)

			_, err := b.CreatePolicy("TestPolicy", "/", tt.doc)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestMalformedPolicyDocument_CreateRole(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr  error
		name     string
		trustDoc string
	}{
		{
			name:     "invalid_json_trust_policy",
			trustDoc: "not-json",
			wantErr:  iam.ErrMalformedPolicyDocument,
		},
		{
			name:     "valid_json_trust_policy",
			trustDoc: `{"Version":"2012-10-17","Statement":[]}`,
		},
		{
			name:     "empty_trust_policy_allowed",
			trustDoc: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newIAMBackend(t)

			_, err := b.CreateRole("TestRole", "/", tt.trustDoc)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
