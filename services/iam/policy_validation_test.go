package iam_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iam"
)

// TestCreatePolicy_RejectsMalformedGrammar verifies that CreatePolicy enforces
// the IAM policy grammar, not merely JSON validity, matching real AWS which
// returns MalformedPolicyDocument for structurally valid JSON that violates the
// policy schema.
func TestCreatePolicy_RejectsMalformedGrammar(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		doc     string
		wantErr bool
	}{
		{
			name: "valid_single_statement",
			doc:  `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}`,
		},
		{
			name: "valid_statement_object_not_array",
			doc:  `{"Version":"2012-10-17","Statement":{"Effect":"Allow","Action":"s3:*","Resource":"*"}}`,
		},
		{
			name: "valid_notaction_notresource",
			doc:  `{"Statement":[{"Effect":"Deny","NotAction":"s3:*","NotResource":"*"}]}`,
		},
		{
			name: "valid_no_version_element",
			doc:  `{"Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
		},
		{
			name:    "not_a_json_object",
			doc:     `["not","an","object"]`,
			wantErr: true,
		},
		{
			name:    "missing_statement",
			doc:     `{"Version":"2012-10-17"}`,
			wantErr: true,
		},
		{
			name:    "empty_statement_array",
			doc:     `{"Version":"2012-10-17","Statement":[]}`,
			wantErr: true,
		},
		{
			name:    "unsupported_version",
			doc:     `{"Version":"2099-01-01","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
			wantErr: true,
		},
		{
			name:    "missing_effect",
			doc:     `{"Statement":[{"Action":"*","Resource":"*"}]}`,
			wantErr: true,
		},
		{
			name:    "invalid_effect_value",
			doc:     `{"Statement":[{"Effect":"Permit","Action":"*","Resource":"*"}]}`,
			wantErr: true,
		},
		{
			name:    "effect_wrong_case",
			doc:     `{"Statement":[{"Effect":"allow","Action":"*","Resource":"*"}]}`,
			wantErr: true,
		},
		{
			name:    "missing_action_and_notaction",
			doc:     `{"Statement":[{"Effect":"Allow","Resource":"*"}]}`,
			wantErr: true,
		},
		{
			name:    "both_action_and_notaction",
			doc:     `{"Statement":[{"Effect":"Allow","Action":"*","NotAction":"*","Resource":"*"}]}`,
			wantErr: true,
		},
		{
			name:    "missing_resource_and_notresource",
			doc:     `{"Statement":[{"Effect":"Allow","Action":"*"}]}`,
			wantErr: true,
		},
		{
			name:    "both_resource_and_notresource",
			doc:     `{"Statement":[{"Effect":"Allow","Action":"*","Resource":"*","NotResource":"*"}]}`,
			wantErr: true,
		},
		{
			name:    "principal_in_identity_policy",
			doc:     `{"Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"*","Resource":"*"}]}`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			_, err := b.CreatePolicy("P", "/", tt.doc)
			if tt.wantErr {
				require.ErrorIs(t, err, iam.ErrMalformedPolicyDocument,
					"AWS returns MalformedPolicyDocument for grammar violations")

				return
			}

			require.NoError(t, err)
		})
	}
}

// TestPutInlinePolicy_RejectsMalformedGrammar verifies that inline-policy puts on
// users, roles, and groups all enforce the policy grammar.
func TestPutInlinePolicy_RejectsMalformedGrammar(t *testing.T) {
	t.Parallel()

	const badDoc = `{"Statement":[{"Effect":"Allow","Action":"*"}]}` // missing Resource

	tests := []struct {
		put  func(b *iam.InMemoryBackend) error
		name string
	}{
		{
			name: "user",
			put: func(b *iam.InMemoryBackend) error {
				_, _ = b.CreateUser("alice", "/", "")

				return b.PutUserPolicy("alice", "Inline", badDoc)
			},
		},
		{
			name: "role",
			put: func(b *iam.InMemoryBackend) error {
				_, _ = b.CreateRole("MyRole", "/", "", "")

				return b.PutRolePolicy("MyRole", "Inline", badDoc)
			},
		},
		{
			name: "group",
			put: func(b *iam.InMemoryBackend) error {
				_, _ = b.CreateGroup("Admins", "/")

				return b.PutGroupPolicy("Admins", "Inline", badDoc)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			require.ErrorIs(t, tt.put(b), iam.ErrMalformedPolicyDocument)
		})
	}
}

// TestCreatePolicyVersion_RejectsMalformedGrammar verifies that new policy
// versions are validated, matching AWS.
func TestCreatePolicyVersion_RejectsMalformedGrammar(t *testing.T) {
	t.Parallel()

	b := iam.NewInMemoryBackend()
	pol, err := b.CreatePolicy("P", "/",
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`)
	require.NoError(t, err)

	// Effect "Maybe" is not a valid IAM effect.
	_, err = b.CreatePolicyVersion(pol.Arn,
		`{"Statement":[{"Effect":"Maybe","Action":"*","Resource":"*"}]}`, false)
	require.ErrorIs(t, err, iam.ErrMalformedPolicyDocument)
}
