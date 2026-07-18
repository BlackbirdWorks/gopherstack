package sts_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sts"
)

// TestSessionNameCharacterValidation verifies that RoleSessionName with
// invalid characters is rejected.
func TestSessionNameCharacterValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		sessionName string
		wantErr     bool
	}{
		{
			name:        "valid alphanumeric",
			sessionName: "my-session",
			wantErr:     false,
		},
		{
			name:        "valid with allowed special chars",
			sessionName: "sess+=,.@abc",
			wantErr:     false,
		},
		{
			name:        "invalid colon (not allowed per AWS)",
			sessionName: "sess:abc",
			wantErr:     true,
		},
		{
			name:        "invalid space character",
			sessionName: "my session",
			wantErr:     true,
		},
		{
			name:        "invalid slash",
			sessionName: "my/session",
			wantErr:     true,
		},
		{
			name:        "invalid dollar sign",
			sessionName: "my$session",
			wantErr:     true,
		},
		{
			name:        "invalid semicolon",
			sessionName: "my;session",
			wantErr:     true,
		},
		{
			name:        "too short (1 char)",
			sessionName: "a",
			wantErr:     true,
		},
		{
			name:        "too long (65 chars)",
			sessionName: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := sts.NewInMemoryBackend()
			_, err := backend.AssumeRole(&sts.AssumeRoleInput{
				RoleArn:         "arn:aws:iam::000000000000:role/TestRole",
				RoleSessionName: tt.sessionName,
			})

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestRoleArnValidation verifies that invalid ARNs are rejected.
func TestRoleArnValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		roleArn string
		wantErr bool
	}{
		{
			name:    "valid iam role arn",
			roleArn: "arn:aws:iam::000000000000:role/TestRole",
			wantErr: false,
		},
		{
			name:    "malformed too few parts",
			roleArn: "short/role",
			wantErr: true,
		},
		{
			name:    "malformed no arn prefix",
			roleArn: "aws:iam::000000000000:role/TestRole:extra",
			wantErr: true,
		},
		{
			name:    "empty arn",
			roleArn: "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := sts.NewInMemoryBackend()
			_, err := backend.AssumeRole(&sts.AssumeRoleInput{
				RoleArn:         tt.roleArn,
				RoleSessionName: "valid-session",
			})

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestValidateRoleArn_AccountID verifies the 12-digit account and role/
// resource-prefix requirements of validateRoleArn.
func TestValidateRoleArn_AccountID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		roleArn string
		wantErr bool
	}{
		{
			name:    "valid 12-digit account",
			roleArn: "arn:aws:iam::123456789012:role/MyRole",
			wantErr: false,
		},
		{
			name:    "short account ID rejected",
			roleArn: "arn:aws:iam::1234567:role/MyRole",
			wantErr: true,
		},
		{
			name:    "non-numeric account rejected",
			roleArn: "arn:aws:iam::abcdefghijkl:role/MyRole",
			wantErr: true,
		},
		{
			name:    "empty account rejected",
			roleArn: "arn:aws:iam:::role/MyRole",
			wantErr: true,
		},
		{
			name:    "resource not starting with role/ rejected",
			roleArn: "arn:aws:iam::123456789012:user/bob",
			wantErr: true,
		},
		{
			name:    "assumed-role resource rejected",
			roleArn: "arn:aws:sts::123456789012:assumed-role/R/s",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := sts.NewInMemoryBackend()
			_, err := b.AssumeRole(&sts.AssumeRoleInput{
				RoleArn:         tc.roleArn,
				RoleSessionName: "session",
			})

			if tc.wantErr {
				require.ErrorIs(t, err, sts.ErrInvalidRoleArn)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestSourceIdentityValidation verifies SourceIdentity charset/length rules
// (Accuracy Gap #17).
func TestSourceIdentityValidation(t *testing.T) {
	t.Parallel()

	validIdentities := []string{
		"Alice",
		"user@example.com",
		"user+tag=value",
		"AB",
		"a-b_c.d,e",
	}

	for _, id := range validIdentities {
		t.Run("valid_"+id, func(t *testing.T) {
			t.Parallel()

			b := sts.NewInMemoryBackend()
			_, err := b.AssumeRole(&sts.AssumeRoleInput{
				RoleArn:         "arn:aws:iam::123456789012:role/R",
				RoleSessionName: "session",
				SourceIdentity:  id,
			})
			require.NoError(t, err)
		})
	}

	invalidIdentities := []string{
		"a",                     // too short
		strings.Repeat("x", 65), // too long
		"bad spaces",            // spaces not allowed
		"bad/slash",             // slash not allowed
	}

	for _, id := range invalidIdentities {
		t.Run("invalid_source_identity", func(t *testing.T) {
			t.Parallel()

			b := sts.NewInMemoryBackend()
			_, err := b.AssumeRole(&sts.AssumeRoleInput{
				RoleArn:         "arn:aws:iam::123456789012:role/R",
				RoleSessionName: "session",
				SourceIdentity:  id,
			})
			require.ErrorIs(t, err, sts.ErrInvalidSourceIdentity)
		})
	}
}

// TestSessionTagConstraints verifies session tag key/value constraint
// validation (Accuracy Gap #18).
func TestSessionTagConstraints(t *testing.T) {
	t.Parallel()

	t.Run("aws_prefix_tag_key_rejected", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		_, err := b.AssumeRole(&sts.AssumeRoleInput{
			RoleArn:         "arn:aws:iam::123456789012:role/R",
			RoleSessionName: "session",
			Tags:            []sts.Tag{{Key: "aws:reserved", Value: "val"}},
		})
		require.ErrorIs(t, err, sts.ErrInvalidTagKey)
	})

	t.Run("AWS_uppercase_prefix_rejected", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		_, err := b.AssumeRole(&sts.AssumeRoleInput{
			RoleArn:         "arn:aws:iam::123456789012:role/R",
			RoleSessionName: "session",
			Tags:            []sts.Tag{{Key: "AWS:Tag", Value: "val"}},
		})
		require.ErrorIs(t, err, sts.ErrInvalidTagKey)
	})

	t.Run("empty_tag_key_rejected", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		_, err := b.AssumeRole(&sts.AssumeRoleInput{
			RoleArn:         "arn:aws:iam::123456789012:role/R",
			RoleSessionName: "session",
			Tags:            []sts.Tag{{Key: "", Value: "val"}},
		})
		require.ErrorIs(t, err, sts.ErrInvalidTagKey)
	})

	t.Run("tag_value_over_256_chars_rejected", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		_, err := b.AssumeRole(&sts.AssumeRoleInput{
			RoleArn:         "arn:aws:iam::123456789012:role/R",
			RoleSessionName: "session",
			Tags:            []sts.Tag{{Key: "mykey", Value: strings.Repeat("v", 257)}},
		})
		require.ErrorIs(t, err, sts.ErrInvalidTagValue)
	})

	t.Run("duplicate_tag_key_case_insensitive_rejected", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		_, err := b.AssumeRole(&sts.AssumeRoleInput{
			RoleArn:         "arn:aws:iam::123456789012:role/R",
			RoleSessionName: "session",
			Tags:            []sts.Tag{{Key: "MyKey", Value: "v1"}, {Key: "mykey", Value: "v2"}},
		})
		require.ErrorIs(t, err, sts.ErrInvalidTagKey)
	})

	t.Run("tag_key_128_chars_accepted", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		_, err := b.AssumeRole(&sts.AssumeRoleInput{
			RoleArn:         "arn:aws:iam::123456789012:role/R",
			RoleSessionName: "session",
			Tags:            []sts.Tag{{Key: strings.Repeat("k", 128), Value: "val"}},
		})
		require.NoError(t, err)
	})

	t.Run("tag_key_129_chars_rejected", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		_, err := b.AssumeRole(&sts.AssumeRoleInput{
			RoleArn:         "arn:aws:iam::123456789012:role/R",
			RoleSessionName: "session",
			Tags:            []sts.Tag{{Key: strings.Repeat("k", 129), Value: "val"}},
		})
		require.ErrorIs(t, err, sts.ErrInvalidTagKey)
	})

	t.Run("tag_value_256_chars_accepted", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		_, err := b.AssumeRole(&sts.AssumeRoleInput{
			RoleArn:         "arn:aws:iam::123456789012:role/R",
			RoleSessionName: "session",
			Tags:            []sts.Tag{{Key: "mykey", Value: strings.Repeat("v", 256)}},
		})
		require.NoError(t, err)
	})
}

// TestPackedPolicySizeWithArns verifies PackedPolicySize includes managed
// policy ARNs (Accuracy Gap #19).
func TestPackedPolicySizeWithArns(t *testing.T) {
	t.Parallel()

	t.Run("managed_policy_arns_contribute_to_size", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		policyArn := "arn:aws:iam::aws:policy/ReadOnlyAccess"
		resp, err := b.AssumeRole(&sts.AssumeRoleInput{
			RoleArn:         "arn:aws:iam::123456789012:role/R",
			RoleSessionName: "session",
			PolicyArns:      []string{policyArn},
		})
		require.NoError(t, err)

		// With an ARN but no inline policy, size should be > 0.
		assert.Positive(t, resp.AssumeRoleResult.PackedPolicySize)
	})

	t.Run("size_exceeds_budget_returns_error", func(t *testing.T) {
		t.Parallel()

		// A single valid JSON policy whose total size exceeds 2048 bytes.
		bigPolicy := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"` +
			strings.Repeat("arn:aws:s3:::my-bucket/prefix/", 80) + `*"}]}`
		b := sts.NewInMemoryBackend()
		_, err := b.AssumeRole(&sts.AssumeRoleInput{
			RoleArn:         "arn:aws:iam::123456789012:role/R",
			RoleSessionName: "session",
			Policy:          bigPolicy,
		})
		require.ErrorIs(t, err, sts.ErrPackedPolicyTooLarge)
	})

	t.Run("ceiling_rounding_correct", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		smallPolicy := `{"Statement":[]}`
		resp, err := b.AssumeRole(&sts.AssumeRoleInput{
			RoleArn:         "arn:aws:iam::123456789012:role/R",
			RoleSessionName: "session",
			Policy:          smallPolicy,
		})
		require.NoError(t, err)
		assert.GreaterOrEqual(t, resp.AssumeRoleResult.PackedPolicySize, int32(1))
	})
}

// TestProvidedContextsValidation verifies ProvidedContexts count/length
// constraints (Accuracy Gap #3).
func TestProvidedContextsValidation(t *testing.T) {
	t.Parallel()

	t.Run("too_many_provided_contexts_rejected", func(t *testing.T) {
		t.Parallel()

		ctxs := make([]sts.ProvidedContext, 6)
		for i := range ctxs {
			ctxs[i] = sts.ProvidedContext{
				ProviderArn:      "arn:aws:iam::123456789012:oidc-provider/example.com",
				ContextAssertion: "assertion",
			}
		}

		b := sts.NewInMemoryBackend()
		_, err := b.AssumeRole(&sts.AssumeRoleInput{
			RoleArn:          "arn:aws:iam::123456789012:role/R",
			RoleSessionName:  "session",
			ProvidedContexts: ctxs,
		})
		require.ErrorIs(t, err, sts.ErrInvalidProvidedContext)
	})

	t.Run("context_assertion_over_2048_chars_rejected", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		_, err := b.AssumeRole(&sts.AssumeRoleInput{
			RoleArn:         "arn:aws:iam::123456789012:role/R",
			RoleSessionName: "session",
			ProvidedContexts: []sts.ProvidedContext{
				{
					ProviderArn:      "arn:aws:iam::123456789012:oidc-provider/example.com",
					ContextAssertion: strings.Repeat("x", 2049),
				},
			},
		})
		require.ErrorIs(t, err, sts.ErrInvalidProvidedContext)
	})
}

// TestPolicyArnsValidation verifies managed-policy-ARN count and shape
// validation (Accuracy Gap #4).
func TestPolicyArnsValidation(t *testing.T) {
	t.Parallel()

	t.Run("too_many_policy_arns_rejected", func(t *testing.T) {
		t.Parallel()

		arns := make([]string, 11)
		for i := range arns {
			arns[i] = "arn:aws:iam::aws:policy/Policy" + string(rune('A'+i))
		}

		b := sts.NewInMemoryBackend()
		_, err := b.AssumeRole(&sts.AssumeRoleInput{
			RoleArn:         "arn:aws:iam::123456789012:role/R",
			RoleSessionName: "session",
			PolicyArns:      arns,
		})
		require.ErrorIs(t, err, sts.ErrTooManyPolicyArns)
	})

	t.Run("malformed_policy_arn_rejected", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		_, err := b.AssumeRole(&sts.AssumeRoleInput{
			RoleArn:         "arn:aws:iam::123456789012:role/R",
			RoleSessionName: "session",
			PolicyArns:      []string{"not-an-arn"},
		})
		require.ErrorIs(t, err, sts.ErrInvalidPolicyArn)
	})

	t.Run("ten_policy_arns_accepted", func(t *testing.T) {
		t.Parallel()

		arns := make([]string, 10)
		for i := range arns {
			arns[i] = "arn:aws:iam::aws:policy/Policy" + string(rune('A'+i))
		}

		b := sts.NewInMemoryBackend()
		_, err := b.AssumeRole(&sts.AssumeRoleInput{
			RoleArn:         "arn:aws:iam::123456789012:role/R",
			RoleSessionName: "session",
			PolicyArns:      arns,
		})
		require.NoError(t, err)
	})
}

// TestValidatePolicyArns_Format verifies the stricter managed-policy-ARN format
// (AWS-managed vs customer-managed vs non-policy ARNs).
func TestValidatePolicyArns_Format(t *testing.T) {
	t.Parallel()

	t.Run("aws_managed_policy_arn_accepted", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		_, err := b.AssumeRole(&sts.AssumeRoleInput{
			RoleArn:         "arn:aws:iam::123456789012:role/R",
			RoleSessionName: "session",
			PolicyArns:      []string{"arn:aws:iam::aws:policy/ReadOnlyAccess"},
		})
		require.NoError(t, err)
	})

	t.Run("customer_managed_policy_arn_accepted", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		_, err := b.AssumeRole(&sts.AssumeRoleInput{
			RoleArn:         "arn:aws:iam::123456789012:role/R",
			RoleSessionName: "session",
			PolicyArns:      []string{"arn:aws:iam::123456789012:policy/MyPolicy"},
		})
		require.NoError(t, err)
	})

	t.Run("s3_bucket_arn_rejected", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		_, err := b.AssumeRole(&sts.AssumeRoleInput{
			RoleArn:         "arn:aws:iam::123456789012:role/R",
			RoleSessionName: "session",
			PolicyArns:      []string{"arn:aws:s3:::my-bucket"},
		})
		require.ErrorIs(t, err, sts.ErrInvalidPolicyArn)
	})

	t.Run("iam_role_arn_rejected_as_policy", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		_, err := b.AssumeRole(&sts.AssumeRoleInput{
			RoleArn:         "arn:aws:iam::123456789012:role/R",
			RoleSessionName: "session",
			PolicyArns:      []string{"arn:aws:iam::123456789012:role/SomeRole"},
		})
		require.ErrorIs(t, err, sts.ErrInvalidPolicyArn)
	})
}

// TestValidateInlinePolicy_Statement verifies inline session policies must
// contain a Statement field.
func TestValidateInlinePolicy_Statement(t *testing.T) {
	t.Parallel()

	t.Run("policy_without_statement_rejected", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		_, err := b.AssumeRole(&sts.AssumeRoleInput{
			RoleArn:         "arn:aws:iam::123456789012:role/R",
			RoleSessionName: "session",
			Policy:          `{"Version":"2012-10-17"}`,
		})
		require.ErrorIs(t, err, sts.ErrMalformedPolicyDocument)
	})

	t.Run("policy_with_statement_accepted", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		_, err := b.AssumeRole(&sts.AssumeRoleInput{
			RoleArn:         "arn:aws:iam::123456789012:role/R",
			RoleSessionName: "session",
			Policy:          `{"Statement":[]}`,
		})
		require.NoError(t, err)
	})
}
