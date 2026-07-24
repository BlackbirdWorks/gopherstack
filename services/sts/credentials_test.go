package sts_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sts"
)

// TestDeriveRoleID_NoDuplicates verifies deriveRoleID does not collide for
// role names sharing a common prefix.
func TestDeriveRoleID_NoDuplicates(t *testing.T) {
	t.Parallel()

	b := sts.NewInMemoryBackend()

	roleARNs := []string{
		"arn:aws:iam::123456789012:role/MyRole",
		"arn:aws:iam::123456789012:role/MyRoleAdmin",
		"arn:aws:iam::123456789012:role/MyRoleReadOnly",
		"arn:aws:iam::123456789012:role/MyRolePowerUser",
	}

	seenIDs := make(map[string]struct{})
	for _, arn := range roleARNs {
		resp, err := b.AssumeRole(&sts.AssumeRoleInput{
			RoleArn:         arn,
			RoleSessionName: "session",
		})
		require.NoError(t, err)

		id := resp.AssumeRoleResult.AssumedRoleUser.AssumedRoleID
		// The AssumedRoleID is "AROAXXX:session"; check the AROA prefix part
		parts := strings.SplitN(id, ":", 2)
		aroaPart := parts[0]
		_, dup := seenIDs[aroaPart]
		assert.False(t, dup, "duplicate role ID %s for ARN %s", aroaPart, arn)
		seenIDs[aroaPart] = struct{}{}
	}
}

// TestAssumedRoleArnPathStripped verifies the assumed-role ARN construction
// strips any IAM path from the role: a role at
// arn:aws:iam::ACCT:role/team/dev/MyRole yields
// arn:aws:sts::ACCT:assumed-role/MyRole/SESSION — only the final role-name
// segment is carried over, not the intermediate path components. See
// https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRole.html.
func TestAssumedRoleArnPathStripped(t *testing.T) {
	t.Parallel()

	const acct = "123456789012"

	tests := []struct {
		name        string
		roleArn     string
		sessionName string
		wantArn     string
		wantRoleID  string // expected AssumedRoleId prefix (before the ":session" suffix)
	}{
		{
			name:        "no_path",
			roleArn:     "arn:aws:iam::" + acct + ":role/MyRole",
			sessionName: "sess",
			wantArn:     "arn:aws:sts::" + acct + ":assumed-role/MyRole/sess",
		},
		{
			name:        "single_path_segment",
			roleArn:     "arn:aws:iam::" + acct + ":role/dev/MyRole",
			sessionName: "sess",
			wantArn:     "arn:aws:sts::" + acct + ":assumed-role/MyRole/sess",
		},
		{
			name:        "multi_path_segments",
			roleArn:     "arn:aws:iam::" + acct + ":role/team/dev/eu/MyRole",
			sessionName: "sess",
			wantArn:     "arn:aws:sts::" + acct + ":assumed-role/MyRole/sess",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sts.NewInMemoryBackend()
			resp, err := b.AssumeRole(&sts.AssumeRoleInput{
				RoleArn:         tt.roleArn,
				RoleSessionName: tt.sessionName,
			})
			require.NoError(t, err)

			got := resp.AssumeRoleResult.AssumedRoleUser.Arn
			assert.Equal(t, tt.wantArn, got, "assumed-role ARN must strip the IAM path")

			// The AssumedRoleId session suffix and the ARN session suffix must agree.
			assert.True(t,
				strings.HasSuffix(resp.AssumeRoleResult.AssumedRoleUser.AssumedRoleID, ":"+tt.sessionName),
				"AssumedRoleId must end with the session name",
			)

			// GetCallerIdentity on the issued key must echo the same path-stripped ARN.
			ci, err := b.GetCallerIdentity(
				resp.AssumeRoleResult.Credentials.AccessKeyID,
				resp.AssumeRoleResult.Credentials.SessionToken,
			)
			require.NoError(t, err)
			assert.Equal(t, tt.wantArn, ci.GetCallerIdentityResult.Arn)
		})
	}
}

// TestAssumedRoleArnPathStrippedWebIdentityAndSAML verifies the same
// path-stripping rule applies to AssumeRoleWithWebIdentity and
// AssumeRoleWithSAML, which share the assumed-role ARN construction with
// AssumeRole.
func TestAssumedRoleArnPathStrippedWebIdentityAndSAML(t *testing.T) {
	t.Parallel()

	const (
		acct    = "123456789012"
		roleArn = "arn:aws:iam::" + acct + ":role/svc/team/MyRole"
		wantArn = "arn:aws:sts::" + acct + ":assumed-role/MyRole/sess"
	)

	t.Run("web_identity", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		resp, err := b.AssumeRoleWithWebIdentity(&sts.AssumeRoleWithWebIdentityInput{
			RoleArn:          roleArn,
			RoleSessionName:  "sess",
			WebIdentityToken: buildJWT(t, map[string]any{"sub": "user", "iss": "https://example.com"}),
		})
		require.NoError(t, err)
		assert.Equal(t, wantArn, resp.AssumeRoleWithWebIdentityResult.AssumedRoleUser.Arn)
	})

	t.Run("saml", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		resp, err := b.AssumeRoleWithSAML(&sts.AssumeRoleWithSAMLInput{
			RoleArn:      roleArn,
			PrincipalArn: "arn:aws:iam::" + acct + ":saml-provider/Example",
			SAMLAssertion: buildSAMLAssertionWithAttributes(t, map[string]string{
				samlAttrRoleSessionNameTest: "sess",
			}),
		})
		require.NoError(t, err)
		assert.Equal(t, wantArn, resp.AssumeRoleWithSAMLResult.AssumedRoleUser.Arn)
	})
}
