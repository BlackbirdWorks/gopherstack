package sts_test

import (
	"encoding/base64"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sts"
)

// samlAssertionWithWindow builds a base64-encoded SAML assertion whose
// <Conditions> element declares the given validity window relative to now.
func samlAssertionWithWindow(t *testing.T, notBefore, notOnOrAfter time.Duration) string {
	t.Helper()

	now := time.Now().UTC()
	xmlDoc := `<samlp:Response xmlns:samlp="urn:oasis:names:tc:SAML:2.0:protocol">` +
		`<saml:Assertion xmlns:saml="urn:oasis:names:tc:SAML:2.0:assertion">` +
		`<saml:Conditions NotBefore="` + now.Add(notBefore).Format(time.RFC3339) +
		`" NotOnOrAfter="` + now.Add(notOnOrAfter).Format(time.RFC3339) + `"/>` +
		`</saml:Assertion></samlp:Response>`

	return base64.StdEncoding.EncodeToString([]byte(xmlDoc))
}

// TestAssumeRole_TrustPolicyPrincipal_EndToEnd verifies that AssumeRole enforces
// the target role's trust-policy Principal against the resolved caller ARN, while
// remaining permissive when no caller ARN is available.
func TestAssumeRole_TrustPolicyPrincipal_EndToEnd(t *testing.T) {
	t.Parallel()

	const (
		roleArn     = "arn:aws:iam::123456789012:role/Target"
		allowedArn  = "arn:aws:iam::123456789012:role/AppRole"
		callerAllow = "arn:aws:sts::123456789012:assumed-role/AppRole/session"
		callerDeny  = "arn:aws:sts::123456789012:assumed-role/OtherRole/session"
	)

	trustDoc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
		`"Principal":{"AWS":"` + allowedArn + `"},"Action":"sts:AssumeRole"}]}`

	tests := []struct {
		name      string
		callerArn string
		wantErr   bool
	}{
		{name: "permitted_caller", callerArn: callerAllow, wantErr: false},
		{name: "disallowed_caller_denied", callerArn: callerDeny, wantErr: true},
		{name: "no_caller_arn_permissive", callerArn: "", wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := sts.NewInMemoryBackend()
			backend.SetRoleLookup(&stubRoleLookup{meta: &sts.RoleMeta{TrustPolicy: trustDoc}})

			_, err := backend.AssumeRole(&sts.AssumeRoleInput{
				RoleArn:         roleArn,
				RoleSessionName: "session",
				CallerArn:       tt.callerArn,
			})

			if tt.wantErr {
				require.ErrorIs(t, err, sts.ErrAccessDenied)

				return
			}

			require.NoError(t, err)
		})
	}
}

// TestAssumeRole_TrustPolicyExternalIdWithPrincipal verifies that a statement
// grants only when both the Principal and the sts:ExternalId condition match.
func TestAssumeRole_TrustPolicyExternalIdWithPrincipal(t *testing.T) {
	t.Parallel()

	const (
		roleArn   = "arn:aws:iam::123456789012:role/Target"
		callerArn = "arn:aws:iam::123456789012:user/ci"
	)

	trustDoc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
		`"Principal":{"AWS":"` + callerArn + `"},"Action":"sts:AssumeRole",` +
		`"Condition":{"StringEquals":{"sts:ExternalId":"handshake"}}}]}`

	tests := []struct {
		name       string
		externalID string
		wantErr    bool
	}{
		{name: "principal_and_external_id_match", externalID: "handshake", wantErr: false},
		{name: "external_id_mismatch_denied", externalID: "nope", wantErr: true},
		{name: "external_id_missing_denied", externalID: "", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := sts.NewInMemoryBackend()
			backend.SetRoleLookup(&stubRoleLookup{meta: &sts.RoleMeta{TrustPolicy: trustDoc}})

			_, err := backend.AssumeRole(&sts.AssumeRoleInput{
				RoleArn:         roleArn,
				RoleSessionName: "session",
				CallerArn:       callerArn,
				ExternalID:      tt.externalID,
			})

			if tt.wantErr {
				require.ErrorIs(t, err, sts.ErrAccessDenied)

				return
			}

			require.NoError(t, err)
		})
	}
}

// TestAssumeRoleWithWebIdentity_TrustAndTokenValidation verifies both the
// federated OIDC trust-policy check and the JWT temporal validation.
func TestAssumeRoleWithWebIdentity_TrustAndTokenValidation(t *testing.T) {
	t.Parallel()

	const roleArn = "arn:aws:iam::123456789012:role/WebRole"

	trustDoc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
		`"Principal":{"Federated":"arn:aws:iam::123456789012:oidc-provider/accounts.google.com"},` +
		`"Action":"sts:AssumeRoleWithWebIdentity"}]}`

	future := time.Now().Add(time.Hour).Unix()
	past := time.Now().Add(-time.Hour).Unix()

	tests := []struct {
		wantErr error
		name    string
		token   string
	}{
		{
			name: "trusted_issuer_valid_token",
			token: buildJWT(
				t,
				map[string]any{
					"iss": "https://accounts.google.com",
					"sub": "u",
					"aud": "app",
					"exp": future,
				},
			),
			wantErr: nil,
		},
		{
			name: "untrusted_issuer_denied",
			token: buildJWT(
				t,
				map[string]any{
					"iss": "https://evil.example.com",
					"sub": "u",
					"aud": "app",
					"exp": future,
				},
			),
			wantErr: sts.ErrAccessDenied,
		},
		{
			name: "trusted_issuer_expired_token",
			token: buildJWT(
				t,
				map[string]any{
					"iss": "https://accounts.google.com",
					"sub": "u",
					"aud": "app",
					"exp": past,
				},
			),
			wantErr: sts.ErrExpiredToken,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := sts.NewInMemoryBackend()
			backend.SetRoleLookup(&stubRoleLookup{meta: &sts.RoleMeta{TrustPolicy: trustDoc}})

			_, err := backend.AssumeRoleWithWebIdentity(&sts.AssumeRoleWithWebIdentityInput{
				RoleArn:          roleArn,
				RoleSessionName:  "web-session",
				WebIdentityToken: tt.token,
			})

			if tt.wantErr == nil {
				require.NoError(t, err)

				return
			}

			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}

// TestAssumeRoleWithSAML_TrustAndTemporal verifies the federated SAML provider
// trust check and the assertion NotOnOrAfter window enforcement.
func TestAssumeRoleWithSAML_TrustAndTemporal(t *testing.T) {
	t.Parallel()

	const (
		roleArn        = "arn:aws:iam::123456789012:role/SAMLRole"
		trustedSAMLArn = "arn:aws:iam::123456789012:saml-provider/Okta"
		otherSAMLArn   = "arn:aws:iam::123456789012:saml-provider/Other"
	)

	trustDoc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
		`"Principal":{"Federated":"` + trustedSAMLArn + `"},"Action":"sts:AssumeRoleWithSAML"}]}`

	validAssertion := samlAssertionWithWindow(t, -time.Hour, time.Hour)
	expiredAssertion := samlAssertionWithWindow(t, -2*time.Hour, -time.Hour)

	tests := []struct {
		wantErr      error
		name         string
		principalArn string
		assertion    string
	}{
		{
			name:         "trusted_provider_valid_window",
			principalArn: trustedSAMLArn,
			assertion:    validAssertion,
			wantErr:      nil,
		},
		{
			name:         "untrusted_provider_denied",
			principalArn: otherSAMLArn,
			assertion:    validAssertion,
			wantErr:      sts.ErrAccessDenied,
		},
		{
			name:         "trusted_provider_expired_assertion",
			principalArn: trustedSAMLArn,
			assertion:    expiredAssertion,
			wantErr:      sts.ErrInvalidSAMLAssertion,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := sts.NewInMemoryBackend()
			backend.SetRoleLookup(&stubRoleLookup{meta: &sts.RoleMeta{TrustPolicy: trustDoc}})

			_, err := backend.AssumeRoleWithSAML(&sts.AssumeRoleWithSAMLInput{
				RoleArn:         roleArn,
				RoleSessionName: "saml-session",
				PrincipalArn:    tt.principalArn,
				SAMLAssertion:   tt.assertion,
			})

			if tt.wantErr == nil {
				require.NoError(t, err)

				return
			}

			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}

// TestAssumeRole_TrustPolicy_NoRoleLookup_Permissive confirms that without a
// wired RoleLookup, a resolved caller ARN never causes a denial (there is no
// trust policy to evaluate).
func TestAssumeRole_TrustPolicy_NoRoleLookup_Permissive(t *testing.T) {
	t.Parallel()

	backend := sts.NewInMemoryBackend()

	resp, err := backend.AssumeRole(&sts.AssumeRoleInput{
		RoleArn:         "arn:aws:iam::123456789012:role/Any",
		RoleSessionName: "session",
		CallerArn:       "arn:aws:iam::123456789012:user/anyone",
	})
	require.NoError(t, err)
	assert.NotEmpty(t, resp.AssumeRoleResult.Credentials.AccessKeyID)
}
