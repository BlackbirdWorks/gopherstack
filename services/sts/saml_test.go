package sts_test

import (
	"encoding/base64"
	"encoding/xml"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sts"
)

// testSAMLAssertion is a minimal base64-encoded SAML XML fragment used across tests.
// Decodes to: <samlp:Assertion>.
const testSAMLAssertion = "PHNhbWxwOkFzc2VydGlvbj4="

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

// ---- AssumeRoleWithSAML tests -----------------------------------------------

func TestAssumeRoleWithSAML_Success(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input *sts.AssumeRoleWithSAMLInput
		name  string
	}{
		{
			name: "default_duration",
			input: &sts.AssumeRoleWithSAMLInput{
				RoleArn:       "arn:aws:iam::123456789012:role/SAMLRole",
				PrincipalArn:  "arn:aws:iam::123456789012:saml-provider/MySAMLIdP",
				SAMLAssertion: "PHNhbWxwOkFzc2VydGlvbj4NCjwvc2FtbHA6QXNzZXJ0aW9uPg==",
			},
		},
		{
			name: "custom_duration",
			input: &sts.AssumeRoleWithSAMLInput{
				RoleArn:         "arn:aws:iam::123456789012:role/SAMLRole",
				PrincipalArn:    "arn:aws:iam::123456789012:saml-provider/MySAMLIdP",
				SAMLAssertion:   "PHNhbWxwOkFzc2VydGlvbj4NCjwvc2FtbHA6QXNzZXJ0aW9uPg==",
				DurationSeconds: 1800,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sts.NewInMemoryBackend()
			resp, err := b.AssumeRoleWithSAML(tt.input)
			require.NoError(t, err)
			require.NotNil(t, resp)

			res := resp.AssumeRoleWithSAMLResult
			assert.True(t, strings.HasPrefix(res.Credentials.AccessKeyID, "ASIA"))
			assert.NotEmpty(t, res.Credentials.SecretAccessKey)
			assert.NotEmpty(t, res.Credentials.SessionToken)
			assert.NotEmpty(t, res.Credentials.Expiration)
			assert.Contains(t, res.AssumedRoleUser.Arn, "assumed-role")
			assert.Contains(t, res.AssumedRoleUser.Arn, "SAMLRole")
			assert.NotEmpty(t, res.Audience)
			assert.NotEmpty(t, res.Issuer)
			assert.NotEmpty(t, res.Subject)
			assert.NotEmpty(t, res.SubjectType)
			assert.NotEmpty(t, resp.ResponseMetadata.RequestID)
			assert.Equal(t, sts.STSNamespace, resp.Xmlns)
		})
	}
}

func TestAssumeRoleWithSAML_ValidationErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		input   *sts.AssumeRoleWithSAMLInput
		name    string
	}{
		{
			name: "missing_role_arn",
			input: &sts.AssumeRoleWithSAMLInput{
				PrincipalArn:  "arn:aws:iam::123456789012:saml-provider/MySAMLIdP",
				SAMLAssertion: "PHNhbWxwOkFzc2VydGlvbj4=",
			},
			wantErr: sts.ErrMissingRoleArn,
		},
		{
			name: "missing_principal_arn",
			input: &sts.AssumeRoleWithSAMLInput{
				RoleArn:       "arn:aws:iam::123456789012:role/SAMLRole",
				SAMLAssertion: "PHNhbWxwOkFzc2VydGlvbj4=",
			},
			wantErr: sts.ErrMissingPrincipalArn,
		},
		{
			name: "missing_saml_assertion",
			input: &sts.AssumeRoleWithSAMLInput{
				RoleArn:      "arn:aws:iam::123456789012:role/SAMLRole",
				PrincipalArn: "arn:aws:iam::123456789012:saml-provider/MySAMLIdP",
			},
			wantErr: sts.ErrMissingSAMLAssertion,
		},
		{
			name: "duration_too_short",
			input: &sts.AssumeRoleWithSAMLInput{
				RoleArn:         "arn:aws:iam::123456789012:role/SAMLRole",
				PrincipalArn:    "arn:aws:iam::123456789012:saml-provider/MySAMLIdP",
				SAMLAssertion:   "PHNhbWxwOkFzc2VydGlvbj4=",
				DurationSeconds: 100,
			},
			wantErr: sts.ErrInvalidDuration,
		},
		{
			name: "duration_too_long",
			input: &sts.AssumeRoleWithSAMLInput{
				RoleArn:         "arn:aws:iam::123456789012:role/SAMLRole",
				PrincipalArn:    "arn:aws:iam::123456789012:saml-provider/MySAMLIdP",
				SAMLAssertion:   "PHNhbWxwOkFzc2VydGlvbj4=",
				DurationSeconds: sts.MaxDurationSeconds + 1,
			},
			wantErr: sts.ErrInvalidDuration,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sts.NewInMemoryBackend()
			_, err := b.AssumeRoleWithSAML(tt.input)
			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}

func TestAssumeRoleWithSAML_SessionTrackedForCallerIdentity(t *testing.T) {
	t.Parallel()

	b := sts.NewInMemoryBackend()
	resp, err := b.AssumeRoleWithSAML(&sts.AssumeRoleWithSAMLInput{
		RoleArn:       "arn:aws:iam::123456789012:role/SAMLRole",
		PrincipalArn:  "arn:aws:iam::123456789012:saml-provider/MySAMLIdP",
		SAMLAssertion: "PHNhbWxwOkFzc2VydGlvbj4=",
	})
	require.NoError(t, err)

	accessKeyID := resp.AssumeRoleWithSAMLResult.Credentials.AccessKeyID

	ciResp, err := b.GetCallerIdentity(accessKeyID, "")
	require.NoError(t, err)
	assert.Contains(t, ciResp.GetCallerIdentityResult.Arn, "assumed-role")
}

func TestHandler_AssumeRoleWithSAML(t *testing.T) {
	t.Parallel()

	tests := []struct {
		formValues url.Values
		name       string
		wantCode   string
		wantStatus int
	}{
		{
			name: "success",
			formValues: url.Values{
				"Action":        {"AssumeRoleWithSAML"},
				"Version":       {"2011-06-15"},
				"RoleArn":       {"arn:aws:iam::123456789012:role/SAMLRole"},
				"PrincipalArn":  {"arn:aws:iam::123456789012:saml-provider/MySAMLIdP"},
				"SAMLAssertion": {"PHNhbWxwOkFzc2VydGlvbj4="},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "missing_principal_arn_returns_400",
			formValues: url.Values{
				"Action":        {"AssumeRoleWithSAML"},
				"Version":       {"2011-06-15"},
				"RoleArn":       {"arn:aws:iam::123456789012:role/SAMLRole"},
				"SAMLAssertion": {"PHNhbWxwOkFzc2VydGlvbj4="},
			},
			wantStatus: http.StatusBadRequest,
			wantCode:   "MissingParameter",
		},
		{
			name: "missing_saml_assertion_returns_400",
			formValues: url.Values{
				"Action":       {"AssumeRoleWithSAML"},
				"Version":      {"2011-06-15"},
				"RoleArn":      {"arn:aws:iam::123456789012:role/SAMLRole"},
				"PrincipalArn": {"arn:aws:iam::123456789012:saml-provider/MySAMLIdP"},
			},
			wantStatus: http.StatusBadRequest,
			wantCode:   "MissingParameter",
		},
		{
			name: "missing_role_arn_returns_400",
			formValues: url.Values{
				"Action":        {"AssumeRoleWithSAML"},
				"Version":       {"2011-06-15"},
				"PrincipalArn":  {"arn:aws:iam::123456789012:saml-provider/MySAMLIdP"},
				"SAMLAssertion": {"PHNhbWxwOkFzc2VydGlvbj4="},
			},
			wantStatus: http.StatusBadRequest,
			wantCode:   "MissingParameter",
		},
		{
			name: "invalid_duration_returns_400",
			formValues: url.Values{
				"Action":          {"AssumeRoleWithSAML"},
				"Version":         {"2011-06-15"},
				"RoleArn":         {"arn:aws:iam::123456789012:role/SAMLRole"},
				"PrincipalArn":    {"arn:aws:iam::123456789012:saml-provider/MySAMLIdP"},
				"SAMLAssertion":   {"PHNhbWxwOkFzc2VydGlvbj4="},
				"DurationSeconds": {"100"},
			},
			wantStatus: http.StatusBadRequest,
			wantCode:   "ValidationError",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sts.NewInMemoryBackend()
			h := sts.NewHandler(b)
			e := echo.New()

			rec := postForm(t, e, h, tt.formValues)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantCode != "" {
				var errResp sts.ErrorResponse
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
				assert.Equal(t, tt.wantCode, errResp.Error.Code)
			} else {
				var resp sts.AssumeRoleWithSAMLResponse
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp.AssumeRoleWithSAMLResult.Credentials.AccessKeyID)
				assert.Contains(t, resp.AssumeRoleWithSAMLResult.AssumedRoleUser.Arn, "assumed-role")
			}
		})
	}
}

// TestAssumeRoleWithSAMLNameQualifier verifies NameQualifier is non-empty in SAML response.
func TestAssumeRoleWithSAMLNameQualifier(t *testing.T) {
	t.Parallel()

	b := sts.NewInMemoryBackend()
	input := &sts.AssumeRoleWithSAMLInput{
		RoleArn:       "arn:aws:iam::000000000000:role/test-role",
		PrincipalArn:  "arn:aws:iam::000000000000:saml-provider/MyIdP",
		SAMLAssertion: "PHNhbWxwOkFzc2VydGlvbj4=",
	}

	resp, err := b.AssumeRoleWithSAML(input)
	require.NoError(t, err)
	assert.NotEmpty(t, resp.AssumeRoleWithSAMLResult.NameQualifier)
}

// TestAssumeRoleWithSAMLSessionName verifies session name uses input.RoleSessionName.
func TestAssumeRoleWithSAMLSessionName(t *testing.T) {
	t.Parallel()

	b := sts.NewInMemoryBackend()
	input := &sts.AssumeRoleWithSAMLInput{
		RoleArn:         "arn:aws:iam::000000000000:role/test-role",
		PrincipalArn:    "arn:aws:iam::000000000000:saml-provider/MyIdP",
		SAMLAssertion:   "PHNhbWxwOkFzc2VydGlvbj4=",
		RoleSessionName: "my-saml-session",
	}

	resp, err := b.AssumeRoleWithSAML(input)
	require.NoError(t, err)
	assert.Contains(t, resp.AssumeRoleWithSAMLResult.AssumedRoleUser.Arn, "my-saml-session")
}

// TestAssumeRoleWithSAMLSourceIdentity verifies SourceIdentity flows through SAML.
func TestAssumeRoleWithSAMLSourceIdentity(t *testing.T) {
	t.Parallel()

	b := sts.NewInMemoryBackend()
	resp, err := b.AssumeRoleWithSAML(&sts.AssumeRoleWithSAMLInput{
		RoleArn:        "arn:aws:iam::000000000000:role/test-role",
		PrincipalArn:   "arn:aws:iam::000000000000:saml-provider/MyIdP",
		SAMLAssertion:  "PHNhbWxwOkFzc2VydGlvbj4=",
		SourceIdentity: "my-saml-identity",
	})
	require.NoError(t, err)
	assert.Equal(t, "my-saml-identity", resp.AssumeRoleWithSAMLResult.SourceIdentity)
}

// TestAssumeRoleWithSAMLWithPolicyArns verifies PolicyArns parsed in SAML request.
func TestAssumeRoleWithSAMLWithPolicyArns(t *testing.T) {
	t.Parallel()

	b := sts.NewInMemoryBackend()
	h := sts.NewHandler(b)

	rec := r1PostForm(t, h, url.Values{
		"Action":                  {"AssumeRoleWithSAML"},
		"Version":                 {"2011-06-15"},
		"RoleArn":                 {"arn:aws:iam::000000000000:role/test-role"},
		"PrincipalArn":            {"arn:aws:iam::000000000000:saml-provider/MyIdP"},
		"SAMLAssertion":           {"PHNhbWxwOkFzc2VydGlvbj4="},
		"PolicyArns.member.1.arn": {"arn:aws:iam::aws:policy/ReadOnlyAccess"},
	})

	assert.Equal(t, http.StatusOK, rec.Code)

	var result sts.AssumeRoleWithSAMLResponse
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &result))
	assert.NotEmpty(t, result.AssumeRoleWithSAMLResult.Credentials.AccessKeyID)
}

// TestAssumeRoleWithSAML_Tags exercises Tags support for AssumeRoleWithSAML.
func TestAssumeRoleWithSAML_Tags(t *testing.T) {
	t.Parallel()

	t.Run("aws_prefix_tag_rejected", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		_, err := b.AssumeRoleWithSAML(&sts.AssumeRoleWithSAMLInput{
			RoleArn:       "arn:aws:iam::123456789012:role/R",
			PrincipalArn:  "arn:aws:iam::123456789012:saml-provider/MyIdP",
			SAMLAssertion: "PHNhbWxwOkFzc2VydGlvbj4=",
			Tags:          []sts.Tag{{Key: "aws:reserved", Value: "v"}},
		})
		require.ErrorIs(t, err, sts.ErrInvalidTagKey)
	})

	t.Run("duplicate_tag_key_rejected", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		_, err := b.AssumeRoleWithSAML(&sts.AssumeRoleWithSAMLInput{
			RoleArn:       "arn:aws:iam::123456789012:role/R",
			PrincipalArn:  "arn:aws:iam::123456789012:saml-provider/MyIdP",
			SAMLAssertion: "PHNhbWxwOkFzc2VydGlvbj4=",
			Tags:          []sts.Tag{{Key: "k", Value: "v1"}, {Key: "K", Value: "v2"}},
		})
		require.ErrorIs(t, err, sts.ErrInvalidTagKey)
	})

	t.Run("valid_tags_stored_in_session", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		resp, err := b.AssumeRoleWithSAML(&sts.AssumeRoleWithSAMLInput{
			RoleArn:       "arn:aws:iam::123456789012:role/R",
			PrincipalArn:  "arn:aws:iam::123456789012:saml-provider/MyIdP",
			SAMLAssertion: "PHNhbWxwOkFzc2VydGlvbj4=",
			Tags:          []sts.Tag{{Key: "team", Value: "eng"}},
		})
		require.NoError(t, err)

		// Verify session stores the tags via GetCallerIdentity lookup.
		key := resp.AssumeRoleWithSAMLResult.Credentials.AccessKeyID
		ci, err := b.GetCallerIdentity(key, "")
		require.NoError(t, err)
		assert.Contains(t, ci.GetCallerIdentityResult.Arn, "assumed-role")
	})
}

// TestAssumeRoleWithSAML_PrincipalArnValidation exercises PrincipalArn shape validation.
func TestAssumeRoleWithSAML_PrincipalArnValidation(t *testing.T) {
	t.Parallel()

	t.Run("valid_saml_provider_arn_accepted", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		_, err := b.AssumeRoleWithSAML(&sts.AssumeRoleWithSAMLInput{
			RoleArn:       "arn:aws:iam::123456789012:role/R",
			PrincipalArn:  "arn:aws:iam::123456789012:saml-provider/MyIdP",
			SAMLAssertion: "PHNhbWxwOkFzc2VydGlvbj4=",
		})
		require.NoError(t, err)
	})

	t.Run("non_saml_provider_arn_rejected", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		_, err := b.AssumeRoleWithSAML(&sts.AssumeRoleWithSAMLInput{
			RoleArn:       "arn:aws:iam::123456789012:role/R",
			PrincipalArn:  "arn:aws:iam::123456789012:role/NotASAMLProvider",
			SAMLAssertion: "PHNhbWxwOkFzc2VydGlvbj4=",
		})
		require.ErrorIs(t, err, sts.ErrInvalidPrincipalArn)
	})

	t.Run("plain_string_principal_rejected", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		_, err := b.AssumeRoleWithSAML(&sts.AssumeRoleWithSAMLInput{
			RoleArn:       "arn:aws:iam::123456789012:role/R",
			PrincipalArn:  "not-an-arn",
			SAMLAssertion: "PHNhbWxwOkFzc2VydGlvbj4=",
		})
		require.ErrorIs(t, err, sts.ErrInvalidPrincipalArn)
	})
}

// TestAssumeRoleWithSAML_RoleSessionName exercises RoleSessionName validation for SAML.
func TestAssumeRoleWithSAML_RoleSessionName(t *testing.T) {
	t.Parallel()

	t.Run("valid_session_name_accepted", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		_, err := b.AssumeRoleWithSAML(&sts.AssumeRoleWithSAMLInput{
			RoleArn:         "arn:aws:iam::123456789012:role/R",
			PrincipalArn:    "arn:aws:iam::123456789012:saml-provider/MyIdP",
			SAMLAssertion:   "PHNhbWxwOkFzc2VydGlvbj4=",
			RoleSessionName: "my-session",
		})
		require.NoError(t, err)
	})

	t.Run("colon_in_session_name_rejected", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		_, err := b.AssumeRoleWithSAML(&sts.AssumeRoleWithSAMLInput{
			RoleArn:         "arn:aws:iam::123456789012:role/R",
			PrincipalArn:    "arn:aws:iam::123456789012:saml-provider/MyIdP",
			SAMLAssertion:   "PHNhbWxwOkFzc2VydGlvbj4=",
			RoleSessionName: "bad:session",
		})
		require.ErrorIs(t, err, sts.ErrInvalidSessionName)
	})

	t.Run("empty_session_name_accepted_uses_default", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		resp, err := b.AssumeRoleWithSAML(&sts.AssumeRoleWithSAMLInput{
			RoleArn:       "arn:aws:iam::123456789012:role/R",
			PrincipalArn:  "arn:aws:iam::123456789012:saml-provider/MyIdP",
			SAMLAssertion: "PHNhbWxwOkFzc2VydGlvbj4=",
		})
		require.NoError(t, err)
		assert.NotEmpty(t, resp.AssumeRoleWithSAMLResult.Credentials.AccessKeyID)
	})
}

// TestAssumeRoleWithSAML_PolicyArnsValidation exercises PolicyArns validation for SAML.
func TestAssumeRoleWithSAML_PolicyArnsValidation(t *testing.T) {
	t.Parallel()

	t.Run("valid_aws_policy_arn_accepted", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		_, err := b.AssumeRoleWithSAML(&sts.AssumeRoleWithSAMLInput{
			RoleArn:       "arn:aws:iam::123456789012:role/R",
			PrincipalArn:  "arn:aws:iam::123456789012:saml-provider/MyIdP",
			SAMLAssertion: "PHNhbWxwOkFzc2VydGlvbj4=",
			PolicyArns:    []string{"arn:aws:iam::aws:policy/ReadOnlyAccess"},
		})
		require.NoError(t, err)
	})

	t.Run("too_many_policy_arns_rejected", func(t *testing.T) {
		t.Parallel()

		arns := make([]string, 11)
		for i := range arns {
			arns[i] = "arn:aws:iam::aws:policy/P"
		}

		b := sts.NewInMemoryBackend()
		_, err := b.AssumeRoleWithSAML(&sts.AssumeRoleWithSAMLInput{
			RoleArn:       "arn:aws:iam::123456789012:role/R",
			PrincipalArn:  "arn:aws:iam::123456789012:saml-provider/MyIdP",
			SAMLAssertion: "PHNhbWxwOkFzc2VydGlvbj4=",
			PolicyArns:    arns,
		})
		require.ErrorIs(t, err, sts.ErrTooManyPolicyArns)
	})
}

// TestAssumeRoleWithSAML_PackedPolicySizeWithArns verifies PackedPolicySize includes PolicyArns for SAML.
func TestAssumeRoleWithSAML_PackedPolicySizeWithArns(t *testing.T) {
	t.Parallel()

	b := sts.NewInMemoryBackend()
	resp, err := b.AssumeRoleWithSAML(&sts.AssumeRoleWithSAMLInput{
		RoleArn:       "arn:aws:iam::123456789012:role/R",
		PrincipalArn:  "arn:aws:iam::123456789012:saml-provider/MyIdP",
		SAMLAssertion: "PHNhbWxwOkFzc2VydGlvbj4=",
		PolicyArns:    []string{"arn:aws:iam::aws:policy/ReadOnlyAccess"},
	})
	require.NoError(t, err)
	assert.Positive(t, resp.AssumeRoleWithSAMLResult.PackedPolicySize)
}

// TestAssumeRoleWithSAML_MalformedPolicy verifies a malformed inline policy is rejected.
func TestAssumeRoleWithSAML_MalformedPolicy(t *testing.T) {
	t.Parallel()

	b := sts.NewInMemoryBackend()
	_, err := b.AssumeRoleWithSAML(&sts.AssumeRoleWithSAMLInput{
		RoleArn:       "arn:aws:iam::123456789012:role/R",
		PrincipalArn:  "arn:aws:iam::123456789012:saml-provider/MyIdP",
		SAMLAssertion: "PHNhbWxwOkFzc2VydGlvbj4=",
		Policy:        "not-json",
	})
	require.ErrorIs(t, err, sts.ErrMalformedPolicyDocument)
}

// TestAssumeRoleWithSAMLSAMLAssertionValidation exercises base64 + XML validation of SAMLAssertion.
func TestAssumeRoleWithSAMLSAMLAssertionValidation(t *testing.T) {
	t.Parallel()

	validRoleArn := "arn:aws:iam::123456789012:role/R"
	validPrincipalArn := "arn:aws:iam::123456789012:saml-provider/MyIdP"

	tests := []struct {
		wantErr       error
		name          string
		samlAssertion string
		wantSuccess   bool
	}{
		{
			name:          "not_base64_rejected",
			samlAssertion: "not!!base64###",
			wantErr:       sts.ErrInvalidSAMLAssertion,
		},
		{
			name:          "bare_word_rejected",
			samlAssertion: "assertion",
			wantErr:       sts.ErrInvalidSAMLAssertion,
		},
		{
			name:          "valid_base64_non_xml_accepted",
			samlAssertion: "dGVzdA==", // "test" — valid base64; emulator accepts non-XML payloads
			wantErr:       nil,
		},
		{
			name:          "valid_base64_xml_accepted",
			samlAssertion: testSAMLAssertion, // <samlp:Assertion>
			wantSuccess:   true,
		},
		{
			name:          "full_saml_response_accepted",
			samlAssertion: "PHNhbWxwOlJlc3BvbnNlIHhtbG5zOnNhbWxwPSJ1cm46b2FzaXM6bmFtZXM6dGM6U0FNTDoyLjA6cHJvdG9jb2wiLz4=",
			wantSuccess:   true,
		},
		{
			name:          "empty_assertion_returns_missing_error",
			samlAssertion: "",
			wantErr:       sts.ErrMissingSAMLAssertion,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sts.NewInMemoryBackend()
			_, err := b.AssumeRoleWithSAML(&sts.AssumeRoleWithSAMLInput{
				RoleArn:       validRoleArn,
				PrincipalArn:  validPrincipalArn,
				SAMLAssertion: tt.samlAssertion,
			})

			if tt.wantSuccess {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tt.wantErr)
			}
		})
	}
}

// TestAssumeRoleWithSAMLSAMLAssertionViaHandler exercises SAMLAssertion validation via the HTTP handler.
func TestAssumeRoleWithSAMLSAMLAssertionViaHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		samlAssertion string
		wantError     string
		wantCode      int
	}{
		{
			name:          "invalid_base64_returns_400_InvalidIdentityToken",
			samlAssertion: "not!!base64",
			wantCode:      http.StatusBadRequest,
			wantError:     "InvalidIdentityToken",
		},
		{
			name:          "valid_base64_non_xml_returns_200",
			samlAssertion: "dGVzdA==",
			wantCode:      http.StatusOK,
		},
		{
			name:          "valid_saml_xml_returns_200",
			samlAssertion: testSAMLAssertion,
			wantCode:      http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _, e := accuracyHandler(t)
			form := url.Values{
				"Action":        {"AssumeRoleWithSAML"},
				"Version":       {"2011-06-15"},
				"RoleArn":       {"arn:aws:iam::123456789012:role/R"},
				"PrincipalArn":  {"arn:aws:iam::123456789012:saml-provider/MyIdP"},
				"SAMLAssertion": {tt.samlAssertion},
			}
			rec := accuracyPost(t, h, e, form)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantError != "" {
				errResp := decodeError(t, rec.Body.Bytes())
				assert.Equal(t, tt.wantError, errResp.Error.Code)
			}
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

// TestAssumeRoleWithSAMLResultBeforeMetadata verifies the XML result element precedes ResponseMetadata.
func TestAssumeRoleWithSAMLResultBeforeMetadata(t *testing.T) {
	t.Parallel()

	h, _, e := accuracyHandler(t)
	form := url.Values{
		"Action":        {"AssumeRoleWithSAML"},
		"Version":       {"2011-06-15"},
		"RoleArn":       {"arn:aws:iam::123456789012:role/R"},
		"PrincipalArn":  {"arn:aws:iam::123456789012:saml-provider/MyIdP"},
		"SAMLAssertion": {"PHNhbWxwOkFzc2VydGlvbj4="},
	}
	rec := accuracyPost(t, h, e, form)
	require.Equal(t, http.StatusOK, rec.Code)

	order := xmlElementOrder(t, rec.Body.Bytes())
	require.Len(t, order, 2)
	assert.Equal(t, "AssumeRoleWithSAMLResult", order[0])
	assert.Equal(t, "ResponseMetadata", order[1])
}

// TestAssumeRoleWithSAMLRespectsRoleMaxSessionDuration verifies AssumeRoleWithSAML
// clamps to and enforces the role's MaxSessionDuration.
func TestAssumeRoleWithSAMLRespectsRoleMaxSessionDuration(t *testing.T) {
	t.Parallel()

	t.Run("default_clamped_to_role_max", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		b.SetRoleLookup(&stubRoleLookup{meta: &sts.RoleMeta{MaxSessionDuration: 900}})

		resp, err := b.AssumeRoleWithSAML(&sts.AssumeRoleWithSAMLInput{
			RoleArn:       "arn:aws:iam::123456789012:role/SmallMaxRole",
			PrincipalArn:  "arn:aws:iam::123456789012:saml-provider/MyIdP",
			SAMLAssertion: "PHNhbWxwOkFzc2VydGlvbj4=",
		})
		require.NoError(t, err)
		assert.NotEmpty(t, resp.AssumeRoleWithSAMLResult.Credentials.AccessKeyID)
	})

	t.Run("explicit_duration_exceeding_role_max_rejected", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		b.SetRoleLookup(&stubRoleLookup{meta: &sts.RoleMeta{MaxSessionDuration: 900}})

		_, err := b.AssumeRoleWithSAML(&sts.AssumeRoleWithSAMLInput{
			RoleArn:         "arn:aws:iam::123456789012:role/SmallMaxRole",
			PrincipalArn:    "arn:aws:iam::123456789012:saml-provider/MyIdP",
			SAMLAssertion:   "PHNhbWxwOkFzc2VydGlvbj4=",
			DurationSeconds: 1800,
		})
		require.ErrorIs(t, err, sts.ErrInvalidDuration)
	})

	t.Run("no_role_lookup_uses_global_max", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		resp, err := b.AssumeRoleWithSAML(&sts.AssumeRoleWithSAMLInput{
			RoleArn:         "arn:aws:iam::123456789012:role/R",
			PrincipalArn:    "arn:aws:iam::123456789012:saml-provider/MyIdP",
			SAMLAssertion:   "PHNhbWxwOkFzc2VydGlvbj4=",
			DurationSeconds: sts.MaxDurationSeconds,
		})
		require.NoError(t, err)
		assert.NotEmpty(t, resp.AssumeRoleWithSAMLResult.Credentials.AccessKeyID)
	})
}
