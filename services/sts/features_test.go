package sts_test

import (
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/services/sts"
)

// errStubRoleNotFound is the sentinel error returned by stubRoleLookup when a role is not found.
var errStubRoleNotFound = errors.New("role not found")

// ---- External ID validation tests ------------------------------------------

func TestAssumeRole_ExternalID_NotRequired(t *testing.T) {
	t.Parallel()

	// Role has no ExternalId condition: any call should succeed regardless of ExternalId.
	backend := sts.NewInMemoryBackend()
	backend.SetRoleLookup(&stubRoleLookup{
		meta: &sts.RoleMeta{
			TrustPolicy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"sts:AssumeRole"}]}`,
		},
	})

	resp, err := backend.AssumeRole(&sts.AssumeRoleInput{
		RoleArn:         "arn:aws:iam::123456789012:role/MyRole",
		RoleSessionName: "session",
	})
	require.NoError(t, err)
	assert.NotNil(t, resp)
}

func TestAssumeRole_ExternalID_MatchingValue(t *testing.T) {
	t.Parallel()

	trustDoc := `{
		"Version":"2012-10-17",
		"Statement":[{
			"Effect":"Allow",
			"Action":"sts:AssumeRole",
			"Condition":{
				"StringEquals":{"sts:ExternalId":"correct-id"}
			}
		}]
	}`

	backend := sts.NewInMemoryBackend()
	backend.SetRoleLookup(&stubRoleLookup{meta: &sts.RoleMeta{TrustPolicy: trustDoc}})

	resp, err := backend.AssumeRole(&sts.AssumeRoleInput{
		RoleArn:         "arn:aws:iam::123456789012:role/MyRole",
		RoleSessionName: "session",
		ExternalID:      "correct-id",
	})
	require.NoError(t, err)
	assert.NotNil(t, resp)
}

func TestAssumeRole_ExternalID_WrongValue(t *testing.T) {
	t.Parallel()

	trustDoc := `{
		"Version":"2012-10-17",
		"Statement":[{
			"Effect":"Allow",
			"Action":"sts:AssumeRole",
			"Condition":{
				"StringEquals":{"sts:ExternalId":"correct-id"}
			}
		}]
	}`

	backend := sts.NewInMemoryBackend()
	backend.SetRoleLookup(&stubRoleLookup{meta: &sts.RoleMeta{TrustPolicy: trustDoc}})

	_, err := backend.AssumeRole(&sts.AssumeRoleInput{
		RoleArn:         "arn:aws:iam::123456789012:role/MyRole",
		RoleSessionName: "session",
		ExternalID:      "wrong-id",
	})
	require.Error(t, err)
	require.ErrorIs(t, err, sts.ErrAccessDenied)
}

func TestAssumeRole_ExternalID_MissingWhenRequired(t *testing.T) {
	t.Parallel()

	trustDoc := `{
		"Version":"2012-10-17",
		"Statement":[{
			"Effect":"Allow",
			"Action":"sts:AssumeRole",
			"Condition":{
				"StringEquals":{"sts:ExternalId":"required-id"}
			}
		}]
	}`

	backend := sts.NewInMemoryBackend()
	backend.SetRoleLookup(&stubRoleLookup{meta: &sts.RoleMeta{TrustPolicy: trustDoc}})

	_, err := backend.AssumeRole(&sts.AssumeRoleInput{
		RoleArn:         "arn:aws:iam::123456789012:role/MyRole",
		RoleSessionName: "session",
		// ExternalID not provided
	})
	require.Error(t, err)
	require.ErrorIs(t, err, sts.ErrAccessDenied)
}

func TestAssumeRole_ExternalID_ArrayOfValues(t *testing.T) {
	t.Parallel()

	// Trust policy with multiple allowed ExternalId values.
	trustDoc := `{
		"Version":"2012-10-17",
		"Statement":[{
			"Effect":"Allow",
			"Action":"sts:AssumeRole",
			"Condition":{
				"StringEquals":{"sts:ExternalId":["id-one","id-two","id-three"]}
			}
		}]
	}`

	backend := sts.NewInMemoryBackend()
	backend.SetRoleLookup(&stubRoleLookup{meta: &sts.RoleMeta{TrustPolicy: trustDoc}})

	tests := []struct {
		name       string
		externalID string
		wantErr    bool
	}{
		{name: "first_allowed", externalID: "id-one", wantErr: false},
		{name: "second_allowed", externalID: "id-two", wantErr: false},
		{name: "third_allowed", externalID: "id-three", wantErr: false},
		{name: "not_in_list", externalID: "id-four", wantErr: true},
		{name: "empty", externalID: "", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := backend.AssumeRole(&sts.AssumeRoleInput{
				RoleArn:         "arn:aws:iam::123456789012:role/MyRole",
				RoleSessionName: "session",
				ExternalID:      tt.externalID,
			})

			if tt.wantErr {
				require.ErrorIs(t, err, sts.ErrAccessDenied)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestAssumeRole_ExternalID_MultipleStatements_ORSemantics(t *testing.T) {
	t.Parallel()

	// Two statements with different ExternalId conditions: the caller must match any one.
	trustDoc := `{
		"Version":"2012-10-17",
		"Statement":[
			{
				"Effect":"Allow",
				"Action":"sts:AssumeRole",
				"Condition":{"StringEquals":{"sts:ExternalId":"id-alpha"}}
			},
			{
				"Effect":"Allow",
				"Action":"sts:AssumeRole",
				"Condition":{"StringEquals":{"sts:ExternalId":"id-beta"}}
			}
		]
	}`

	tests := []struct {
		name       string
		externalID string
		wantErr    bool
	}{
		{name: "matches_first_statement", externalID: "id-alpha", wantErr: false},
		{name: "matches_second_statement", externalID: "id-beta", wantErr: false},
		{name: "matches_neither", externalID: "id-gamma", wantErr: true},
		{name: "empty_matches_neither", externalID: "", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := sts.NewInMemoryBackend()
			backend.SetRoleLookup(&stubRoleLookup{meta: &sts.RoleMeta{TrustPolicy: trustDoc}})

			_, err := backend.AssumeRole(&sts.AssumeRoleInput{
				RoleArn:         "arn:aws:iam::123456789012:role/MyRole",
				RoleSessionName: "session",
				ExternalID:      tt.externalID,
			})

			if tt.wantErr {
				require.ErrorIs(t, err, sts.ErrAccessDenied)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestAssumeRole_ExternalID_RoleLookupError(t *testing.T) {
	t.Parallel()

	// When the role cannot be found in the lookup, AssumeRole still succeeds
	// (the role may exist but not be in IAM — passthrough mode).
	backend := sts.NewInMemoryBackend()
	backend.SetRoleLookup(&stubRoleLookup{err: errStubRoleNotFound})

	resp, err := backend.AssumeRole(&sts.AssumeRoleInput{
		RoleArn:         "arn:aws:iam::123456789012:role/Unknown",
		RoleSessionName: "session",
	})
	require.NoError(t, err)
	assert.NotNil(t, resp)
}

func TestAssumeRole_ExternalID_EmptyTrustPolicy(t *testing.T) {
	t.Parallel()

	backend := sts.NewInMemoryBackend()
	backend.SetRoleLookup(&stubRoleLookup{meta: &sts.RoleMeta{TrustPolicy: ""}})

	resp, err := backend.AssumeRole(&sts.AssumeRoleInput{
		RoleArn:         "arn:aws:iam::123456789012:role/MyRole",
		RoleSessionName: "session",
	})
	require.NoError(t, err)
	assert.NotNil(t, resp)
}

func TestAssumeRole_ExternalID_MalformedTrustPolicy(t *testing.T) {
	t.Parallel()

	backend := sts.NewInMemoryBackend()
	backend.SetRoleLookup(&stubRoleLookup{meta: &sts.RoleMeta{TrustPolicy: "not-valid-json"}})

	// Malformed policy should not block the call.
	resp, err := backend.AssumeRole(&sts.AssumeRoleInput{
		RoleArn:         "arn:aws:iam::123456789012:role/MyRole",
		RoleSessionName: "session",
	})
	require.NoError(t, err)
	assert.NotNil(t, resp)
}

// ---- Duration enforcement tests --------------------------------------------

func TestAssumeRole_Duration_RespectRoleMaxSessionDuration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		duration int32
		wantErr  bool
	}{
		{name: "within_limit", duration: 900, wantErr: false},
		{name: "at_limit", duration: 1800, wantErr: false},
		{name: "exceeds_limit", duration: 3600, wantErr: true},
		// When DurationSeconds is 0, the default (3600) is used, which exceeds the 1800 max.
		{name: "default_exceeds_max", duration: 0, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend2 := sts.NewInMemoryBackend()
			backend2.SetRoleLookup(&stubRoleLookup{meta: &sts.RoleMeta{
				MaxSessionDuration: 1800,
			}})

			_, err := backend2.AssumeRole(&sts.AssumeRoleInput{
				RoleArn:         "arn:aws:iam::123456789012:role/MyRole",
				RoleSessionName: "session",
				DurationSeconds: tt.duration,
			})

			if tt.wantErr {
				require.ErrorIs(t, err, sts.ErrInvalidDuration)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestAssumeRole_Duration_NoRoleMaxUsesSystemDefault(t *testing.T) {
	t.Parallel()

	// When MaxSessionDuration is 0, the system default (43200) is used.
	backend := sts.NewInMemoryBackend()
	backend.SetRoleLookup(&stubRoleLookup{meta: &sts.RoleMeta{MaxSessionDuration: 0}})

	resp, err := backend.AssumeRole(&sts.AssumeRoleInput{
		RoleArn:         "arn:aws:iam::123456789012:role/MyRole",
		RoleSessionName: "session",
		DurationSeconds: 7200, // 2 hours — within system default 12 hours
	})
	require.NoError(t, err)
	assert.NotNil(t, resp)
}

// ---- Source identity tests -------------------------------------------------

func TestAssumeRole_SourceIdentity_ReturnedInResult(t *testing.T) {
	t.Parallel()

	backend := sts.NewInMemoryBackend()

	resp, err := backend.AssumeRole(&sts.AssumeRoleInput{
		RoleArn:         "arn:aws:iam::123456789012:role/MyRole",
		RoleSessionName: "session",
		SourceIdentity:  "admin-user",
	})
	require.NoError(t, err)
	assert.Equal(t, "admin-user", resp.AssumeRoleResult.SourceIdentity)
}

func TestAssumeRole_SourceIdentity_EmptyWhenNotProvided(t *testing.T) {
	t.Parallel()

	backend := sts.NewInMemoryBackend()

	resp, err := backend.AssumeRole(&sts.AssumeRoleInput{
		RoleArn:         "arn:aws:iam::123456789012:role/MyRole",
		RoleSessionName: "session",
	})
	require.NoError(t, err)
	assert.Empty(t, resp.AssumeRoleResult.SourceIdentity)
}

// ---- Session tags tests ----------------------------------------------------

func TestAssumeRole_SessionTags_StoredInSession(t *testing.T) {
	t.Parallel()

	backend := sts.NewInMemoryBackend()

	tags := []sts.Tag{
		{Key: "department", Value: "engineering"},
		{Key: "team", Value: "platform"},
	}

	resp, err := backend.AssumeRole(&sts.AssumeRoleInput{
		RoleArn:           "arn:aws:iam::123456789012:role/MyRole",
		RoleSessionName:   "session",
		Tags:              tags,
		TransitiveTagKeys: []string{"department"},
	})
	require.NoError(t, err)
	assert.NotNil(t, resp)

	// Credentials are returned; tags are stored in session.
	assert.NotEmpty(t, resp.AssumeRoleResult.Credentials.AccessKeyID)
}

// ---- GetCallerIdentity with assumed-role credentials tests -----------------

func TestGetCallerIdentity_AssumedRole_ReturnsAssumedRoleArn(t *testing.T) {
	t.Parallel()

	backend := sts.NewInMemoryBackend()

	assumeResp, err := backend.AssumeRole(&sts.AssumeRoleInput{
		RoleArn:         "arn:aws:iam::123456789012:role/TestRole",
		RoleSessionName: "my-session",
		SourceIdentity:  "caller",
	})
	require.NoError(t, err)

	accessKeyID := assumeResp.AssumeRoleResult.Credentials.AccessKeyID

	ciResp, err := backend.GetCallerIdentity(accessKeyID)
	require.NoError(t, err)

	assert.Equal(t, "123456789012", ciResp.GetCallerIdentityResult.Account)
	assert.Contains(t, ciResp.GetCallerIdentityResult.Arn, "assumed-role")
	assert.Contains(t, ciResp.GetCallerIdentityResult.Arn, "TestRole")
	assert.Contains(t, ciResp.GetCallerIdentityResult.Arn, "my-session")
	assert.Truef(t, strings.HasPrefix(ciResp.GetCallerIdentityResult.UserID, "AROA"),
		"expected UserID to start with AROA, got %s", ciResp.GetCallerIdentityResult.UserID)
	assert.Contains(t, ciResp.GetCallerIdentityResult.UserID, "my-session")
}

func TestGetCallerIdentity_UnknownAccessKey_ReturnsDefaultIdentity(t *testing.T) {
	t.Parallel()

	backend := sts.NewInMemoryBackend()

	resp, err := backend.GetCallerIdentity("ASIANOTISSUED1234567")
	require.NoError(t, err)

	// Falls back to default (root) identity.
	assert.Equal(t, sts.MockAccountID, resp.GetCallerIdentityResult.Account)
	assert.Equal(t, sts.MockUserArn, resp.GetCallerIdentityResult.Arn)
}

func TestGetCallerIdentity_EmptyAccessKey_ReturnsDefaultIdentity(t *testing.T) {
	t.Parallel()

	backend := sts.NewInMemoryBackend()

	resp, err := backend.GetCallerIdentity("")
	require.NoError(t, err)

	assert.Equal(t, sts.MockAccountID, resp.GetCallerIdentityResult.Account)
	assert.Equal(t, sts.MockUserArn, resp.GetCallerIdentityResult.Arn)
	assert.Equal(t, sts.MockUserID, resp.GetCallerIdentityResult.UserID)
}

// ---- Handler tests for new features ----------------------------------------

func TestHandler_AssumeRole_WithSourceIdentity(t *testing.T) {
	t.Parallel()

	h, e := newTestHandler(t)
	rec := postForm(t, e, h, url.Values{
		"Action":          {"AssumeRole"},
		"Version":         {"2011-06-15"},
		"RoleArn":         {"arn:aws:iam::123456789012:role/TestRole"},
		"RoleSessionName": {"session"},
		"SourceIdentity":  {"admin"},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var resp sts.AssumeRoleResponse
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "admin", resp.AssumeRoleResult.SourceIdentity)
}

func TestHandler_AssumeRole_WithSessionTags(t *testing.T) {
	t.Parallel()

	h, e := newTestHandler(t)
	rec := postForm(t, e, h, url.Values{
		"Action":                     {"AssumeRole"},
		"Version":                    {"2011-06-15"},
		"RoleArn":                    {"arn:aws:iam::123456789012:role/TestRole"},
		"RoleSessionName":            {"session"},
		"Tags.member.1.Key":          {"dept"},
		"Tags.member.1.Value":        {"eng"},
		"Tags.member.2.Key":          {"team"},
		"Tags.member.2.Value":        {"platform"},
		"TransitiveTagKeys.member.1": {"dept"},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var resp sts.AssumeRoleResponse
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp.AssumeRoleResult.Credentials.AccessKeyID)
}

func TestHandler_AssumeRole_AccessDenied(t *testing.T) {
	t.Parallel()

	backend := sts.NewInMemoryBackend()
	backend.SetRoleLookup(&stubRoleLookup{meta: &sts.RoleMeta{
		TrustPolicy: `{
			"Version":"2012-10-17",
			"Statement":[{
				"Effect":"Allow",
				"Action":"sts:AssumeRole",
				"Condition":{"StringEquals":{"sts:ExternalId":"required"}}
			}]
		}`,
	}})

	h := sts.NewHandler(backend)
	e := echo.New()

	body := url.Values{
		"Action":          {"AssumeRole"},
		"Version":         {"2011-06-15"},
		"RoleArn":         {"arn:aws:iam::123456789012:role/MyRole"},
		"RoleSessionName": {"session"},
	}.Encode()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req = req.WithContext(logger.Save(req.Context(), logger.NewTestLogger()))
	rec := httptest.NewRecorder()

	require.NoError(t, h.Handler()(e.NewContext(req, rec)))

	assert.Equal(t, http.StatusForbidden, rec.Code)

	var errResp sts.ErrorResponse
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "AccessDenied", errResp.Error.Code)
	assert.Equal(t, "Sender", errResp.Error.Type)
}

func TestHandler_GetCallerIdentity_WithAssumedRoleCredentials(t *testing.T) {
	t.Parallel()

	backend := sts.NewInMemoryBackend()
	h := sts.NewHandler(backend)
	e := echo.New()

	// First: AssumeRole to get a credential.
	assumeBody := url.Values{
		"Action":          {"AssumeRole"},
		"Version":         {"2011-06-15"},
		"RoleArn":         {"arn:aws:iam::123456789012:role/MyRole"},
		"RoleSessionName": {"my-session"},
	}.Encode()
	assumeReq := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(assumeBody))
	assumeReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	assumeReq = assumeReq.WithContext(logger.Save(assumeReq.Context(), logger.NewTestLogger()))
	assumeRec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(assumeReq, assumeRec)))
	require.Equal(t, http.StatusOK, assumeRec.Code)

	var assumeResp sts.AssumeRoleResponse
	require.NoError(t, xml.Unmarshal(assumeRec.Body.Bytes(), &assumeResp))
	accessKeyID := assumeResp.AssumeRoleResult.Credentials.AccessKeyID

	// Second: GetCallerIdentity with the assumed-role access key in Authorization.
	ciBody := "Action=GetCallerIdentity&Version=2011-06-15"
	ciReq := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(ciBody))
	ciReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	ciReq.Header.Set(
		"Authorization",
		fmt.Sprintf(
			"AWS4-HMAC-SHA256 Credential=%s/20230101/us-east-1/sts/aws4_request, SignedHeaders=host, Signature=abc",
			accessKeyID,
		),
	)
	ciReq = ciReq.WithContext(logger.Save(ciReq.Context(), logger.NewTestLogger()))
	ciRec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(ciReq, ciRec)))
	require.Equal(t, http.StatusOK, ciRec.Code)

	var ciResp sts.GetCallerIdentityResponse
	require.NoError(t, xml.Unmarshal(ciRec.Body.Bytes(), &ciResp))
	assert.Equal(t, "123456789012", ciResp.GetCallerIdentityResult.Account)
	assert.Contains(t, ciResp.GetCallerIdentityResult.Arn, "assumed-role")
	assert.Contains(t, ciResp.GetCallerIdentityResult.Arn, "MyRole")
	assert.Contains(t, ciResp.GetCallerIdentityResult.Arn, "my-session")
}

// ---- stubRoleLookup --------------------------------------------------------

// stubRoleLookup is a test double for sts.RoleLookup.
type stubRoleLookup struct {
	meta *sts.RoleMeta
	err  error
}

func (s *stubRoleLookup) GetRoleByArn(_ string) (*sts.RoleMeta, error) {
	if s.err != nil {
		return nil, s.err
	}

	return s.meta, nil
}
