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

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	stssdk "github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/services/sts"
)

// ---- Shared test helpers used across the family test files -----------------
// These build/post through a Handler or Echo context; they are intentionally
// kept as distinct helpers (rather than unified into one) since each was
// authored against a slightly different calling convention and unifying them
// would require touching every call site across the package.

// newTestHandler builds a fresh in-memory-backed Handler and Echo instance.
func newTestHandler(t *testing.T) (*sts.Handler, *echo.Echo) {
	t.Helper()

	backend := sts.NewInMemoryBackend()

	h := sts.NewHandler(backend)
	e := echo.New()

	return h, e
}

// postForm posts URL-encoded form values through the handler and returns the recorder.
func postForm(t *testing.T, e *echo.Echo, h *sts.Handler, values url.Values) *httptest.ResponseRecorder {
	t.Helper()

	body := values.Encode()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	return rec
}

// r1PostForm is a test helper that posts form values through the STS handler.
func r1PostForm(t *testing.T, h *sts.Handler, values url.Values) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()
	body := values.Encode()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	return rec
}

// accuracyHandler builds a fresh backend, handler, and Echo instance.
func accuracyHandler(t *testing.T) (*sts.Handler, *sts.InMemoryBackend, *echo.Echo) {
	t.Helper()

	b := sts.NewInMemoryBackend()
	h := sts.NewHandler(b)
	e := echo.New()

	return h, b, e
}

// accuracyPost posts a form through the handler with a logger-bearing context.
func accuracyPost(t *testing.T, h *sts.Handler, e *echo.Echo, form url.Values) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	req = req.WithContext(logger.Save(req.Context(), nil))

	require.NoError(t, h.Handler()(e.NewContext(req, rec)))

	return rec
}

// decodeError unmarshals an STS XML error envelope.
func decodeError(t *testing.T, body []byte) sts.ErrorResponse {
	t.Helper()

	var resp sts.ErrorResponse
	require.NoError(t, xml.Unmarshal(body, &resp))

	return resp
}

// xmlElementOrder parses raw XML and returns the top-level child element names in order.
// Used to verify Result-before-ResponseMetadata field ordering across every operation.
func xmlElementOrder(t *testing.T, data []byte) []string {
	t.Helper()

	d := xml.NewDecoder(strings.NewReader(string(data)))

	var order []string

	depth := 0

	for {
		tok, err := d.Token()
		if err != nil {
			break
		}

		switch se := tok.(type) {
		case xml.StartElement:
			depth++
			if depth == 2 {
				order = append(order, se.Name.Local)
			}
		case xml.EndElement:
			depth--
		}
	}

	return order
}

// buildSTSClient creates an AWS STS SDK client pointed at the given endpoint URL.
func buildSTSClient(t *testing.T, endpoint string) *stssdk.Client {
	t.Helper()

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("dummy", "dummy", "")),
	)
	require.NoError(t, err)

	return stssdk.NewFromConfig(cfg, func(o *stssdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// ---- Core routing / dispatch tests ------------------------------------------

func TestHandler_UnknownAction(t *testing.T) {
	t.Parallel()

	h, e := newTestHandler(t)
	rec := postForm(t, e, h, url.Values{
		"Action": {"UnknownOperation"},
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp sts.ErrorResponse
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "InvalidAction", errResp.Error.Code)
}

func TestHandler_MissingAction(t *testing.T) {
	t.Parallel()

	h, e := newTestHandler(t)
	rec := postForm(t, e, h, url.Values{"Version": {"2011-06-15"}})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_MethodNotAllowed(t *testing.T) {
	t.Parallel()

	h, e := newTestHandler(t)
	req := httptest.NewRequest(http.MethodPut, "/", nil)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	require.NoError(t, h.Handler()(c))
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestHandler_GetRequest_ListsOperations(t *testing.T) {
	t.Parallel()

	h, e := newTestHandler(t)
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	require.NoError(t, h.Handler()(c))
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "AssumeRole")
}

func TestHandler_Name(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)
	assert.Equal(t, "STS", h.Name())
}

func TestHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)
	assert.Equal(t, 90, h.MatchPriority())
}

func TestHandler_RouteMatcher_Matches(t *testing.T) {
	t.Parallel()

	h, e := newTestHandler(t)
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("Version=2011-06-15"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded; charset=utf-8")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	assert.True(t, h.RouteMatcher()(c))
}

func TestHandler_RouteMatcher_NoMatch(t *testing.T) {
	t.Parallel()

	h, e := newTestHandler(t)
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	assert.False(t, h.RouteMatcher()(c))
}

// TestHandler_RouteMatcher_ExcludesDashboard ensures that browser form POSTs to
// dashboard paths are not intercepted by the STS handler (they have the same
// Content-Type but should be served by the Dashboard handler instead).
func TestHandler_RouteMatcher_ExcludesDashboard(t *testing.T) {
	t.Parallel()

	h, e := newTestHandler(t)
	paths := []string{"/dashboard", "/dashboard/", "/dashboard/dynamodb/tables", "/dashboard/sts"}

	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodPost, path, nil)
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.False(t, h.RouteMatcher()(c), "STS should not match dashboard path %s", path)
		})
	}
}

// TestRouteMatcher_ContentTypeCaseInsensitive verifies the service routes
// correctly with an uppercase Content-Type header (validated indirectly via
// the existing HTTP tests, which all use the standard-case content type; the
// RouteMatcher itself lowercases before comparing).
func TestRouteMatcher_ContentTypeCaseInsensitive(t *testing.T) {
	t.Parallel()

	h, e := newTestHandler(t)
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("Version=2011-06-15"))
	req.Header.Set("Content-Type", "APPLICATION/X-WWW-FORM-URLENCODED")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	assert.True(t, h.RouteMatcher()(c))
}

func TestHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     string
		expected string
	}{
		{body: "Action=GetCallerIdentity&Version=2011-06-15", expected: "GetCallerIdentity"},
		{body: "Action=AssumeRole&RoleArn=arn:aws:iam::123:role/X", expected: "AssumeRole"},
		{body: "", expected: "Unknown"},
		{body: "Version=2011-06-15", expected: "Unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			t.Parallel()

			h, e := newTestHandler(t)
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tt.body))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.expected, h.ExtractOperation(c))
		})
	}
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	h, e := newTestHandler(t)

	roleArn := "arn:aws:iam::123456789012:role/TestRole"
	body := fmt.Sprintf("Action=AssumeRole&RoleArn=%s&RoleSessionName=sess",
		url.QueryEscape(roleArn))
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	resource := h.ExtractResource(c)
	assert.Equal(t, roleArn, resource)
}

func TestHandler_ExtractResource_Empty(t *testing.T) {
	t.Parallel()

	h, e := newTestHandler(t)
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("Action=GetCallerIdentity"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	assert.Empty(t, h.ExtractResource(c))
}

func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "AssumeRole")
	assert.Contains(t, ops, "GetCallerIdentity")
}

// TestSTSHandler_ViaSDK exercises the handler using the real AWS STS SDK client.
func TestSTSHandler_ViaSDK(t *testing.T) {
	t.Parallel()

	// Build an in-process server serving the STS handler.
	backend := sts.NewInMemoryBackend()

	h := sts.NewHandler(backend)
	e := echo.New()
	e.Use(func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			ct := c.Request().Header.Get("Content-Type")
			if strings.Contains(ct, "application/x-www-form-urlencoded") {
				return h.Handler()(c)
			}

			return next(c)
		}
	})

	server := httptest.NewServer(e)
	defer server.Close()

	// Build the STS client pointing at the test server.
	stsClient := buildSTSClient(t, server.URL)

	// GetCallerIdentity
	idOut, err := stsClient.GetCallerIdentity(
		t.Context(),
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, sts.MockAccountID, *idOut.Account)

	// AssumeRole
	roleOut, err := stsClient.AssumeRole(t.Context(), &stssdk.AssumeRoleInput{
		RoleArn:         aws.String("arn:aws:iam::123456789012:role/TestRole"),
		RoleSessionName: aws.String("sdk-test"),
	})
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(*roleOut.Credentials.AccessKeyId, "ASIA"))
}

// ---- Error-mapping / error-path tests ---------------------------------------

// TestHandleErrorRequestIDNotZero verifies errors return a non-zero request ID.
func TestHandleErrorRequestIDNotZero(t *testing.T) {
	t.Parallel()

	b := sts.NewInMemoryBackend()
	h := sts.NewHandler(b)

	rec := r1PostForm(t, h, url.Values{
		"Action":  {"AssumeRole"},
		"Version": {"2011-06-15"},
		// Missing RoleArn and RoleSessionName.
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp sts.ErrorResponse
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.NotEqual(t, "00000000-0000-0000-0000-000000000000", errResp.RequestID)
	assert.NotEmpty(t, errResp.RequestID)
}

// TestErrorMappingMissingParam verifies error mapping for missing params.
func TestErrorMappingMissingParam(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		values     url.Values
		wantCode   string
		wantStatus int
	}{
		{
			name: "missing_role_arn",
			values: url.Values{
				"Action":          {"AssumeRole"},
				"Version":         {"2011-06-15"},
				"RoleSessionName": {"session"},
			},
			wantCode:   "MissingParameter",
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "invalid_action",
			values: url.Values{
				"Action":  {"NonExistentAction"},
				"Version": {"2011-06-15"},
			},
			wantCode:   "InvalidAction",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sts.NewInMemoryBackend()
			h := sts.NewHandler(b)

			rec := r1PostForm(t, h, tt.values)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var errResp sts.ErrorResponse
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
			assert.Equal(t, tt.wantCode, errResp.Error.Code)
		})
	}
}

// TestErrorCodes verifies error-code mapping for a selection of validation failures.
func TestErrorCodes(t *testing.T) {
	t.Parallel()

	t.Run("malformed_policy_returns_MalformedPolicyDocument", func(t *testing.T) {
		t.Parallel()

		h, _, e := accuracyHandler(t)
		form := url.Values{
			"Action":          {"AssumeRole"},
			"Version":         {"2011-06-15"},
			"RoleArn":         {"arn:aws:iam::123456789012:role/R"},
			"RoleSessionName": {"session"},
			"Policy":          {"not-valid-json"},
		}
		rec := accuracyPost(t, h, e, form)
		assert.Equal(t, http.StatusBadRequest, rec.Code)

		errResp := decodeError(t, rec.Body.Bytes())
		assert.Equal(t, "MalformedPolicyDocument", errResp.Error.Code)
	})

	t.Run("policy_too_large_returns_PackedPolicyTooLarge", func(t *testing.T) {
		t.Parallel()

		h, _, e := accuracyHandler(t)
		// Build a valid JSON string that exceeds the 2048-byte session-policy limit.
		bigPolicy := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"` +
			strings.Repeat("arn:aws:s3:::my-bucket/prefix/", 80) + `*"}]}`
		form := url.Values{
			"Action":          {"AssumeRole"},
			"Version":         {"2011-06-15"},
			"RoleArn":         {"arn:aws:iam::123456789012:role/R"},
			"RoleSessionName": {"session"},
			"Policy":          {bigPolicy},
		}
		rec := accuracyPost(t, h, e, form)
		assert.Equal(t, http.StatusBadRequest, rec.Code)

		errResp := decodeError(t, rec.Body.Bytes())
		assert.Equal(t, "PackedPolicyTooLarge", errResp.Error.Code)
	})

	t.Run("too_many_policy_arns_returns_ValidationError", func(t *testing.T) {
		t.Parallel()

		h, _, e := accuracyHandler(t)
		form := url.Values{
			"Action":                   {"AssumeRole"},
			"Version":                  {"2011-06-15"},
			"RoleArn":                  {"arn:aws:iam::123456789012:role/R"},
			"RoleSessionName":          {"session"},
			"PolicyArns.member.1.arn":  {"arn:aws:iam::aws:policy/A"},
			"PolicyArns.member.2.arn":  {"arn:aws:iam::aws:policy/B"},
			"PolicyArns.member.3.arn":  {"arn:aws:iam::aws:policy/C"},
			"PolicyArns.member.4.arn":  {"arn:aws:iam::aws:policy/D"},
			"PolicyArns.member.5.arn":  {"arn:aws:iam::aws:policy/E"},
			"PolicyArns.member.6.arn":  {"arn:aws:iam::aws:policy/F"},
			"PolicyArns.member.7.arn":  {"arn:aws:iam::aws:policy/G"},
			"PolicyArns.member.8.arn":  {"arn:aws:iam::aws:policy/H"},
			"PolicyArns.member.9.arn":  {"arn:aws:iam::aws:policy/I"},
			"PolicyArns.member.10.arn": {"arn:aws:iam::aws:policy/J"},
			"PolicyArns.member.11.arn": {"arn:aws:iam::aws:policy/K"},
		}
		rec := accuracyPost(t, h, e, form)
		assert.Equal(t, http.StatusBadRequest, rec.Code)

		errResp := decodeError(t, rec.Body.Bytes())
		assert.Equal(t, "ValidationError", errResp.Error.Code)
	})

	t.Run(
		"outbound_federation_disabled_returns_OutboundWebIdentityFederationDisabledException",
		func(t *testing.T) {
			t.Parallel()

			h, b, e := accuracyHandler(t)
			b.SetOIDCLookup(&fakeOIDCAccountSettingsLookup{outboundFederationEnabled: false})

			form := url.Values{
				"Action":            {"GetWebIdentityToken"},
				"Version":           {"2011-06-15"},
				"Audience.member.1": {"https://example.com"},
				"SigningAlgorithm":  {"RS256"},
			}
			rec := accuracyPost(t, h, e, form)
			assert.Equal(t, http.StatusBadRequest, rec.Code)

			errResp := decodeError(t, rec.Body.Bytes())
			assert.Equal(t, "OutboundWebIdentityFederationDisabledException", errResp.Error.Code)
		},
	)
}

// errBackendFailure is returned by errorBackend to trigger the InternalFailure path.
var errBackendFailure = errors.New("unexpected backend failure")

// errorBackend is a test double that always returns an unexpected error.
type errorBackend struct{}

func (b *errorBackend) AssumeRole(_ *sts.AssumeRoleInput) (*sts.AssumeRoleResponse, error) {
	return nil, fmt.Errorf("AssumeRole: %w", errBackendFailure)
}

func (b *errorBackend) AssumeRoleWithSAML(_ *sts.AssumeRoleWithSAMLInput) (*sts.AssumeRoleWithSAMLResponse, error) {
	return nil, fmt.Errorf("AssumeRoleWithSAML: %w", errBackendFailure)
}

func (b *errorBackend) AssumeRoleWithWebIdentity(
	_ *sts.AssumeRoleWithWebIdentityInput,
) (*sts.AssumeRoleWithWebIdentityResponse, error) {
	return nil, fmt.Errorf("AssumeRoleWithWebIdentity: %w", errBackendFailure)
}

func (b *errorBackend) AssumeRoot(_ *sts.AssumeRootInput) (*sts.AssumeRootResponse, error) {
	return nil, fmt.Errorf("AssumeRoot: %w", errBackendFailure)
}

func (b *errorBackend) GetCallerIdentity(_, _ string) (*sts.GetCallerIdentityResponse, error) {
	return nil, fmt.Errorf("GetCallerIdentity: %w", errBackendFailure)
}

func (b *errorBackend) ValidateSessionCredential(_, _ string) (*sts.SessionInfo, error) {
	return nil, fmt.Errorf("ValidateSessionCredential: %w", errBackendFailure)
}

func (b *errorBackend) GetDelegatedAccessToken(
	_ *sts.GetDelegatedAccessTokenInput,
) (*sts.GetDelegatedAccessTokenResponse, error) {
	return nil, fmt.Errorf("GetDelegatedAccessToken: %w", errBackendFailure)
}

func (b *errorBackend) GetFederationToken(_ *sts.GetFederationTokenInput) (*sts.GetFederationTokenResponse, error) {
	return nil, fmt.Errorf("GetFederationToken: %w", errBackendFailure)
}

func (b *errorBackend) GetSessionToken(_ *sts.GetSessionTokenInput) (*sts.GetSessionTokenResponse, error) {
	return nil, fmt.Errorf("GetSessionToken: %w", errBackendFailure)
}

func (b *errorBackend) GetWebIdentityToken(_ *sts.GetWebIdentityTokenInput) (*sts.GetWebIdentityTokenResponse, error) {
	return nil, fmt.Errorf("GetWebIdentityToken: %w", errBackendFailure)
}

func (b *errorBackend) IssueEncodedAuthorizationMessage(_ string) string {
	return ""
}

func (b *errorBackend) VerifyEncodedAuthorizationMessage(_ string) (string, error) {
	return "", fmt.Errorf("VerifyEncodedAuthorizationMessage: %w", errBackendFailure)
}

func (b *errorBackend) LookupSession(_, _ string) *sts.SessionInfo { return nil }

// TestHandler_InternalError tests the default (InternalFailure) path in handleError.
func TestHandler_InternalError(t *testing.T) {
	t.Parallel()

	h := sts.NewHandler(&errorBackend{})
	e := echo.New()
	e.Use(func(_ echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			ctx := logger.Save(c.Request().Context(), logger.NewTestLogger())

			return h.Handler()(echo.NewContext(c.Request().WithContext(ctx), c.Response()))
		}
	})
	e.Any("/*", func(_ *echo.Context) error { return nil })

	rec := postForm(t, e, h, url.Values{
		"Action":  {"GetCallerIdentity"},
		"Version": {"2011-06-15"},
	})

	// Should return 500 InternalFailure with "Receiver" error type.
	assert.Equal(t, http.StatusInternalServerError, rec.Code)

	var errResp sts.ErrorResponse
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "Receiver", errResp.Error.Type)
	assert.Equal(t, "InternalFailure", errResp.Error.Code)
}

// TestHandler_ParseFormValues_SkipMalformedPair tests that malformed pairs
// (no '=') are skipped without panicking.
func TestHandler_ParseFormValues_SkipMalformedPair(t *testing.T) {
	t.Parallel()

	h, e := newTestHandler(t)

	// Body contains "noequals" pair (no '=') - should be skipped gracefully.
	body := "Action=GetCallerIdentity&noequals&Version=2011-06-15"
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	// ExtractOperation should still find the Action.
	assert.Equal(t, "GetCallerIdentity", h.ExtractOperation(c))
}

// ---- Read-error / ParseForm-error paths ------------------------------------

// errReader is an [io.ReadCloser] that always returns an error.
type errReader struct{}

func (r errReader) Read(_ []byte) (int, error) {
	return 0, fmt.Errorf("read error: %w", errBackendFailure)
}
func (r errReader) Close() error { return nil }

// TestExtractOperation_ReadBodyError covers the httputils.ReadBody error path
// in ExtractOperation (returns "Unknown" when the request body cannot be read).
func TestExtractOperation_ReadBodyError(t *testing.T) {
	t.Parallel()

	h, e := newTestHandler(t)
	req := httptest.NewRequest(http.MethodPost, "/", errReader{})
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	assert.Equal(t, "Unknown", h.ExtractOperation(c))
}

// TestExtractResource_ReadBodyError covers the httputils.ReadBody error path
// in ExtractResource (returns "" when the request body cannot be read).
func TestExtractResource_ReadBodyError(t *testing.T) {
	t.Parallel()

	h, e := newTestHandler(t)
	req := httptest.NewRequest(http.MethodPost, "/", errReader{})
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	assert.Empty(t, h.ExtractResource(c))
}

// TestDispatch_ParseFormError covers the r.ParseForm() error path in dispatch.
// An errReader body causes ParseForm to fail when it tries to read form fields.
func TestDispatch_ParseFormError(t *testing.T) {
	t.Parallel()

	h := sts.NewHandler(sts.NewInMemoryBackend())
	e := echo.New()
	e.Use(func(_ echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			ctx := logger.Save(c.Request().Context(), logger.NewTestLogger())

			return h.Handler()(echo.NewContext(c.Request().WithContext(ctx), c.Response()))
		}
	})
	e.Any("/*", func(_ *echo.Context) error { return nil })

	req := httptest.NewRequest(http.MethodPost, "/", errReader{})
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()

	e.ServeHTTP(rec, req)

	// ParseForm failure is an InternalFailure → 500
	assert.Equal(t, http.StatusInternalServerError, rec.Code)
}

// ---- XML wire-envelope ordering (Result before ResponseMetadata) -----------
//
// AWS query-protocol responses always nest the operation Result element before
// ResponseMetadata. This table exercises every credential-issuing operation to
// confirm that ordering holds across the board.
func TestXMLResponseResultBeforeMetadata(t *testing.T) {
	t.Parallel()

	tests := []struct {
		form       url.Values
		name       string
		wantResult string
	}{
		{
			name: "AssumeRole",
			form: url.Values{
				"Action":          {"AssumeRole"},
				"Version":         {"2011-06-15"},
				"RoleArn":         {"arn:aws:iam::123456789012:role/R"},
				"RoleSessionName": {"session"},
			},
			wantResult: "AssumeRoleResult",
		},
		{
			name: "GetFederationToken",
			form: url.Values{
				"Action":  {"GetFederationToken"},
				"Version": {"2011-06-15"},
				"Name":    {"alice"},
			},
			wantResult: "GetFederationTokenResult",
		},
		{
			name: "AssumeRoleWithSAML",
			form: url.Values{
				"Action":        {"AssumeRoleWithSAML"},
				"Version":       {"2011-06-15"},
				"RoleArn":       {"arn:aws:iam::123456789012:role/R"},
				"PrincipalArn":  {"arn:aws:iam::123456789012:saml-provider/MyIdP"},
				"SAMLAssertion": {"PHNhbWxwOkFzc2VydGlvbj4="},
			},
			wantResult: "AssumeRoleWithSAMLResult",
		},
		{
			name: "AssumeRoot",
			form: url.Values{
				"Action":          {"AssumeRoot"},
				"Version":         {"2011-06-15"},
				"TargetPrincipal": {"123456789012"},
				"TaskPolicyArn":   {"arn:aws:iam::aws:policy/root-task/IAMAuditRootUserCredentials"},
			},
			wantResult: "AssumeRootResult",
		},
		{
			name: "GetDelegatedAccessToken",
			form: url.Values{
				"Action":       {"GetDelegatedAccessToken"},
				"Version":      {"2011-06-15"},
				"TradeInToken": {"my-token"},
			},
			wantResult: "GetDelegatedAccessTokenResult",
		},
		{
			name: "GetWebIdentityToken",
			form: url.Values{
				"Action":            {"GetWebIdentityToken"},
				"Version":           {"2011-06-15"},
				"Audience.member.1": {"https://example.com"},
				"SigningAlgorithm":  {"RS256"},
			},
			wantResult: "GetWebIdentityTokenResult",
		},
		{
			name: "AssumeRoleWithWebIdentity",
			form: url.Values{
				"Action":           {"AssumeRoleWithWebIdentity"},
				"Version":          {"2011-06-15"},
				"RoleArn":          {"arn:aws:iam::123456789012:role/R"},
				"RoleSessionName":  {"session"},
				"WebIdentityToken": {"token"},
			},
			wantResult: "AssumeRoleWithWebIdentityResult",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _, e := accuracyHandler(t)
			rec := accuracyPost(t, h, e, tt.form)
			require.Equal(t, http.StatusOK, rec.Code)

			order := xmlElementOrder(t, rec.Body.Bytes())
			require.Len(t, order, 2)
			assert.Equal(t, tt.wantResult, order[0])
			assert.Equal(t, "ResponseMetadata", order[1])
		})
	}
}
