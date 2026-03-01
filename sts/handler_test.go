package sts_test

import (
	"encoding/base64"
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
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/sts"
)

// ---- Backend tests ---------------------------------------------------------

func TestGetCallerIdentity(t *testing.T) {
	t.Parallel()

	backend := sts.NewInMemoryBackend()

	resp, err := backend.GetCallerIdentity()
	require.NoError(t, err)
	require.NotNil(t, resp)

	assert.Equal(t, sts.MockAccountID, resp.GetCallerIdentityResult.Account)
	assert.Equal(t, sts.MockUserArn, resp.GetCallerIdentityResult.Arn)
	assert.Equal(t, sts.MockUserID, resp.GetCallerIdentityResult.UserID)
	assert.NotEmpty(t, resp.ResponseMetadata.RequestID)
	assert.Equal(t, sts.STSNamespace, resp.Xmlns)
}

func TestAssumeRole_Success(t *testing.T) {
	t.Parallel()

	backend := sts.NewInMemoryBackend()

	resp, err := backend.AssumeRole(&sts.AssumeRoleInput{
		RoleArn:         "arn:aws:iam::123456789012:role/TestRole",
		RoleSessionName: "my-session",
	})
	require.NoError(t, err)
	require.NotNil(t, resp)

	creds := resp.AssumeRoleResult.Credentials
	assert.True(t, strings.HasPrefix(creds.AccessKeyID, "ASIA"), "AccessKeyId should start with ASIA")
	assert.Len(t, creds.AccessKeyID, 20, "AccessKeyId should be 20 chars")
	assert.Len(t, creds.SecretAccessKey, 40, "SecretAccessKey should be 40 chars")
	assert.NotEmpty(t, creds.SessionToken)
	assert.NotEmpty(t, creds.Expiration)

	user := resp.AssumeRoleResult.AssumedRoleUser
	assert.Contains(t, user.Arn, "assumed-role")
	assert.Contains(t, user.Arn, "my-session")
	assert.Contains(t, user.AssumedRoleID, "my-session")
}

func TestAssumeRole_DefaultDuration(t *testing.T) {
	t.Parallel()

	backend := sts.NewInMemoryBackend()

	resp, err := backend.AssumeRole(&sts.AssumeRoleInput{
		RoleArn:         "arn:aws:iam::123456789012:role/TestRole",
		RoleSessionName: "session",
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.NotEmpty(t, resp.AssumeRoleResult.Credentials.Expiration)
}

func TestAssumeRole_CustomDuration(t *testing.T) {
	t.Parallel()

	backend := sts.NewInMemoryBackend()

	resp, err := backend.AssumeRole(&sts.AssumeRoleInput{
		RoleArn:         "arn:aws:iam::123456789012:role/TestRole",
		RoleSessionName: "session",
		DurationSeconds: 1800,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, resp.AssumeRoleResult.Credentials.Expiration)
}

func TestAssumeRole_MissingRoleArn(t *testing.T) {
	t.Parallel()

	backend := sts.NewInMemoryBackend()

	_, err := backend.AssumeRole(&sts.AssumeRoleInput{
		RoleSessionName: "session",
	})
	require.ErrorIs(t, err, sts.ErrMissingRoleArn)
}

func TestAssumeRole_MissingSessionName(t *testing.T) {
	t.Parallel()

	backend := sts.NewInMemoryBackend()

	_, err := backend.AssumeRole(&sts.AssumeRoleInput{
		RoleArn: "arn:aws:iam::123456789012:role/TestRole",
	})
	require.ErrorIs(t, err, sts.ErrMissingSessionName)
}

func TestAssumeRole_InvalidDurationTooShort(t *testing.T) {
	t.Parallel()

	backend := sts.NewInMemoryBackend()

	_, err := backend.AssumeRole(&sts.AssumeRoleInput{
		RoleArn:         "arn:aws:iam::123456789012:role/TestRole",
		RoleSessionName: "session",
		DurationSeconds: 100,
	})
	require.ErrorIs(t, err, sts.ErrInvalidDuration)
}

func TestAssumeRole_InvalidDurationTooLong(t *testing.T) {
	t.Parallel()

	backend := sts.NewInMemoryBackend()

	_, err := backend.AssumeRole(&sts.AssumeRoleInput{
		RoleArn:         "arn:aws:iam::123456789012:role/TestRole",
		RoleSessionName: "session",
		DurationSeconds: 99999,
	})
	require.ErrorIs(t, err, sts.ErrInvalidDuration)
}

func TestAssumeRole_CredentialsAreUnique(t *testing.T) {
	t.Parallel()

	backend := sts.NewInMemoryBackend()
	input := &sts.AssumeRoleInput{
		RoleArn:         "arn:aws:iam::123456789012:role/TestRole",
		RoleSessionName: "session",
	}

	r1, err := backend.AssumeRole(input)
	require.NoError(t, err)

	r2, err := backend.AssumeRole(input)
	require.NoError(t, err)

	// Each call should produce unique credentials.
	assert.NotEqual(t, r1.AssumeRoleResult.Credentials.AccessKeyID, r2.AssumeRoleResult.Credentials.AccessKeyID)
}

// ---- Handler tests ---------------------------------------------------------

func newTestHandler(t *testing.T) (*sts.Handler, *echo.Echo) {
	t.Helper()

	backend := sts.NewInMemoryBackend()
	log := logger.NewTestLogger()
	h := sts.NewHandler(backend, log)
	e := echo.New()

	return h, e
}

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

func TestHandler_GetCallerIdentity(t *testing.T) {
	t.Parallel()

	h, e := newTestHandler(t)
	rec := postForm(t, e, h, url.Values{
		"Action":  {"GetCallerIdentity"},
		"Version": {"2011-06-15"},
	})

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Header().Get("Content-Type"), "text/xml")

	var resp sts.GetCallerIdentityResponse
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, sts.MockAccountID, resp.GetCallerIdentityResult.Account)
	assert.Equal(t, sts.MockUserArn, resp.GetCallerIdentityResult.Arn)
}

func TestHandler_AssumeRole(t *testing.T) {
	t.Parallel()

	h, e := newTestHandler(t)
	rec := postForm(t, e, h, url.Values{
		"Action":          {"AssumeRole"},
		"Version":         {"2011-06-15"},
		"RoleArn":         {"arn:aws:iam::123456789012:role/TestRole"},
		"RoleSessionName": {"test-session"},
	})

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp sts.AssumeRoleResponse
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.True(t, strings.HasPrefix(resp.AssumeRoleResult.Credentials.AccessKeyID, "ASIA"))
	assert.Contains(t, resp.AssumeRoleResult.AssumedRoleUser.Arn, "assumed-role")
}

func TestHandler_AssumeRole_WithDuration(t *testing.T) {
	t.Parallel()

	h, e := newTestHandler(t)
	rec := postForm(t, e, h, url.Values{
		"Action":          {"AssumeRole"},
		"Version":         {"2011-06-15"},
		"RoleArn":         {"arn:aws:iam::123456789012:role/TestRole"},
		"RoleSessionName": {"test-session"},
		"DurationSeconds": {"1800"},
	})

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp sts.AssumeRoleResponse
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp.AssumeRoleResult.Credentials.Expiration)
}

func TestHandler_AssumeRole_InvalidDuration(t *testing.T) {
	t.Parallel()

	h, e := newTestHandler(t)
	rec := postForm(t, e, h, url.Values{
		"Action":          {"AssumeRole"},
		"Version":         {"2011-06-15"},
		"RoleArn":         {"arn:aws:iam::123456789012:role/TestRole"},
		"RoleSessionName": {"test-session"},
		"DurationSeconds": {"not-a-number"},
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp sts.ErrorResponse
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "ValidationError", errResp.Error.Code)
}

func TestHandler_AssumeRole_MissingRoleArn(t *testing.T) {
	t.Parallel()

	h, e := newTestHandler(t)
	rec := postForm(t, e, h, url.Values{
		"Action":          {"AssumeRole"},
		"RoleSessionName": {"session"},
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp sts.ErrorResponse
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "MissingParameter", errResp.Error.Code)
	assert.Equal(t, "Sender", errResp.Error.Type)
}

func TestHandler_AssumeRole_MissingSessionName(t *testing.T) {
	t.Parallel()

	h, e := newTestHandler(t)
	rec := postForm(t, e, h, url.Values{
		"Action":  {"AssumeRole"},
		"RoleArn": {"arn:aws:iam::123456789012:role/TestRole"},
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp sts.ErrorResponse
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "MissingParameter", errResp.Error.Code)
	assert.Equal(t, "Sender", errResp.Error.Type)
}

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
	log := logger.NewTestLogger()
	h := sts.NewHandler(backend, log)
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

// ---- Provider tests ---------------------------------------------------------

func TestProvider_Name(t *testing.T) {
	t.Parallel()

	p := &sts.Provider{}
	assert.Equal(t, "STS", p.Name())
}

func TestProvider_Init(t *testing.T) {
	t.Parallel()

	p := &sts.Provider{}
	appCtx := &service.AppContext{
		Logger: logger.NewTestLogger(),
	}

	reg, err := p.Init(appCtx)
	require.NoError(t, err)
	assert.NotNil(t, reg)
	assert.Equal(t, "STS", reg.Name())
}

// ---- Additional backend tests -----------------------------------------------

func TestAssumeRole_MalformedArn(t *testing.T) {
	t.Parallel()

	// An ARN with fewer than 6 colon-separated components triggers the fallback
	// path in buildAssumedRoleArn.
	backend := sts.NewInMemoryBackend()
	resp, err := backend.AssumeRole(&sts.AssumeRoleInput{
		RoleArn:         "short/role",
		RoleSessionName: "session",
	})
	require.NoError(t, err)

	assert.Contains(t, resp.AssumeRoleResult.AssumedRoleUser.Arn, "session")
}

// ---- Handler error-path tests -----------------------------------------------

// errBackendFailure is returned by errorBackend to trigger the InternalFailure path.
var errBackendFailure = errors.New("unexpected backend failure")

// errorBackend is a test double that always returns an unexpected error.
type errorBackend struct{}

func (b *errorBackend) AssumeRole(_ *sts.AssumeRoleInput) (*sts.AssumeRoleResponse, error) {
	return nil, fmt.Errorf("AssumeRole: %w", errBackendFailure)
}

func (b *errorBackend) GetCallerIdentity() (*sts.GetCallerIdentityResponse, error) {
	return nil, fmt.Errorf("GetCallerIdentity: %w", errBackendFailure)
}

func (b *errorBackend) GetSessionToken(_ *sts.GetSessionTokenInput) (*sts.GetSessionTokenResponse, error) {
	return nil, fmt.Errorf("GetSessionToken: %w", errBackendFailure)
}

// TestHandler_InternalError tests the default (InternalFailure) path in handleError.
func TestHandler_InternalError(t *testing.T) {
	t.Parallel()

	log := logger.NewTestLogger()
	h := sts.NewHandler(&errorBackend{}, log)
	e := echo.New()
	e.Use(func(_ echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			ctx := logger.Save(c.Request().Context(), log)

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

// TestExtractOperation_ReadBodyError covers the httputil.ReadBody error path
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

// TestExtractResource_ReadBodyError covers the httputil.ReadBody error path
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

	log := logger.NewTestLogger()
	h := sts.NewHandler(sts.NewInMemoryBackend(), log)
	e := echo.New()
	e.Use(func(_ echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			ctx := logger.Save(c.Request().Context(), log)

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

// TestGetAccessKeyInfo verifies the GetAccessKeyInfo action.
func TestGetAccessKeyInfo(t *testing.T) {
	t.Parallel()

	backend := sts.NewInMemoryBackend()
	h := sts.NewHandler(backend, nil)
	e := echo.New()

	form := url.Values{
		"Action":      {"GetAccessKeyInfo"},
		"Version":     {"2011-06-15"},
		"AccessKeyId": {"AKIAIOSFODNN7EXAMPLE"},
	}

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()

	ctxWithLogger := logger.Save(req.Context(), nil)
	req = req.WithContext(ctxWithLogger)

	err := h.Handler()(e.NewContext(req, rec))
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		XMLName xml.Name `xml:"GetAccessKeyInfoResponse"`
		Result  struct {
			Account string `xml:"Account"`
		} `xml:"GetAccessKeyInfoResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, sts.MockAccountID, resp.Result.Account)
}

// TestDecodeAuthorizationMessage verifies the DecodeAuthorizationMessage action.
func TestDecodeAuthorizationMessage(t *testing.T) {
	t.Parallel()

	backend := sts.NewInMemoryBackend()
	h := sts.NewHandler(backend, nil)
	e := echo.New()

	original := "this is a test message"
	encoded := base64.StdEncoding.EncodeToString([]byte(original))

	form := url.Values{
		"Action":         {"DecodeAuthorizationMessage"},
		"Version":        {"2011-06-15"},
		"EncodedMessage": {encoded},
	}

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()

	ctxWithLogger := logger.Save(req.Context(), nil)
	req = req.WithContext(ctxWithLogger)

	err := h.Handler()(e.NewContext(req, rec))
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		XMLName xml.Name `xml:"DecodeAuthorizationMessageResponse"`
		Result  struct {
			DecodedMessage string `xml:"DecodedMessage"`
		} `xml:"DecodeAuthorizationMessageResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, original, resp.Result.DecodedMessage)
}
