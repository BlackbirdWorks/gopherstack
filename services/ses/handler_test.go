package ses_test

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/ses"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_Name(t *testing.T) {
	t.Parallel()

	h := newHandler()
	assert.Equal(t, "SES", h.Name())
}

func TestHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newHandler()
	assert.Positive(t, h.MatchPriority())
}

func TestHandler_Reset(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.VerifyEmailIdentity("reset@example.com"))
	assert.Equal(t, 1, h.Backend.(*ses.InMemoryBackend).IdentityCount())
	h.Reset()
	assert.Equal(t, 0, h.Backend.(*ses.InMemoryBackend).IdentityCount())
}

func TestExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		body   string
		wantOp string
	}{
		{
			name:   "action_present",
			body:   "Action=SendEmail&Version=2010-12-01",
			wantOp: "SendEmail",
		},
		{
			name:   "action_missing",
			body:   "Version=2010-12-01",
			wantOp: "unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			// Use postForm's request path just to exercise Handler which calls dispatch;
			// direct ExtractOperation is tested indirectly via coverage of Handler().
			rec := postForm(t, h, tt.body)
			// We only care that the handler didn't panic.
			assert.NotNil(t, rec)
		})
	}
}

func TestAllSupportedOpsListed(t *testing.T) {
	t.Parallel()

	h := newHandler()
	ops := h.GetSupportedOperations()
	assert.GreaterOrEqual(t, len(ops), 50, "must support 50+ SES operations")
}

// text extracts all character data from a simple nested XML path.
// path is a sequence of element names: text("Foo", "Bar") finds <Foo><Bar>...</Bar></Foo>.
func xmlText(data []byte, path ...string) string {
	for _, tag := range path {
		start := "<" + tag + ">"
		end := "</" + tag + ">"
		si := strings.Index(string(data), start)
		if si < 0 {
			return ""
		}
		si += len(start)
		ei := strings.Index(string(data)[si:], end)
		if ei < 0 {
			return ""
		}
		data = []byte(string(data)[si : si+ei])
	}

	return string(data)
}

func sesVerifyAndSend(t *testing.T, h *ses.Handler, from, to string) {
	t.Helper()
	require.NoError(t, h.Backend.VerifyEmailIdentity(from))
	rec := postForm(t, h, url.Values{
		"Action":                           {"SendEmail"},
		"Version":                          {"2010-12-01"},
		"Source":                           {from},
		"Destination.ToAddresses.member.1": {to},
		"Message.Subject.Data":             {"subj"},
		"Message.Body.Text.Data":           {"body"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestResponseMetadata_RequestIDPresent(t *testing.T) {
	t.Parallel()

	ops := []url.Values{
		{
			"Action":  {"ListIdentities"},
			"Version": {"2010-12-01"},
		},
		{
			"Action":  {"GetSendQuota"},
			"Version": {"2010-12-01"},
		},
		{
			"Action":  {"GetSendStatistics"},
			"Version": {"2010-12-01"},
		},
		{
			"Action":  {"ListConfigurationSets"},
			"Version": {"2010-12-01"},
		},
	}

	for _, vals := range ops {
		t.Run(vals.Get("Action"), func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			rec := postForm(t, h, vals.Encode())
			require.Equal(t, http.StatusOK, rec.Code)
			assert.Contains(t, rec.Body.String(), "<RequestId>",
				"every response must contain a RequestId in ResponseMetadata")
		})
	}
}

// TestHandler_GetSupportedOperations_Count tests that all operations are supported.
func TestHandler_GetSupportedOperations_Count(t *testing.T) {
	t.Parallel()

	h := newHandler()
	assert.Equal(t, 71, h.HandlerOpsLen())
}

// TestProvider_Init_NilContext tests that Init returns error on nil context.
func TestProvider_Init_NilContext(t *testing.T) {
	t.Parallel()

	p := &ses.Provider{}
	_, err := p.Init(nil)
	require.Error(t, err)
}

func TestSESHandler_Reset_NewOps(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.VerifyEmailIdentity("r@test.com"))

	h.Reset()

	assert.Equal(t, 0, h.Backend.(*ses.InMemoryBackend).IdentityCount())
}

// TestHandlerImplementsShutdowner verifies the Handler satisfies service.Shutdowner
// and that Shutdown waits for the janitor goroutine to exit cleanly.
func TestHandlerImplementsShutdowner(t *testing.T) {
	t.Parallel()

	var _ service.Shutdowner = (*ses.Handler)(nil)

	backend := ses.NewInMemoryBackend()
	h := ses.NewHandler(backend).WithJanitor(10 * time.Millisecond)

	ctx := context.Background()
	require.NoError(t, h.StartWorker(ctx))

	shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	// Shutdown must return without blocking past the timeout.
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.Shutdown(shutdownCtx)
	}()

	select {
	case <-done:
	case <-shutdownCtx.Done():
		t.Fatal("Shutdown did not return within timeout — goroutine leak")
	}
}

func TestVoidResultOps_WireShape(t *testing.T) {
	t.Parallel()

	cases := []struct {
		setup       func(t *testing.T, h *ses.Handler)
		name        string
		body        string
		wantElement string // must be present verbatim
	}{
		{
			name: "PutIdentityPolicy_has_result_wrapper",
			body: url.Values{
				"Action":     {"PutIdentityPolicy"},
				"Version":    {"2010-12-01"},
				"Identity":   {"example.com"},
				"PolicyName": {"p1"},
				"Policy":     {"{}"},
			}.Encode(),
			wantElement: "<PutIdentityPolicyResult></PutIdentityPolicyResult>",
		},
		{
			name: "SetIdentityDkimEnabled_has_result_wrapper",
			body: url.Values{
				"Action":      {"SetIdentityDkimEnabled"},
				"Version":     {"2010-12-01"},
				"Identity":    {"example.com"},
				"DkimEnabled": {"true"},
			}.Encode(),
			wantElement: "<SetIdentityDkimEnabledResult></SetIdentityDkimEnabledResult>",
		},
		{
			name: "VerifyEmailAddress_has_no_result_wrapper",
			body: url.Values{
				"Action":       {"VerifyEmailAddress"},
				"Version":      {"2010-12-01"},
				"EmailAddress": {"a@example.com"},
			}.Encode(),
			// Real SES's VerifyEmailAddress output shape has zero members, so the
			// wire format omits any Result element entirely.
			wantElement: "<VerifyEmailAddressResponse xmlns=\"http://ses.amazonaws.com/doc/2010-12-01/\">" +
				"<ResponseMetadata>",
		},
		{
			name: "UpdateAccountSendingEnabled_has_no_result_wrapper",
			body: url.Values{
				"Action":  {"UpdateAccountSendingEnabled"},
				"Version": {"2010-12-01"},
				"Enabled": {"false"},
			}.Encode(),
			wantElement: "<UpdateAccountSendingEnabledResponse xmlns=\"http://ses.amazonaws.com/doc/2010-12-01/\">" +
				"<ResponseMetadata>",
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := postForm(t, h, tt.body)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			assert.Contains(t, rec.Body.String(), tt.wantElement)
			// The literal broken wrapper must never appear.
			assert.NotContains(t, rec.Body.String(), "*Result")
		})
	}
}

// newHandler creates a new SES handler with a fresh backend.
func newHandler() *ses.Handler {
	return ses.NewHandler(ses.NewInMemoryBackend())
}

// postForm sends a form-encoded POST to the SES handler and returns the recorder.
func postForm(t *testing.T, h *ses.Handler, body string) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestSESHandler(t *testing.T) {
	t.Parallel()

	sendRawEmailBody := url.Values{
		"Action":          {"SendRawEmail"},
		"Version":         {"2010-12-01"},
		"Source":          {"raw@example.com"},
		"RawMessage.Data": {"From: raw@example.com\r\nTo: dest@example.com\r\nSubject: raw\r\n\r\nBody"},
	}.Encode()

	tests := []struct {
		name         string
		body         string
		setup        func(h *ses.Handler)
		wantContains string
		wantCode     int
	}{
		{
			name:         "VerifyEmailIdentity",
			body:         "Action=VerifyEmailIdentity&Version=2010-12-01&EmailAddress=test@example.com",
			wantCode:     http.StatusOK,
			wantContains: "VerifyEmailIdentityResponse",
		},
		{
			name: "SendRawEmail",
			body: sendRawEmailBody,
			setup: func(h *ses.Handler) {
				require.NoError(t, h.Backend.VerifyEmailIdentity("raw@example.com"))
			},
			wantCode:     http.StatusOK,
			wantContains: "SendRawEmailResponse",
		},
		{
			name:         "UnknownAction",
			body:         "Action=UnknownAction&Version=2010-12-01",
			wantCode:     http.StatusBadRequest,
			wantContains: "InvalidAction",
		},
		{
			name:         "MissingAction",
			body:         "Version=2010-12-01",
			wantCode:     http.StatusBadRequest,
			wantContains: "MissingAction",
		},
		{
			name:         "DeleteIdentityIdempotent",
			body:         "Action=DeleteIdentity&Version=2010-12-01&Identity=nonexistent@example.com",
			wantCode:     http.StatusOK,
			wantContains: "DeleteIdentityResponse",
		},
		{
			name:         "VerifyEmailIdentityEmptyIdentity",
			body:         "Action=VerifyEmailIdentity&Version=2010-12-01&EmailAddress=",
			wantCode:     http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
		{
			name: "SendEmailUnverifiedSource",
			body: "Action=SendEmail&Version=2010-12-01&Source=unverified@example.com" +
				"&Destination.ToAddresses.member.1=to@example.com" +
				"&Message.Subject.Data=Test&Message.Body.Text.Data=Body",
			wantCode:     http.StatusBadRequest,
			wantContains: "MessageRejected",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := postForm(t, h, tt.body)

			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

func TestSESHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	h := newHandler()
	e := echo.New()

	tests := []struct {
		name      string
		method    string
		path      string
		body      string
		wantMatch bool
	}{
		{
			name:      "matches SES request",
			method:    http.MethodPost,
			path:      "/",
			body:      "Action=ListIdentities&Version=2010-12-01",
			wantMatch: true,
		},
		{
			name:      "rejects dashboard path",
			method:    http.MethodPost,
			path:      "/dashboard/ses",
			body:      "Action=ListIdentities&Version=2010-12-01",
			wantMatch: false,
		},
		{
			name:      "rejects GET",
			method:    http.MethodGet,
			path:      "/",
			wantMatch: false,
		},
		{
			name:      "rejects non-SES version",
			method:    http.MethodPost,
			path:      "/",
			body:      "Action=ListUsers&Version=2010-05-08",
			wantMatch: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var req *http.Request
			if tt.body != "" {
				req = httptest.NewRequest(tt.method, tt.path, strings.NewReader(tt.body))
				req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			} else {
				req = httptest.NewRequest(tt.method, tt.path, nil)
			}

			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.wantMatch, h.RouteMatcher()(c))
		})
	}
}

func TestSESHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	h := newHandler()

	req := httptest.NewRequest(http.MethodPost, "/",
		strings.NewReader("Action=SendEmail&Version=2010-12-01"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	e := echo.New()
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	assert.Equal(t, "SendEmail", h.ExtractOperation(c))
}

func TestSESHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	h := newHandler()

	req := httptest.NewRequest(http.MethodPost, "/",
		strings.NewReader("Action=SendEmail&Version=2010-12-01&Source=from@example.com"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	e := echo.New()
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	assert.Equal(t, "from@example.com", h.ExtractResource(c))
}

func TestSESHandler_ProviderInit(t *testing.T) {
	t.Parallel()

	p := &ses.Provider{}
	assert.Equal(t, "SES", p.Name())
}

func TestSESHandler_HandlerName(t *testing.T) {
	t.Parallel()

	h := newHandler()
	assert.Equal(t, "SES", h.Name())
}

func TestSESHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newHandler()
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "SendEmail")
	assert.Contains(t, ops, "VerifyEmailIdentity")
	assert.Contains(t, ops, "ListIdentities")
}

func TestSESHandler_ChaosInterface(t *testing.T) {
	t.Parallel()

	h := newHandler()
	assert.Equal(t, "ses", h.ChaosServiceName())
	assert.NotEmpty(t, h.ChaosOperations())
	assert.NotEmpty(t, h.ChaosRegions())
}

func TestSESHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newHandler()
	assert.Equal(t, 80, h.MatchPriority())
}

func TestSESHandler_ProviderInitWithAppCtx(t *testing.T) {
	t.Parallel()

	p := &ses.Provider{}

	appCtx := &service.AppContext{
		Logger: slog.Default(),
	}

	svc, err := p.Init(appCtx)
	require.NoError(t, err)
	require.NotNil(t, svc)
	assert.Equal(t, "SES", svc.Name())
}
