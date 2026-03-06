package ses_test

import (
	"encoding/xml"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/ses"
)

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
			name:         "SendRawEmail",
			body:         sendRawEmailBody,
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
			name:         "DeleteIdentityNotFound",
			body:         "Action=DeleteIdentity&Version=2010-12-01&Identity=nonexistent@example.com",
			wantCode:     http.StatusBadRequest,
			wantContains: "NoSuchEntity",
		},
		{
			name:         "VerifyEmailIdentityEmptyIdentity",
			body:         "Action=VerifyEmailIdentity&Version=2010-12-01&EmailAddress=",
			wantCode:     http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			rec := postForm(t, h, tt.body)

			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

func TestSESHandler_ListIdentities(t *testing.T) {
	t.Parallel()

	h := newHandler()

	// Verify identities first.
	postForm(t, h, "Action=VerifyEmailIdentity&Version=2010-12-01&EmailAddress=alice@example.com")
	postForm(t, h, "Action=VerifyEmailIdentity&Version=2010-12-01&EmailAddress=bob@example.com")

	rec := postForm(t, h, "Action=ListIdentities&Version=2010-12-01")

	assert.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, "ListIdentitiesResponse")
	assert.Contains(t, body, "alice@example.com")
	assert.Contains(t, body, "bob@example.com")
}

func TestSESHandler_DeleteIdentity(t *testing.T) {
	t.Parallel()

	h := newHandler()

	// Verify an identity first.
	postForm(t, h, "Action=VerifyEmailIdentity&Version=2010-12-01&EmailAddress=del@example.com")

	// Delete it.
	rec := postForm(t, h, "Action=DeleteIdentity&Version=2010-12-01&Identity=del@example.com")

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DeleteIdentityResponse")

	// Verify it's gone.
	listRec := postForm(t, h, "Action=ListIdentities&Version=2010-12-01")
	assert.NotContains(t, listRec.Body.String(), "del@example.com")
}

func TestSESHandler_GetIdentityVerificationAttributes(t *testing.T) {
	t.Parallel()

	h := newHandler()

	// Verify an identity first.
	postForm(t, h, "Action=VerifyEmailIdentity&Version=2010-12-01&EmailAddress=verified@example.com")

	body := url.Values{
		"Action":              {"GetIdentityVerificationAttributes"},
		"Version":             {"2010-12-01"},
		"Identities.member.1": {"verified@example.com"},
		"Identities.member.2": {"unknown@example.com"},
	}

	rec := postForm(t, h, body.Encode())

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		XMLName xml.Name `xml:"GetIdentityVerificationAttributesResponse"`
		Result  struct {
			VerificationAttributes struct {
				Entries []struct {
					Key   string `xml:"key"`
					Value struct {
						Status string `xml:"VerificationStatus"`
					} `xml:"value"`
				} `xml:"entry"`
			} `xml:"VerificationAttributes"`
		} `xml:"GetIdentityVerificationAttributesResult"`
	}

	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))

	statusByID := make(map[string]string)
	for _, e := range resp.Result.VerificationAttributes.Entries {
		statusByID[e.Key] = e.Value.Status
	}

	assert.Equal(t, "Success", statusByID["verified@example.com"])
	assert.Equal(t, "NotStarted", statusByID["unknown@example.com"])
}

func TestSESHandler_SendEmail(t *testing.T) {
	t.Parallel()

	h := newHandler()

	body := url.Values{
		"Action":                           {"SendEmail"},
		"Version":                          {"2010-12-01"},
		"Source":                           {"sender@example.com"},
		"Destination.ToAddresses.member.1": {"recipient@example.com"},
		"Message.Subject.Data":             {"Hello World"},
		"Message.Body.Text.Data":           {"Test body"},
		"Message.Body.Html.Data":           {"<p>Test body</p>"},
	}

	rec := postForm(t, h, body.Encode())

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		XMLName xml.Name `xml:"SendEmailResponse"`
		Result  struct {
			MessageID string `xml:"MessageId"`
		} `xml:"SendEmailResult"`
	}

	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp.Result.MessageID)

	// Verify email was captured.
	emails := h.Backend.ListEmails()
	require.Len(t, emails, 1)
	assert.Equal(t, "sender@example.com", emails[0].From)
	assert.Equal(t, []string{"recipient@example.com"}, emails[0].To)
	assert.Equal(t, "Hello World", emails[0].Subject)
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
