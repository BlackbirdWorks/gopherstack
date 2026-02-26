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
	"github.com/blackbirdworks/gopherstack/ses"
)

// newHandler creates a new SES handler with a fresh backend.
func newHandler() *ses.Handler {
	return ses.NewHandler(ses.NewInMemoryBackend(), slog.Default())
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

func TestSES_VerifyEmailIdentity(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := postForm(t, h, "Action=VerifyEmailIdentity&Version=2010-12-01&EmailAddress=test@example.com")

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "VerifyEmailIdentityResponse")
}

func TestSES_ListIdentities(t *testing.T) {
	t.Parallel()

	h := newHandler()

	// Verify an identity first.
	postForm(t, h, "Action=VerifyEmailIdentity&Version=2010-12-01&EmailAddress=alice@example.com")
	postForm(t, h, "Action=VerifyEmailIdentity&Version=2010-12-01&EmailAddress=bob@example.com")

	rec := postForm(t, h, "Action=ListIdentities&Version=2010-12-01")

	assert.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "ListIdentitiesResponse")
	assert.Contains(t, body, "alice@example.com")
	assert.Contains(t, body, "bob@example.com")
}

func TestSES_DeleteIdentity(t *testing.T) {
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

func TestSES_GetIdentityVerificationAttributes(t *testing.T) {
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

func TestSES_SendEmail(t *testing.T) {
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

func TestSES_SendRawEmail(t *testing.T) {
	t.Parallel()

	h := newHandler()

	body := url.Values{
		"Action":          {"SendRawEmail"},
		"Version":         {"2010-12-01"},
		"Source":          {"raw@example.com"},
		"RawMessage.Data": {"From: raw@example.com\r\nTo: dest@example.com\r\nSubject: raw\r\n\r\nBody"},
	}

	rec := postForm(t, h, body.Encode())

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "SendRawEmailResponse")
}

func TestSES_UnknownAction(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := postForm(t, h, "Action=UnknownAction&Version=2010-12-01")

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidAction")
}

func TestSES_MissingAction(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := postForm(t, h, "Version=2010-12-01")

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "MissingAction")
}

func TestSES_DeleteIdentity_NotFound(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := postForm(t, h, "Action=DeleteIdentity&Version=2010-12-01&Identity=nonexistent@example.com")

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "NoSuchEntity")
}

func TestSES_VerifyEmailIdentity_EmptyIdentity(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := postForm(t, h, "Action=VerifyEmailIdentity&Version=2010-12-01&EmailAddress=")

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidParameterValue")
}

func TestSES_Provider_Init(t *testing.T) {
	t.Parallel()

	p := &ses.Provider{}
	assert.Equal(t, "SES", p.Name())
}

func TestSES_Handler_Name(t *testing.T) {
	t.Parallel()

	h := newHandler()
	assert.Equal(t, "SES", h.Name())
}

func TestSES_Handler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newHandler()
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "SendEmail")
	assert.Contains(t, ops, "VerifyEmailIdentity")
	assert.Contains(t, ops, "ListIdentities")
}

func TestSES_Handler_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newHandler()
	assert.Equal(t, 80, h.MatchPriority())
}

func TestSES_Handler_ExtractOperation(t *testing.T) {
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

func TestSES_Handler_ExtractResource(t *testing.T) {
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

func TestSES_Handler_RouteMatcher(t *testing.T) {
	t.Parallel()

	h := newHandler()

	e := echo.New()

	t.Run("matches SES request", func(t *testing.T) {
		t.Parallel()

		req := httptest.NewRequest(http.MethodPost, "/",
			strings.NewReader("Action=ListIdentities&Version=2010-12-01"))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		assert.True(t, h.RouteMatcher()(c))
	})

	t.Run("rejects dashboard path", func(t *testing.T) {
		t.Parallel()

		req := httptest.NewRequest(http.MethodPost, "/dashboard/ses",
			strings.NewReader("Action=ListIdentities&Version=2010-12-01"))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		assert.False(t, h.RouteMatcher()(c))
	})

	t.Run("rejects GET", func(t *testing.T) {
		t.Parallel()

		req := httptest.NewRequest(http.MethodGet, "/", nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		assert.False(t, h.RouteMatcher()(c))
	})

	t.Run("rejects non-SES version", func(t *testing.T) {
		t.Parallel()

		req := httptest.NewRequest(http.MethodPost, "/",
			strings.NewReader("Action=ListUsers&Version=2010-05-08"))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		assert.False(t, h.RouteMatcher()(c))
	})
}

func TestSES_Provider_Init_WithAppCtx(t *testing.T) {
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
