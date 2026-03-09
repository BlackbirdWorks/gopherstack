package sesv2_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/sesv2"
)

// newHandler creates a new SES v2 handler with a fresh backend.
func newHandler() *sesv2.Handler {
	return sesv2.NewHandler(sesv2.NewInMemoryBackend())
}

// doRequest performs a request against the handler and returns the recorder.
func doRequest(t *testing.T, h *sesv2.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyReader *bytes.Reader

	if body != nil {
		b, err := json.Marshal(body)
		require.NoError(t, err)

		bodyReader = bytes.NewReader(b)
	} else {
		bodyReader = bytes.NewReader(nil)
	}

	req := httptest.NewRequest(method, path, bodyReader)
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestSESv2Handler_CreateEmailIdentity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantType string
		wantCode int
	}{
		{
			name:     "creates email identity",
			body:     map[string]any{"EmailIdentity": "test@example.com"},
			wantCode: http.StatusOK,
			wantType: "EMAIL_ADDRESS",
		},
		{
			name:     "creates domain identity",
			body:     map[string]any{"EmailIdentity": "example.com"},
			wantCode: http.StatusOK,
			wantType: "DOMAIN",
		},
		{
			name:     "duplicate identity returns conflict",
			body:     map[string]any{"EmailIdentity": "dup@example.com"},
			wantCode: http.StatusConflict,
		},
		{
			name:     "empty identity returns bad request",
			body:     map[string]any{"EmailIdentity": ""},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()

			if tt.name == "duplicate identity returns conflict" {
				// Create once first.
				doRequest(
					t,
					h,
					http.MethodPost,
					"/v2/email/identities",
					map[string]any{"EmailIdentity": "dup@example.com"},
				)
			}

			rec := doRequest(t, h, http.MethodPost, "/v2/email/identities", tt.body)

			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantType != "" {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Equal(t, tt.wantType, out["IdentityType"])
				assert.Equal(t, true, out["VerifiedForSending"])
			}
		})
	}
}

func TestSESv2Handler_GetEmailIdentity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		identity string
		wantCode int
	}{
		{
			name:     "gets existing identity",
			identity: "get@example.com",
			wantCode: http.StatusOK,
		},
		{
			name:     "not found returns 404",
			identity: "notfound@example.com",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()

			if tt.wantCode == http.StatusOK {
				doRequest(t, h, http.MethodPost, "/v2/email/identities", map[string]any{"EmailIdentity": tt.identity})
			}

			rec := doRequest(t, h, http.MethodGet, "/v2/email/identities/"+tt.identity, nil)

			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Equal(t, tt.identity, out["EmailIdentity"])
			}
		})
	}
}

func TestSESv2Handler_ListEmailIdentities(t *testing.T) {
	t.Parallel()

	h := newHandler()

	doRequest(t, h, http.MethodPost, "/v2/email/identities", map[string]any{"EmailIdentity": "alice@example.com"})
	doRequest(t, h, http.MethodPost, "/v2/email/identities", map[string]any{"EmailIdentity": "bob@example.com"})

	rec := doRequest(t, h, http.MethodGet, "/v2/email/identities", nil)

	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	identities, ok := out["EmailIdentities"].([]any)
	require.True(t, ok)
	assert.Len(t, identities, 2)
}

func TestSESv2Handler_DeleteEmailIdentity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		identity string
		wantCode int
	}{
		{
			name:     "deletes existing identity",
			identity: "del@example.com",
			wantCode: http.StatusOK,
		},
		{
			name:     "not found returns 404",
			identity: "notfound@example.com",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()

			if tt.wantCode == http.StatusOK {
				doRequest(t, h, http.MethodPost, "/v2/email/identities", map[string]any{"EmailIdentity": tt.identity})
			}

			rec := doRequest(t, h, http.MethodDelete, "/v2/email/identities/"+tt.identity, nil)

			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestSESv2Handler_SendEmail(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "sends email successfully",
			body: map[string]any{
				"FromEmailAddress": "sender@example.com",
				"Destination": map[string]any{
					"ToAddresses": []string{"recipient@example.com"},
				},
				"Content": map[string]any{
					"Simple": map[string]any{
						"Subject": map[string]any{"Data": "Hello"},
						"Body": map[string]any{
							"Text": map[string]any{"Data": "Hello World"},
							"Html": map[string]any{"Data": "<b>Hello World</b>"},
						},
					},
				},
			},
			wantCode: http.StatusOK,
		},
		{
			name: "missing from address returns bad request",
			body: map[string]any{
				"Destination": map[string]any{
					"ToAddresses": []string{"recipient@example.com"},
				},
				"Content": map[string]any{
					"Simple": map[string]any{
						"Subject": map[string]any{"Data": "Hello"},
						"Body":    map[string]any{},
					},
				},
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			rec := doRequest(t, h, http.MethodPost, "/v2/email/outbound-emails", tt.body)

			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.NotEmpty(t, out["MessageId"])

				emails := h.Backend.ListEmails()
				require.Len(t, emails, 1)
				assert.Equal(t, "sender@example.com", emails[0].From)
			}
		})
	}
}

func TestSESv2Handler_CreateConfigurationSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name:     "creates config set",
			body:     map[string]any{"ConfigurationSetName": "my-config"},
			wantCode: http.StatusOK,
		},
		{
			name:     "duplicate returns conflict",
			body:     map[string]any{"ConfigurationSetName": "dup-config"},
			wantCode: http.StatusConflict,
		},
		{
			name:     "empty name returns bad request",
			body:     map[string]any{"ConfigurationSetName": ""},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()

			if tt.name == "duplicate returns conflict" {
				doRequest(
					t,
					h,
					http.MethodPost,
					"/v2/email/configuration-sets",
					map[string]any{"ConfigurationSetName": "dup-config"},
				)
			}

			rec := doRequest(t, h, http.MethodPost, "/v2/email/configuration-sets", tt.body)

			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestSESv2Handler_GetConfigurationSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		csName   string
		wantCode int
	}{
		{
			name:     "gets existing config set",
			csName:   "my-config",
			wantCode: http.StatusOK,
		},
		{
			name:     "not found returns 404",
			csName:   "notfound",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()

			if tt.wantCode == http.StatusOK {
				doRequest(
					t,
					h,
					http.MethodPost,
					"/v2/email/configuration-sets",
					map[string]any{"ConfigurationSetName": tt.csName},
				)
			}

			rec := doRequest(t, h, http.MethodGet, "/v2/email/configuration-sets/"+tt.csName, nil)

			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Equal(t, tt.csName, out["ConfigurationSetName"])
			}
		})
	}
}

func TestSESv2Handler_ListConfigurationSets(t *testing.T) {
	t.Parallel()

	h := newHandler()

	doRequest(t, h, http.MethodPost, "/v2/email/configuration-sets", map[string]any{"ConfigurationSetName": "config-a"})
	doRequest(t, h, http.MethodPost, "/v2/email/configuration-sets", map[string]any{"ConfigurationSetName": "config-b"})

	rec := doRequest(t, h, http.MethodGet, "/v2/email/configuration-sets", nil)

	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	sets, ok := out["ConfigurationSets"].([]any)
	require.True(t, ok)
	assert.Len(t, sets, 2)
}

func TestSESv2Handler_DeleteConfigurationSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		csName   string
		wantCode int
	}{
		{
			name:     "deletes existing config set",
			csName:   "del-config",
			wantCode: http.StatusOK,
		},
		{
			name:     "not found returns 404",
			csName:   "notfound",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()

			if tt.wantCode == http.StatusOK {
				doRequest(
					t,
					h,
					http.MethodPost,
					"/v2/email/configuration-sets",
					map[string]any{"ConfigurationSetName": tt.csName},
				)
			}

			rec := doRequest(t, h, http.MethodDelete, "/v2/email/configuration-sets/"+tt.csName, nil)

			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestSESv2Handler_RouteMatcher(t *testing.T) {
	t.Parallel()

	h := newHandler()
	e := echo.New()

	tests := []struct {
		name      string
		method    string
		path      string
		wantMatch bool
	}{
		{
			name:      "matches v2 email path",
			method:    http.MethodGet,
			path:      "/v2/email/identities",
			wantMatch: true,
		},
		{
			name:      "matches v2 config sets path",
			method:    http.MethodGet,
			path:      "/v2/email/configuration-sets",
			wantMatch: true,
		},
		{
			name:      "rejects dashboard path",
			method:    http.MethodGet,
			path:      "/dashboard/ses",
			wantMatch: false,
		},
		{
			name:      "rejects non-v2 path",
			method:    http.MethodGet,
			path:      "/",
			wantMatch: false,
		},
		{
			name:      "rejects v1 SES path",
			method:    http.MethodPost,
			path:      "/",
			wantMatch: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(tt.method, tt.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.wantMatch, h.RouteMatcher()(c))
		})
	}
}

func TestSESv2Handler_ExtractOperation(t *testing.T) {
	t.Parallel()

	h := newHandler()
	e := echo.New()

	tests := []struct {
		name   string
		method string
		path   string
		wantOp string
	}{
		{
			name:   "GET identities list",
			method: http.MethodGet,
			path:   "/v2/email/identities",
			wantOp: "ListEmailIdentities",
		},
		{
			name:   "POST identities create",
			method: http.MethodPost,
			path:   "/v2/email/identities",
			wantOp: "CreateEmailIdentity",
		},
		{
			name:   "GET identity by name",
			method: http.MethodGet,
			path:   "/v2/email/identities/test@example.com",
			wantOp: "GetEmailIdentity",
		},
		{
			name:   "DELETE identity",
			method: http.MethodDelete,
			path:   "/v2/email/identities/test@example.com",
			wantOp: "DeleteEmailIdentity",
		},
		{
			name:   "POST outbound-emails",
			method: http.MethodPost,
			path:   "/v2/email/outbound-emails",
			wantOp: "SendEmail",
		},
		{
			name:   "POST configuration-sets",
			method: http.MethodPost,
			path:   "/v2/email/configuration-sets",
			wantOp: "CreateConfigurationSet",
		},
		{
			name:   "GET configuration-sets list",
			method: http.MethodGet,
			path:   "/v2/email/configuration-sets",
			wantOp: "ListConfigurationSets",
		},
		{
			name:   "GET configuration set by name",
			method: http.MethodGet,
			path:   "/v2/email/configuration-sets/my-config",
			wantOp: "GetConfigurationSet",
		},
		{
			name:   "DELETE configuration set",
			method: http.MethodDelete,
			path:   "/v2/email/configuration-sets/my-config",
			wantOp: "DeleteConfigurationSet",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(tt.method, tt.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.wantOp, h.ExtractOperation(c))
		})
	}
}

func TestSESv2Handler_ExtractResource(t *testing.T) {
	t.Parallel()

	h := newHandler()
	e := echo.New()

	req := httptest.NewRequest(http.MethodGet, "/v2/email/identities/test@example.com", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	assert.Equal(t, "test@example.com", h.ExtractResource(c))
}

func TestSESv2Handler_HandlerName(t *testing.T) {
	t.Parallel()

	h := newHandler()
	assert.Equal(t, "SESv2", h.Name())
}

func TestSESv2Handler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newHandler()
	ops := h.GetSupportedOperations()

	assert.Contains(t, ops, "CreateEmailIdentity")
	assert.Contains(t, ops, "GetEmailIdentity")
	assert.Contains(t, ops, "ListEmailIdentities")
	assert.Contains(t, ops, "DeleteEmailIdentity")
	assert.Contains(t, ops, "SendEmail")
	assert.Contains(t, ops, "CreateConfigurationSet")
	assert.Contains(t, ops, "GetConfigurationSet")
	assert.Contains(t, ops, "ListConfigurationSets")
	assert.Contains(t, ops, "DeleteConfigurationSet")
}

func TestSESv2Handler_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newHandler()
	assert.Equal(t, 85, h.MatchPriority())
}

func TestSESv2Handler_ProviderInit(t *testing.T) {
	t.Parallel()

	p := &sesv2.Provider{}
	assert.Equal(t, "SESv2", p.Name())
}

func TestSESv2Handler_ProviderInitWithAppCtx(t *testing.T) {
	t.Parallel()

	p := &sesv2.Provider{}

	appCtx := &service.AppContext{
		Logger: slog.Default(),
	}

	svc, err := p.Init(appCtx)
	require.NoError(t, err)
	require.NotNil(t, svc)
	assert.Equal(t, "SESv2", svc.Name())
}

func TestSESv2Handler_UnknownRoute(t *testing.T) {
	t.Parallel()

	h := newHandler()

	req := httptest.NewRequest(http.MethodGet, "/v2/email/unknown-resource", nil)
	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestSESv2Handler_ChaosProvider(t *testing.T) {
	t.Parallel()

	h := newHandler()

	assert.Equal(t, "sesv2", h.ChaosServiceName())
	assert.Equal(t, h.GetSupportedOperations(), h.ChaosOperations())
	assert.NotEmpty(t, h.ChaosRegions())
}

func TestSESv2Handler_Persistence(t *testing.T) {
	t.Parallel()

	h := newHandler()

	// Create some state.
	doRequest(t, h, http.MethodPost, "/v2/email/identities", map[string]any{"EmailIdentity": "persist@example.com"})
	doRequest(
		t,
		h,
		http.MethodPost,
		"/v2/email/configuration-sets",
		map[string]any{"ConfigurationSetName": "persist-config"},
	)

	// Snapshot.
	snap := h.Snapshot()
	assert.NotEmpty(t, snap)

	// Restore to a fresh backend.
	h2 := newHandler()
	require.NoError(t, h2.Restore(snap))

	// Verify state was restored.
	rec := doRequest(t, h2, http.MethodGet, "/v2/email/identities/persist@example.com", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	rec2 := doRequest(t, h2, http.MethodGet, "/v2/email/configuration-sets/persist-config", nil)
	assert.Equal(t, http.StatusOK, rec2.Code)
}

func TestSESv2Handler_InvalidJSON(t *testing.T) {
	t.Parallel()

	h := newHandler()

	req := httptest.NewRequest(http.MethodPost, "/v2/email/identities", strings.NewReader("not-json"))
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
