package sesv2_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func errBody(t *testing.T, body []byte) (string, string) {
	t.Helper()

	var out struct {
		Type    string `json:"__type"`
		Message string `json:"message"`
	}
	require.NoError(t, json.Unmarshal(body, &out))

	return out.Type, out.Message
}

func TestCreateValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		path       string
		wantStatus int
	}{
		{
			name: "identity email", path: "/v2/email/identities",
			body: map[string]any{"EmailIdentity": "user@example.com"}, wantStatus: http.StatusOK,
		},
		{
			name: "identity domain", path: "/v2/email/identities",
			body: map[string]any{"EmailIdentity": "example.org"}, wantStatus: http.StatusOK,
		},
		{
			name: "identity spaces", path: "/v2/email/identities",
			body: map[string]any{"EmailIdentity": "not an email"}, wantStatus: http.StatusBadRequest,
		},
		{
			name: "identity double at", path: "/v2/email/identities",
			body: map[string]any{"EmailIdentity": "a@@b.com"}, wantStatus: http.StatusBadRequest,
		},
		{
			name: "identity empty local part", path: "/v2/email/identities",
			body: map[string]any{"EmailIdentity": "@example.com"}, wantStatus: http.StatusBadRequest,
		},
		{
			name: "config set ok", path: "/v2/email/configuration-sets",
			body: map[string]any{"ConfigurationSetName": "my_set-1"}, wantStatus: http.StatusOK,
		},
		{
			name: "config set spaces", path: "/v2/email/configuration-sets",
			body: map[string]any{"ConfigurationSetName": "bad name"}, wantStatus: http.StatusBadRequest,
		},
		{
			name: "config set too long", path: "/v2/email/configuration-sets",
			body: map[string]any{"ConfigurationSetName": strings.Repeat("a", 65)}, wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, newHandler(), http.MethodPost, tt.path, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())

			if tt.wantStatus == http.StatusBadRequest {
				code, _ := errBody(t, rec.Body.Bytes())
				assert.Equal(t, "BadRequestException", code)
			}
		})
	}
}

func TestSendEmailAddressValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		to         string
		wantStatus int
	}{
		{name: "plain", to: "a@example.com", wantStatus: http.StatusOK},
		{name: "display name", to: "Alice <a@example.com>", wantStatus: http.StatusOK},
		{name: "no at", to: "notanemail", wantStatus: http.StatusBadRequest},
		{name: "empty domain", to: "a@", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			require.Equal(t, http.StatusOK, doRequest(t, h, http.MethodPost, "/v2/email/identities",
				map[string]any{"EmailIdentity": "from@example.com"}).Code)

			rec := doRequest(t, h, http.MethodPost, "/v2/email/outbound-emails", map[string]any{
				"FromEmailAddress": "from@example.com",
				"Destination":      map[string]any{"ToAddresses": []string{tt.to}},
				"Content": map[string]any{"Simple": map[string]any{
					"Subject": map[string]any{"Data": "s"},
					"Body":    map[string]any{"Text": map[string]any{"Data": "b"}},
				}},
			})
			assert.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())
		})
	}
}

func TestPagingQueryValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		path       string
		wantStatus int
	}{
		{name: "garbage token", path: "/v2/email/identities?NextToken=garbage", wantStatus: http.StatusBadRequest},
		{
			name: "garbage config sets token", path: "/v2/email/configuration-sets?NextToken=!!!",
			wantStatus: http.StatusBadRequest,
		},
		{name: "page size too big", path: "/v2/email/identities?PageSize=1001", wantStatus: http.StatusBadRequest},
		{name: "valid token", path: "/v2/email/identities?NextToken=MQ==", wantStatus: http.StatusOK},
		{name: "valid page size", path: "/v2/email/identities?PageSize=1000", wantStatus: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, newHandler(), http.MethodGet, tt.path, nil)
			assert.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())
		})
	}
}

func TestNotFoundWording(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
		want string
	}{
		{
			name: "identity", path: "/v2/email/identities/ghost@example.com",
			want: "Email identity ghost@example.com does not exist.",
		},
		{
			name: "config set", path: "/v2/email/configuration-sets/ghost",
			want: "Configuration set <ghost> does not exist.",
		},
		{name: "template", path: "/v2/email/templates/ghost", want: "Template ghost does not exist."},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, newHandler(), http.MethodGet, tt.path, nil)
			require.Equal(t, http.StatusNotFound, rec.Code)

			code, msg := errBody(t, rec.Body.Bytes())
			assert.Equal(t, "NotFoundException", code)
			assert.Equal(t, tt.want, msg)
		})
	}
}
