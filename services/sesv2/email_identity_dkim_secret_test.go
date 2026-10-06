package sesv2_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEmailIdentity_NoDkimPrivateKeyInResponses(t *testing.T) {
	t.Parallel()

	const secret = "BYODKIM-private-key-material"

	tests := []struct {
		body   map[string]any
		name   string
		method string
		path   string
	}{
		{name: "create", method: http.MethodPost, path: "/v2/email/identities", body: map[string]any{
			"EmailIdentity": "secret.example.com",
			"DkimSigningAttributes": map[string]any{
				"DomainSigningPrivateKey": secret, "DomainSigningSelector": "sel",
			},
		}},
		{name: "get", method: http.MethodGet, path: "/v2/email/identities/secret.example.com"},
		{name: "list", method: http.MethodGet, path: "/v2/email/identities"},
		{
			name: "put_dkim_signing", method: http.MethodPut,
			path: "/v1/email/identities/secret.example.com/dkim/signing",
			body: map[string]any{
				"SigningAttributesOrigin": "EXTERNAL",
				"SigningAttributes": map[string]any{
					"DomainSigningPrivateKey": secret, "DomainSigningSelector": "sel",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()

			create := doRequest(t, h, http.MethodPost, "/v2/email/identities", map[string]any{
				"EmailIdentity": "secret.example.com",
				"DkimSigningAttributes": map[string]any{
					"DomainSigningPrivateKey": secret, "DomainSigningSelector": "sel",
				},
			})
			require.Equal(t, http.StatusOK, create.Code)
			assert.NotContains(t, create.Body.String(), secret)

			rec := doRequest(t, h, tt.method, tt.path, tt.body)
			assert.NotContains(t, rec.Body.String(), secret)
			assert.NotContains(t, rec.Body.String(), "DomainSigningPrivateKey")
		})
	}
}
