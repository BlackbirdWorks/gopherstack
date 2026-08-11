package quicksight_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestQuickSight_ActionConnector_AuthConfigRedaction drives real create+read
// traffic through the handler and checks the raw response bytes -- not a
// parsed Go struct -- because a struct-level assertion can pass while a
// secret is still present on the wire under a field the struct doesn't map.
func TestQuickSight_ActionConnector_AuthConfigRedaction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		authConfig  map[string]any
		wantAbsent  []string
		wantPresent []string
	}{
		{
			name: "api key metadata drops api key",
			authConfig: map[string]any{
				"AuthenticationType": "API_KEY",
				"AuthenticationMetadata": map[string]any{
					"ApiKeyConnectionMetadata": map[string]any{
						"ApiKey":       "sk-topsecret-apikey-001",
						"BaseEndpoint": "https://apikey.example.com",
						"Email":        "apikey-user@example.com",
					},
				},
			},
			wantAbsent:  []string{"sk-topsecret-apikey-001"},
			wantPresent: []string{"https://apikey.example.com", "apikey-user@example.com"},
		},
		{
			name: "basic auth metadata drops password",
			authConfig: map[string]any{
				"AuthenticationType": "BASIC",
				"AuthenticationMetadata": map[string]any{
					"BasicAuthConnectionMetadata": map[string]any{
						"BaseEndpoint": "https://basic.example.com",
						"Username":     "basic-auth-user",
						"Password":     "hunter2-topsecret-pw",
					},
				},
			},
			wantAbsent:  []string{"hunter2-topsecret-pw"},
			wantPresent: []string{"https://basic.example.com", "basic-auth-user"},
		},
		{
			name: "client credentials grant metadata drops client secret",
			authConfig: map[string]any{
				"AuthenticationType": "OAUTH2_CLIENT_CREDENTIALS",
				"AuthenticationMetadata": map[string]any{
					"ClientCredentialsGrantMetadata": map[string]any{
						"BaseEndpoint":            "https://cc.example.com",
						"ClientCredentialsSource": "SPECIFIED",
						"ClientCredentialsDetails": map[string]any{
							"ClientCredentialsGrantDetails": map[string]any{
								"ClientId":      "cc-client-id-xyz",
								"ClientSecret":  "cc-topsecret-clientsecret",
								"TokenEndpoint": "https://cc.example.com/token",
							},
						},
					},
				},
			},
			wantAbsent:  []string{"cc-topsecret-clientsecret"},
			wantPresent: []string{"cc-client-id-xyz", "https://cc.example.com/token"},
		},
		{
			name: "authorization code grant metadata drops client secret",
			authConfig: map[string]any{
				"AuthenticationType": "OAUTH2_AUTHORIZATION_CODE",
				"AuthenticationMetadata": map[string]any{
					"AuthorizationCodeGrantMetadata": map[string]any{
						"BaseEndpoint": "https://acg.example.com",
						"RedirectUrl":  "https://acg.example.com/callback",
						"AuthorizationCodeGrantCredentialsSource": "SPECIFIED",
						"AuthorizationCodeGrantCredentialsDetails": map[string]any{
							"AuthorizationCodeGrantDetails": map[string]any{
								"AuthorizationEndpoint": "https://acg.example.com/authorize",
								"ClientId":              "acg-client-id-xyz",
								"ClientSecret":          "acg-topsecret-clientsecret",
								"TokenEndpoint":         "https://acg.example.com/token",
							},
						},
					},
				},
			},
			wantAbsent: []string{"acg-topsecret-clientsecret"},
			wantPresent: []string{
				"acg-client-id-xyz", "https://acg.example.com/authorize", "https://acg.example.com/token",
			},
		},
		{
			name: "iam connection metadata gains source arn",
			authConfig: map[string]any{
				"AuthenticationType": "IAM",
				"AuthenticationMetadata": map[string]any{
					"IamConnectionMetadata": map[string]any{
						"RoleArn": "arn:aws:iam::000000000000:role/QuickSightConnectorRole",
					},
				},
			},
			wantPresent: []string{"arn:aws:iam::000000000000:role/QuickSightConnectorRole", "SourceArn"},
		},
		{
			name: "none connection metadata is unchanged",
			authConfig: map[string]any{
				"AuthenticationType": "NONE",
				"AuthenticationMetadata": map[string]any{
					"NoneConnectionMetadata": map[string]any{
						"BaseEndpoint": "https://none.example.com",
					},
				},
			},
			wantPresent: []string{"https://none.example.com"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			id := "ac-redact-" + sanitizeID(tt.name)

			createRec := doRequest(t, h, http.MethodPost, accountPath("/action-connectors"), map[string]any{
				"ActionConnectorId":    id,
				"Name":                 id,
				"Type":                 "GENERIC_HTTP",
				"AuthenticationConfig": tt.authConfig,
			})
			require.Equal(t, http.StatusOK, createRec.Code)

			describeRec := doRequest(t, h, http.MethodGet, accountPath("/action-connectors/"+id), nil)
			require.Equal(t, http.StatusOK, describeRec.Code)
			describeBody := describeRec.Body.String()

			for _, secret := range tt.wantAbsent {
				assert.NotContains(t, describeBody, secret, "describe response must not echo secret material")
			}
			for _, want := range tt.wantPresent {
				assert.Contains(t, describeBody, want, "describe response must retain non-secret fields")
			}

			listRec := doRequest(t, h, http.MethodGet, accountPath("/action-connectors"), nil)
			require.Equal(t, http.StatusOK, listRec.Code)
			listBody := listRec.Body.String()

			for _, secret := range tt.wantAbsent {
				assert.NotContains(t, listBody, secret, "list response must not echo secret material")
			}
		})
	}
}

func sanitizeID(name string) string {
	out := make([]byte, 0, len(name))
	for _, r := range name {
		if r == ' ' {
			out = append(out, '-')

			continue
		}
		out = append(out, byte(r))
	}

	return string(out)
}
