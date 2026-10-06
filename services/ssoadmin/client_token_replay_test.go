package ssoadmin_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateOps_ClientTokenReplay(t *testing.T) {
	t.Parallel()

	tests := []struct {
		first      func(inst string) map[string]any
		second     func(inst string) map[string]any
		name       string
		op         string
		arnKey     string
		wantStatus int
		wantSame   bool
	}{
		{
			name:       "instance same token replays",
			op:         "CreateInstance",
			arnKey:     "InstanceArn",
			first:      func(string) map[string]any { return map[string]any{"Name": "i", "ClientToken": "tok"} },
			second:     func(string) map[string]any { return map[string]any{"Name": "i", "ClientToken": "tok"} },
			wantStatus: http.StatusOK,
			wantSame:   true,
		},
		{
			name:       "instance token reuse with new params conflicts",
			op:         "CreateInstance",
			arnKey:     "InstanceArn",
			first:      func(string) map[string]any { return map[string]any{"Name": "i", "ClientToken": "tok"} },
			second:     func(string) map[string]any { return map[string]any{"Name": "other", "ClientToken": "tok"} },
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "instance without token creates twice",
			op:         "CreateInstance",
			arnKey:     "InstanceArn",
			first:      func(string) map[string]any { return map[string]any{"Name": "i"} },
			second:     func(string) map[string]any { return map[string]any{"Name": "i"} },
			wantStatus: http.StatusOK,
		},
		{
			name:   "application same token replays",
			op:     "CreateApplication",
			arnKey: "ApplicationArn",
			first: func(inst string) map[string]any {
				return map[string]any{
					"InstanceArn": inst, "Name": "app", "ClientToken": "tok",
					"ApplicationProviderArn": "arn:aws:sso::aws:applicationProvider/custom",
				}
			},
			second: func(inst string) map[string]any {
				return map[string]any{
					"InstanceArn": inst, "Name": "app", "ClientToken": "tok",
					"ApplicationProviderArn": "arn:aws:sso::aws:applicationProvider/custom",
				}
			},
			wantStatus: http.StatusOK,
			wantSame:   true,
		},
		{
			name:   "application without token conflicts on name",
			op:     "CreateApplication",
			arnKey: "ApplicationArn",
			first: func(inst string) map[string]any {
				return map[string]any{
					"InstanceArn": inst, "Name": "app",
					"ApplicationProviderArn": "arn:aws:sso::aws:applicationProvider/custom",
				}
			},
			second: func(inst string) map[string]any {
				return map[string]any{
					"InstanceArn": inst, "Name": "app",
					"ApplicationProviderArn": "arn:aws:sso::aws:applicationProvider/custom",
				}
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:   "trusted token issuer same token replays",
			op:     "CreateTrustedTokenIssuer",
			arnKey: "TrustedTokenIssuerArn",
			first: func(inst string) map[string]any {
				return map[string]any{
					"InstanceArn": inst, "Name": "tti", "ClientToken": "tok",
					"TrustedTokenIssuerConfiguration": map[string]any{
						"OidcJwtConfiguration": map[string]any{
							"IssuerUrl": "https://idp.example.com", "ClaimAttributePath": "email",
							"IdentityStoreAttributePath": "emails.value", "JwksRetrievalOption": "OPEN_ID_DISCOVERY",
						},
					},
				}
			},
			second: func(inst string) map[string]any {
				return map[string]any{
					"InstanceArn": inst, "Name": "tti", "ClientToken": "tok",
					"TrustedTokenIssuerConfiguration": map[string]any{
						"OidcJwtConfiguration": map[string]any{
							"IssuerUrl": "https://idp.example.com", "ClaimAttributePath": "email",
							"IdentityStoreAttributePath": "emails.value", "JwksRetrievalOption": "OPEN_ID_DISCOVERY",
						},
					},
				}
			},
			wantStatus: http.StatusOK,
			wantSame:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			inst := createInstance(t, h, "base")

			r1 := doRequest(t, h, tt.op, tt.first(inst))
			require.Equal(t, http.StatusOK, r1.Code, r1.Body.String())
			arn1, _ := parseResponse(t, r1)[tt.arnKey].(string)

			r2 := doRequest(t, h, tt.op, tt.second(inst))
			require.Equal(t, tt.wantStatus, r2.Code, r2.Body.String())

			if tt.wantStatus != http.StatusOK {
				assert.Contains(t, r2.Body.String(), "ConflictException")

				return
			}

			arn2, _ := parseResponse(t, r2)[tt.arnKey].(string)
			if tt.wantSame {
				assert.Equal(t, arn1, arn2)
			} else {
				assert.NotEqual(t, arn1, arn2)
			}
		})
	}
}
