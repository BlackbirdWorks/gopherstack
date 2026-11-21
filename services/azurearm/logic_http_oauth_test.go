package azurearm_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/logic/armlogic"
	"github.com/stretchr/testify/assert"
)

func TestLogicHTTPActiveDirectoryOAuth(t *testing.T) {
	t.Parallel()

	tests := []struct {
		auth       la
		name       string
		wantStatus armlogic.WorkflowStatus
		wantCode   string
		wantAuth   string
	}{
		{
			name: "secret credential gets a bearer token", wantStatus: armlogic.WorkflowStatusSucceeded,
			wantAuth: "Bearer tok-cid",
			auth: la{
				"tenant": "t1", "audience": "https://aud/", "clientId": "cid", "credentialType": "Secret",
				"secret": "s3",
			},
		},
		{
			name:       "rejected secret fails the action",
			wantStatus: armlogic.WorkflowStatusFailed,
			wantCode:   "ActionFailed",
			auth:       la{"tenant": "t1", "audience": "https://aud/", "clientId": "cid", "secret": "wrong"},
		},
		{
			name:       "certificate credential with an undecodable pfx",
			wantStatus: armlogic.WorkflowStatusFailed,
			wantCode:   "BadRequest",
			auth: la{
				"tenant":         "t1",
				"audience":       "a",
				"clientId":       "cid",
				"credentialType": "Certificate",
				"pfx":            "x",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			gotAuth := make(chan string, 1)
			backend := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
				gotAuth <- r.Header.Get("Authorization")
			}))
			t.Cleanup(backend.Close)

			authority := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, "/t1/oauth2/token", r.URL.Path)
				assert.NoError(t, r.ParseForm())
				assert.Equal(t, "client_credentials", r.PostForm.Get("grant_type"))
				assert.Equal(t, "https://aud/", r.PostForm.Get("resource"))

				if r.PostForm.Get("client_secret") != "s3" {
					http.Error(w, `{"error":"invalid_client"}`, http.StatusUnauthorized)

					return
				}

				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write(
					[]byte(`{"token_type":"Bearer","access_token":"tok-` + r.PostForm.Get("client_id") + `"}`),
				)
			}))
			t.Cleanup(authority.Close)

			auth := tc.auth
			auth["type"], auth["authority"] = "ActiveDirectoryOAuth", authority.URL+"/"

			s := newLogicStack(t)
			s.create(t, "oauth", logicDef(requestTrigger(nil), la{
				"Call": la{
					"type": "Http", "runAfter": la{},
					"inputs": la{
						"method": "GET", "uri": backend.URL, "authentication": auth, "retryPolicy": la{"type": "none"},
					},
				},
			}))

			call := s.invoke(t, "oauth", `{}`).actions["Call"].Properties

			assert.Equal(t, tc.wantStatus, *call.Status)

			if tc.wantCode != "" {
				assert.Equal(t, tc.wantCode, *call.Code)
			}

			if tc.wantAuth != "" {
				select {
				case got := <-gotAuth:
					assert.Equal(t, tc.wantAuth, got)
				default:
					t.Error("the backend was not called")
				}
			}
		})
	}
}
