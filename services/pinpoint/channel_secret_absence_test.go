package pinpoint_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestChannelResponses_NoSecretsInRawBody(t *testing.T) {
	t.Parallel()

	tests := []struct {
		req         map[string]any
		name        string
		channel     string
		absentKeys  []string
		absentVals  []string
		hasTokenKey bool
	}{
		{
			name:    "apns",
			channel: "apns",
			req: map[string]any{
				"BundleId": "com.example.b", "Certificate": "cert-secret", "PrivateKey": "pk-secret",
				"TeamId": "TEAM1", "TokenKey": "tok-secret", "TokenKeyId": "tokid", "Enabled": true,
			},
			absentKeys:  []string{"BundleId", "Certificate", "PrivateKey", "TeamId", "TokenKey", "TokenKeyId"},
			absentVals:  []string{"cert-secret", "pk-secret", "tok-secret"},
			hasTokenKey: true,
		},
		{
			name:    "apns_voip_sandbox",
			channel: "apns_voip_sandbox",
			req: map[string]any{
				"Certificate": "cert-secret", "PrivateKey": "pk-secret", "Enabled": true,
			},
			absentKeys: []string{"Certificate", "PrivateKey"},
			absentVals: []string{"cert-secret", "pk-secret"},
		},
		{
			name:    "adm",
			channel: "adm",
			req: map[string]any{
				"ClientId": "adm-id", "ClientSecret": "adm-secret", "Enabled": true,
			},
			absentKeys: []string{"ClientId", "ClientSecret"},
			absentVals: []string{"adm-secret"},
		},
		{
			name:    "baidu",
			channel: "baidu",
			req: map[string]any{
				"ApiKey": "baidu-api", "SecretKey": "baidu-secret", "Enabled": true,
			},
			absentKeys: []string{"ApiKey", "SecretKey"},
			absentVals: []string{"baidu-secret"},
		},
		{
			name:    "gcm",
			channel: "gcm",
			req: map[string]any{
				"ApiKey": "gcm-api", "ServiceJson": `{"private_key":"gcm-svc-secret"}`, "Enabled": true,
			},
			absentKeys: []string{"ApiKey", "ServiceJson"},
			absentVals: []string{"gcm-svc-secret"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			appID := createTestApp(t, h, "secret-absence-"+tt.name)
			path := "/v1/apps/" + appID + "/channels/" + tt.channel

			put := doPinpointRequest(t, h, http.MethodPut, path, tt.req)
			require.Equal(t, http.StatusOK, put.Code)

			get := doPinpointRequest(t, h, http.MethodGet, path, nil)
			require.Equal(t, http.StatusOK, get.Code)

			all := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/channels", nil)
			require.Equal(t, http.StatusOK, all.Code)

			for label, rec := range map[string][]byte{"put": put.Body.Bytes(), "get": get.Body.Bytes()} {
				body := pinpointJSON(t, rec)

				for _, k := range tt.absentKeys {
					assert.NotContains(t, body, k, label)
				}

				for _, v := range tt.absentVals {
					assert.NotContains(t, string(rec), v, label)
				}
			}

			for _, v := range tt.absentVals {
				assert.NotContains(t, all.Body.String(), v, "channels")
			}

			listed := pinpointJSON(t, all.Body.Bytes())["Channels"].(map[string]any)
			for _, entry := range listed {
				for _, k := range tt.absentKeys {
					assert.NotContains(t, entry, k, "channels")
				}
			}

			if tt.hasTokenKey {
				assert.Equal(t, true, pinpointJSON(t, get.Body.Bytes())["HasTokenKey"])
			}
		})
	}
}
