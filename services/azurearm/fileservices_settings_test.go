package azurearm_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func fileServicesProps(t *testing.T, body map[string]any) map[string]any {
	t.Helper()

	props, ok := body["properties"].(map[string]any)
	require.True(t, ok)

	return props
}

func TestFileServices_SettingsRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, props map[string]any)
		name  string
		put   string
	}{
		{name: "defaults", put: ``, check: func(t *testing.T, props map[string]any) {
			t.Helper()

			policy, _ := props["shareDeleteRetentionPolicy"].(map[string]any)
			assert.Equal(t, true, policy["enabled"])
			assert.EqualValues(t, 7, policy["days"])
		}},
		{
			name: "cors and smb applied",
			put: `{"properties":{"cors":{"corsRules":[{"allowedOrigins":["https://a.example"],` +
				`"allowedMethods":["GET","PUT"],"allowedHeaders":["x-ms-*"],"exposedHeaders":["x-ms-meta-*"],` +
				`"maxAgeInSeconds":60}]},"protocolSettings":{"smb":{"versions":"SMB3.0;SMB3.1.1",` +
				`"multichannel":{"enabled":true}}}}}`,
			check: func(t *testing.T, props map[string]any) {
				t.Helper()

				cors, _ := props["cors"].(map[string]any)
				rules, _ := cors["corsRules"].([]any)
				require.Len(t, rules, 1)

				rule, _ := rules[0].(map[string]any)
				assert.Equal(t, []any{"GET", "PUT"}, rule["allowedMethods"])
				assert.EqualValues(t, 60, rule["maxAgeInSeconds"])

				ps, _ := props["protocolSettings"].(map[string]any)
				smb, _ := ps["smb"].(map[string]any)
				assert.Equal(t, "SMB3.0;SMB3.1.1", smb["versions"])

				mc, _ := smb["multichannel"].(map[string]any)
				assert.Equal(t, true, mc["enabled"])
			},
		},
		{name: "retention disabled", put: `{"properties":{"shareDeleteRetentionPolicy":{"enabled":false}}}`,
			check: func(t *testing.T, props map[string]any) {
				t.Helper()

				policy, _ := props["shareDeleteRetentionPolicy"].(map[string]any)
				assert.Equal(t, false, policy["enabled"])
			}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			base := "/subscriptions/" + h.Settings.SubscriptionID
			status, _ := doRequest(t, h, http.MethodPut, base+"/resourcegroups/rg1", []byte(`{"location":"westus"}`))
			require.Equal(t, http.StatusCreated, status)

			acct := base + "/resourceGroups/rg1/providers/Microsoft.Storage/storageAccounts/acct1"
			status, _ = doRequest(t, h, http.MethodPut, acct, []byte(`{"location":"westus"}`))
			require.Equal(t, http.StatusOK, status)

			if tt.put != "" {
				status, _ = doRequest(t, h, http.MethodPut, acct+"/fileServices/default", []byte(tt.put))
				require.Equal(t, http.StatusOK, status)
			}

			status, body := doRequest(t, h, http.MethodGet, acct+"/fileServices/default", nil)
			require.Equal(t, http.StatusOK, status)
			tt.check(t, fileServicesProps(t, body))
		})
	}
}
