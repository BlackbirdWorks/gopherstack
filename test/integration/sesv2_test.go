package integration_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sesv2Do performs a JSON request to the SES v2 REST endpoint.
func sesv2Do(t *testing.T, method, path string, body any) *http.Response {
	t.Helper()

	var bodyReader io.Reader

	if body != nil {
		b, err := json.Marshal(body)
		require.NoError(t, err)

		bodyReader = bytes.NewReader(b)
	}

	req, err := http.NewRequestWithContext(t.Context(), method, endpoint+path, bodyReader)
	require.NoError(t, err)

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	return resp
}

func sesv2ReadBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return string(data)
}

func TestIntegration_SESv2_CreateEmailIdentity(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	resp := sesv2Do(t, http.MethodPost, "/v2/email/identities", map[string]any{
		"EmailIdentity": "integ-sesv2@example.com",
	})
	body := sesv2ReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)

	var out map[string]any
	require.NoError(t, json.Unmarshal([]byte(body), &out))
	assert.Equal(t, "EMAIL_ADDRESS", out["IdentityType"])
	assert.Equal(t, true, out["VerifiedForSending"])
}

func TestIntegration_SESv2_GetEmailIdentity(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	// Create identity first.
	identity := "integ-sesv2-get@example.com"
	sesv2ReadBody(t, sesv2Do(t, http.MethodPost, "/v2/email/identities", map[string]any{
		"EmailIdentity": identity,
	}))

	resp := sesv2Do(t, http.MethodGet, "/v2/email/identities/"+identity, nil)
	body := sesv2ReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)

	var out map[string]any
	require.NoError(t, json.Unmarshal([]byte(body), &out))
	assert.Equal(t, identity, out["EmailIdentity"])
	assert.Equal(t, "EMAIL_ADDRESS", out["IdentityType"])
}

func TestIntegration_SESv2_ListEmailIdentities(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	// Create identities.
	sesv2ReadBody(t, sesv2Do(t, http.MethodPost, "/v2/email/identities", map[string]any{
		"EmailIdentity": "list-a-sesv2@integ-test.com",
	}))
	sesv2ReadBody(t, sesv2Do(t, http.MethodPost, "/v2/email/identities", map[string]any{
		"EmailIdentity": "list-b-sesv2@integ-test.com",
	}))

	resp := sesv2Do(t, http.MethodGet, "/v2/email/identities", nil)
	body := sesv2ReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)

	var out map[string]any
	require.NoError(t, json.Unmarshal([]byte(body), &out))

	identities, ok := out["EmailIdentities"].([]any)
	require.True(t, ok)
	assert.GreaterOrEqual(t, len(identities), 2)
}

func TestIntegration_SESv2_DeleteEmailIdentity(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	identity := "integ-sesv2-del@example.com"

	// Create.
	sesv2ReadBody(t, sesv2Do(t, http.MethodPost, "/v2/email/identities", map[string]any{
		"EmailIdentity": identity,
	}))

	// Delete.
	resp := sesv2Do(t, http.MethodDelete, "/v2/email/identities/"+identity, nil)
	sesv2ReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Verify it's gone.
	resp2 := sesv2Do(t, http.MethodGet, "/v2/email/identities/"+identity, nil)
	sesv2ReadBody(t, resp2)

	assert.Equal(t, http.StatusNotFound, resp2.StatusCode)
}

func TestIntegration_SESv2_SendEmail(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	resp := sesv2Do(t, http.MethodPost, "/v2/email/outbound-emails", map[string]any{
		"FromEmailAddress": "sender-sesv2@integ-test.com",
		"Destination": map[string]any{
			"ToAddresses": []string{"recipient-sesv2@integ-test.com"},
		},
		"Content": map[string]any{
			"Simple": map[string]any{
				"Subject": map[string]any{"Data": "SES v2 Integration Test"},
				"Body": map[string]any{
					"Text": map[string]any{"Data": "Hello from SES v2"},
					"Html": map[string]any{"Data": "<p>Hello from SES v2</p>"},
				},
			},
		},
	})
	body := sesv2ReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)

	var out map[string]any
	require.NoError(t, json.Unmarshal([]byte(body), &out))
	assert.NotEmpty(t, out["MessageId"])
}

func TestIntegration_SESv2_CreateConfigurationSet(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	resp := sesv2Do(t, http.MethodPost, "/v2/email/configuration-sets", map[string]any{
		"ConfigurationSetName": "integ-config-set",
	})
	body := sesv2ReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
}

func TestIntegration_SESv2_GetConfigurationSet(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	name := "integ-get-config-set"

	sesv2ReadBody(t, sesv2Do(t, http.MethodPost, "/v2/email/configuration-sets", map[string]any{
		"ConfigurationSetName": name,
	}))

	resp := sesv2Do(t, http.MethodGet, "/v2/email/configuration-sets/"+name, nil)
	body := sesv2ReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)

	var out map[string]any
	require.NoError(t, json.Unmarshal([]byte(body), &out))
	assert.Equal(t, name, out["ConfigurationSetName"])
}

func TestIntegration_SESv2_ListConfigurationSets(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	sesv2ReadBody(t, sesv2Do(t, http.MethodPost, "/v2/email/configuration-sets", map[string]any{
		"ConfigurationSetName": "integ-list-config-a",
	}))
	sesv2ReadBody(t, sesv2Do(t, http.MethodPost, "/v2/email/configuration-sets", map[string]any{
		"ConfigurationSetName": "integ-list-config-b",
	}))

	resp := sesv2Do(t, http.MethodGet, "/v2/email/configuration-sets", nil)
	body := sesv2ReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)

	var out map[string]any
	require.NoError(t, json.Unmarshal([]byte(body), &out))

	sets, ok := out["ConfigurationSets"].([]any)
	require.True(t, ok)
	assert.GreaterOrEqual(t, len(sets), 2)
}

func TestIntegration_SESv2_DeleteConfigurationSet(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	name := "integ-del-config-set"

	sesv2ReadBody(t, sesv2Do(t, http.MethodPost, "/v2/email/configuration-sets", map[string]any{
		"ConfigurationSetName": name,
	}))

	resp := sesv2Do(t, http.MethodDelete, "/v2/email/configuration-sets/"+name, nil)
	sesv2ReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	resp2 := sesv2Do(t, http.MethodGet, "/v2/email/configuration-sets/"+name, nil)
	sesv2ReadBody(t, resp2)

	assert.Equal(t, http.StatusNotFound, resp2.StatusCode)
}
