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

func ramRequest(t *testing.T, method, path string, body any) *http.Response {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	req, err := http.NewRequestWithContext(t.Context(), method, endpoint+path, bytes.NewReader(bodyBytes))
	require.NoError(t, err)

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20230101/us-east-1/ram/aws4_request, "+
		"SignedHeaders=host;x-amz-date, Signature=fakesignature")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	return resp
}

func ramReadBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return string(data)
}

func TestIntegration_RAM_CreateResourceShare(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	resp := ramRequest(t, http.MethodPost, "/createresourceshare", map[string]any{
		"name":                    "integ-share",
		"allowExternalPrincipals": true,
	})
	body := ramReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	assert.Contains(t, body, "integ-share")
}

func TestIntegration_RAM_GetResourceShares(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	ramRequest(t, http.MethodPost, "/createresourceshare", map[string]any{
		"name":                    "list-share-integ",
		"allowExternalPrincipals": true,
	})

	resp := ramRequest(t, http.MethodPost, "/getresourceshares", map[string]any{
		"resourceOwner": "SELF",
	})
	body := ramReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	assert.Contains(t, body, "resourceShares")
}

func TestIntegration_RAM_UpdateResourceShare(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	createResp := ramRequest(t, http.MethodPost, "/createresourceshare", map[string]any{
		"name":                    "update-share-integ",
		"allowExternalPrincipals": true,
	})
	createBody := ramReadBody(t, createResp)
	require.Equal(t, http.StatusOK, createResp.StatusCode, "create body: %s", createBody)

	var createResult map[string]any
	require.NoError(t, json.Unmarshal([]byte(createBody), &createResult))

	shareARN := createResult["resourceShare"].(map[string]any)["resourceShareArn"].(string)

	resp := ramRequest(t, http.MethodPost, "/updateresourceshare", map[string]any{
		"resourceShareArn": shareARN,
		"name":             "updated-share-integ",
	})
	body := ramReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	assert.Contains(t, body, "updated-share-integ")
}

func TestIntegration_RAM_DeleteResourceShare(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	createResp := ramRequest(t, http.MethodPost, "/createresourceshare", map[string]any{
		"name": "delete-share-integ",
	})
	createBody := ramReadBody(t, createResp)
	require.Equal(t, http.StatusOK, createResp.StatusCode, "create body: %s", createBody)

	var createResult map[string]any
	require.NoError(t, json.Unmarshal([]byte(createBody), &createResult))

	shareARN := createResult["resourceShare"].(map[string]any)["resourceShareArn"].(string)

	resp := ramRequest(t, http.MethodDelete, "/deleteresourceshare?resourceShareArn="+shareARN, nil)
	body := ramReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	assert.Contains(t, body, "returnValue")
}

func TestIntegration_RAM_AssociateResourceShare(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	createResp := ramRequest(t, http.MethodPost, "/createresourceshare", map[string]any{
		"name":                    "assoc-share-integ",
		"allowExternalPrincipals": true,
	})
	createBody := ramReadBody(t, createResp)
	require.Equal(t, http.StatusOK, createResp.StatusCode)

	var createResult map[string]any
	require.NoError(t, json.Unmarshal([]byte(createBody), &createResult))

	shareARN := createResult["resourceShare"].(map[string]any)["resourceShareArn"].(string)

	resp := ramRequest(t, http.MethodPost, "/associateresourceshare", map[string]any{
		"resourceShareArn": shareARN,
		"principals":       []string{"123456789012"},
	})
	body := ramReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	assert.Contains(t, body, "resourceShareAssociations")
}

func TestIntegration_RAM_TagResource(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	createResp := ramRequest(t, http.MethodPost, "/createresourceshare", map[string]any{
		"name": "tag-share-integ",
	})
	createBody := ramReadBody(t, createResp)
	require.Equal(t, http.StatusOK, createResp.StatusCode)

	var createResult map[string]any
	require.NoError(t, json.Unmarshal([]byte(createBody), &createResult))

	shareARN := createResult["resourceShare"].(map[string]any)["resourceShareArn"].(string)

	resp := ramRequest(t, http.MethodPost, "/tagresource", map[string]any{
		"resourceShareArn": shareARN,
		"tags":             []map[string]string{{"key": "Env", "value": "test"}},
	})
	body := ramReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
}
