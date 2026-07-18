package apigateway_test

import (
	"archive/zip"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigateway"
)

func TestAPIGateway_GetSdkTypes(t *testing.T) {
	t.Parallel()

	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend)

	rec := restCall(t, h, http.MethodGet, "/sdktypes", "", "")
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	items, _ := resp["item"].([]any)
	require.NotEmpty(t, items)

	ids := make([]string, 0, len(items))
	for _, it := range items {
		ids = append(ids, it.(map[string]any)["id"].(string))
	}

	assert.Contains(t, ids, "java")
	assert.Contains(t, ids, "javascript")
	assert.Contains(t, ids, "android")
}

func TestAPIGateway_GetSdkType(t *testing.T) {
	t.Parallel()

	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend)

	rec := restCall(t, h, http.MethodGet, "/sdktypes/java", "", "")
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "java", resp["id"])
	assert.Equal(t, "Java", resp["friendlyName"])

	notFoundRec := restCall(t, h, http.MethodGet, "/sdktypes/cobol", "", "")
	assert.Equal(t, http.StatusNotFound, notFoundRec.Code)
}

func TestAPIGateway_GetSdk(t *testing.T) {
	t.Parallel()

	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend)

	api, err := backend.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "sdk-api"})
	require.NoError(t, err)
	_, err = backend.CreateDeployment(api.ID, "prod", "")
	require.NoError(t, err)

	rec := restCall(t, h, http.MethodGet, "/restapis/"+api.ID+"/stages/prod/sdks/java", "", "")
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "application/octet-stream", resp["contentType"])

	bodyB64, _ := resp["body"].(string)
	require.NotEmpty(t, bodyB64)

	raw, err := base64.StdEncoding.DecodeString(bodyB64)
	require.NoError(t, err)

	zr, err := zip.NewReader(bytes.NewReader(raw), int64(len(raw)))
	require.NoError(t, err, "GetSdk must return a real ZIP archive, not a fabricated blob")

	names := make([]string, 0, len(zr.File))
	for _, f := range zr.File {
		names = append(names, f.Name)
	}

	assert.Contains(t, names, "swagger.json")
	assert.Contains(t, names, "README.txt")

	// Unknown sdkType must be rejected, not silently accepted.
	badRec := restCall(t, h, http.MethodGet, "/restapis/"+api.ID+"/stages/prod/sdks/cobol", "", "")
	assert.Equal(t, http.StatusBadRequest, badRec.Code)

	// Unknown stage must 404.
	missingStageRec := restCall(t, h, http.MethodGet, "/restapis/"+api.ID+"/stages/nope/sdks/java", "", "")
	assert.Equal(t, http.StatusNotFound, missingStageRec.Code)
}
