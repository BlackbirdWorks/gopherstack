package apigatewaymanagementapi_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParity_GoneException_Shape verifies a GoneException uses the AWS rest-json
// shape: the modeled type is carried in the X-Amzn-Errortype header and the
// body __type field, with a human-readable "message" (not the type name).
func TestParity_GoneException_Shape(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/@connections/conn-missing", []byte(`{"message":"hi"}`))

	require.Equal(t, http.StatusGone, rec.Code)
	assert.Equal(t, "GoneException", rec.Header().Get("X-Amzn-Errortype"))

	var body map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))

	assert.Equal(t, "GoneException", body["__type"])
	assert.NotEqual(t, "GoneException", body["message"],
		"message must be a human-readable string, not the error type")
	assert.NotEmpty(t, body["message"])
}
