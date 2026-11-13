package rekognition_test

import (
	"encoding/json/v2"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

func TestDetectCustomLabels_ReturnsEmptyList(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "DetectCustomLabels", map[string]any{
		"ProjectVersionArn": "arn:xxx",
		"Image":             map[string]any{},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp, jsonx.V1()))
	assert.NotNil(t, resp["CustomLabels"])
}
