package securityhub_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/securityhub"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Batch-1 accuracy gap: ListSecurityControlDefinitions is GET /securityControls/definitions.
func TestListSecurityControlDefinitionsPath(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodGet, "/securityControls/definitions", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	defs, _ := resp["SecurityControlDefinitions"].([]any)
	assert.NotEmpty(t, defs)

	d0 := defs[0].(map[string]any)
	assert.NotEmpty(t, d0["SecurityControlId"])
	assert.NotEmpty(t, d0["Title"])
	assert.NotEmpty(t, d0["SeverityRating"])
}

// Batch-1 accuracy gap: GetSecurityControlDefinition is GET /securityControl/definition?SecurityControlId=...
func TestGetSecurityControlDefinitionPath(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/securityControl/definition?SecurityControlId=CloudTrail.1", nil)
	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)

	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	def, _ := resp["SecurityControlDefinition"].(map[string]any)
	assert.Equal(t, "CloudTrail.1", def["SecurityControlId"])
}

// Batch-1 accuracy gap: BatchGetSecurityControls is POST /securityControls/batchGet.
func TestBatchGetSecurityControlsPath(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/securityControls/batchGet", map[string]any{
		"SecurityControlIds": []string{"IAM.1", "S3.1"},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	controls, _ := resp["SecurityControls"].([]any)
	assert.Len(t, controls, 2)

	for _, ctrl := range controls {
		cm := ctrl.(map[string]any)
		assert.NotEmpty(t, cm["SecurityControlId"])
		assert.NotEmpty(t, cm["SecurityControlArn"])
		assert.Contains(t, cm["SecurityControlArn"].(string), "arn:aws:securityhub:")
		assert.Contains(t, cm["SecurityControlArn"].(string), ":security-control/")
		assert.Equal(t, "ENABLED", cm["SecurityControlStatus"])
	}
}

func TestBackend_UpdateSecurityControl(t *testing.T) {
	t.Parallel()

	tests := []struct {
		params    map[string]any
		name      string
		controlID string
	}{
		{
			name:      "update known control",
			controlID: "CloudTrail.1",
			params:    map[string]any{"key": "value"},
		},
		{
			name:      "update unknown control stores params",
			controlID: "Unknown.99",
			params:    map[string]any{"threshold": 10},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := securityhub.NewInMemoryBackend("000000000000", "us-east-1")
			err := b.UpdateSecurityControl(tc.controlID, tc.params, "")
			require.NoError(t, err)

			// verify via BatchGetSecurityControls
			if tc.controlID == "CloudTrail.1" {
				controls, unprocessed := b.BatchGetSecurityControls([]string{tc.controlID})
				require.Len(t, controls, 1)
				assert.Empty(t, unprocessed)
				assert.Equal(t, "value", controls[0].Parameters["key"])
			}
		})
	}
}

func TestHandler_UpdateSecurityControl(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{name: "update security control parameters", wantCode: http.StatusOK},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPatch, "/securityControl/update", map[string]any{
				"SecurityControlId": "CloudTrail.1",
				"Parameters":        map[string]any{"key": "value"},
				"LastUpdateReason":  "Testing",
			})
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}
