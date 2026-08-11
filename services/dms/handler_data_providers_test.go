package dms_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/dms"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeleteDataProvider(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	h.Backend.AddDataProviderInternal("del-dp", "mysql")

	rec := doDMS(t, h, "DeleteDataProvider", map[string]any{
		"DataProviderIdentifier": "del-dp",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 0, h.Backend.DataProviderCount())
}

func TestModifyDataProvider(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	h.Backend.AddDataProviderInternal("mod-dp", "mysql")

	rec := doDMS(t, h, "ModifyDataProvider", map[string]any{
		"DataProviderIdentifier": "mod-dp",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec2 := doDMS(t, h, "ModifyDataProvider", map[string]any{
		"DataProviderIdentifier": "nonexistent",
	})
	assert.Equal(t, http.StatusNotFound, rec2.Code)
}

func TestDeleteDataProvider_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	rec := doDMS(t, h, "DeleteDataProvider", map[string]any{
		"DataProviderIdentifier": "arn:aws:dms:us-east-1:123:data-provider:nonexistent",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)

	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "ResourceNotFoundFault", body["__type"])
}

func TestHandler_CreateDataProvider(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, h *dms.Handler)
		name string
	}{
		{
			name: "create_success",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				rec := doDMS(t, h, "CreateDataProvider", map[string]any{
					"DataProviderName": "my-provider",
					"Engine":           "mysql",
					"Description":      "My MySQL provider",
					"Tags": []map[string]string{
						{"Key": "Team", "Value": "infra"},
					},
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec)
				dp, ok := resp["DataProvider"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "my-provider", dp["DataProviderName"])
				assert.Equal(t, "mysql", dp["Engine"])
				assert.Equal(t, "My MySQL provider", dp["Description"])
				assert.NotEmpty(t, dp["DataProviderArn"])
			},
		},
		{
			name: "create_duplicate_conflict",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				doDMS(t, h, "CreateDataProvider", map[string]any{
					"DataProviderName": "dup-provider",
					"Engine":           "postgres",
				})
				rec := doDMS(t, h, "CreateDataProvider", map[string]any{
					"DataProviderName": "dup-provider",
					"Engine":           "postgres",
				})
				assert.Equal(t, http.StatusConflict, rec.Code)
			},
		},
		{
			name: "missing_name",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				rec := doDMS(t, h, "CreateDataProvider", map[string]any{
					"Engine": "mysql",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "missing_engine",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				rec := doDMS(t, h, "CreateDataProvider", map[string]any{
					"DataProviderName": "no-engine",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestDMSHandler()
			tt.run(t, h)
		})
	}
}
