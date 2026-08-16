package lakeformation_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lakeformation"
)

func TestHandler_GetDataLakeSettings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "empty_body",
			body:       "",
			wantStatus: http.StatusOK,
		},
		{
			name:       "with_catalog_id",
			body:       `{"CatalogId":"123456789012"}`,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			rec := doLFRequest(t, h, "/GetDataLakeSettings", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Contains(t, resp, "DataLakeSettings")
		})
	}
}

func TestHandler_PutDataLakeSettings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name: "valid_settings",
			body: `{"DataLakeSettings":{"DataLakeAdmins":[` +
				`{"DataLakePrincipalIdentifier":"arn:aws:iam::123456789012:user/admin"}]}}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing_settings",
			body:       `{}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid_json",
			body:       `not-json`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			rec := doLFRequest(t, h, "/PutDataLakeSettings", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestDataLakeSettings_FullRoundTrip(t *testing.T) {
	t.Parallel()

	trueVal := true

	tests := []struct {
		name     string
		settings map[string]any
		checkKey string
	}{
		{
			name: "ReadOnlyAdmins persisted",
			settings: map[string]any{
				"DataLakeSettings": map[string]any{
					"ReadOnlyAdmins": []any{
						map[string]any{"DataLakePrincipalIdentifier": "arn:aws:iam::123:user/readonly"},
					},
				},
			},
			checkKey: "ReadOnlyAdmins",
		},
		{
			name: "Parameters persisted",
			settings: map[string]any{
				"DataLakeSettings": map[string]any{
					"Parameters": map[string]any{
						"CROSS_ACCOUNT_VERSION": "3",
					},
				},
			},
			checkKey: "Parameters",
		},
		{
			name: "AllowExternalDataFiltering persisted",
			settings: map[string]any{
				"DataLakeSettings": map[string]any{
					"AllowExternalDataFiltering": trueVal,
				},
			},
			checkKey: "AllowExternalDataFiltering",
		},
		{
			name: "AuthorizedSessionTagValueList persisted",
			settings: map[string]any{
				"DataLakeSettings": map[string]any{
					"AuthorizedSessionTagValueList": []any{"val1", "val2"},
				},
			},
			checkKey: "AuthorizedSessionTagValueList",
		},
		{
			name: "AllowFullTableExternalDataAccess persisted",
			settings: map[string]any{
				"DataLakeSettings": map[string]any{
					"AllowFullTableExternalDataAccess": trueVal,
				},
			},
			checkKey: "AllowFullTableExternalDataAccess",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := lakeformation.NewInMemoryBackend()
			h := lakeformation.NewHandler(b)

			rec := postJSON(t, h, "/PutDataLakeSettings", tt.settings)
			require.Equal(t, http.StatusOK, rec.Code)

			rec2 := postJSON(t, h, "/GetDataLakeSettings", map[string]any{})
			require.Equal(t, http.StatusOK, rec2.Code)

			var out map[string]any
			require.NoError(t, jsonDecode(rec2.Body, &out))

			settings, ok := out["DataLakeSettings"].(map[string]any)
			require.True(t, ok, "DataLakeSettings missing from response")
			assert.NotNil(t, settings[tt.checkKey], "field %q not persisted", tt.checkKey)
		})
	}
}
