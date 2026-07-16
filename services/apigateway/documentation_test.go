package apigateway_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDocumentationPart_CRUD tests GetDocumentationPart, GetDocumentationParts, DeleteDocumentationPart.
func TestDocumentationPart_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		getWantCode     int
		deleteWantCode  int
		afterDeleteCode int
		listWantLen     int
		useValidID      bool
	}{
		{
			name:            "get_and_delete_existing",
			getWantCode:     http.StatusOK,
			deleteWantCode:  http.StatusNoContent,
			afterDeleteCode: http.StatusNotFound,
			listWantLen:     1,
			useValidID:      true,
		},
		{
			name:        "get_not_found",
			getWantCode: http.StatusNotFound,
			useValidID:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, e := boostSetup()
			apiID := boostAPI(t, handler, e)
			partID := boostDocPart(t, handler, e, apiID)

			lookupID := partID
			if !tt.useValidID {
				lookupID = "notexist"
			}

			// GetDocumentationPart
			rec := postWithHandler(t, handler, e, "GetDocumentationPart",
				fmt.Sprintf(`{"restApiId":%q,"docPartId":%q}`, apiID, lookupID))
			assert.Equal(t, tt.getWantCode, rec.Code)

			if tt.listWantLen > 0 {
				// GetDocumentationParts
				rec2 := postWithHandler(t, handler, e, "GetDocumentationParts",
					fmt.Sprintf(`{"restApiId":%q}`, apiID))
				assert.Equal(t, http.StatusOK, rec2.Code)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))
				assert.Len(t, resp["item"].([]any), tt.listWantLen)
			}

			if tt.deleteWantCode != 0 {
				rec3 := postWithHandler(t, handler, e, "DeleteDocumentationPart",
					fmt.Sprintf(`{"restApiId":%q,"docPartId":%q}`, apiID, partID))
				assert.Equal(t, tt.deleteWantCode, rec3.Code)

				rec4 := postWithHandler(t, handler, e, "GetDocumentationPart",
					fmt.Sprintf(`{"restApiId":%q,"docPartId":%q}`, apiID, partID))
				assert.Equal(t, tt.afterDeleteCode, rec4.Code)
			}
		})
	}
}

// TestDocumentationVersion_CRUD tests doc versions. //nolint:lll // existing issue.
func TestDocumentationVersion_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		version         string
		getWantCode     int
		deleteWantCode  int
		afterDeleteCode int
		listWantLen     int
		useValidVersion bool
	}{
		{
			name:            "get_and_delete_existing",
			version:         "1.0",
			getWantCode:     http.StatusOK,
			deleteWantCode:  http.StatusNoContent,
			afterDeleteCode: http.StatusNotFound,
			listWantLen:     1,
			useValidVersion: true,
		},
		{
			name:            "get_not_found",
			version:         "notexist",
			getWantCode:     http.StatusNotFound,
			useValidVersion: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, e := boostSetup()
			apiID := boostAPI(t, handler, e)
			boostDocVersion(t, handler, e, apiID, "1.0")

			lookupVersion := "1.0"
			if !tt.useValidVersion {
				lookupVersion = "notexist"
			}

			rec := postWithHandler(t, handler, e, "GetDocumentationVersion",
				fmt.Sprintf(`{"restApiId":%q,"documentationVersion":%q}`, apiID, lookupVersion))
			assert.Equal(t, tt.getWantCode, rec.Code)

			if tt.listWantLen > 0 {
				rec2 := postWithHandler(t, handler, e, "GetDocumentationVersions",
					fmt.Sprintf(`{"restApiId":%q}`, apiID))
				assert.Equal(t, http.StatusOK, rec2.Code)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))
				assert.Len(t, resp["item"].([]any), tt.listWantLen)
			}

			if tt.deleteWantCode != 0 {
				rec3 := postWithHandler(t, handler, e, "DeleteDocumentationVersion",
					fmt.Sprintf(`{"restApiId":%q,"documentationVersion":"1.0"}`, apiID))
				assert.Equal(t, tt.deleteWantCode, rec3.Code)

				rec4 := postWithHandler(t, handler, e, "GetDocumentationVersion",
					fmt.Sprintf(`{"restApiId":%q,"documentationVersion":"1.0"}`, apiID))
				assert.Equal(t, tt.afterDeleteCode, rec4.Code)
			}
		})
	}
}

// TestUpdateDocumentationPart tests UpdateDocumentationPart.
func TestUpdateDocumentationPart(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		properties string
		wantCode   int
		useValid   bool
	}{
		{
			name:       "update_properties",
			properties: `{"description":"updated"}`,
			wantCode:   http.StatusOK,
			useValid:   true,
		},
		{
			name:     "not_found",
			wantCode: http.StatusNotFound,
			useValid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, e := boostSetup()
			apiID := boostAPI(t, handler, e)
			partID := boostDocPart(t, handler, e, apiID)

			lookupID := partID
			if !tt.useValid {
				lookupID = "notexist"
			}

			rec := postWithHandler(t, handler, e, "UpdateDocumentationPart",
				fmt.Sprintf(`{"restApiId":%q,"docPartId":%q,"properties":%q}`, apiID, lookupID, tt.properties))
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestUpdateDocumentationVersion tests UpdateDocumentationVersion.
func TestUpdateDocumentationVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		version     string
		description string
		wantCode    int
	}{
		{
			name:        "update_description",
			version:     "1.0",
			description: "new description",
			wantCode:    http.StatusOK,
		},
		{
			name:     "not_found",
			version:  "notexist",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, e := boostSetup()
			apiID := boostAPI(t, handler, e)
			boostDocVersion(t, handler, e, apiID, "1.0")

			rec := postWithHandler(t, handler, e, "UpdateDocumentationVersion",
				fmt.Sprintf(`{"restApiId":%q,"documentationVersion":%q,"description":%q}`,
					apiID, tt.version, tt.description))
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}
