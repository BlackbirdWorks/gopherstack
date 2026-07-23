package opensearch_test

import (
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/opensearch"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateApplication_EmptyName(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend(testAccountID, testRegion)
	_, err := b.CreateApplication("", nil, nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, opensearch.ErrInvalidParameter)
}

func TestCreateApplication_Duplicate(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend(testAccountID, testRegion)
	_, err := b.CreateApplication("my-app", nil, nil)
	require.NoError(t, err)

	_, err = b.CreateApplication("my-app", nil, nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, opensearch.ErrApplicationAlreadyExists)
}

func TestApplications_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		appName     string
		appConfigs  []map[string]string
		wantInList  bool
		wantGetOK   bool
		wantDeleted bool
	}{
		{
			name:       "create_and_list",
			appName:    "my-app",
			wantInList: true,
			wantGetOK:  true,
		},
		{
			name:        "create_and_delete",
			appName:     "del-app",
			wantInList:  true,
			wantDeleted: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := opensearch.NewInMemoryBackend("123456789012", "us-east-1")

			app, err := b.CreateApplication(tt.appName, nil, nil)
			require.NoError(t, err)
			assert.NotEmpty(t, app.ID)
			assert.Equal(t, tt.appName, app.Name)

			apps := b.ListApplications()
			if tt.wantInList {
				require.NotEmpty(t, apps)
				found := false
				for _, a := range apps {
					if a.Name == tt.appName {
						found = true
					}
				}
				assert.True(t, found, "app should appear in list")
			}

			if tt.wantGetOK {
				got, getErr := b.GetApplication(app.ID)
				require.NoError(t, getErr)
				assert.Equal(t, tt.appName, got.Name)
			}

			if tt.wantDeleted {
				err = b.DeleteApplication(app.ID)
				require.NoError(t, err)

				apps = b.ListApplications()
				for _, a := range apps {
					assert.NotEqual(t, app.ID, a.ID, "deleted app should not appear in list")
				}
			}
		})
	}
}

func TestApplications_HTTPHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		appName    string
		wantFields []string
	}{
		{
			name:       "get_returns_real_app_data",
			appName:    "real-app",
			wantFields: []string{"Id", "Name", "Arn"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			// Create app via HTTP.
			cr := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/application",
				map[string]any{"Name": tt.appName, "AppConfigs": []any{}, "DataSources": []any{}})
			cr.Body.Close()
			require.Equal(t, http.StatusOK, cr.StatusCode)

			// List apps via HTTP.
			lr := doRequest(t, h, http.MethodGet, "/2021-01-01/opensearch/application", nil)
			defer lr.Body.Close()
			require.Equal(t, http.StatusOK, lr.StatusCode)

			var out map[string]any
			require.NoError(t, json.NewDecoder(lr.Body).Decode(&out))

			apps, ok := out["ApplicationSummaries"].([]any)
			require.True(t, ok)
			require.NotEmpty(t, apps)

			first := apps[0].(map[string]any)
			for _, field := range tt.wantFields {
				assert.Contains(t, first, field, "expected field %q in application list", field)
			}
			assert.Equal(t, tt.appName, first["Name"])
		})
	}
}

func TestApplications_GetAndUpdateWireShape(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	cr := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/application",
		map[string]any{"Name": "wire-app"})
	var cOut map[string]any
	require.NoError(t, json.NewDecoder(cr.Body).Decode(&cOut))
	cr.Body.Close()
	require.Equal(t, http.StatusOK, cr.StatusCode)

	appID := cOut["Id"].(string)
	assert.NotEmpty(t, cOut["CreatedAt"])
	// CreateApplicationOutput has no Status field on the real API.
	assert.NotContains(t, cOut, "Status")

	// GetApplication must include Status, Endpoint, CreatedAt, LastUpdatedAt.
	gr := doRequest(t, h, http.MethodGet, "/2021-01-01/opensearch/application/"+appID, nil)
	defer gr.Body.Close()
	require.Equal(t, http.StatusOK, gr.StatusCode)

	var gOut map[string]any
	require.NoError(t, json.NewDecoder(gr.Body).Decode(&gOut))
	assert.Equal(t, "ACTIVE", gOut["Status"])
	assert.NotEmpty(t, gOut["Endpoint"])
	assert.NotEmpty(t, gOut["CreatedAt"])
	assert.NotEmpty(t, gOut["LastUpdatedAt"])

	// UpdateApplication must not carry a Status field on the real API.
	ur := doRequest(t, h, http.MethodPut, "/2021-01-01/opensearch/application/"+appID,
		map[string]any{"AppConfigs": []any{}, "DataSources": []any{}})
	defer ur.Body.Close()
	require.Equal(t, http.StatusOK, ur.StatusCode)

	var uOut map[string]any
	require.NoError(t, json.NewDecoder(ur.Body).Decode(&uOut))
	assert.NotContains(t, uOut, "Status")
	assert.NotEmpty(t, uOut["CreatedAt"])
	assert.NotEmpty(t, uOut["LastUpdatedAt"])
}

func TestOpenSearchHandler_CreateApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		appName      string
		wantContains []string
		wantCode     int
	}{
		{
			name:         "success",
			appName:      "my-app",
			wantCode:     http.StatusOK,
			wantContains: []string{"my-app", "Id", "Arn"},
		},
		{
			name:     "no_name",
			appName:  "",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "duplicate",
			appName:  "dup-app",
			wantCode: http.StatusConflict,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			if tt.name == "duplicate" {
				r := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/application",
					map[string]any{"Name": "dup-app"})
				r.Body.Close()
			}

			body := map[string]any{"Name": tt.appName}
			resp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/application", body)
			defer resp.Body.Close()

			assert.Equal(t, tt.wantCode, resp.StatusCode)

			if len(tt.wantContains) > 0 {
				bodyBytes, err := io.ReadAll(resp.Body)
				require.NoError(t, err)
				for _, s := range tt.wantContains {
					assert.Contains(t, string(bodyBytes), s)
				}
			}
		})
	}
}

func TestOpenSearchHandler_CreateApplication_WithConfigs(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	body := map[string]any{
		"Name": "configured-app",
		"AppConfigs": []map[string]string{
			{"Key": "opensearchDashboards.enabled", "Value": "true"},
		},
		"DataSources": []map[string]string{
			{"DataSourceArn": "arn:aws:opensearch:us-east-1:123456789012:domain/test"},
		},
	}

	resp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/application", body)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	bodyBytes, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Contains(t, string(bodyBytes), "configured-app")
	assert.Contains(t, string(bodyBytes), "opensearchDashboards.enabled")
}
