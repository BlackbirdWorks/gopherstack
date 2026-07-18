package lakeformation_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/lakeformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_CreateLakeFormationIdentityCenterConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "success_with_instance_arn",
			body:       `{"CatalogId":"123456789012","InstanceArn":"arn:aws:sso:::instance/ssoins-123"}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "success_no_catalog_id",
			body:       `{"InstanceArn":"arn:aws:sso:::instance/ssoins-456"}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "empty_body",
			body:       `{}`,
			wantStatus: http.StatusOK,
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

			rec := doLFRequest(t, h, "/CreateLakeFormationIdentityCenterConfiguration", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp, "ApplicationArn")
			}
		})
	}
}

func TestIdentityCenterConfiguration_RoundTrip(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/CreateLakeFormationIdentityCenterConfiguration", map[string]any{
		"CatalogId":   "123456789012",
		"InstanceArn": "arn:aws:sso:::instance/ssoins-0000000000000000",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.NotEmpty(t, out["ApplicationArn"])
	assert.Contains(t, out["ApplicationArn"].(string), "arn:aws:sso")
}

func TestIdentityCenter_DeleteAndDescribe(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)
	h.AccountID = "123456789012"

	// Create
	postJSON(t, h, "/CreateLakeFormationIdentityCenterConfiguration", map[string]any{
		"CatalogId":   "123456789012",
		"InstanceArn": "arn:aws:sso:::instance/ssoins-0000000000000000",
	})

	// Describe
	rec := postJSON(t, h, "/DescribeLakeFormationIdentityCenterConfiguration", map[string]any{
		"CatalogId": "123456789012",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, jsonDecode(rec.Body, &out))
	assert.NotEmpty(t, out["ApplicationArn"])

	// Delete
	rec2 := postJSON(t, h, "/DeleteLakeFormationIdentityCenterConfiguration", map[string]any{
		"CatalogId": "123456789012",
	})
	assert.Equal(t, http.StatusOK, rec2.Code)

	// Describe after delete should 404
	rec3 := postJSON(t, h, "/DescribeLakeFormationIdentityCenterConfiguration", map[string]any{
		"CatalogId": "123456789012",
	})
	assert.Equal(t, http.StatusNotFound, rec3.Code)
}

func TestIdentityCenter_UpdateExternalFiltering(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)
	h.AccountID = "111111111111"

	// Create first
	postJSON(t, h, "/CreateLakeFormationIdentityCenterConfiguration", map[string]any{
		"CatalogId":   "111111111111",
		"InstanceArn": "arn:aws:sso:::instance/ssoins-abc",
	})

	// Update external filtering
	rec := postJSON(t, h, "/UpdateLakeFormationIdentityCenterConfiguration", map[string]any{
		"CatalogId": "111111111111",
		"ExternalFiltering": map[string]any{
			"Status":            "ENABLED",
			"AuthorizedTargets": []string{"arn:aws:s3:::my-bucket"},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// --- GetQueryState and GetQueryStatistics ---

func TestCreateIdentityCenter_AlreadyExists(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	body := map[string]any{
		"CatalogId":   "123456789012",
		"InstanceArn": "arn:aws:sso:::instance/ssoins-abc",
	}

	rec1 := postJSON(t, h, "/CreateLakeFormationIdentityCenterConfiguration", body)
	require.Equal(t, http.StatusOK, rec1.Code)

	rec2 := postJSON(t, h, "/CreateLakeFormationIdentityCenterConfiguration", body)
	assert.Equal(t, http.StatusConflict, rec2.Code, "duplicate create should return 409")
}

func TestCreateIdentityCenter_WithExternalFiltering(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/CreateLakeFormationIdentityCenterConfiguration", map[string]any{
		"CatalogId":   "123456789012",
		"InstanceArn": "arn:aws:sso:::instance/ssoins-abc",
		"ExternalFiltering": map[string]any{
			"Status": "ENABLED",
		},
		"ShareRecipients": []any{
			map[string]any{"DataLakePrincipalIdentifier": "arn:aws:iam::456:root"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := postJSON(t, h, "/DescribeLakeFormationIdentityCenterConfiguration", map[string]any{
		"CatalogId": "123456789012",
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var out map[string]any
	require.NoError(t, jsonDecode(rec2.Body, &out))
	assert.NotNil(t, out["ExternalFiltering"])
	assert.NotEmpty(t, out["ShareRecipients"])
}

// --- #18: UpdateLakeFormationIdentityCenterConfiguration ApplicationStatus ---

func TestUpdateIdentityCenter_ApplicationStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		appStatus  string
		wantStatus int
	}{
		{
			name:       "ENABLED accepted",
			appStatus:  "ENABLED",
			wantStatus: http.StatusOK,
		},
		{
			name:       "DISABLED accepted",
			appStatus:  "DISABLED",
			wantStatus: http.StatusOK,
		},
		{
			name:       "invalid status rejected",
			appStatus:  "BROKEN",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := lakeformation.NewInMemoryBackend()
			h := lakeformation.NewHandler(b)

			// First create the config
			_, _ = b.CreateLakeFormationIdentityCenterConfiguration(
				"123456789012",
				"arn:aws:sso:::instance/i",
				nil,
				nil,
			)

			rec := postJSON(t, h, "/UpdateLakeFormationIdentityCenterConfiguration", map[string]any{
				"CatalogId":         "123456789012",
				"ApplicationStatus": tt.appStatus,
			})
			assert.Equal(t, tt.wantStatus, rec.Code, "test: %s", tt.name)

			if tt.wantStatus == http.StatusOK {
				rec2 := postJSON(t, h, "/DescribeLakeFormationIdentityCenterConfiguration", map[string]any{
					"CatalogId": "123456789012",
				})
				require.Equal(t, http.StatusOK, rec2.Code)

				var out map[string]any
				require.NoError(t, jsonDecode(rec2.Body, &out))
				assert.Equal(t, tt.appStatus, out["ApplicationStatus"])
			}
		})
	}
}
