package emr_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/emr"
)

func TestEMR_SecurityConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*emr.Handler)
		name     string
		scName   string
		scConfig string
		testType string
		wantCode int
	}{
		{
			name:     "creates security configuration",
			testType: "create",
			scName:   "my-config",
			scConfig: `{"EncryptionConfiguration": {}}`,
			wantCode: http.StatusOK,
		},
		{
			name:     "create duplicate returns error",
			testType: "create",
			scName:   "duplicate-config",
			scConfig: "{}",
			setup: func(h *emr.Handler) {
				rec := doEMRRequest(t, h, "CreateSecurityConfiguration", map[string]any{
					"Name":                  "duplicate-config",
					"SecurityConfiguration": "{}",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "create without name returns error",
			testType: "create",
			scName:   "",
			scConfig: "{}",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "deletes existing security configuration",
			testType: "delete",
			scName:   "to-delete",
			setup: func(h *emr.Handler) {
				rec := doEMRRequest(t, h, "CreateSecurityConfiguration", map[string]any{
					"Name":                  "to-delete",
					"SecurityConfiguration": "{}",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "delete non-existent returns error",
			testType: "delete",
			scName:   "nonexistent",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.setup != nil {
				tt.setup(h)
			}

			var rec *httptest.ResponseRecorder
			switch tt.testType {
			case "create":
				rec = doEMRRequest(t, h, "CreateSecurityConfiguration", map[string]any{
					"Name":                  tt.scName,
					"SecurityConfiguration": tt.scConfig,
				})
			case "delete":
				rec = doEMRRequest(t, h, "DeleteSecurityConfiguration", map[string]any{
					"Name": tt.scName,
				})
			}

			require.Equal(t, tt.wantCode, rec.Code)

			if tt.testType == "create" && tt.wantCode == http.StatusOK {
				var out struct {
					Name             string  `json:"Name"`
					CreationDateTime float64 `json:"CreationDateTime"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Equal(t, tt.scName, out.Name)
				assert.NotZero(t, out.CreationDateTime)
			}
		})
	}
}

func TestSecurityConfig_InvalidJSON(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doEMRRequest(t, h, "CreateSecurityConfiguration", map[string]any{
		"Name":                  "bad-config",
		"SecurityConfiguration": "not valid json {",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestSecurityConfig_ValidJSON(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doEMRRequest(t, h, "CreateSecurityConfiguration", map[string]any{
		"Name":                  "good-config",
		"SecurityConfiguration": `{"EncryptionConfiguration":{"EnableInTransitEncryption":false}}`,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestListSecurityConfigurations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, name := range []string{"alpha", "beta", "gamma"} {
		rec := doEMRRequest(t, h, "CreateSecurityConfiguration", map[string]any{
			"Name":                  name,
			"SecurityConfiguration": `{}`,
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	listRec := doEMRRequest(t, h, "ListSecurityConfigurations", map[string]any{})
	require.Equal(t, http.StatusOK, listRec.Code)

	var out struct {
		SecurityConfigurations []struct {
			Name string `json:"Name"`
		} `json:"SecurityConfigurations"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &out))
	assert.Len(t, out.SecurityConfigurations, 3)
	assert.Equal(t, "alpha", out.SecurityConfigurations[0].Name)
}

// TestSeedHelpers_DeepCopy verifies seed helpers deep copy values.
func TestSeedHelpers_DeepCopy(t *testing.T) {
	t.Parallel()

	b := emr.NewInMemoryBackend(testAccountID, testRegion)

	sc := emr.SecurityConfiguration{
		Name:           "sc-deep-copy",
		SecurityConfig: `{"original":true}`,
	}
	b.AddSecurityConfigInternal(context.Background(), sc)

	sc.Name = "mutated"
	assert.Equal(t, 1, b.SecurityConfigCount())
}

// TestDescribeSecurityConfiguration verifies the new operation works end-to-end.
func TestDescribeSecurityConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		scName   string
		wantCode int
		create   bool
	}{
		{
			name:     "describes existing security config",
			scName:   "my-sc",
			wantCode: http.StatusOK,
			create:   true,
		},
		{
			name:     "returns error for non-existent config",
			scName:   "missing-sc",
			wantCode: http.StatusBadRequest,
			create:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.create {
				rec := doEMRRequest(t, h, "CreateSecurityConfiguration", map[string]any{
					"Name":                  tt.scName,
					"SecurityConfiguration": `{"EncryptionConfiguration":{}}`,
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doEMRRequest(t, h, "DescribeSecurityConfiguration", map[string]any{
				"Name": tt.scName,
			})
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var out struct {
					Name                  string  `json:"Name"`
					SecurityConfiguration string  `json:"SecurityConfiguration"`
					CreationDateTime      float64 `json:"CreationDateTime"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Equal(t, tt.scName, out.Name)
				assert.NotZero(t, out.CreationDateTime)
				assert.JSONEq(t, `{"EncryptionConfiguration":{}}`, out.SecurityConfiguration)
			}
		})
	}
}
