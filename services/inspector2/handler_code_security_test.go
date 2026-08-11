package inspector2_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/inspector2"
)

func TestCodeSecurityIntegrationLifecycle(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)

	// Create
	rec := auditDo(t, h, http.MethodPost, "/codesecurity/integration/create", map[string]any{
		"name": "my-integration",
		"type": "GITHUB",
		"tags": map[string]string{"env": "test"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	integARN, _ := createResp["integrationArn"].(string)
	require.NotEmpty(t, integARN)
	assert.Equal(t, "ACTIVE", createResp["status"])

	// Get
	rec = auditDo(t, h, http.MethodPost, "/codesecurity/integration/get", map[string]any{
		"integrationArn": integARN,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
	assert.Equal(t, integARN, getResp["integrationArn"])

	// List
	rec = auditDo(t, h, http.MethodPost, "/codesecurity/integration/list", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	integrations, _ := listResp["integrations"].([]any)
	assert.Len(t, integrations, 1)

	// Update
	rec = auditDo(t, h, http.MethodPost, "/codesecurity/integration/update", map[string]any{
		"integrationArn": integARN,
		"details":        map[string]any{"webhookSecret": "new-secret"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Delete
	rec = auditDo(t, h, http.MethodPost, "/codesecurity/integration/delete", map[string]any{
		"integrationArn": integARN,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Get after delete returns 404
	rec = auditDo(t, h, http.MethodPost, "/codesecurity/integration/get", map[string]any{
		"integrationArn": integARN,
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestCreateCodeSecurityIntegration_NameValidation covers
// CreateCodeSecurityIntegration's real "name" length/charset constraint (AWS
// API Reference for CreateCodeSecurityIntegration: "Minimum length of 1.
// Maximum length of 60." Pattern: `[a-zA-Z0-9-_$:.]*` -- identical to
// CreateCodeSecurityScanConfiguration's name constraint). A prior revision
// accepted any non-empty string.
func TestCreateCodeSecurityIntegration_NameValidation(t *testing.T) {
	t.Parallel()

	tooLongName := make([]byte, 61)
	for i := range tooLongName {
		tooLongName[i] = 'a'
	}

	tests := []struct {
		name     string
		intName  string
		wantCode int
	}{
		{name: "empty_name_rejected", intName: "", wantCode: http.StatusBadRequest},
		{name: "too_long_name_rejected", intName: string(tooLongName), wantCode: http.StatusBadRequest},
		{name: "invalid_charset_name_rejected", intName: "has a space", wantCode: http.StatusBadRequest},
		{name: "single_char_name_accepted", intName: "a", wantCode: http.StatusOK},
		{name: "exactly_60_char_name_accepted", intName: string(tooLongName[:60]), wantCode: http.StatusOK},
		{name: "all_valid_charset_chars_name_accepted", intName: "a-Z0_9$:.name", wantCode: http.StatusOK},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newAuditHandler(t)
			rec := auditDo(t, h, http.MethodPost, "/codesecurity/integration/create", map[string]any{
				"name": tc.intName,
				"type": "GITHUB",
			})
			assert.Equal(t, tc.wantCode, rec.Code, rec.Body.String())
		})
	}
}

func TestCodeSecurityScanConfigurationLifecycle(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)

	// Create
	rec := auditDo(t, h, http.MethodPost, "/codesecurity/scan-configuration/create", map[string]any{
		"name":          "my-scan-config",
		"level":         "ACCOUNT",
		"scopeSettings": map[string]any{"projectSelectionScope": "ALL"},
		"tags":          map[string]string{"env": "test"},
		"configuration": map[string]any{
			"ruleSetCategories": []string{"SAST", "IAC"},
			"continuousIntegrationScanConfiguration": map[string]any{
				"supportedEvents": []string{"PULL_REQUEST"},
			},
			"periodicScanConfiguration": map[string]any{"frequency": "WEEKLY"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	cfgARN, _ := createResp["scanConfigurationArn"].(string)
	require.NotEmpty(t, cfgARN)

	// Get
	rec = auditDo(t, h, http.MethodPost, "/codesecurity/scan-configuration/get", map[string]any{
		"scanConfigurationArn": cfgARN,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
	assert.Equal(t, "ACCOUNT", getResp["level"])
	assert.NotContains(t, getResp, "status")
	configuration, ok := getResp["configuration"].(map[string]any)
	require.True(t, ok)
	assert.ElementsMatch(t, []any{"SAST", "IAC"}, configuration["ruleSetCategories"])

	// List
	rec = auditDo(t, h, http.MethodPost, "/codesecurity/scan-configuration/list", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	cfgs, _ := listResp["configurations"].([]any)
	require.Len(t, cfgs, 1)
	summary, ok := cfgs[0].(map[string]any)
	require.True(t, ok)
	assert.NotEmpty(t, summary["ownerAccountId"])
	assert.ElementsMatch(t, []any{"SAST", "IAC"}, summary["ruleSetCategories"])
	assert.NotContains(t, summary, "configuration")

	// Update
	rec = auditDo(t, h, http.MethodPost, "/codesecurity/scan-configuration/update", map[string]any{
		"scanConfigurationArn": cfgARN,
		"configuration": map[string]any{
			"ruleSetCategories": []string{"SCA"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	// Batch associate
	rec = auditDo(t, h, http.MethodPost, "/codesecurity/scan-configuration/batch/associate", map[string]any{
		"scanConfigurationArn": cfgARN,
		"associateConfigurationRequests": []any{
			map[string]any{"resource": "arn:aws:codecommit:us-east-1:123456789012:my-repo"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// List associations
	rec = auditDo(t, h, http.MethodPost, "/codesecurity/scan-configuration/associations/list", map[string]any{
		"scanConfigurationArn": cfgARN,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var assocResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &assocResp))
	assocs, _ := assocResp["associations"].([]any)
	assert.Len(t, assocs, 1)

	// Batch disassociate
	rec = auditDo(t, h, http.MethodPost, "/codesecurity/scan-configuration/batch/disassociate", map[string]any{
		"scanConfigurationArn": cfgARN,
		"disassociateConfigurationRequests": []any{
			map[string]any{"resource": "arn:aws:codecommit:us-east-1:123456789012:my-repo"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// List associations now empty
	rec = auditDo(t, h, http.MethodPost, "/codesecurity/scan-configuration/associations/list", map[string]any{
		"scanConfigurationArn": cfgARN,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var assocResp2 map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &assocResp2))
	assocs2, _ := assocResp2["associations"].([]any)
	assert.Empty(t, assocs2)

	// Delete
	rec = auditDo(t, h, http.MethodPost, "/codesecurity/scan-configuration/delete", map[string]any{
		"scanConfigurationArn": cfgARN,
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

// TestCodeSecurityScanConfigurationValidation covers
// CreateCodeSecurityScanConfiguration/UpdateCodeSecurityScanConfiguration's
// required "level" and "configuration.ruleSetCategories" members, plus
// "name"'s real length/charset constraint (AWS API Reference for
// CreateCodeSecurityScanConfiguration: "Minimum length of 1. Maximum length
// of 60." Pattern: `[a-zA-Z0-9-_$:.]*`). Real
// CreateCodeSecurityScanConfigurationInput requires level/ruleSetCategories;
// an earlier revision decoded scopeSettings/periodicScanConfiguration at the
// top level and never looked for either, so no validation ever ran. name's
// length/charset constraint was similarly unmodeled until this pass (any
// non-empty string was accepted).
func TestCodeSecurityScanConfigurationValidation(t *testing.T) {
	t.Parallel()

	tooLongName := make([]byte, 61)
	for i := range tooLongName {
		tooLongName[i] = 'a'
	}

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "missing_level_rejected",
			body: map[string]any{
				"name": "no-level",
				"configuration": map[string]any{
					"ruleSetCategories": []string{"SAST"},
				},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "invalid_level_rejected",
			body: map[string]any{
				"name":  "bad-level",
				"level": "GLOBAL",
				"configuration": map[string]any{
					"ruleSetCategories": []string{"SAST"},
				},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "missing_rule_set_categories_rejected",
			body: map[string]any{
				"name":  "no-rule-set",
				"level": "ACCOUNT",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "invalid_rule_set_category_rejected",
			body: map[string]any{
				"name":  "bad-rule-set",
				"level": "ACCOUNT",
				"configuration": map[string]any{
					"ruleSetCategories": []string{"NOT_A_CATEGORY"},
				},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "empty_name_rejected",
			body: map[string]any{
				"name":  "",
				"level": "ACCOUNT",
				"configuration": map[string]any{
					"ruleSetCategories": []string{"SAST"},
				},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "too_long_name_rejected",
			body: map[string]any{
				"name":  string(tooLongName),
				"level": "ACCOUNT",
				"configuration": map[string]any{
					"ruleSetCategories": []string{"SAST"},
				},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "invalid_charset_name_rejected",
			body: map[string]any{
				"name":  "has a space",
				"level": "ACCOUNT",
				"configuration": map[string]any{
					"ruleSetCategories": []string{"SAST"},
				},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "exactly_60_char_name_accepted",
			body: map[string]any{
				"name":  string(tooLongName[:60]),
				"level": "ACCOUNT",
				"configuration": map[string]any{
					"ruleSetCategories": []string{"SAST"},
				},
			},
			wantCode: http.StatusOK,
		},
		{
			name: "all_valid_charset_chars_name_accepted",
			body: map[string]any{
				"name":  "a-Z0_9$:.name",
				"level": "ACCOUNT",
				"configuration": map[string]any{
					"ruleSetCategories": []string{"SAST"},
				},
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newAuditHandler(t)
			rec := auditDo(t, h, http.MethodPost, "/codesecurity/scan-configuration/create", tc.body)
			assert.Equal(t, tc.wantCode, rec.Code, rec.Body.String())
		})
	}

	t.Run("update_missing_rule_set_categories_rejected", func(t *testing.T) {
		t.Parallel()

		h := newAuditHandler(t)
		rec := auditDo(t, h, http.MethodPost, "/codesecurity/scan-configuration/create", map[string]any{
			"name":  "update-target",
			"level": "ACCOUNT",
			"configuration": map[string]any{
				"ruleSetCategories": []string{"SAST"},
			},
		})
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

		var createResp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
		cfgARN, _ := createResp["scanConfigurationArn"].(string)
		require.NotEmpty(t, cfgARN)

		updateRec := auditDo(t, h, http.MethodPost, "/codesecurity/scan-configuration/update", map[string]any{
			"scanConfigurationArn": cfgARN,
		})
		assert.Equal(t, http.StatusBadRequest, updateRec.Code, updateRec.Body.String())
	})
}

func TestCodeSecurityScanLifecycle(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)

	// Start
	rec := auditDo(t, h, http.MethodPost, "/codesecurity/scan/start", map[string]any{
		"resourceId": "arn:aws:codecommit:us-east-1:123456789012:my-repo",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var startResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &startResp))
	scanID, _ := startResp["scanId"].(string)
	require.NotEmpty(t, scanID)

	// Get
	rec = auditDo(t, h, http.MethodPost, "/codesecurity/scan/get", map[string]any{
		"scanId": scanID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
	assert.Equal(t, scanID, getResp["scanId"])
}

// TestConnectorLifecycle covers the Create/Update/Delete/ListConnectors
// family added for the inspector2@v1.54.1 SDK bump (Azure cloud-provider
// connectors; see connectors.go/handler_connectors.go). It lands in this
// file, rather than a new test file, per this package's testing convention
// of adding new coverage as a new table test in an existing test file.
func TestConnectorLifecycle(t *testing.T) {
	t.Parallel()

	const awsConfigConnectorArn = "arn:aws:config:us-east-1:123456789012:config-connector/default"

	tests := []struct {
		fn   func(t *testing.T, h *inspector2.Handler)
		name string
	}{
		{
			name: "create_connector",
			fn: func(t *testing.T, h *inspector2.Handler) {
				t.Helper()

				connectorArn := auditCreateConnector(t, h, "my-connector", awsConfigConnectorArn)
				assert.Contains(t, connectorArn, "arn:aws:inspector2:")
				assert.Contains(t, connectorArn, "connector/")
			},
		},
		{
			name: "create_missing_name_returns_400",
			fn: func(t *testing.T, h *inspector2.Handler) {
				t.Helper()

				rec := auditDo(t, h, http.MethodPost, "/connector/create", map[string]any{
					"provider": "AZURE",
					"providerDetail": map[string]any{
						"azure": map[string]any{
							"awsConfigConnectorArn": awsConfigConnectorArn,
							"azureRegions":          []string{"eastus"},
						},
					},
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "create_invalid_provider_returns_400",
			fn: func(t *testing.T, h *inspector2.Handler) {
				t.Helper()

				rec := auditDo(t, h, http.MethodPost, "/connector/create", map[string]any{
					"name":     "bad-provider-connector",
					"provider": "GCP",
					"providerDetail": map[string]any{
						"azure": map[string]any{
							"awsConfigConnectorArn": awsConfigConnectorArn,
							"azureRegions":          []string{"eastus"},
						},
					},
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "create_missing_aws_config_connector_arn_returns_400",
			fn: func(t *testing.T, h *inspector2.Handler) {
				t.Helper()

				rec := auditDo(t, h, http.MethodPost, "/connector/create", map[string]any{
					"name":     "missing-config-arn-connector",
					"provider": "AZURE",
					"providerDetail": map[string]any{
						"azure": map[string]any{
							"azureRegions": []string{"eastus"},
						},
					},
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "list_connectors_returns_created_pending_authorization",
			fn: func(t *testing.T, h *inspector2.Handler) {
				t.Helper()

				connectorArn := auditCreateConnector(t, h, "list-me-connector", awsConfigConnectorArn)

				rec := auditDo(t, h, http.MethodPost, "/connector/list", map[string]any{})
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				items, ok := resp["items"].([]any)
				require.True(t, ok)
				require.Len(t, items, 1)

				item := items[0].(map[string]any)
				assert.Equal(t, connectorArn, item["connectorArn"])
				assert.Equal(t, "AZURE", item["provider"])
				assert.Equal(t, "PENDING_ENABLEMENT", item["enablementStatus"])

				health, ok := item["health"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "PENDING_AUTHORIZATION", health["connectorStatus"])
			},
		},
		{
			name: "list_connectors_filtered_by_provider_excludes_no_match",
			fn: func(t *testing.T, h *inspector2.Handler) {
				t.Helper()

				auditCreateConnector(t, h, "azure-connector", awsConfigConnectorArn)

				rec := auditDo(t, h, http.MethodPost, "/connector/list", map[string]any{
					"filterCriteria": map[string]any{
						"provider": []any{
							map[string]any{"comparison": "EQUALS", "value": "AZURE"},
						},
					},
				})
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				items, _ := resp["items"].([]any)
				assert.Len(t, items, 1)

				const noMatchArn = "arn:aws:inspector2:us-east-1:123456789012:connector/does-not-exist"

				rec2 := auditDo(t, h, http.MethodPost, "/connector/list", map[string]any{
					"filterCriteria": map[string]any{
						"connectorArns": []any{
							map[string]any{"comparison": "EQUALS", "value": noMatchArn},
						},
					},
				})
				require.Equal(t, http.StatusOK, rec2.Code, rec2.Body.String())

				var resp2 map[string]any
				require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp2))
				items2, _ := resp2["items"].([]any)
				assert.Empty(t, items2)
			},
		},
		{
			name: "update_connector",
			fn: func(t *testing.T, h *inspector2.Handler) {
				t.Helper()

				connectorArn := auditCreateConnector(t, h, "update-me-connector", awsConfigConnectorArn)

				rec := auditDo(t, h, http.MethodPost, "/connector/update", map[string]any{
					"connectorArn": connectorArn,
					"description":  "updated description",
				})
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, connectorArn, resp["connectorArn"])

				listRec := auditDo(t, h, http.MethodPost, "/connector/list", map[string]any{})
				var listResp map[string]any
				require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
				items := listResp["items"].([]any)
				require.Len(t, items, 1)

				item := items[0].(map[string]any)
				assert.Equal(t, "updated description", item["description"])
				assert.Equal(t, "PENDING_UPDATE", item["enablementStatus"])
			},
		},
		{
			name: "update_connector_not_found",
			fn: func(t *testing.T, h *inspector2.Handler) {
				t.Helper()

				rec := auditDo(t, h, http.MethodPost, "/connector/update", map[string]any{
					"connectorArn": "arn:aws:inspector2:us-east-1:123456789012:connector/nonexistent",
					"description":  "won't apply",
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "delete_connector",
			fn: func(t *testing.T, h *inspector2.Handler) {
				t.Helper()

				connectorArn := auditCreateConnector(t, h, "delete-me-connector", awsConfigConnectorArn)

				rec := auditDo(t, h, http.MethodPost, "/connector/delete", map[string]any{
					"connectorArn": connectorArn,
				})
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

				listRec := auditDo(t, h, http.MethodPost, "/connector/list", map[string]any{})
				var listResp map[string]any
				require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
				assert.Empty(t, listResp["items"].([]any))
			},
		},
		{
			name: "delete_connector_not_found",
			fn: func(t *testing.T, h *inspector2.Handler) {
				t.Helper()

				rec := auditDo(t, h, http.MethodPost, "/connector/delete", map[string]any{
					"connectorArn": "arn:aws:inspector2:us-east-1:123456789012:connector/does-not-exist",
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t, newAuditHandler(t))
		})
	}
}

// TestConnectorScanConfigurationLifecycle covers
// ListConnectorScanConfigurations/UpdateConnectorScanConfiguration, added
// alongside the connector family for the inspector2@v1.54.1 SDK bump. There
// is no CreateConnectorScanConfiguration operation in the real API (Update
// is the only write path), and UpdateConnectorScanConfiguration must reject
// an awsConfigConnectorArn with no associated connector rather than
// accepting any ID.
func TestConnectorScanConfigurationLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T, h *inspector2.Handler)
		name string
	}{
		{
			name: "update_scan_configuration_requires_existing_connector",
			fn: func(t *testing.T, h *inspector2.Handler) {
				t.Helper()

				rec := auditDo(t, h, http.MethodPost, "/connectorscanconfiguration/update", map[string]any{
					"awsConfigConnectorArn": "arn:aws:config:us-east-1:123456789012:config-connector/unknown",
					"scanConfiguration": map[string]any{
						"containerImageScanning": map[string]any{
							"pullDuration": "DAYS_30",
							"pushDuration": "DAYS_30",
						},
					},
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "update_and_list_scan_configuration",
			fn: func(t *testing.T, h *inspector2.Handler) {
				t.Helper()

				const awsConfigConnectorArn = "arn:aws:config:us-east-1:123456789012:config-connector/scan-cfg"

				connectorArn := auditCreateConnector(t, h, "scan-cfg-connector", awsConfigConnectorArn)

				rec := auditDo(t, h, http.MethodPost, "/connectorscanconfiguration/update", map[string]any{
					"awsConfigConnectorArn": awsConfigConnectorArn,
					"scanConfiguration": map[string]any{
						"containerImageScanning": map[string]any{
							"pullDuration": "DAYS_30",
							"pushDuration": "DAYS_60",
						},
					},
				})
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
				assert.JSONEq(t, "{}", rec.Body.String())

				listRec := auditDo(t, h, http.MethodPost, "/connectorscanconfigurations/list", map[string]any{})
				require.Equal(t, http.StatusOK, listRec.Code, listRec.Body.String())

				var listResp map[string]any
				require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
				cfgs, ok := listResp["scanConfigurations"].([]any)
				require.True(t, ok)
				require.Len(t, cfgs, 1)

				cfg := cfgs[0].(map[string]any)
				assert.Equal(t, awsConfigConnectorArn, cfg["awsConfigConnectorArn"])

				connArns, ok := cfg["connectorArns"].([]any)
				require.True(t, ok)
				require.Len(t, connArns, 1)
				assert.Equal(t, connectorArn, connArns[0])

				sc := cfg["scanConfiguration"].(map[string]any)
				cis := sc["containerImageScanning"].(map[string]any)
				assert.Equal(t, "DAYS_30", cis["pullDuration"])
				assert.Equal(t, "DAYS_60", cis["pushDuration"])
			},
		},
		{
			name: "list_scan_configurations_filtered_by_aws_config_connector_arn",
			fn: func(t *testing.T, h *inspector2.Handler) {
				t.Helper()

				const arnA = "arn:aws:config:us-east-1:123456789012:config-connector/a"
				const arnB = "arn:aws:config:us-east-1:123456789012:config-connector/b"

				auditCreateConnector(t, h, "connector-a", arnA)
				auditCreateConnector(t, h, "connector-b", arnB)

				for _, target := range []string{arnA, arnB} {
					rec := auditDo(t, h, http.MethodPost, "/connectorscanconfiguration/update", map[string]any{
						"awsConfigConnectorArn": target,
						"scanConfiguration": map[string]any{
							"containerImageScanning": map[string]any{"pullDuration": "DAYS_7"},
						},
					})
					require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
				}

				rec := auditDo(t, h, http.MethodPost, "/connectorscanconfigurations/list", map[string]any{
					"awsConfigConnectorArns": []string{arnA},
				})
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				cfgs, _ := resp["scanConfigurations"].([]any)
				require.Len(t, cfgs, 1)
				assert.Equal(t, arnA, cfgs[0].(map[string]any)["awsConfigConnectorArn"])
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t, newAuditHandler(t))
		})
	}
}
