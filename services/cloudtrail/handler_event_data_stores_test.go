package cloudtrail_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudtrail"
)

// TestCloudTrailEventDataStore exercises CreateEventDataStore and DeleteEventDataStore.
func TestCloudTrailEventDataStore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops  func(t *testing.T, h *cloudtrail.Handler)
		name string
	}{
		{
			name: "create_event_data_store_success",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "CreateEventDataStore", map[string]any{
					"Name":                         "my-eds",
					"MultiRegionEnabled":           true,
					"OrganizationEnabled":          false,
					"TerminationProtectionEnabled": true,
					"RetentionPeriod":              90,
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseCloudTrailResp(t, rec)
				assert.NotEmpty(t, resp["EventDataStoreArn"])
				assert.Equal(t, "my-eds", resp["Name"])
				assert.Equal(t, "ENABLED", resp["Status"])
				assert.Equal(t, true, resp["MultiRegionEnabled"])
				assert.Equal(t, true, resp["TerminationProtectionEnabled"])
			},
		},
		{
			name: "create_event_data_store_missing_name",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "CreateEventDataStore", map[string]any{
					"RetentionPeriod": 90,
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "delete_event_data_store_success",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				createRec := doCloudTrailOp(t, h, "CreateEventDataStore", map[string]any{
					"Name": "del-eds",
				})
				createResp := parseCloudTrailResp(t, createRec)
				edsARN := createResp["EventDataStoreArn"].(string)
				rec := doCloudTrailOp(t, h, "DeleteEventDataStore", map[string]any{
					"EventDataStore": edsARN,
				})
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
		{
			name: "delete_event_data_store_not_found",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "DeleteEventDataStore", map[string]any{
					"EventDataStore": "eds-missing",
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestCloudTrailHandler()
			tt.ops(t, h)
		})
	}
}

// TestEDSFederation verifies EnableFederation and DisableFederation properly
// track federation status on event data stores.
func TestEDSFederation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops  func(t *testing.T, h *cloudtrail.Handler)
		name string
	}{
		{
			name: "new_eds_has_disabled_federation",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "CreateEventDataStore", map[string]any{
					"Name": "fed-test-eds",
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseCloudTrailResp(t, rec)
				assert.Equal(t, "DISABLED", resp["FederationStatus"])
			},
		},
		{
			name: "enable_federation_sets_status_and_role",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				createRec := doCloudTrailOp(t, h, "CreateEventDataStore", map[string]any{
					"Name": "enable-fed-eds",
				})
				createResp := parseCloudTrailResp(t, createRec)
				edsARN := createResp["EventDataStoreArn"].(string)

				roleArn := "arn:aws:iam::123456789012:role/CloudTrailFederationRole"
				rec := doCloudTrailOp(t, h, "EnableFederation", map[string]any{
					"EventDataStore":    edsARN,
					"FederationRoleArn": roleArn,
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseCloudTrailResp(t, rec)
				assert.Equal(t, "ENABLED", resp["FederationStatus"])
				assert.Equal(t, roleArn, resp["FederationRoleArn"])
				assert.Equal(t, edsARN, resp["EventDataStoreArn"])
			},
		},
		{
			name: "disable_federation_after_enable",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				createRec := doCloudTrailOp(t, h, "CreateEventDataStore", map[string]any{
					"Name": "disable-fed-eds",
				})
				createResp := parseCloudTrailResp(t, createRec)
				edsARN := createResp["EventDataStoreArn"].(string)

				doCloudTrailOp(t, h, "EnableFederation", map[string]any{
					"EventDataStore":    edsARN,
					"FederationRoleArn": "arn:aws:iam::123456789012:role/TestRole",
				})

				rec := doCloudTrailOp(t, h, "DisableFederation", map[string]any{
					"EventDataStore": edsARN,
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseCloudTrailResp(t, rec)
				assert.Equal(t, "DISABLED", resp["FederationStatus"])
				_, hasRole := resp["FederationRoleArn"]
				assert.False(t, hasRole, "FederationRoleArn should be cleared after disable")
			},
		},
		{
			name: "federation_status_persisted_in_get_eds",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				createRec := doCloudTrailOp(t, h, "CreateEventDataStore", map[string]any{
					"Name": "fed-persist-eds",
				})
				createResp := parseCloudTrailResp(t, createRec)
				edsARN := createResp["EventDataStoreArn"].(string)

				roleArn := "arn:aws:iam::123456789012:role/FedRole"
				doCloudTrailOp(t, h, "EnableFederation", map[string]any{
					"EventDataStore":    edsARN,
					"FederationRoleArn": roleArn,
				})

				// GetEventDataStore should reflect updated federation status.
				getRec := doCloudTrailOp(t, h, "GetEventDataStore", map[string]any{
					"EventDataStore": edsARN,
				})
				assert.Equal(t, http.StatusOK, getRec.Code)
				getResp := parseCloudTrailResp(t, getRec)
				assert.Equal(t, "ENABLED", getResp["FederationStatus"])
				assert.Equal(t, roleArn, getResp["FederationRoleArn"])
			},
		},
		{
			name: "enable_federation_eds_not_found",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "EnableFederation", map[string]any{
					"EventDataStore":    "nonexistent-eds",
					"FederationRoleArn": "arn:aws:iam::123:role/R",
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "disable_federation_eds_not_found",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "DisableFederation", map[string]any{
					"EventDataStore": "nonexistent-eds",
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "enable_federation_missing_eds_field",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "EnableFederation", map[string]any{
					"FederationRoleArn": "arn:aws:iam::123:role/R",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "disable_federation_missing_eds_field",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "DisableFederation", map[string]any{})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestCloudTrailHandler()
			tt.ops(t, h)
		})
	}
}

// TestTerminationProtection verifies that DeleteEventDataStore refuses to delete
// event data stores with TerminationProtectionEnabled=true.
func TestTerminationProtection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops  func(t *testing.T, h *cloudtrail.Handler)
		name string
	}{
		{
			name: "delete_protected_eds_returns_409",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				createRec := doCloudTrailOp(t, h, "CreateEventDataStore", map[string]any{
					"Name":                         "protected-eds",
					"TerminationProtectionEnabled": true,
				})
				assert.Equal(t, http.StatusOK, createRec.Code)
				createResp := parseCloudTrailResp(t, createRec)
				edsARN := createResp["EventDataStoreArn"].(string)

				rec := doCloudTrailOp(t, h, "DeleteEventDataStore", map[string]any{
					"EventDataStore": edsARN,
				})
				assert.Equal(t, http.StatusConflict, rec.Code)
				resp := parseCloudTrailResp(t, rec)
				assert.Contains(t, resp["__type"], "TerminationProtect")
			},
		},
		{
			name: "delete_unprotected_eds_succeeds",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				createRec := doCloudTrailOp(t, h, "CreateEventDataStore", map[string]any{
					"Name":                         "unprotected-eds",
					"TerminationProtectionEnabled": false,
				})
				createResp := parseCloudTrailResp(t, createRec)
				edsARN := createResp["EventDataStoreArn"].(string)

				rec := doCloudTrailOp(t, h, "DeleteEventDataStore", map[string]any{
					"EventDataStore": edsARN,
				})
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
		{
			name: "disable_protection_then_delete_succeeds",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				createRec := doCloudTrailOp(t, h, "CreateEventDataStore", map[string]any{
					"Name":                         "was-protected-eds",
					"TerminationProtectionEnabled": true,
				})
				createResp := parseCloudTrailResp(t, createRec)
				edsARN := createResp["EventDataStoreArn"].(string)

				// Update to disable termination protection.
				boolFalse := false
				doCloudTrailOp(t, h, "UpdateEventDataStore", map[string]any{
					"EventDataStore":               edsARN,
					"TerminationProtectionEnabled": &boolFalse,
				})

				rec := doCloudTrailOp(t, h, "DeleteEventDataStore", map[string]any{
					"EventDataStore": edsARN,
				})
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestCloudTrailHandler()
			tt.ops(t, h)
		})
	}
}

// TestEDSAdvancedEventSelectors verifies that EventDataStore supports AdvancedEventSelectors
// in Create, Get, and Update operations.
func TestEDSAdvancedEventSelectors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops  func(t *testing.T, h *cloudtrail.Handler)
		name string
	}{
		{
			name: "create_eds_with_advanced_event_selectors",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "CreateEventDataStore", map[string]any{
					"Name": "adv-sel-eds",
					"AdvancedEventSelectors": []map[string]any{
						{
							"Name": "Log S3 data events",
							"FieldSelectors": []map[string]any{
								{"Field": "eventCategory", "Equals": []string{"Data"}},
								{"Field": "resources.type", "Equals": []string{"AWS::S3::Object"}},
							},
						},
					},
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseCloudTrailResp(t, rec)
				assert.NotEmpty(t, resp["EventDataStoreArn"])
				advSels, ok := resp["AdvancedEventSelectors"].([]any)
				require.True(t, ok)
				assert.Len(t, advSels, 1)
			},
		},
		{
			name: "get_eds_returns_advanced_event_selectors",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				createRec := doCloudTrailOp(t, h, "CreateEventDataStore", map[string]any{
					"Name": "get-adv-eds",
					"AdvancedEventSelectors": []map[string]any{
						{
							"Name": "All management events",
							"FieldSelectors": []map[string]any{
								{"Field": "eventCategory", "Equals": []string{"Management"}},
							},
						},
					},
				})
				createResp := parseCloudTrailResp(t, createRec)
				edsARN := createResp["EventDataStoreArn"].(string)

				getRec := doCloudTrailOp(t, h, "GetEventDataStore", map[string]any{
					"EventDataStore": edsARN,
				})
				assert.Equal(t, http.StatusOK, getRec.Code)
				getResp := parseCloudTrailResp(t, getRec)
				advSels, ok := getResp["AdvancedEventSelectors"].([]any)
				require.True(t, ok)
				assert.Len(t, advSels, 1)
				sel := advSels[0].(map[string]any)
				assert.Equal(t, "All management events", sel["Name"])
			},
		},
		{
			name: "update_eds_advanced_event_selectors",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				createRec := doCloudTrailOp(t, h, "CreateEventDataStore", map[string]any{
					"Name": "update-adv-eds",
				})
				createResp := parseCloudTrailResp(t, createRec)
				edsARN := createResp["EventDataStoreArn"].(string)

				rec := doCloudTrailOp(t, h, "UpdateEventDataStore", map[string]any{
					"EventDataStore": edsARN,
					"AdvancedEventSelectors": []map[string]any{
						{
							"Name": "Updated selector",
							"FieldSelectors": []map[string]any{
								{"Field": "eventCategory", "Equals": []string{"Data"}},
							},
						},
					},
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseCloudTrailResp(t, rec)
				advSels, ok := resp["AdvancedEventSelectors"].([]any)
				require.True(t, ok)
				assert.Len(t, advSels, 1)
				sel := advSels[0].(map[string]any)
				assert.Equal(t, "Updated selector", sel["Name"])
			},
		},
		{
			name: "create_eds_with_billing_mode",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "CreateEventDataStore", map[string]any{
					"Name":        "billing-eds",
					"BillingMode": "FIXED_RETENTION_PRICING",
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseCloudTrailResp(t, rec)
				assert.Equal(t, "FIXED_RETENTION_PRICING", resp["BillingMode"])
			},
		},
		{
			name: "create_eds_default_billing_mode",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "CreateEventDataStore", map[string]any{
					"Name": "default-billing-eds",
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseCloudTrailResp(t, rec)
				assert.Equal(t, "EXTENDABLE_RETENTION_PRICING", resp["BillingMode"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestCloudTrailHandler()
			tt.ops(t, h)
		})
	}
}

// TestCloudTrailEventDataStoreLifecycle covers CreateEventDataStore, GetEventDataStore,
// UpdateEventDataStore, ListEventDataStores, RestoreEventDataStore,
// StartEventDataStoreIngestion, StopEventDataStoreIngestion, DeleteEventDataStore.
func TestCloudTrailEventDataStoreLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestCloudTrailHandler()

	// CreateEventDataStore.
	rec := doCloudTrailOp(t, h, "CreateEventDataStore", map[string]any{
		"Name":                         "test-eds",
		"MultiRegionEnabled":           true,
		"OrganizationEnabled":          false,
		"RetentionPeriod":              90,
		"TerminationProtectionEnabled": false,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseCloudTrailResp(t, rec)
	edsARN, _ := resp["EventDataStoreArn"].(string)
	require.NotEmpty(t, edsARN)

	// GetEventDataStore.
	rec = doCloudTrailOp(t, h, "GetEventDataStore", map[string]any{"EventDataStore": edsARN})
	assert.Equal(t, http.StatusOK, rec.Code)

	// UpdateEventDataStore.
	rec = doCloudTrailOp(t, h, "UpdateEventDataStore", map[string]any{
		"EventDataStore":  edsARN,
		"RetentionPeriod": 180,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// ListEventDataStores.
	rec = doCloudTrailOp(t, h, "ListEventDataStores", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	// StartEventDataStoreIngestion.
	rec = doCloudTrailOp(t, h, "StartEventDataStoreIngestion", map[string]any{"EventDataStore": edsARN})
	assert.Equal(t, http.StatusOK, rec.Code)

	// StopEventDataStoreIngestion.
	rec = doCloudTrailOp(t, h, "StopEventDataStoreIngestion", map[string]any{"EventDataStore": edsARN})
	assert.Equal(t, http.StatusOK, rec.Code)

	// RestoreEventDataStore.
	rec = doCloudTrailOp(t, h, "RestoreEventDataStore", map[string]any{"EventDataStore": edsARN})
	assert.Equal(t, http.StatusOK, rec.Code)

	// DeleteEventDataStore.
	rec = doCloudTrailOp(t, h, "DeleteEventDataStore", map[string]any{"EventDataStore": edsARN})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestCloudTrailFederationSmoke covers EnableFederation and DisableFederation.
func TestCloudTrailFederationSmoke(t *testing.T) {
	t.Parallel()

	h := newTestCloudTrailHandler()

	// Create an EDS.
	rec := doCloudTrailOp(t, h, "CreateEventDataStore", map[string]any{
		"Name": "fed-eds-cov",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseCloudTrailResp(t, rec)
	edsARN, _ := resp["EventDataStoreArn"].(string)
	require.NotEmpty(t, edsARN)

	// New EDS has DISABLED federation.
	assert.Equal(t, "DISABLED", resp["FederationStatus"])

	// EnableFederation.
	roleArn := "arn:aws:iam::123456789012:role/FedRole"
	rec = doCloudTrailOp(t, h, "EnableFederation", map[string]any{
		"EventDataStore":    edsARN,
		"FederationRoleArn": roleArn,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	enableResp := parseCloudTrailResp(t, rec)
	assert.Equal(t, "ENABLED", enableResp["FederationStatus"])
	assert.Equal(t, roleArn, enableResp["FederationRoleArn"])

	// DisableFederation.
	rec = doCloudTrailOp(t, h, "DisableFederation", map[string]any{
		"EventDataStore": edsARN,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	disableResp := parseCloudTrailResp(t, rec)
	assert.Equal(t, "DISABLED", disableResp["FederationStatus"])
}

// TestCloudTrailTerminationProtectionSmoke verifies termination protection
// on event data stores.
func TestCloudTrailTerminationProtectionSmoke(t *testing.T) {
	t.Parallel()

	h := newTestCloudTrailHandler()

	// Create a protected EDS.
	rec := doCloudTrailOp(t, h, "CreateEventDataStore", map[string]any{
		"Name":                         "term-prot-eds",
		"TerminationProtectionEnabled": true,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseCloudTrailResp(t, rec)
	edsARN, _ := resp["EventDataStoreArn"].(string)
	require.NotEmpty(t, edsARN)
	assert.Equal(t, true, resp["TerminationProtectionEnabled"])

	// Delete should fail.
	rec = doCloudTrailOp(t, h, "DeleteEventDataStore", map[string]any{
		"EventDataStore": edsARN,
	})
	assert.Equal(t, http.StatusConflict, rec.Code)

	// Disable protection.
	boolFalse := false
	rec = doCloudTrailOp(t, h, "UpdateEventDataStore", map[string]any{
		"EventDataStore":               edsARN,
		"TerminationProtectionEnabled": &boolFalse,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Delete should now succeed.
	rec = doCloudTrailOp(t, h, "DeleteEventDataStore", map[string]any{
		"EventDataStore": edsARN,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestEDSRetentionPeriodDefault verifies that CreateEventDataStore defaults
// RetentionPeriod to 2557 days (7 years) when not specified, matching real AWS behavior.
func TestEDSRetentionPeriodDefault(t *testing.T) {
	t.Parallel()

	h := newTestCloudTrailHandler()

	rec := doCloudTrailOp(t, h, "CreateEventDataStore", map[string]any{
		"Name": "rp-default-eds",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseCloudTrailResp(t, rec)
	rp, _ := resp["RetentionPeriod"].(float64)
	assert.InDelta(t, float64(2557), rp, 0, "RetentionPeriod must default to 2557 days")
}

// TestEDSAdvancedEventSelectorsAlwaysPresent verifies that AdvancedEventSelectors
// is always present in EDS responses, even as empty array, matching real AWS behavior.
func TestEDSAdvancedEventSelectorsAlwaysPresent(t *testing.T) {
	t.Parallel()

	h := newTestCloudTrailHandler()

	rec := doCloudTrailOp(t, h, "CreateEventDataStore", map[string]any{
		"Name": "aes-always-eds",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseCloudTrailResp(t, rec)
	aes, hasField := resp["AdvancedEventSelectors"]
	assert.True(t, hasField, "AdvancedEventSelectors must always be present in EDS response")

	aesList, ok := aes.([]any)
	assert.True(t, ok, "AdvancedEventSelectors must be an array")
	assert.Empty(t, aesList, "AdvancedEventSelectors must be empty array when none configured")
}

// TestEDSAdvancedEventSelectorsGetDataStore verifies AdvancedEventSelectors
// is present in GetEventDataStore response.
func TestEDSAdvancedEventSelectorsGetDataStore(t *testing.T) {
	t.Parallel()

	h := newTestCloudTrailHandler()

	createRec := doCloudTrailOp(t, h, "CreateEventDataStore", map[string]any{
		"Name": "aes-get-eds",
	})
	require.Equal(t, http.StatusOK, createRec.Code)
	edsARN, _ := parseCloudTrailResp(t, createRec)["EventDataStoreArn"].(string)

	getRec := doCloudTrailOp(t, h, "GetEventDataStore", map[string]any{
		"EventDataStore": edsARN,
	})
	require.Equal(t, http.StatusOK, getRec.Code)

	resp := parseCloudTrailResp(t, getRec)
	_, hasField := resp["AdvancedEventSelectors"]
	assert.True(t, hasField, "AdvancedEventSelectors must be present in GetEventDataStore response")
}
