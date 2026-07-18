package dms_test

import (
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/dms"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeleteDataMigration(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	h.Backend.AddDataMigrationInternal("del-dm", "full-load")

	rec := doDMS(t, h, "DeleteDataMigration", map[string]any{
		"DataMigrationIdentifier": "del-dm",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 0, h.Backend.DataMigrationCount())

	rec2 := doDMS(t, h, "DeleteDataMigration", map[string]any{
		"DataMigrationIdentifier": "del-dm",
	})
	assert.Equal(t, http.StatusNotFound, rec2.Code)
}

func TestModifyDataMigration(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	h.Backend.AddDataMigrationInternal("mod-dm", "full-load")

	rec := doDMS(t, h, "ModifyDataMigration", map[string]any{
		"DataMigrationIdentifier": "mod-dm",
		"DataMigrationType":       "cdc",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec2 := doDMS(t, h, "ModifyDataMigration", map[string]any{
		"DataMigrationIdentifier": "nonexistent",
	})
	assert.Equal(t, http.StatusNotFound, rec2.Code)
}

func TestStartStopDataMigration(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	h.Backend.AddDataMigrationInternal("start-stop-dm", "full-load")

	startRec := doDMS(t, h, "StartDataMigration", map[string]any{
		"DataMigrationIdentifier": "start-stop-dm",
	})
	assert.Equal(t, http.StatusOK, startRec.Code)

	stopRec := doDMS(t, h, "StopDataMigration", map[string]any{
		"DataMigrationIdentifier": "start-stop-dm",
	})
	assert.Equal(t, http.StatusOK, stopRec.Code)

	// Not found cases.
	rec := doDMS(t, h, "StartDataMigration", map[string]any{
		"DataMigrationIdentifier": "nonexistent",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)

	rec2 := doDMS(t, h, "StopDataMigration", map[string]any{
		"DataMigrationIdentifier": "nonexistent",
	})
	assert.Equal(t, http.StatusNotFound, rec2.Code)
}

func TestHandler_CreateDataMigration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, h *dms.Handler)
		name string
	}{
		{
			name: "create_success",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				rec := doDMS(t, h, "CreateDataMigration", map[string]any{
					"DataMigrationName":          "my-migration",
					"MigrationProjectIdentifier": "proj-1",
					"DataMigrationType":          "full-load",
					"ServiceAccessRoleArn":       "arn:aws:iam::123456789012:role/dms-role",
					"NumberOfJobs":               2,
					"EnableCloudwatchLogs":       true,
					"Tags": []map[string]string{
						{"Key": "Env", "Value": "test"},
					},
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec)
				dm, ok := resp["DataMigration"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "my-migration", dm["DataMigrationName"])
				assert.Equal(t, "full-load", dm["DataMigrationType"])
				assert.Equal(t, "ready", dm["DataMigrationStatus"])
				assert.NotEmpty(t, dm["DataMigrationArn"])
			},
		},
		{
			name: "create_duplicate_conflict",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				doDMS(t, h, "CreateDataMigration", map[string]any{
					"DataMigrationName": "dup-migration",
					"DataMigrationType": "full-load",
				})
				rec := doDMS(t, h, "CreateDataMigration", map[string]any{
					"DataMigrationName": "dup-migration",
					"DataMigrationType": "full-load",
				})
				assert.Equal(t, http.StatusConflict, rec.Code)
			},
		},
		{
			name: "missing_name",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				rec := doDMS(t, h, "CreateDataMigration", map[string]any{
					"DataMigrationType": "full-load",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "missing_type",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				rec := doDMS(t, h, "CreateDataMigration", map[string]any{
					"DataMigrationName": "no-type-migration",
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

func TestMigrationTypeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		typeStr    string
		wantStatus int
	}{
		{name: "valid_full_load", typeStr: "full-load", wantStatus: http.StatusOK},
		{name: "valid_cdc", typeStr: "cdc", wantStatus: http.StatusOK},
		{name: "valid_full_load_and_cdc", typeStr: "full-load-and-cdc", wantStatus: http.StatusOK},
		{name: "invalid_type", typeStr: "unknown-type", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestDMSHandler()
			rec := doDMS(t, h, "CreateDataMigration", map[string]any{
				"DataMigrationName": "dm-" + tt.typeStr,
				"DataMigrationType": tt.typeStr,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestDataMigrationSeedHelper(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	h.Backend.AddDataMigrationInternal("seed-migration", "full-load")
	assert.Equal(t, 1, h.Backend.DataMigrationCount())
}
