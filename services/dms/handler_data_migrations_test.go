package dms_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
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
