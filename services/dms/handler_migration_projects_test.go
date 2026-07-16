package dms_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMigrationProjectLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()

	createRec := doDMS(t, h, "CreateMigrationProject", map[string]any{
		"MigrationProjectName": "proj-1",
	})
	require.Equal(t, http.StatusOK, createRec.Code)
	projArn := parseJSON(t, createRec)["MigrationProject"].(map[string]any)["MigrationProjectArn"].(string)

	// Duplicate.
	dupRec := doDMS(t, h, "CreateMigrationProject", map[string]any{
		"MigrationProjectName": "proj-1",
	})
	assert.Equal(t, http.StatusConflict, dupRec.Code)

	// Describe.
	descRec := doDMS(t, h, "DescribeMigrationProjects", map[string]any{})
	assert.Equal(t, http.StatusOK, descRec.Code)

	// Modify by ARN.
	modRec := doDMS(t, h, "ModifyMigrationProject", map[string]any{
		"MigrationProjectArn": projArn,
		"Description":         "updated",
	})
	assert.Equal(t, http.StatusOK, modRec.Code)

	// Modify not found.
	notFoundRec := doDMS(t, h, "ModifyMigrationProject", map[string]any{
		"MigrationProjectArn": "nonexistent",
	})
	assert.Equal(t, http.StatusNotFound, notFoundRec.Code)

	// Delete.
	delRec := doDMS(t, h, "DeleteMigrationProject", map[string]any{
		"MigrationProjectArn": projArn,
	})
	assert.Equal(t, http.StatusOK, delRec.Code)

	delRec2 := doDMS(t, h, "DeleteMigrationProject", map[string]any{
		"MigrationProjectArn": projArn,
	})
	assert.Equal(t, http.StatusNotFound, delRec2.Code)
}

func TestModifyMigrationProject_UpdatesDescription(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		lookupByArn bool
	}{
		{name: "lookup_by_name", lookupByArn: false},
		{name: "lookup_by_arn", lookupByArn: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestDMSHandler()

			createRec := doDMS(t, h, "CreateMigrationProject", map[string]any{
				"MigrationProjectName": "mp-modify",
				"Description":          "original",
			})
			require.Equal(t, http.StatusOK, createRec.Code)
			mp := parseJSON(t, createRec)["MigrationProject"].(map[string]any)
			mpName := mp["MigrationProjectName"].(string)
			mpArn := mp["MigrationProjectArn"].(string)

			lookupKey := mpName
			if tt.lookupByArn {
				lookupKey = mpArn
			}

			modRec := doDMS(t, h, "ModifyMigrationProject", map[string]any{
				"MigrationProjectArn": lookupKey,
				"Description":         "updated description",
			})
			require.Equal(t, http.StatusOK, modRec.Code)

			updated := parseJSON(t, modRec)["MigrationProject"].(map[string]any)
			assert.Equal(t, "updated description", updated["Description"],
				"ModifyMigrationProject must persist the updated description")
		})
	}
}
