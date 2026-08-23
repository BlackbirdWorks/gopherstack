package dms_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dms"
)

func TestDeleteFleetAdvisorCollector(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	h.Backend.AddFleetAdvisorCollectorInternal("del-fac")

	rec := doDMS(t, h, "DeleteFleetAdvisorCollector", map[string]any{
		"CollectorReferencedId": "del-fac",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 0, h.Backend.FleetAdvisorCollectorCount())
}

func TestDeleteFleetAdvisorCollector_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	rec := doDMS(t, h, "DeleteFleetAdvisorCollector", map[string]any{
		"CollectorReferencedId": "nonexistent-collector-id",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)

	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "ResourceNotFoundFault", body["__type"])
}

func TestFleetAdvisorDatabases(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()

	// Create a collector — seeding databases.
	rec := doDMS(t, h, "CreateFleetAdvisorCollector", map[string]any{
		"CollectorName":        "col1",
		"ServiceAccessRoleArn": "arn:aws:iam::123456789012:role/dms-role",
		"S3BucketName":         "my-bucket",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// DescribeFleetAdvisorDatabases must return the seeded databases.
	rec = doDMS(t, h, "DescribeFleetAdvisorDatabases", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	body := parseJSON(t, rec)
	dbs, ok := body["Databases"].([]any)
	require.True(t, ok)
	assert.Len(t, dbs, 2, "two databases seeded per collector")

	db0, ok := dbs[0].(map[string]any)
	require.True(t, ok)
	dbID := db0["DatabaseId"].(string)
	assert.NotEmpty(t, dbID)

	softwareDetails, ok := db0["SoftwareDetails"].(map[string]any)
	require.True(t, ok,
		"engine info belongs under SoftwareDetails.Engine (DatabaseResponse has no top-level EngineName)")
	assert.NotEmpty(t, softwareDetails["Engine"])

	// DeleteFleetAdvisorDatabases removes the specified DB.
	rec = doDMS(t, h, "DeleteFleetAdvisorDatabases", map[string]any{
		"DatabaseIds": []string{dbID},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	deleted := parseJSON(t, rec)["DatabaseIds"].([]any)
	require.Len(t, deleted, 1)
	assert.Equal(t, dbID, deleted[0])

	// After delete only one database remains.
	rec = doDMS(t, h, "DescribeFleetAdvisorDatabases", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)
	remaining := parseJSON(t, rec)["Databases"].([]any)
	assert.Len(t, remaining, 1)
}

func TestHandler_CreateFleetAdvisorCollector(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, h *dms.Handler)
		name string
	}{
		{
			name: "create_success",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				rec := doDMS(t, h, "CreateFleetAdvisorCollector", map[string]any{
					"CollectorName":        "my-collector",
					"Description":          "My Fleet Advisor collector",
					"ServiceAccessRoleArn": "arn:aws:iam::123456789012:role/fleet-role",
					"S3BucketName":         "my-bucket",
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec)
				assert.Equal(t, "my-collector", resp["CollectorName"])
				assert.NotEmpty(t, resp["CollectorReferencedId"])
				assert.Equal(t, "arn:aws:iam::123456789012:role/fleet-role", resp["ServiceAccessRoleArn"])
				assert.Equal(t, "my-bucket", resp["S3BucketName"])
			},
		},
		{
			name: "create_duplicate_conflict",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				doDMS(t, h, "CreateFleetAdvisorCollector", map[string]any{
					"CollectorName": "dup-collector",
				})
				rec := doDMS(t, h, "CreateFleetAdvisorCollector", map[string]any{
					"CollectorName": "dup-collector",
				})
				assert.Equal(t, http.StatusConflict, rec.Code)
			},
		},
		{
			name: "missing_collector_name",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				rec := doDMS(t, h, "CreateFleetAdvisorCollector", map[string]any{
					"S3BucketName": "my-bucket",
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

func TestFleetAdvisorCollectorTagsOnReset(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	h.Backend.AddFleetAdvisorCollectorInternal("col-reset-test")

	// Should not panic on reset even with collector tags.
	assert.NotPanics(t, func() { h.Backend.Reset() })
	assert.Equal(t, 0, h.Backend.FleetAdvisorCollectorCount())
}
