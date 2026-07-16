package dms_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	assert.NotEmpty(t, db0["EngineName"])

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
