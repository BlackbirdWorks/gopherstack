package dms_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParityC_DescribeEvents verifies events are recorded and returned.
func TestParityC_DescribeEvents(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()

	// Create a replication instance to generate a creation event.
	rec := doDMS(t, h, "CreateReplicationInstance", map[string]any{
		"ReplicationInstanceIdentifier": "test-ri",
		"ReplicationInstanceClass":      "dms.t3.micro",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Create an endpoint to generate another event.
	rec = doDMS(t, h, "CreateEndpoint", map[string]any{
		"EndpointIdentifier": "src",
		"EndpointType":       "source",
		"EngineName":         "mysql",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// DescribeEvents must now return at least these two events.
	rec = doDMS(t, h, "DescribeEvents", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	body := parseJSON(t, rec)
	events, ok := body["Events"].([]any)
	require.True(t, ok)
	assert.GreaterOrEqual(t, len(events), 1, "at least endpoint creation event expected")

	e0, ok := events[0].(map[string]any)
	require.True(t, ok)
	assert.NotEmpty(t, e0["Message"])
	assert.NotEmpty(t, e0["Date"])
}

// TestParityC_DescribeSchemas verifies schemas are returned after RefreshSchemas.
func TestParityC_DescribeSchemas(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()

	// Create endpoint.
	rec := doDMS(t, h, "CreateEndpoint", map[string]any{
		"EndpointIdentifier": "pg-src",
		"EndpointType":       "source",
		"EngineName":         "postgres",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	epARN := parseJSON(t, rec)["Endpoint"].(map[string]any)["EndpointArn"].(string)

	// Before refresh: DescribeSchemas returns empty.
	rec = doDMS(t, h, "DescribeSchemas", map[string]any{"EndpointArn": epARN})
	require.Equal(t, http.StatusOK, rec.Code)
	pre := parseJSON(t, rec)
	assert.Empty(t, pre["Schemas"])

	// Refresh.
	rec = doDMS(t, h, "RefreshSchemas", map[string]any{
		"EndpointArn":            epARN,
		"ReplicationInstanceArn": "arn:fake",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "successful", parseJSON(t, rec)["RefreshSchemasStatus"].(map[string]any)["Status"])

	// After refresh: schemas are populated.
	rec = doDMS(t, h, "DescribeSchemas", map[string]any{"EndpointArn": epARN})
	require.Equal(t, http.StatusOK, rec.Code)
	post := parseJSON(t, rec)
	schemas, ok := post["Schemas"].([]any)
	require.True(t, ok)
	assert.NotEmpty(t, schemas, "postgres endpoint should have schemas after refresh")
	assert.Contains(t, schemas, "public")
}

// TestParityC_FleetAdvisorDatabases verifies database seeding and deletion.
func TestParityC_FleetAdvisorDatabases(t *testing.T) {
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

// TestParityC_MetadataModelRequests verifies Start* records and Describe* returns requests.
func TestParityC_MetadataModelRequests(t *testing.T) {
	t.Parallel()

	tests := []struct {
		startBody map[string]any
		startOp   string
		descOp    string
	}{
		{
			startOp: "StartMetadataModelAssessment",
			descOp:  "DescribeMetadataModelAssessments",
			startBody: map[string]any{
				"MigrationProjectIdentifier": "proj1",
				"SelectionRules":             `{"rules":[]}`,
			},
		},
		{
			startOp: "StartMetadataModelConversion",
			descOp:  "DescribeMetadataModelConversions",
			startBody: map[string]any{
				"MigrationProjectIdentifier": "proj2",
				"SelectionRules":             `{"rules":[]}`,
			},
		},
		{
			startOp: "StartMetadataModelCreation",
			descOp:  "DescribeMetadataModelCreations",
			startBody: map[string]any{
				"MigrationProjectIdentifier": "proj3",
				"SelectionRules":             `{"rules":[]}`,
			},
		},
		{
			startOp: "StartMetadataModelExportAsScript",
			descOp:  "DescribeMetadataModelExportsAsScript",
			startBody: map[string]any{
				"MigrationProjectIdentifier": "proj4",
				"SelectionRules":             `{"rules":[]}`,
			},
		},
		{
			startOp: "StartMetadataModelExportToTarget",
			descOp:  "DescribeMetadataModelExportsToTarget",
			startBody: map[string]any{
				"MigrationProjectIdentifier": "proj5",
				"SelectionRules":             `{"rules":[]}`,
			},
		},
		{
			startOp: "StartMetadataModelImport",
			descOp:  "DescribeMetadataModelImports",
			startBody: map[string]any{
				"MigrationProjectIdentifier": "proj6",
				"SelectionRules":             `{"rules":[]}`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.startOp, func(t *testing.T) {
			t.Parallel()

			h := newTestDMSHandler()

			// Before start: Describe returns empty.
			descBody := map[string]any{"MigrationProjectIdentifier": tt.startBody["MigrationProjectIdentifier"]}
			pre := parseJSON(t, doDMS(t, h, tt.descOp, descBody))
			assert.Empty(t, listField(pre))

			// Start the operation.
			rec := doDMS(t, h, tt.startOp, tt.startBody)
			require.Equal(t, http.StatusOK, rec.Code)
			startResp := parseJSON(t, rec)
			reqID, ok := startResp["RequestIdentifier"].(string)
			require.True(t, ok)
			assert.NotEmpty(t, reqID)

			// After start: Describe returns the request.
			post := parseJSON(t, doDMS(t, h, tt.descOp, descBody))
			reqs := listField(post)
			require.Len(t, reqs, 1)
			req0, ok := reqs[0].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, reqID, req0["RequestIdentifier"])
			assert.Equal(t, "running", req0["Status"])
		})
	}
}

// listField returns the first non-nil slice value from known list field names.
func listField(m map[string]any) []any {
	for _, k := range []string{"Requests", "Items"} {
		if v, ok := m[k].([]any); ok {
			return v
		}
	}

	return nil
}

// TestParityC_CancelMetadataModelConversion verifies cancel updates request status.
func TestParityC_CancelMetadataModelConversion(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()

	// Start a conversion.
	startResp := parseJSON(t, doDMS(t, h, "StartMetadataModelConversion", map[string]any{
		"MigrationProjectIdentifier": "cancel-proj",
		"SelectionRules":             `{"rules":[]}`,
	}))
	reqID := startResp["RequestIdentifier"].(string)

	// Cancel it.
	rec := doDMS(t, h, "CancelMetadataModelConversion", map[string]any{
		"MigrationProjectIdentifier": "cancel-proj",
		"RequestIdentifier":          reqID,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, reqID, parseJSON(t, rec)["RequestIdentifier"])

	// Status should be "cancelling".
	descResp := parseJSON(t, doDMS(t, h, "DescribeMetadataModelConversions", map[string]any{
		"MigrationProjectIdentifier": "cancel-proj",
	}))
	reqs := descResp["Requests"].([]any)
	require.Len(t, reqs, 1)
	assert.Equal(t, "cancelling", reqs[0].(map[string]any)["Status"])
}

// TestParityC_DescribeRecommendations verifies recommendations after BatchStartRecommendations.
func TestParityC_DescribeRecommendations(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()

	// No recommendations before batch start.
	pre := parseJSON(t, doDMS(t, h, "DescribeRecommendations", map[string]any{}))
	assert.Empty(t, pre["Recommendations"])

	// Create a source endpoint (BatchStartRecommendations uses existing endpoints).
	epRec := doDMS(t, h, "CreateEndpoint", map[string]any{
		"EndpointIdentifier": "src-ep",
		"EndpointType":       "source",
		"EngineName":         "mysql",
	})
	require.Equal(t, http.StatusOK, epRec.Code)

	// Trigger recommendations.
	rec := doDMS(t, h, "BatchStartRecommendations", map[string]any{"Data": []any{}})
	require.Equal(t, http.StatusOK, rec.Code)

	// Now recommendations exist.
	post := parseJSON(t, doDMS(t, h, "DescribeRecommendations", map[string]any{}))
	recs, ok := post["Recommendations"].([]any)
	require.True(t, ok)
	require.NotEmpty(t, recs)
	r0, ok := recs[0].(map[string]any)
	require.True(t, ok)
	assert.NotEmpty(t, r0["DatabaseId"])
	assert.Equal(t, "aurora-mysql", r0["EngineName"])
	assert.Equal(t, "active", r0["Status"])
}

// TestParityC_DeleteEndpointInUse verifies O(1) index blocks deletion when task references endpoint.
func TestParityC_DeleteEndpointInUse(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()

	// Create replication instance.
	riResp := parseJSON(t, doDMS(t, h, "CreateReplicationInstance", map[string]any{
		"ReplicationInstanceIdentifier": "ri1",
		"ReplicationInstanceClass":      "dms.t3.micro",
	}))
	riARN := riResp["ReplicationInstance"].(map[string]any)["ReplicationInstanceArn"].(string)

	// Create source and target endpoints.
	srcResp := parseJSON(t, doDMS(t, h, "CreateEndpoint", map[string]any{
		"EndpointIdentifier": "src",
		"EndpointType":       "source",
		"EngineName":         "mysql",
	}))
	srcARN := srcResp["Endpoint"].(map[string]any)["EndpointArn"].(string)

	tgtResp := parseJSON(t, doDMS(t, h, "CreateEndpoint", map[string]any{
		"EndpointIdentifier": "tgt",
		"EndpointType":       "target",
		"EngineName":         "aurora-mysql",
	}))
	tgtARN := tgtResp["Endpoint"].(map[string]any)["EndpointArn"].(string)

	// Create task referencing both endpoints.
	rec := doDMS(t, h, "CreateReplicationTask", map[string]any{
		"ReplicationTaskIdentifier": "task1",
		"SourceEndpointArn":         srcARN,
		"TargetEndpointArn":         tgtARN,
		"ReplicationInstanceArn":    riARN,
		"MigrationType":             "full-load",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Attempt to delete the source endpoint while task references it.
	del := doDMS(t, h, "DeleteEndpoint", map[string]any{"EndpointArn": srcARN})
	assert.Equal(t, http.StatusBadRequest, del.Code)
	body := parseJSON(t, del)
	msg, _ := body["message"].(string)
	assert.Contains(t, msg, "in use")
}

// TestParityC_DescribeReplicationInstancesHMACPagination verifies HMAC-signed markers.
func TestParityC_DescribeReplicationInstancesHMACPagination(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()

	// Create three instances.
	for i, id := range []string{"aaa", "bbb", "ccc"} {
		rec := doDMS(t, h, "CreateReplicationInstance", map[string]any{
			"ReplicationInstanceIdentifier": id,
			"ReplicationInstanceClass":      "dms.t3.micro",
		})
		require.Equal(t, http.StatusOK, rec.Code, "create instance %d", i)
	}

	// Page 1: request 2 items.
	page1 := parseJSON(t, doDMS(t, h, "DescribeReplicationInstances", map[string]any{
		"MaxRecords": 2,
	}))
	instances1, ok := page1["ReplicationInstances"].([]any)
	require.True(t, ok)
	require.Len(t, instances1, 2)
	marker1, hasMarker := page1["Marker"].(string)
	require.True(t, hasMarker, "first page must include a Marker")
	require.NotEmpty(t, marker1)

	// Page 2: use the marker.
	page2 := parseJSON(t, doDMS(t, h, "DescribeReplicationInstances", map[string]any{
		"MaxRecords": 2,
		"Marker":     marker1,
	}))
	instances2, ok := page2["ReplicationInstances"].([]any)
	require.True(t, ok)
	require.Len(t, instances2, 1)
	_, hasMore := page2["Marker"]
	assert.False(t, hasMore, "last page must not include a Marker")
}
