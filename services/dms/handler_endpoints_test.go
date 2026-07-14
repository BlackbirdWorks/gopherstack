package dms_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestModifyEndpoint(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	h.Backend.AddEndpointInternal("mod-ep", "source", "mysql")

	// Describe to get ARN.
	descRec := doDMS(t, h, "DescribeEndpoints", map[string]any{
		"Filters": []map[string]any{{"Name": "endpoint-id", "Values": []string{"mod-ep"}}},
	})
	require.Equal(t, http.StatusOK, descRec.Code)
	eps := parseJSON(t, descRec)["Endpoints"].([]any)
	require.Len(t, eps, 1)
	epArn := eps[0].(map[string]any)["EndpointArn"].(string)

	rec := doDMS(t, h, "ModifyEndpoint", map[string]any{
		"EndpointArn": epArn,
		"ServerName":  "new-server",
		"Port":        float64(5432),
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Not found case.
	rec2 := doDMS(t, h, "ModifyEndpoint", map[string]any{
		"EndpointArn": "arn:nonexistent",
	})
	assert.Equal(t, http.StatusNotFound, rec2.Code)
}

func TestDeleteEndpoint_RejectsIfInUse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		isSource bool
	}{
		{name: "source_endpoint_in_use", isSource: true},
		{name: "target_endpoint_in_use", isSource: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestDMSHandler()

			riRec := doDMS(t, h, "CreateReplicationInstance", map[string]any{
				"ReplicationInstanceIdentifier": "ep-inuse-ri",
				"ReplicationInstanceClass":      "dms.t3.medium",
			})
			require.Equal(t, http.StatusOK, riRec.Code)
			riArn := parseJSON(t, riRec)["ReplicationInstance"].(map[string]any)["ReplicationInstanceArn"].(string)

			srcRec := doDMS(t, h, "CreateEndpoint", map[string]any{
				"EndpointIdentifier": "ep-inuse-src",
				"EndpointType":       "source",
				"EngineName":         "mysql",
			})
			require.Equal(t, http.StatusOK, srcRec.Code)
			srcArn := parseJSON(t, srcRec)["Endpoint"].(map[string]any)["EndpointArn"].(string)

			tgtRec := doDMS(t, h, "CreateEndpoint", map[string]any{
				"EndpointIdentifier": "ep-inuse-tgt",
				"EndpointType":       "target",
				"EngineName":         "s3",
			})
			require.Equal(t, http.StatusOK, tgtRec.Code)
			tgtArn := parseJSON(t, tgtRec)["Endpoint"].(map[string]any)["EndpointArn"].(string)

			taskRec := doDMS(t, h, "CreateReplicationTask", map[string]any{
				"ReplicationTaskIdentifier": "ep-inuse-task",
				"SourceEndpointArn":         srcArn,
				"TargetEndpointArn":         tgtArn,
				"ReplicationInstanceArn":    riArn,
				"MigrationType":             "full-load",
			})
			require.Equal(t, http.StatusOK, taskRec.Code)

			// Delete whichever endpoint is in use — must fail with state error.
			deleteArn := tgtArn
			if tt.isSource {
				deleteArn = srcArn
			}

			delRec := doDMS(t, h, "DeleteEndpoint", map[string]any{
				"EndpointArn": deleteArn,
			})
			require.Equal(t, http.StatusBadRequest, delRec.Code)

			var body map[string]any
			require.NoError(t, json.Unmarshal(delRec.Body.Bytes(), &body))
			assert.Equal(t, "InvalidResourceStateFault", body["__type"],
				"deleting an endpoint used by a task must return InvalidResourceStateFault")
		})
	}
}

func TestDescribeSchemas(t *testing.T) {
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

func TestDeleteEndpointInUse(t *testing.T) {
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
