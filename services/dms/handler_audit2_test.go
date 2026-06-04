package dms_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── ValidationException for missing required fields ──────────────────────────

func TestAudit2_ValidationException_NotInvalidResourceStateFault(t *testing.T) {
	t.Parallel()

	tests := []struct {
		action string
		body   map[string]any
		name   string
	}{
		{
			name:   "CreateReplicationInstance missing identifier",
			action: "CreateReplicationInstance",
			body:   map[string]any{"ReplicationInstanceClass": "dms.t3.medium"},
		},
		{
			name:   "CreateReplicationInstance missing class",
			action: "CreateReplicationInstance",
			body:   map[string]any{"ReplicationInstanceIdentifier": "ri-1"},
		},
		{
			name:   "CreateEndpoint missing identifier",
			action: "CreateEndpoint",
			body:   map[string]any{"EndpointType": "source", "EngineName": "mysql"},
		},
		{
			name:   "CreateEndpoint missing engine",
			action: "CreateEndpoint",
			body:   map[string]any{"EndpointIdentifier": "ep-1", "EndpointType": "source"},
		},
		{
			name:   "CreateReplicationTask missing identifier",
			action: "CreateReplicationTask",
			body: map[string]any{
				"SourceEndpointArn":      "arn:src",
				"TargetEndpointArn":      "arn:tgt",
				"ReplicationInstanceArn": "arn:ri",
				"MigrationType":          "full-load",
			},
		},
		{
			name:   "CreateDataMigration missing name",
			action: "CreateDataMigration",
			body:   map[string]any{"DataMigrationType": "full-load"},
		},
		{
			name:   "StartReplicationTask invalid type",
			action: "StartReplicationTask",
			body:   map[string]any{"ReplicationTaskArn": "arn:task", "StartReplicationTaskType": "bad-type"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestDMSHandler()
			rec := doDMS(t, h, tt.action, tt.body)
			require.Equal(t, http.StatusBadRequest, rec.Code)

			var body map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))

			errType, ok := body["__type"].(string)
			require.True(t, ok, "response must have __type field")
			assert.Equal(t, "ValidationException", errType,
				"validation errors must return ValidationException not InvalidResourceStateFault")
		})
	}
}

// ── InvalidResourceStateFault for state errors ────────────────────────────────

func TestAudit2_InvalidResourceStateFault_ForStateErrors(t *testing.T) {
	t.Parallel()

	t.Run("start_running_task_returns_invalid_state", func(t *testing.T) {
		t.Parallel()

		h := newTestDMSHandler()

		// Create RI + endpoints + task.
		riRec := doDMS(t, h, "CreateReplicationInstance", map[string]any{
			"ReplicationInstanceIdentifier": "state-ri",
			"ReplicationInstanceClass":      "dms.t3.medium",
		})
		require.Equal(t, http.StatusOK, riRec.Code)
		riArn := parseJSON(t, riRec)["ReplicationInstance"].(map[string]any)["ReplicationInstanceArn"].(string)

		srcRec := doDMS(t, h, "CreateEndpoint", map[string]any{
			"EndpointIdentifier": "state-src",
			"EndpointType":       "source",
			"EngineName":         "mysql",
		})
		require.Equal(t, http.StatusOK, srcRec.Code)
		srcArn := parseJSON(t, srcRec)["Endpoint"].(map[string]any)["EndpointArn"].(string)

		tgtRec := doDMS(t, h, "CreateEndpoint", map[string]any{
			"EndpointIdentifier": "state-tgt",
			"EndpointType":       "target",
			"EngineName":         "s3",
		})
		require.Equal(t, http.StatusOK, tgtRec.Code)
		tgtArn := parseJSON(t, tgtRec)["Endpoint"].(map[string]any)["EndpointArn"].(string)

		taskRec := doDMS(t, h, "CreateReplicationTask", map[string]any{
			"ReplicationTaskIdentifier": "state-task",
			"SourceEndpointArn":         srcArn,
			"TargetEndpointArn":         tgtArn,
			"ReplicationInstanceArn":    riArn,
			"MigrationType":             "full-load",
		})
		require.Equal(t, http.StatusOK, taskRec.Code)
		taskArn := parseJSON(t, taskRec)["ReplicationTask"].(map[string]any)["ReplicationTaskArn"].(string)

		// Start it once (succeeds).
		startRec := doDMS(t, h, "StartReplicationTask", map[string]any{
			"ReplicationTaskArn":       taskArn,
			"StartReplicationTaskType": "start-replication",
		})
		require.Equal(t, http.StatusOK, startRec.Code)

		// Start it again while running (should fail with state error).
		startAgainRec := doDMS(t, h, "StartReplicationTask", map[string]any{
			"ReplicationTaskArn":       taskArn,
			"StartReplicationTaskType": "start-replication",
		})
		require.Equal(t, http.StatusBadRequest, startAgainRec.Code)

		var body map[string]any
		require.NoError(t, json.Unmarshal(startAgainRec.Body.Bytes(), &body))

		errType, ok := body["__type"].(string)
		require.True(t, ok, "response must have __type field")
		assert.Equal(t, "InvalidResourceStateFault", errType,
			"state errors must return InvalidResourceStateFault")
	})
}

// ── ReplicationInstance private IP addresses always present ───────────────────

func TestAudit2_DescribeReplicationInstances_PrivateIpAddresses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "via_create"},
		{name: "via_describe"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestDMSHandler()

			createRec := doDMS(t, h, "CreateReplicationInstance", map[string]any{
				"ReplicationInstanceIdentifier": "ip-ri",
				"ReplicationInstanceClass":      "dms.t3.medium",
			})
			require.Equal(t, http.StatusOK, createRec.Code)

			var ri map[string]any

			if tt.name == "via_create" {
				body := parseJSON(t, createRec)
				ri = body["ReplicationInstance"].(map[string]any)
			} else {
				descRec := doDMS(t, h, "DescribeReplicationInstances", map[string]any{})
				require.Equal(t, http.StatusOK, descRec.Code)
				body := parseJSON(t, descRec)
				instances := body["ReplicationInstances"].([]any)
				require.Len(t, instances, 1)
				ri = instances[0].(map[string]any)
			}

			privateIPs, hasKey := ri["ReplicationInstancePrivateIpAddresses"]
			assert.True(t, hasKey, "ReplicationInstancePrivateIpAddresses must be present")
			ipList, ok := privateIPs.([]any)
			assert.True(t, ok, "ReplicationInstancePrivateIpAddresses must be an array")
			assert.NotEmpty(t, ipList, "ReplicationInstancePrivateIpAddresses must have at least one IP")
			assert.Equal(t, "10.0.0.1", ipList[0].(string))

			publicIPs, hasPublicKey := ri["ReplicationInstancePublicIpAddresses"]
			assert.True(t, hasPublicKey, "ReplicationInstancePublicIpAddresses must be present")
			pubList, ok := publicIPs.([]any)
			assert.True(t, ok, "ReplicationInstancePublicIpAddresses must be an array")
			assert.Empty(t, pubList, "ReplicationInstancePublicIpAddresses must be [] (no public IPs)")

			vpcSGs, hasSGKey := ri["VpcSecurityGroups"]
			assert.True(t, hasSGKey, "VpcSecurityGroups must be present")
			sgList, ok := vpcSGs.([]any)
			assert.True(t, ok, "VpcSecurityGroups must be an array")
			assert.Empty(t, sgList, "VpcSecurityGroups must be [] when no SGs configured")
		})
	}
}

// ── DeleteDataProvider 404 for nonexistent ─────────────────────────────────────

func TestAudit2_DeleteDataProvider_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	rec := doDMS(t, h, "DeleteDataProvider", map[string]any{
		"DataProviderArn": "arn:aws:dms:us-east-1:123:data-provider:nonexistent",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)

	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "ResourceNotFoundFault", body["__type"])
}

// ── DeleteFleetAdvisorCollector 404 for nonexistent ────────────────────────────

func TestAudit2_DeleteFleetAdvisorCollector_NotFound(t *testing.T) {
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

// ── CreateEventSubscription duplicate returns correct error ────────────────────

func TestAudit2_CreateEventSubscription_Duplicate(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()

	body := map[string]any{
		"SubscriptionName": "dup-sub",
		"SnsTopicArn":      "arn:aws:sns:us-east-1:123:topic",
	}

	rec1 := doDMS(t, h, "CreateEventSubscription", body)
	require.Equal(t, http.StatusOK, rec1.Code)

	rec2 := doDMS(t, h, "CreateEventSubscription", body)
	require.Equal(t, http.StatusConflict, rec2.Code)

	var errBody map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &errBody))
	assert.Equal(t, "ResourceAlreadyExistsFault", errBody["__type"])
}
