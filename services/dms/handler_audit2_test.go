package dms_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dms"
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

// ── CreateReplicationTask validates referenced ARNs exist ──────────────────────

func TestAudit2_CreateReplicationTask_ARNValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		omitSource        bool
		omitTarget        bool
		omitInstance      bool
		badSourceArn      bool
		badTargetArn      bool
		badInstanceArn    bool
	}{
		{name: "nonexistent_source_endpoint", badSourceArn: true},
		{name: "nonexistent_target_endpoint", badTargetArn: true},
		{name: "nonexistent_replication_instance", badInstanceArn: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestDMSHandler()

			riRec := doDMS(t, h, "CreateReplicationInstance", map[string]any{
				"ReplicationInstanceIdentifier": "arn-ri",
				"ReplicationInstanceClass":      "dms.t3.medium",
			})
			require.Equal(t, http.StatusOK, riRec.Code)
			riArn := parseJSON(t, riRec)["ReplicationInstance"].(map[string]any)["ReplicationInstanceArn"].(string)

			srcRec := doDMS(t, h, "CreateEndpoint", map[string]any{
				"EndpointIdentifier": "arn-src",
				"EndpointType":       "source",
				"EngineName":         "mysql",
			})
			require.Equal(t, http.StatusOK, srcRec.Code)
			srcArn := parseJSON(t, srcRec)["Endpoint"].(map[string]any)["EndpointArn"].(string)

			tgtRec := doDMS(t, h, "CreateEndpoint", map[string]any{
				"EndpointIdentifier": "arn-tgt",
				"EndpointType":       "target",
				"EngineName":         "s3",
			})
			require.Equal(t, http.StatusOK, tgtRec.Code)
			tgtArn := parseJSON(t, tgtRec)["Endpoint"].(map[string]any)["EndpointArn"].(string)

			useSrcArn := srcArn
			useTgtArn := tgtArn
			useRiArn := riArn

			if tt.badSourceArn {
				useSrcArn = "arn:aws:dms:us-east-1:123:endpoint:nonexistent-src"
			}

			if tt.badTargetArn {
				useTgtArn = "arn:aws:dms:us-east-1:123:endpoint:nonexistent-tgt"
			}

			if tt.badInstanceArn {
				useRiArn = "arn:aws:dms:us-east-1:123:rep:nonexistent-ri"
			}

			rec := doDMS(t, h, "CreateReplicationTask", map[string]any{
				"ReplicationTaskIdentifier": "arn-task",
				"SourceEndpointArn":         useSrcArn,
				"TargetEndpointArn":         useTgtArn,
				"ReplicationInstanceArn":    useRiArn,
				"MigrationType":             "full-load",
			})

			require.Equal(t, http.StatusNotFound, rec.Code)

			var body map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
			assert.Equal(t, "ResourceNotFoundFault", body["__type"],
				"non-existent ARN in CreateReplicationTask must return ResourceNotFoundFault")
		})
	}
}

// ── DeleteEndpoint rejects endpoints in use by tasks ─────────────────────────

func TestAudit2_DeleteEndpoint_RejectsIfInUse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		isSource   bool
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

// ── Assessment run lifecycle: start, describe, delete, cancel ─────────────────

func TestAudit2_AssessmentRun_Lifecycle(t *testing.T) {
	t.Parallel()

	// Helper to build RI + endpoints + task.
	setupTask := func(t *testing.T, h *dms.Handler, prefix string) string {
		t.Helper()

		riRec := doDMS(t, h, "CreateReplicationInstance", map[string]any{
			"ReplicationInstanceIdentifier": prefix + "-ri",
			"ReplicationInstanceClass":      "dms.t3.medium",
		})
		require.Equal(t, http.StatusOK, riRec.Code)
		riArn := parseJSON(t, riRec)["ReplicationInstance"].(map[string]any)["ReplicationInstanceArn"].(string)

		srcRec := doDMS(t, h, "CreateEndpoint", map[string]any{
			"EndpointIdentifier": prefix + "-src",
			"EndpointType":       "source",
			"EngineName":         "mysql",
		})
		require.Equal(t, http.StatusOK, srcRec.Code)
		srcArn := parseJSON(t, srcRec)["Endpoint"].(map[string]any)["EndpointArn"].(string)

		tgtRec := doDMS(t, h, "CreateEndpoint", map[string]any{
			"EndpointIdentifier": prefix + "-tgt",
			"EndpointType":       "target",
			"EngineName":         "s3",
		})
		require.Equal(t, http.StatusOK, tgtRec.Code)
		tgtArn := parseJSON(t, tgtRec)["Endpoint"].(map[string]any)["EndpointArn"].(string)

		taskRec := doDMS(t, h, "CreateReplicationTask", map[string]any{
			"ReplicationTaskIdentifier": prefix + "-task",
			"SourceEndpointArn":         srcArn,
			"TargetEndpointArn":         tgtArn,
			"ReplicationInstanceArn":    riArn,
			"MigrationType":             "full-load",
		})
		require.Equal(t, http.StatusOK, taskRec.Code)

		return parseJSON(t, taskRec)["ReplicationTask"].(map[string]any)["ReplicationTaskArn"].(string)
	}

	t.Run("start_nonexistent_task_returns_404", func(t *testing.T) {
		t.Parallel()

		h := newTestDMSHandler()
		rec := doDMS(t, h, "StartReplicationTaskAssessmentRun", map[string]any{
			"ReplicationTaskArn":   "arn:aws:dms:us-east-1:123:task:nonexistent",
			"ServiceAccessRoleArn": "arn:aws:iam::123:role/role",
			"ResultLocationBucket": "my-bucket",
			"AssessmentRunName":    "test-run",
		})
		require.Equal(t, http.StatusNotFound, rec.Code)
	})

	t.Run("start_stores_run_describable_deletable", func(t *testing.T) {
		t.Parallel()

		h := newTestDMSHandler()
		taskArn := setupTask(t, h, "ar-lifecycle")

		// Start assessment run.
		startRec := doDMS(t, h, "StartReplicationTaskAssessmentRun", map[string]any{
			"ReplicationTaskArn":   taskArn,
			"ServiceAccessRoleArn": "arn:aws:iam::123:role/role",
			"ResultLocationBucket": "my-bucket",
			"AssessmentRunName":    "my-run",
		})
		require.Equal(t, http.StatusOK, startRec.Code)
		runBody := parseJSON(t, startRec)["ReplicationTaskAssessmentRun"].(map[string]any)
		runArn, _ := runBody["ReplicationTaskAssessmentRunArn"].(string)
		assert.NotEmpty(t, runArn, "assessment run ARN must be non-empty")

		// DescribeReplicationTaskAssessmentRuns must return it.
		descRec := doDMS(t, h, "DescribeReplicationTaskAssessmentRuns", map[string]any{})
		require.Equal(t, http.StatusOK, descRec.Code)
		runs := parseJSON(t, descRec)["ReplicationTaskAssessmentRuns"].([]any)
		assert.Len(t, runs, 1)

		// DeleteReplicationTaskAssessmentRun must succeed.
		delRec := doDMS(t, h, "DeleteReplicationTaskAssessmentRun", map[string]any{
			"ReplicationTaskAssessmentRunArn": runArn,
		})
		require.Equal(t, http.StatusOK, delRec.Code)

		// Second delete must return 404.
		del2Rec := doDMS(t, h, "DeleteReplicationTaskAssessmentRun", map[string]any{
			"ReplicationTaskAssessmentRunArn": runArn,
		})
		require.Equal(t, http.StatusNotFound, del2Rec.Code)
	})

	t.Run("cancel_existing_run_succeeds", func(t *testing.T) {
		t.Parallel()

		h := newTestDMSHandler()
		taskArn := setupTask(t, h, "ar-cancel")

		startRec := doDMS(t, h, "StartReplicationTaskAssessmentRun", map[string]any{
			"ReplicationTaskArn":   taskArn,
			"ServiceAccessRoleArn": "arn:aws:iam::123:role/role",
			"ResultLocationBucket": "bucket",
			"AssessmentRunName":    "cancel-run",
		})
		require.Equal(t, http.StatusOK, startRec.Code)
		runArn := parseJSON(t, startRec)["ReplicationTaskAssessmentRun"].(map[string]any)["ReplicationTaskAssessmentRunArn"].(string)

		cancelRec := doDMS(t, h, "CancelReplicationTaskAssessmentRun", map[string]any{
			"ReplicationTaskAssessmentRunArn": runArn,
		})
		require.Equal(t, http.StatusOK, cancelRec.Code)
	})

	t.Run("cancel_nonexistent_run_returns_404", func(t *testing.T) {
		t.Parallel()

		h := newTestDMSHandler()
		rec := doDMS(t, h, "CancelReplicationTaskAssessmentRun", map[string]any{
			"ReplicationTaskAssessmentRunArn": "arn:aws:dms:us-east-1:123:assessment-run:nonexistent",
		})
		require.Equal(t, http.StatusNotFound, rec.Code)
	})
}

// ── CreateReplicationTask MigrationType validation ────────────────────────────

func TestAudit2_CreateReplicationTask_InvalidMigrationType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		migrationType string
	}{
		{name: "bad_type", migrationType: "bad-type"},
		{name: "empty_after_required_check", migrationType: "full_load"},
		{name: "cdc_caps", migrationType: "CDC"},
		{name: "unknown", migrationType: "incremental"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestDMSHandler()
			rec := doDMS(t, h, "CreateReplicationTask", map[string]any{
				"ReplicationTaskIdentifier": "task-1",
				"SourceEndpointArn":         "arn:aws:dms:us-east-1:123:endpoint:src",
				"TargetEndpointArn":         "arn:aws:dms:us-east-1:123:endpoint:tgt",
				"ReplicationInstanceArn":    "arn:aws:dms:us-east-1:123:rep:ri",
				"MigrationType":             tt.migrationType,
			})

			require.Equal(t, http.StatusBadRequest, rec.Code)

			var body map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
			assert.Equal(t, "ValidationException", body["__type"],
				"invalid MigrationType must return ValidationException")
		})
	}
}

// ── StopReplicationTask state validation ──────────────────────────────────────

func TestAudit2_StopReplicationTask_NotRunning(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		stop bool // whether to stop it before trying a second stop
	}{
		{name: "stop_ready_task", stop: false},
		{name: "stop_already_stopped_task", stop: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestDMSHandler()

			riRec := doDMS(t, h, "CreateReplicationInstance", map[string]any{
				"ReplicationInstanceIdentifier": "stop-ri",
				"ReplicationInstanceClass":      "dms.t3.medium",
			})
			require.Equal(t, http.StatusOK, riRec.Code)
			riArn := parseJSON(t, riRec)["ReplicationInstance"].(map[string]any)["ReplicationInstanceArn"].(string)

			srcRec := doDMS(t, h, "CreateEndpoint", map[string]any{
				"EndpointIdentifier": "stop-src",
				"EndpointType":       "source",
				"EngineName":         "mysql",
			})
			require.Equal(t, http.StatusOK, srcRec.Code)
			srcArn := parseJSON(t, srcRec)["Endpoint"].(map[string]any)["EndpointArn"].(string)

			tgtRec := doDMS(t, h, "CreateEndpoint", map[string]any{
				"EndpointIdentifier": "stop-tgt",
				"EndpointType":       "target",
				"EngineName":         "s3",
			})
			require.Equal(t, http.StatusOK, tgtRec.Code)
			tgtArn := parseJSON(t, tgtRec)["Endpoint"].(map[string]any)["EndpointArn"].(string)

			taskRec := doDMS(t, h, "CreateReplicationTask", map[string]any{
				"ReplicationTaskIdentifier": "stop-task",
				"SourceEndpointArn":         srcArn,
				"TargetEndpointArn":         tgtArn,
				"ReplicationInstanceArn":    riArn,
				"MigrationType":             "full-load",
			})
			require.Equal(t, http.StatusOK, taskRec.Code)
			taskArn := parseJSON(t, taskRec)["ReplicationTask"].(map[string]any)["ReplicationTaskArn"].(string)

			if tt.stop {
				// Start then stop to put it in stopped state.
				startRec := doDMS(t, h, "StartReplicationTask", map[string]any{
					"ReplicationTaskArn":       taskArn,
					"StartReplicationTaskType": "start-replication",
				})
				require.Equal(t, http.StatusOK, startRec.Code)

				stopRec := doDMS(t, h, "StopReplicationTask", map[string]any{
					"ReplicationTaskArn": taskArn,
				})
				require.Equal(t, http.StatusOK, stopRec.Code)
			}

			// Stop a non-running task (ready or already stopped) — must fail.
			rec := doDMS(t, h, "StopReplicationTask", map[string]any{
				"ReplicationTaskArn": taskArn,
			})

			require.Equal(t, http.StatusBadRequest, rec.Code)

			var body map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
			assert.Equal(t, "InvalidResourceStateFault", body["__type"],
				"stopping a non-running task must return InvalidResourceStateFault")
		})
	}
}

// ── DeleteConnection works after TestConnection ───────────────────────────────

func TestAudit2_DeleteConnection_AfterTestConnection(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()

	riRec := doDMS(t, h, "CreateReplicationInstance", map[string]any{
		"ReplicationInstanceIdentifier": "del-conn-ri",
		"ReplicationInstanceClass":      "dms.t3.medium",
	})
	require.Equal(t, http.StatusOK, riRec.Code)
	riArn := parseJSON(t, riRec)["ReplicationInstance"].(map[string]any)["ReplicationInstanceArn"].(string)

	epRec := doDMS(t, h, "CreateEndpoint", map[string]any{
		"EndpointIdentifier": "del-conn-ep",
		"EndpointType":       "source",
		"EngineName":         "mysql",
	})
	require.Equal(t, http.StatusOK, epRec.Code)
	epArn := parseJSON(t, epRec)["Endpoint"].(map[string]any)["EndpointArn"].(string)

	// TestConnection records the connection.
	testRec := doDMS(t, h, "TestConnection", map[string]any{
		"ReplicationInstanceArn": riArn,
		"EndpointArn":            epArn,
	})
	require.Equal(t, http.StatusOK, testRec.Code)

	// DeleteConnection must succeed (not 404).
	delRec := doDMS(t, h, "DeleteConnection", map[string]any{
		"ReplicationInstanceArn": riArn,
		"EndpointArn":            epArn,
	})
	require.Equal(t, http.StatusOK, delRec.Code)

	conn := parseJSON(t, delRec)["Connection"].(map[string]any)
	assert.Equal(t, riArn, conn["ReplicationInstanceArn"])
	assert.Equal(t, epArn, conn["EndpointArn"])
	assert.Equal(t, "successful", conn["Status"])

	// A second delete must return 404.
	del2Rec := doDMS(t, h, "DeleteConnection", map[string]any{
		"ReplicationInstanceArn": riArn,
		"EndpointArn":            epArn,
	})
	require.Equal(t, http.StatusNotFound, del2Rec.Code)
}

// ── ModifyMigrationProject actually persists description ──────────────────────

func TestAudit2_ModifyMigrationProject_UpdatesDescription(t *testing.T) {
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

// ── ModifyReplicationConfig actually persists ReplicationType ─────────────────

func TestAudit2_ModifyReplicationConfig_UpdatesReplicationType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		lookupByArn bool
	}{
		{name: "lookup_by_identifier", lookupByArn: false},
		{name: "lookup_by_arn", lookupByArn: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestDMSHandler()

			createRec := doDMS(t, h, "CreateReplicationConfig", map[string]any{
				"ReplicationConfigIdentifier": "rc-modify",
				"ReplicationType":             "full-load",
				"SourceEndpointArn":           "arn:aws:dms:us-east-1:123:endpoint:src",
				"TargetEndpointArn":           "arn:aws:dms:us-east-1:123:endpoint:tgt",
			})
			require.Equal(t, http.StatusOK, createRec.Code)
			rc := parseJSON(t, createRec)["ReplicationConfig"].(map[string]any)
			rcIdentifier := rc["ReplicationConfigIdentifier"].(string)
			rcArn := rc["ReplicationConfigArn"].(string)

			lookupKey := rcIdentifier
			if tt.lookupByArn {
				lookupKey = rcArn
			}

			modRec := doDMS(t, h, "ModifyReplicationConfig", map[string]any{
				"ReplicationConfigArn": lookupKey,
				"ReplicationType":      "cdc",
			})
			require.Equal(t, http.StatusOK, modRec.Code)

			updated := parseJSON(t, modRec)["ReplicationConfig"].(map[string]any)
			assert.Equal(t, "cdc", updated["ReplicationType"],
				"ModifyReplicationConfig must persist the updated ReplicationType")
		})
	}
}

// ── StartReplicationTaskAssessment validates task existence ───────────────────

func TestAudit2_StartReplicationTaskAssessment(t *testing.T) {
	t.Parallel()

	t.Run("not_found_returns_404", func(t *testing.T) {
		t.Parallel()

		h := newTestDMSHandler()
		rec := doDMS(t, h, "StartReplicationTaskAssessment", map[string]any{
			"ReplicationTaskArn": "arn:aws:dms:us-east-1:123:task:nonexistent",
		})
		require.Equal(t, http.StatusNotFound, rec.Code)

		var body map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
		assert.Equal(t, "ResourceNotFoundFault", body["__type"])
	})

	t.Run("returns_task_on_success", func(t *testing.T) {
		t.Parallel()

		h := newTestDMSHandler()

		riRec := doDMS(t, h, "CreateReplicationInstance", map[string]any{
			"ReplicationInstanceIdentifier": "assess-ri",
			"ReplicationInstanceClass":      "dms.t3.medium",
		})
		require.Equal(t, http.StatusOK, riRec.Code)
		riArn := parseJSON(t, riRec)["ReplicationInstance"].(map[string]any)["ReplicationInstanceArn"].(string)

		srcRec := doDMS(t, h, "CreateEndpoint", map[string]any{
			"EndpointIdentifier": "assess-src",
			"EndpointType":       "source",
			"EngineName":         "mysql",
		})
		require.Equal(t, http.StatusOK, srcRec.Code)
		srcArn := parseJSON(t, srcRec)["Endpoint"].(map[string]any)["EndpointArn"].(string)

		tgtRec := doDMS(t, h, "CreateEndpoint", map[string]any{
			"EndpointIdentifier": "assess-tgt",
			"EndpointType":       "target",
			"EngineName":         "s3",
		})
		require.Equal(t, http.StatusOK, tgtRec.Code)
		tgtArn := parseJSON(t, tgtRec)["Endpoint"].(map[string]any)["EndpointArn"].(string)

		taskRec := doDMS(t, h, "CreateReplicationTask", map[string]any{
			"ReplicationTaskIdentifier": "assess-task",
			"SourceEndpointArn":         srcArn,
			"TargetEndpointArn":         tgtArn,
			"ReplicationInstanceArn":    riArn,
			"MigrationType":             "full-load",
		})
		require.Equal(t, http.StatusOK, taskRec.Code)
		taskArn := parseJSON(t, taskRec)["ReplicationTask"].(map[string]any)["ReplicationTaskArn"].(string)

		assessRec := doDMS(t, h, "StartReplicationTaskAssessment", map[string]any{
			"ReplicationTaskArn": taskArn,
		})
		require.Equal(t, http.StatusOK, assessRec.Code)

		rt := parseJSON(t, assessRec)["ReplicationTask"].(map[string]any)
		assert.Equal(t, taskArn, rt["ReplicationTaskArn"],
			"StartReplicationTaskAssessment must return the actual task ARN")
		// Status must not be the old hardcoded "test-failed".
		assert.NotEqual(t, "test-failed", rt["Status"],
			"StartReplicationTaskAssessment must not return test-failed as initial status")
	})
}
