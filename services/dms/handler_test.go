package dms_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/dms"
)

func newTestDMSHandler() *dms.Handler {
	backend := dms.NewInMemoryBackend("123456789012", config.DefaultRegion)

	return dms.NewHandler(backend)
}

func doDMS(t *testing.T, h *dms.Handler, action string, body any) *httptest.ResponseRecorder {
	t.Helper()

	bodyBytes, err := json.Marshal(body)
	require.NoError(t, err)

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "AmazonDMSv20160101."+action)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	handlerErr := h.Handler()(c)
	require.NoError(t, handlerErr)

	return rec
}

func parseJSON(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()

	var m map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &m))

	return m
}

func TestHandler_Name(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	assert.Equal(t, "DMS", h.Name())
}

func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	ops := h.GetSupportedOperations()

	expected := []string{
		"CreateReplicationInstance",
		"DescribeReplicationInstances",
		"DeleteReplicationInstance",
		"CreateEndpoint",
		"DescribeEndpoints",
		"DeleteEndpoint",
		"CreateReplicationTask",
		"DescribeReplicationTasks",
		"StartReplicationTask",
		"StopReplicationTask",
		"DeleteReplicationTask",
		"AddTagsToResource",
		"ListTagsForResource",
	}

	for _, op := range expected {
		assert.Contains(t, ops, op, "operation %q should be supported", op)
	}
}

func TestHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	assert.Equal(t, 100, h.MatchPriority())
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		target string
		want   bool
	}{
		{
			name:   "matches_dms_target",
			target: "AmazonDMSv20160101.CreateReplicationInstance",
			want:   true,
		},
		{
			name:   "no_match_other_service",
			target: "AWSCognitoIdentityProviderService.CreateUserPool",
			want:   false,
		},
		{
			name:   "no_match_empty",
			target: "",
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestDMSHandler()
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.want, h.RouteMatcher()(c))
		})
	}
}

func TestHandler_ChaosServiceName(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	assert.Equal(t, "dms", h.ChaosServiceName())
}

func TestHandler_UnknownAction(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	rec := doDMS(t, h, "UnknownAction", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_MissingTarget(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader([]byte(`{}`)))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_NonPostMethod(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	e := echo.New()
	req := httptest.NewRequest(http.MethodPut, "/some-path", nil)
	req.Header.Set("X-Amz-Target", "AmazonDMSv20160101.DescribeReplicationInstances")

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		target string
		want   string
	}{
		{
			name:   "valid_target",
			target: "AmazonDMSv20160101.CreateReplicationInstance",
			want:   "CreateReplicationInstance",
		},
		{
			name:   "empty_target",
			target: "",
			want:   "Unknown",
		},
		{
			name:   "wrong_prefix",
			target: "AmazonOther.CreateReplicationInstance",
			want:   "Unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestDMSHandler()
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.want, h.ExtractOperation(c))
		})
	}
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		target string
		body   map[string]any
		want   string
	}{
		{
			name:   "replication_instance_identifier",
			target: "AmazonDMSv20160101.CreateReplicationInstance",
			body:   map[string]any{"ReplicationInstanceIdentifier": "my-inst"},
			want:   "my-inst",
		},
		{
			name:   "endpoint_identifier",
			target: "AmazonDMSv20160101.CreateEndpoint",
			body:   map[string]any{"EndpointIdentifier": "my-ep"},
			want:   "my-ep",
		},
		{
			name:   "replication_task_identifier",
			target: "AmazonDMSv20160101.CreateReplicationTask",
			body:   map[string]any{"ReplicationTaskIdentifier": "my-task"},
			want:   "my-task",
		},
		{
			name:   "resource_arn_for_tags",
			target: "AmazonDMSv20160101.ListTagsForResource",
			body:   map[string]any{"ResourceArn": "arn:aws:dms:us-east-1:123:rep:inst-1"},
			want:   "arn:aws:dms:us-east-1:123:rep:inst-1",
		},
		{
			name:   "unknown_action_empty",
			target: "AmazonDMSv20160101.SomeOtherAction",
			body:   map[string]any{},
			want:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestDMSHandler()
			bodyBytes, err := json.Marshal(tt.body)
			require.NoError(t, err)

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
			req.Header.Set("X-Amz-Target", tt.target)
			req.Header.Set("Content-Type", "application/x-amz-json-1.1")
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.want, h.ExtractResource(c))
		})
	}
}

func TestHandler_ChaosOperationsAndRegions(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	ops := h.ChaosOperations()
	assert.Equal(t, h.GetSupportedOperations(), ops)

	regions := h.ChaosRegions()
	require.Len(t, regions, 1)
	assert.NotEmpty(t, regions[0])
}

func TestHandler_BackendRegion(t *testing.T) {
	t.Parallel()

	backend := dms.NewInMemoryBackend("123456789012", "eu-west-1")
	assert.Equal(t, "eu-west-1", backend.Region())
}

func TestHandler_InvalidStateError(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()

	// Create an instance and then create a task.
	instRec := doDMS(t, h, "CreateReplicationInstance", map[string]any{
		"ReplicationInstanceIdentifier": "state-inst",
		"ReplicationInstanceClass":      "dms.t3.medium",
	})
	require.Equal(t, http.StatusOK, instRec.Code)
	instArn := parseJSON(t, instRec)["ReplicationInstance"].(map[string]any)["ReplicationInstanceArn"].(string)

	srcRec := doDMS(t, h, "CreateEndpoint", map[string]any{
		"EndpointIdentifier": "state-src",
		"EndpointType":       "source",
		"EngineName":         "mysql",
	})
	require.Equal(t, http.StatusOK, srcRec.Code)
	srcArn := parseJSON(t, srcRec)["Endpoint"].(map[string]any)["EndpointArn"].(string)

	dstRec := doDMS(t, h, "CreateEndpoint", map[string]any{
		"EndpointIdentifier": "state-dst",
		"EndpointType":       "target",
		"EngineName":         "s3",
	})
	require.Equal(t, http.StatusOK, dstRec.Code)
	dstArn := parseJSON(t, dstRec)["Endpoint"].(map[string]any)["EndpointArn"].(string)

	taskRec := doDMS(t, h, "CreateReplicationTask", map[string]any{
		"ReplicationTaskIdentifier": "state-task",
		"SourceEndpointArn":         srcArn,
		"TargetEndpointArn":         dstArn,
		"ReplicationInstanceArn":    instArn,
		"MigrationType":             "full-load",
	})
	require.Equal(t, http.StatusOK, taskRec.Code)
	taskArn := parseJSON(t, taskRec)["ReplicationTask"].(map[string]any)["ReplicationTaskArn"].(string)

	// Start the task.
	startRec := doDMS(t, h, "StartReplicationTask", map[string]any{
		"ReplicationTaskArn":       taskArn,
		"StartReplicationTaskType": "start-replication",
	})
	require.Equal(t, http.StatusOK, startRec.Code)

	// Start again should fail with 400.
	startAgainRec := doDMS(t, h, "StartReplicationTask", map[string]any{
		"ReplicationTaskArn":       taskArn,
		"StartReplicationTaskType": "start-replication",
	})
	assert.Equal(t, http.StatusBadRequest, startAgainRec.Code)
}

func TestHandler_GetSupportedOperationsIncludesExtendedOps(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	ops := h.GetSupportedOperations()

	newOps := []string{
		"ApplyPendingMaintenanceAction",
		"BatchStartRecommendations",
		"CancelMetadataModelConversion",
		"CancelMetadataModelCreation",
		"CancelReplicationTaskAssessmentRun",
		"CreateDataMigration",
		"CreateDataProvider",
		"CreateEventSubscription",
		"CreateFleetAdvisorCollector",
		"CreateInstanceProfile",
	}

	for _, op := range newOps {
		assert.Contains(t, ops, op, "operation %q should be in GetSupportedOperations()", op)
	}
}

func TestReset(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	h.Backend.AddReplicationInstanceInternal("inst1", "dms.t3.medium")
	require.Equal(t, 1, h.Backend.ReplicationInstanceCount())

	h.Backend.Reset()
	assert.Equal(t, 0, h.Backend.ReplicationInstanceCount())
}

func TestHandlerReset(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	h.Backend.AddEndpointInternal("ep1", "source", "mysql")
	require.Equal(t, 1, h.Backend.EndpointCount())

	h.Reset()
	assert.Equal(t, 0, h.Backend.EndpointCount())
}

func TestProviderInit_NilCtx(t *testing.T) {
	t.Parallel()

	p := dms.Provider{}
	_, err := p.Init(nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, dms.ErrNilAppContext)
}

func TestHandlerOpsPreBuilt(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	rec := doDMS(t, h, "CreateReplicationInstance", map[string]any{
		"ReplicationInstanceIdentifier": "pre-built-inst",
		"ReplicationInstanceClass":      "dms.t3.medium",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestDescribeReturnsEmptyListOnFilterMiss(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	rec := doDMS(t, h, "DescribeReplicationInstances", map[string]any{
		"Filters": []map[string]any{
			{"Name": "replication-instance-id", "Values": []string{"nonexistent"}},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	body := parseJSON(t, rec)
	instances := body["ReplicationInstances"].([]any)
	assert.Empty(t, instances)
}

func TestSeedHelpers(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	h.Backend.AddReplicationInstanceInternal("seed-inst", "dms.t3.medium")
	h.Backend.AddEndpointInternal("seed-ep", "source", "mysql")
	h.Backend.AddReplicationTaskInternal("seed-task", "src-arn", "tgt-arn", "inst-arn", "full-load")

	assert.Equal(t, 1, h.Backend.ReplicationInstanceCount())
	assert.Equal(t, 1, h.Backend.EndpointCount())
	assert.Equal(t, 1, h.Backend.ReplicationTaskCount())
}

func TestExportCountHelpers(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	h.Backend.AddDataMigrationInternal("dm1", "cdc")
	h.Backend.AddDataProviderInternal("dp1", "mysql")
	h.Backend.AddEventSubscriptionInternal("sub1", "arn:aws:sns:us-east-1:123:t")
	h.Backend.AddFleetAdvisorCollectorInternal("col1")
	h.Backend.AddInstanceProfileInternal("prof1")

	assert.Equal(t, 1, h.Backend.DataMigrationCount())
	assert.Equal(t, 1, h.Backend.DataProviderCount())
	assert.Equal(t, 1, h.Backend.EventSubscriptionCount())
	assert.Equal(t, 1, h.Backend.FleetAdvisorCollectorCount())
	assert.Equal(t, 1, h.Backend.InstanceProfileCount())
}

func TestPersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	h.Backend.AddReplicationInstanceInternal("persist-inst", "dms.t3.medium")
	h.Backend.AddEndpointInternal("persist-ep", "source", "mysql")

	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	h2 := newTestDMSHandler()
	require.NoError(t, h2.Restore(t.Context(), snap))

	assert.Equal(t, 1, h2.Backend.ReplicationInstanceCount())
	assert.Equal(t, 1, h2.Backend.EndpointCount())
}

func TestValidationMappedTo400(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body   map[string]any
		name   string
		action string
	}{
		{
			name:   "missing_instance_identifier",
			action: "CreateReplicationInstance",
			body:   map[string]any{"ReplicationInstanceClass": "dms.t3.medium"},
		},
		{
			name:   "missing_endpoint_identifier",
			action: "CreateEndpoint",
			body:   map[string]any{"EndpointType": "source", "EngineName": "mysql"},
		},
		{
			name:   "missing_task_identifier",
			action: "CreateReplicationTask",
			body: map[string]any{
				"SourceEndpointArn":      "arn:src",
				"TargetEndpointArn":      "arn:tgt",
				"ReplicationInstanceArn": "arn:inst",
				"MigrationType":          "full-load",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestDMSHandler()
			rec := doDMS(t, h, tt.action, tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

func TestMultipleResetCycle(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	for _, name := range []string{"inst-0", "inst-1", "inst-2"} {
		h.Backend.AddReplicationInstanceInternal(name, "dms.t3.medium")
	}
	require.Equal(t, 3, h.Backend.ReplicationInstanceCount())

	h.Backend.Reset()
	require.Equal(t, 0, h.Backend.ReplicationInstanceCount())

	h.Backend.AddReplicationInstanceInternal("inst-after-reset", "dms.t3.medium")
	assert.Equal(t, 1, h.Backend.ReplicationInstanceCount())
}

// TestDescribeOperationsSmoke checks that a handful of cross-family Describe
// operations succeed once their backing resources exist.
func TestDescribeOperationsSmoke(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	h.Backend.AddDataMigrationInternal("desc-dm", "full-load")
	h.Backend.AddDataProviderInternal("desc-dp", "mysql")
	h.Backend.AddEventSubscriptionInternal("desc-es", "arn:aws:sns:us-east-1:123:topic")
	h.Backend.AddFleetAdvisorCollectorInternal("desc-fac")
	h.Backend.AddInstanceProfileInternal("desc-ip")

	actions := []string{
		"DescribeDataMigrations",
		"DescribeDataProviders",
		"DescribeEventSubscriptions",
		"DescribeFleetAdvisorCollectors",
		"DescribeInstanceProfiles",
	}
	for _, action := range actions {
		t.Run(action, func(t *testing.T) {
			t.Parallel()

			rec := doDMS(t, h, action, map[string]any{})
			assert.Equal(t, http.StatusOK, rec.Code, "action=%s", action)
		})
	}
}

// TestPassThroughOperationsSmoke checks that operations with little or no
// backend state still dispatch successfully (no panic, no unrouted 500).
func TestPassThroughOperationsSmoke(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()

	type passCase struct {
		body   map[string]any
		action string
	}

	mpx := map[string]any{"MigrationProjectIdentifier": "x"}
	mpxRules := map[string]any{"MigrationProjectIdentifier": "x", "SelectionRules": "{}"}
	mpxOriginRules := map[string]any{
		"MigrationProjectIdentifier": "x", "Origin": "SOURCE", "SelectionRules": "{}",
	}

	cases := []passCase{
		{body: map[string]any{"EndpointArn": "ep-arn", "ReplicationInstanceArn": "ri-arn"}, action: "DeleteConnection"},
		{body: map[string]any{}, action: "DescribeAccountAttributes"},
		{body: map[string]any{}, action: "DescribeApplicableIndividualAssessments"},
		{body: map[string]any{}, action: "DescribeConnections"},
		{body: map[string]any{}, action: "DescribeConversionConfiguration"},
		{body: map[string]any{"EngineName": "mysql"}, action: "DescribeEndpointSettings"},
		{body: map[string]any{}, action: "DescribeEndpointTypes"},
		{body: map[string]any{}, action: "DescribeEngineVersions"},
		{body: map[string]any{}, action: "DescribeEventCategories"},
		{body: map[string]any{}, action: "DescribeEvents"},
		{body: mpx, action: "DescribeExtensionPackAssociations"},
		{body: map[string]any{}, action: "DescribeFleetAdvisorDatabases"},
		{body: map[string]any{}, action: "DescribeFleetAdvisorLsaAnalysis"},
		{body: map[string]any{}, action: "DescribeFleetAdvisorSchemaObjectSummary"},
		{body: map[string]any{}, action: "DescribeFleetAdvisorSchemas"},
		{body: mpxOriginRules, action: "DescribeMetadataModel"},
		{body: mpx, action: "DescribeMetadataModelAssessments"},
		{body: mpxOriginRules, action: "DescribeMetadataModelChildren"},
		{body: mpx, action: "DescribeMetadataModelConversions"},
		{body: mpx, action: "DescribeMetadataModelCreations"},
		{body: mpx, action: "DescribeMetadataModelExportsAsScript"},
		{body: mpx, action: "DescribeMetadataModelExportsToTarget"},
		{body: mpx, action: "DescribeMetadataModelImports"},
		{body: map[string]any{}, action: "DescribeOrderableReplicationInstances"},
		{body: map[string]any{}, action: "DescribePendingMaintenanceActions"},
		{body: map[string]any{}, action: "DescribeRecommendationLimitations"},
		{body: map[string]any{}, action: "DescribeRecommendations"},
		{body: map[string]any{}, action: "DescribeRefreshSchemasStatus"},
		{body: map[string]any{"ReplicationInstanceArn": "ri-arn"}, action: "DescribeReplicationInstanceTaskLogs"},
		{body: map[string]any{"ReplicationTaskArn": "rt-arn"}, action: "DescribeReplicationTableStatistics"},
		{body: map[string]any{}, action: "DescribeReplicationTaskAssessmentResults"},
		{body: map[string]any{}, action: "DescribeReplicationTaskAssessmentRuns"},
		{body: map[string]any{}, action: "DescribeReplicationTaskIndividualAssessments"},
		{body: map[string]any{}, action: "DescribeReplications"},
		{body: map[string]any{}, action: "DescribeSchemas"},
		{body: map[string]any{"ReplicationTaskArn": "rt-arn"}, action: "DescribeTableStatistics"},
		{body: mpxRules, action: "ExportMetadataModelAssessment"},
		{body: mpxRules, action: "GetTargetSelectionRules"},
		{body: mpx, action: "ModifyConversionConfiguration"},
		{body: map[string]any{}, action: "RefreshSchemas"},
		{body: map[string]any{}, action: "RunFleetAdvisorLsaAnalysis"},
		{body: mpx, action: "StartExtensionPackAssociation"},
		{body: mpxRules, action: "StartMetadataModelAssessment"},
		{body: mpxRules, action: "StartMetadataModelConversion"},
		{
			body: map[string]any{
				"MigrationProjectIdentifier": "x", "MetadataModelName": "m", "SelectionRules": "{}",
			},
			action: "StartMetadataModelCreation",
		},
		{body: mpxOriginRules, action: "StartMetadataModelExportAsScript"},
		{body: mpxRules, action: "StartMetadataModelExportToTarget"},
		{body: mpxOriginRules, action: "StartMetadataModelImport"},
		{body: map[string]any{}, action: "StartRecommendations"},
		{
			body:   map[string]any{"ReplicationConfigArn": "rc-arn", "StartReplicationType": "full-load"},
			action: "StartReplication",
		},
		{body: map[string]any{"ReplicationTaskArn": "rt-arn"}, action: "StartReplicationTaskAssessment"},
		{
			body:   map[string]any{"ReplicationTaskArn": "rt-arn", "ServiceAccessRoleArn": "arn"},
			action: "StartReplicationTaskAssessmentRun",
		},
		{body: map[string]any{"ReplicationConfigArn": "rc-arn"}, action: "StopReplication"},
		{body: map[string]any{}, action: "UpdateSubscriptionsToEventBridge"},
		{
			body:   map[string]any{"ReplicationTaskAssessmentRunArn": "arn"},
			action: "DeleteReplicationTaskAssessmentRun",
		},
		{body: map[string]any{}, action: "DeleteFleetAdvisorDatabases"},
	}

	// Cases share a single handler instance; the backend is safe for
	// concurrent access, so subtests can still run in parallel.
	for _, tc := range cases {
		t.Run(tc.action, func(t *testing.T) {
			t.Parallel()

			rec := doDMS(t, h, tc.action, tc.body)
			// Just make sure the handler returns a response (not a panic or unrouted).
			assert.NotEqual(t, http.StatusInternalServerError, rec.Code, "action=%s", tc.action)
		})
	}
}

func TestValidationException_NotInvalidResourceStateFault(t *testing.T) {
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

func TestInvalidResourceStateFault_ForStateErrors(t *testing.T) {
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
