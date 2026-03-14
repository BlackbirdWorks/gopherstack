package dms_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
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

func TestHandler_ReplicationInstanceCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, h *dms.Handler)
		name string
	}{
		{
			name: "create_success",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				rec := doDMS(t, h, "CreateReplicationInstance", map[string]any{
					"ReplicationInstanceIdentifier": "my-rep-inst",
					"ReplicationInstanceClass":      "dms.t3.medium",
					"Tags": []map[string]string{
						{"Key": "Env", "Value": "test"},
					},
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec)
				ri, ok := resp["ReplicationInstance"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "my-rep-inst", ri["ReplicationInstanceIdentifier"])
				assert.Equal(t, "dms.t3.medium", ri["ReplicationInstanceClass"])
				assert.Equal(t, "available", ri["ReplicationInstanceStatus"])
				assert.NotEmpty(t, ri["ReplicationInstanceArn"])
			},
		},
		{
			name: "create_duplicate_conflict",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				doDMS(t, h, "CreateReplicationInstance", map[string]any{
					"ReplicationInstanceIdentifier": "dup-inst",
					"ReplicationInstanceClass":      "dms.t3.micro",
				})
				rec := doDMS(t, h, "CreateReplicationInstance", map[string]any{
					"ReplicationInstanceIdentifier": "dup-inst",
					"ReplicationInstanceClass":      "dms.t3.micro",
				})
				assert.Equal(t, http.StatusConflict, rec.Code)
			},
		},
		{
			name: "describe_all",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				doDMS(t, h, "CreateReplicationInstance", map[string]any{
					"ReplicationInstanceIdentifier": "inst-a",
					"ReplicationInstanceClass":      "dms.t3.medium",
				})
				rec := doDMS(t, h, "DescribeReplicationInstances", map[string]any{})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec)
				list, ok := resp["ReplicationInstances"].([]any)
				require.True(t, ok)
				assert.Len(t, list, 1)
			},
		},
		{
			name: "describe_by_filter",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				doDMS(t, h, "CreateReplicationInstance", map[string]any{
					"ReplicationInstanceIdentifier": "filter-inst",
					"ReplicationInstanceClass":      "dms.t3.medium",
				})
				rec := doDMS(t, h, "DescribeReplicationInstances", map[string]any{
					"Filters": []map[string]any{
						{"Name": "replication-instance-id", "Values": []string{"filter-inst"}},
					},
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec)
				list, ok := resp["ReplicationInstances"].([]any)
				require.True(t, ok)
				assert.Len(t, list, 1)
			},
		},
		{
			name: "describe_not_found",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				rec := doDMS(t, h, "DescribeReplicationInstances", map[string]any{
					"Filters": []map[string]any{
						{"Name": "replication-instance-id", "Values": []string{"missing"}},
					},
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "delete_success",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				create := doDMS(t, h, "CreateReplicationInstance", map[string]any{
					"ReplicationInstanceIdentifier": "del-inst",
					"ReplicationInstanceClass":      "dms.t3.medium",
				})
				require.Equal(t, http.StatusOK, create.Code)
				createResp := parseJSON(t, create)
				ri := createResp["ReplicationInstance"].(map[string]any)
				arn := ri["ReplicationInstanceArn"].(string)

				rec := doDMS(t, h, "DeleteReplicationInstance", map[string]any{
					"ReplicationInstanceArn": arn,
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				// Verify gone
				listRec := doDMS(t, h, "DescribeReplicationInstances", map[string]any{})
				listResp := parseJSON(t, listRec)
				list := listResp["ReplicationInstances"].([]any)
				assert.Empty(t, list)
			},
		},
		{
			name: "create_missing_identifier",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				rec := doDMS(t, h, "CreateReplicationInstance", map[string]any{
					"ReplicationInstanceClass": "dms.t3.medium",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "create_missing_class",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				rec := doDMS(t, h, "CreateReplicationInstance", map[string]any{
					"ReplicationInstanceIdentifier": "inst-no-class",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "describe_by_arn_filter",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				create := doDMS(t, h, "CreateReplicationInstance", map[string]any{
					"ReplicationInstanceIdentifier": "arn-filter-inst",
					"ReplicationInstanceClass":      "dms.t3.medium",
				})
				require.Equal(t, http.StatusOK, create.Code)
				createResp := parseJSON(t, create)
				ri := createResp["ReplicationInstance"].(map[string]any)
				arn := ri["ReplicationInstanceArn"].(string)

				rec := doDMS(t, h, "DescribeReplicationInstances", map[string]any{
					"Filters": []map[string]any{
						{"Name": "replication-instance-arn", "Values": []string{arn}},
					},
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec)
				list, ok := resp["ReplicationInstances"].([]any)
				require.True(t, ok)
				assert.Len(t, list, 1)
			},
		},
		{
			name: "delete_not_found",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				rec := doDMS(t, h, "DeleteReplicationInstance", map[string]any{
					"ReplicationInstanceArn": "arn:aws:dms:us-east-1:000000000000:rep:missing",
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
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

func TestHandler_EndpointCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, h *dms.Handler)
		name string
	}{
		{
			name: "create_success",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				rec := doDMS(t, h, "CreateEndpoint", map[string]any{
					"EndpointIdentifier": "src-ep",
					"EndpointType":       "SOURCE",
					"EngineName":         "mysql",
					"ServerName":         "db.example.com",
					"Port":               3306,
					"DatabaseName":       "mydb",
					"Username":           "admin",
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec)
				ep, ok := resp["Endpoint"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "src-ep", ep["EndpointIdentifier"])
				assert.Equal(t, "SOURCE", ep["EndpointType"])
				assert.Equal(t, "mysql", ep["EngineName"])
				assert.Equal(t, "active", ep["Status"])
				assert.NotEmpty(t, ep["EndpointArn"])
			},
		},
		{
			name: "create_duplicate_conflict",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				doDMS(t, h, "CreateEndpoint", map[string]any{
					"EndpointIdentifier": "dup-ep",
					"EndpointType":       "SOURCE",
					"EngineName":         "mysql",
				})
				rec := doDMS(t, h, "CreateEndpoint", map[string]any{
					"EndpointIdentifier": "dup-ep",
					"EndpointType":       "SOURCE",
					"EngineName":         "mysql",
				})
				assert.Equal(t, http.StatusConflict, rec.Code)
			},
		},
		{
			name: "describe_all",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				doDMS(t, h, "CreateEndpoint", map[string]any{
					"EndpointIdentifier": "ep-a",
					"EndpointType":       "SOURCE",
					"EngineName":         "postgres",
				})
				rec := doDMS(t, h, "DescribeEndpoints", map[string]any{})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec)
				list, ok := resp["Endpoints"].([]any)
				require.True(t, ok)
				assert.Len(t, list, 1)
			},
		},
		{
			name: "delete_by_arn",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				create := doDMS(t, h, "CreateEndpoint", map[string]any{
					"EndpointIdentifier": "del-ep",
					"EndpointType":       "TARGET",
					"EngineName":         "s3",
				})
				require.Equal(t, http.StatusOK, create.Code)
				createResp := parseJSON(t, create)
				ep := createResp["Endpoint"].(map[string]any)
				arn := ep["EndpointArn"].(string)

				rec := doDMS(t, h, "DeleteEndpoint", map[string]any{
					"EndpointArn": arn,
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				deleteResp := parseJSON(t, rec)
				delEp, ok := deleteResp["Endpoint"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "del-ep", delEp["EndpointIdentifier"])
			},
		},
		{
			name: "delete_not_found",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				rec := doDMS(t, h, "DeleteEndpoint", map[string]any{
					"EndpointArn": "arn:aws:dms:us-east-1:123:endpoint:nonexistent",
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "create_missing_identifier",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				rec := doDMS(t, h, "CreateEndpoint", map[string]any{
					"EndpointType": "SOURCE",
					"EngineName":   "mysql",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "create_missing_engine",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				rec := doDMS(t, h, "CreateEndpoint", map[string]any{
					"EndpointIdentifier": "ep-no-engine",
					"EndpointType":       "SOURCE",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "describe_by_arn_filter",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				create := doDMS(t, h, "CreateEndpoint", map[string]any{
					"EndpointIdentifier": "arn-ep",
					"EndpointType":       "SOURCE",
					"EngineName":         "mysql",
				})
				require.Equal(t, http.StatusOK, create.Code)
				createResp := parseJSON(t, create)
				ep := createResp["Endpoint"].(map[string]any)
				arnVal := ep["EndpointArn"].(string)

				rec := doDMS(t, h, "DescribeEndpoints", map[string]any{
					"Filters": []map[string]any{
						{"Name": "endpoint-arn", "Values": []string{arnVal}},
					},
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec)
				list, ok := resp["Endpoints"].([]any)
				require.True(t, ok)
				assert.Len(t, list, 1)
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

func TestHandler_ReplicationTaskCRUD(t *testing.T) {
	t.Parallel()

	// Helper: create a fully wired environment with an instance and two endpoints.
	createTaskEnv := func(t *testing.T, h *dms.Handler) (string, string, string) {
		t.Helper()

		instRec := doDMS(t, h, "CreateReplicationInstance", map[string]any{
			"ReplicationInstanceIdentifier": "task-inst",
			"ReplicationInstanceClass":      "dms.t3.medium",
		})
		require.Equal(t, http.StatusOK, instRec.Code)
		instResp := parseJSON(t, instRec)
		ri := instResp["ReplicationInstance"].(map[string]any)
		theInstArn := ri["ReplicationInstanceArn"].(string)

		srcRec := doDMS(t, h, "CreateEndpoint", map[string]any{
			"EndpointIdentifier": "task-src",
			"EndpointType":       "SOURCE",
			"EngineName":         "mysql",
		})
		require.Equal(t, http.StatusOK, srcRec.Code)
		srcResp := parseJSON(t, srcRec)
		theSrcArn := srcResp["Endpoint"].(map[string]any)["EndpointArn"].(string)

		dstRec := doDMS(t, h, "CreateEndpoint", map[string]any{
			"EndpointIdentifier": "task-dst",
			"EndpointType":       "TARGET",
			"EngineName":         "s3",
		})
		require.Equal(t, http.StatusOK, dstRec.Code)
		dstResp := parseJSON(t, dstRec)
		theDstArn := dstResp["Endpoint"].(map[string]any)["EndpointArn"].(string)

		return theSrcArn, theDstArn, theInstArn
	}

	tests := []struct {
		run  func(t *testing.T, h *dms.Handler)
		name string
	}{
		{
			name: "create_success",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				srcArn, dstArn, instArn := createTaskEnv(t, h)
				rec := doDMS(t, h, "CreateReplicationTask", map[string]any{
					"ReplicationTaskIdentifier": "my-task",
					"SourceEndpointArn":         srcArn,
					"TargetEndpointArn":         dstArn,
					"ReplicationInstanceArn":    instArn,
					"MigrationType":             "full-load",
					"TableMappings":             `{"rules":[]}`,
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec)
				rt, ok := resp["ReplicationTask"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "my-task", rt["ReplicationTaskIdentifier"])
				assert.Equal(t, "ready", rt["Status"])
				assert.NotEmpty(t, rt["ReplicationTaskArn"])
			},
		},
		{
			name: "create_duplicate_conflict",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				srcArn, dstArn, instArn := createTaskEnv(t, h)
				body := map[string]any{
					"ReplicationTaskIdentifier": "dup-task",
					"SourceEndpointArn":         srcArn,
					"TargetEndpointArn":         dstArn,
					"ReplicationInstanceArn":    instArn,
					"MigrationType":             "full-load",
				}
				doDMS(t, h, "CreateReplicationTask", body)
				rec := doDMS(t, h, "CreateReplicationTask", body)
				assert.Equal(t, http.StatusConflict, rec.Code)
			},
		},
		{
			name: "describe_all",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				srcArn, dstArn, instArn := createTaskEnv(t, h)
				doDMS(t, h, "CreateReplicationTask", map[string]any{
					"ReplicationTaskIdentifier": "list-task",
					"SourceEndpointArn":         srcArn,
					"TargetEndpointArn":         dstArn,
					"ReplicationInstanceArn":    instArn,
					"MigrationType":             "full-load",
				})
				rec := doDMS(t, h, "DescribeReplicationTasks", map[string]any{})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec)
				list, ok := resp["ReplicationTasks"].([]any)
				require.True(t, ok)
				assert.Len(t, list, 1)
			},
		},
		{
			name: "start_and_stop",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				srcArn, dstArn, instArn := createTaskEnv(t, h)
				create := doDMS(t, h, "CreateReplicationTask", map[string]any{
					"ReplicationTaskIdentifier": "ss-task",
					"SourceEndpointArn":         srcArn,
					"TargetEndpointArn":         dstArn,
					"ReplicationInstanceArn":    instArn,
					"MigrationType":             "full-load",
				})
				require.Equal(t, http.StatusOK, create.Code)
				createResp := parseJSON(t, create)
				taskArn := createResp["ReplicationTask"].(map[string]any)["ReplicationTaskArn"].(string)

				startRec := doDMS(t, h, "StartReplicationTask", map[string]any{
					"ReplicationTaskArn":       taskArn,
					"StartReplicationTaskType": "start-replication",
				})
				assert.Equal(t, http.StatusOK, startRec.Code)
				startResp := parseJSON(t, startRec)
				rt := startResp["ReplicationTask"].(map[string]any)
				assert.Equal(t, "running", rt["Status"])

				stopRec := doDMS(t, h, "StopReplicationTask", map[string]any{
					"ReplicationTaskArn": taskArn,
				})
				assert.Equal(t, http.StatusOK, stopRec.Code)
				stopResp := parseJSON(t, stopRec)
				rtStop := stopResp["ReplicationTask"].(map[string]any)
				assert.Equal(t, "stopped", rtStop["Status"])
			},
		},
		{
			name: "delete_success",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				srcArn, dstArn, instArn := createTaskEnv(t, h)
				create := doDMS(t, h, "CreateReplicationTask", map[string]any{
					"ReplicationTaskIdentifier": "del-task",
					"SourceEndpointArn":         srcArn,
					"TargetEndpointArn":         dstArn,
					"ReplicationInstanceArn":    instArn,
					"MigrationType":             "full-load",
				})
				require.Equal(t, http.StatusOK, create.Code)
				createResp := parseJSON(t, create)
				taskArn := createResp["ReplicationTask"].(map[string]any)["ReplicationTaskArn"].(string)

				rec := doDMS(t, h, "DeleteReplicationTask", map[string]any{
					"ReplicationTaskArn": taskArn,
				})
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
		{
			name: "create_missing_identifier",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				srcArn, dstArn, instArn := createTaskEnv(t, h)
				rec := doDMS(t, h, "CreateReplicationTask", map[string]any{
					"SourceEndpointArn":      srcArn,
					"TargetEndpointArn":      dstArn,
					"ReplicationInstanceArn": instArn,
					"MigrationType":          "full-load",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "create_missing_migration_type",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				srcArn, dstArn, instArn := createTaskEnv(t, h)
				rec := doDMS(t, h, "CreateReplicationTask", map[string]any{
					"ReplicationTaskIdentifier": "no-type-task",
					"SourceEndpointArn":         srcArn,
					"TargetEndpointArn":         dstArn,
					"ReplicationInstanceArn":    instArn,
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "stop_not_found",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				rec := doDMS(t, h, "StopReplicationTask", map[string]any{
					"ReplicationTaskArn": "arn:aws:dms:us-east-1:000000000000:task:nonexistent",
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "delete_not_found",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				rec := doDMS(t, h, "DeleteReplicationTask", map[string]any{
					"ReplicationTaskArn": "arn:aws:dms:us-east-1:000000000000:task:nonexistent",
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
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

func TestHandler_Tags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, h *dms.Handler)
		name string
	}{
		{
			name: "add_and_list_tags_on_replication_instance",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				create := doDMS(t, h, "CreateReplicationInstance", map[string]any{
					"ReplicationInstanceIdentifier": "tag-inst",
					"ReplicationInstanceClass":      "dms.t3.medium",
				})
				require.Equal(t, http.StatusOK, create.Code)
				createResp := parseJSON(t, create)
				arn := createResp["ReplicationInstance"].(map[string]any)["ReplicationInstanceArn"].(string)

				addRec := doDMS(t, h, "AddTagsToResource", map[string]any{
					"ResourceArn": arn,
					"Tags": []map[string]string{
						{"Key": "Project", "Value": "MyProject"},
					},
				})
				assert.Equal(t, http.StatusOK, addRec.Code)

				listRec := doDMS(t, h, "ListTagsForResource", map[string]any{
					"ResourceArn": arn,
				})
				assert.Equal(t, http.StatusOK, listRec.Code)
				listResp := parseJSON(t, listRec)
				tags, ok := listResp["TagList"].([]any)
				require.True(t, ok)
				require.Len(t, tags, 1)
				tag := tags[0].(map[string]any)
				assert.Equal(t, "Project", tag["Key"])
				assert.Equal(t, "MyProject", tag["Value"])
			},
		},
		{
			name: "list_tags_not_found",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				rec := doDMS(t, h, "ListTagsForResource", map[string]any{
					"ResourceArn": "arn:aws:dms:us-east-1:123:rep:nonexistent",
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
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

func TestHandler_TagsOnEndpointAndTask(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, h *dms.Handler)
		name string
	}{
		{
			name: "tags_on_endpoint",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				create := doDMS(t, h, "CreateEndpoint", map[string]any{
					"EndpointIdentifier": "tagged-ep",
					"EndpointType":       "SOURCE",
					"EngineName":         "mysql",
				})
				require.Equal(t, http.StatusOK, create.Code)
				createResp := parseJSON(t, create)
				arn := createResp["Endpoint"].(map[string]any)["EndpointArn"].(string)

				addRec := doDMS(t, h, "AddTagsToResource", map[string]any{
					"ResourceArn": arn,
					"Tags":        []map[string]string{{"Key": "Owner", "Value": "team"}},
				})
				assert.Equal(t, http.StatusOK, addRec.Code)

				listRec := doDMS(t, h, "ListTagsForResource", map[string]any{
					"ResourceArn": arn,
				})
				assert.Equal(t, http.StatusOK, listRec.Code)
				listResp := parseJSON(t, listRec)
				tags := listResp["TagList"].([]any)
				require.Len(t, tags, 1)
				assert.Equal(t, "Owner", tags[0].(map[string]any)["Key"])
			},
		},
		{
			name: "tags_on_task",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				instRec := doDMS(t, h, "CreateReplicationInstance", map[string]any{
					"ReplicationInstanceIdentifier": "tag-task-inst",
					"ReplicationInstanceClass":      "dms.t3.medium",
				})
				require.Equal(t, http.StatusOK, instRec.Code)
				instArn := parseJSON(t, instRec)["ReplicationInstance"].(map[string]any)["ReplicationInstanceArn"].(string)

				srcRec := doDMS(t, h, "CreateEndpoint", map[string]any{
					"EndpointIdentifier": "tag-task-src",
					"EndpointType":       "SOURCE",
					"EngineName":         "mysql",
				})
				require.Equal(t, http.StatusOK, srcRec.Code)
				srcArn := parseJSON(t, srcRec)["Endpoint"].(map[string]any)["EndpointArn"].(string)

				dstRec := doDMS(t, h, "CreateEndpoint", map[string]any{
					"EndpointIdentifier": "tag-task-dst",
					"EndpointType":       "TARGET",
					"EngineName":         "s3",
				})
				require.Equal(t, http.StatusOK, dstRec.Code)
				dstArn := parseJSON(t, dstRec)["Endpoint"].(map[string]any)["EndpointArn"].(string)

				taskRec := doDMS(t, h, "CreateReplicationTask", map[string]any{
					"ReplicationTaskIdentifier": "tagged-task",
					"SourceEndpointArn":         srcArn,
					"TargetEndpointArn":         dstArn,
					"ReplicationInstanceArn":    instArn,
					"MigrationType":             "full-load",
				})
				require.Equal(t, http.StatusOK, taskRec.Code)
				taskArn := parseJSON(t, taskRec)["ReplicationTask"].(map[string]any)["ReplicationTaskArn"].(string)

				addRec := doDMS(t, h, "AddTagsToResource", map[string]any{
					"ResourceArn": taskArn,
					"Tags":        []map[string]string{{"Key": "Stage", "Value": "prod"}},
				})
				assert.Equal(t, http.StatusOK, addRec.Code)

				listRec := doDMS(t, h, "ListTagsForResource", map[string]any{
					"ResourceArn": taskArn,
				})
				assert.Equal(t, http.StatusOK, listRec.Code)
				listResp := parseJSON(t, listRec)
				tags := listResp["TagList"].([]any)
				require.Len(t, tags, 1)
				assert.Equal(t, "Stage", tags[0].(map[string]any)["Key"])
			},
		},
		{
			name: "add_tags_not_found",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				rec := doDMS(t, h, "AddTagsToResource", map[string]any{
					"ResourceArn": "arn:aws:dms:us-east-1:123:rep:nonexistent",
					"Tags":        []map[string]string{{"Key": "K", "Value": "V"}},
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
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

func TestHandler_DescribeEndpointsByFilter(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	doDMS(t, h, "CreateEndpoint", map[string]any{
		"EndpointIdentifier": "ep-filter-1",
		"EndpointType":       "SOURCE",
		"EngineName":         "mysql",
	})
	doDMS(t, h, "CreateEndpoint", map[string]any{
		"EndpointIdentifier": "ep-filter-2",
		"EndpointType":       "TARGET",
		"EngineName":         "s3",
	})

	rec := doDMS(t, h, "DescribeEndpoints", map[string]any{
		"Filters": []map[string]any{
			{"Name": "endpoint-id", "Values": []string{"ep-filter-1"}},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	resp := parseJSON(t, rec)
	list := resp["Endpoints"].([]any)
	assert.Len(t, list, 1)
	assert.Equal(t, "ep-filter-1", list[0].(map[string]any)["EndpointIdentifier"])
}

func TestHandler_DescribeTasksByArn(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()

	instRec := doDMS(t, h, "CreateReplicationInstance", map[string]any{
		"ReplicationInstanceIdentifier": "arn-filter-inst",
		"ReplicationInstanceClass":      "dms.t3.medium",
	})
	require.Equal(t, http.StatusOK, instRec.Code)
	instArn := parseJSON(t, instRec)["ReplicationInstance"].(map[string]any)["ReplicationInstanceArn"].(string)

	srcRec := doDMS(t, h, "CreateEndpoint", map[string]any{
		"EndpointIdentifier": "arn-src",
		"EndpointType":       "SOURCE",
		"EngineName":         "mysql",
	})
	require.Equal(t, http.StatusOK, srcRec.Code)
	srcArn := parseJSON(t, srcRec)["Endpoint"].(map[string]any)["EndpointArn"].(string)

	dstRec := doDMS(t, h, "CreateEndpoint", map[string]any{
		"EndpointIdentifier": "arn-dst",
		"EndpointType":       "TARGET",
		"EngineName":         "s3",
	})
	require.Equal(t, http.StatusOK, dstRec.Code)
	dstArn := parseJSON(t, dstRec)["Endpoint"].(map[string]any)["EndpointArn"].(string)

	taskRec := doDMS(t, h, "CreateReplicationTask", map[string]any{
		"ReplicationTaskIdentifier": "arn-filter-task",
		"SourceEndpointArn":         srcArn,
		"TargetEndpointArn":         dstArn,
		"ReplicationInstanceArn":    instArn,
		"MigrationType":             "full-load",
	})
	require.Equal(t, http.StatusOK, taskRec.Code)
	taskArn := parseJSON(t, taskRec)["ReplicationTask"].(map[string]any)["ReplicationTaskArn"].(string)

	// Filter by ARN.
	rec := doDMS(t, h, "DescribeReplicationTasks", map[string]any{
		"Filters": []map[string]any{
			{"Name": "replication-task-arn", "Values": []string{taskArn}},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	resp := parseJSON(t, rec)
	list := resp["ReplicationTasks"].([]any)
	assert.Len(t, list, 1)
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
		"EndpointType":       "SOURCE",
		"EngineName":         "mysql",
	})
	require.Equal(t, http.StatusOK, srcRec.Code)
	srcArn := parseJSON(t, srcRec)["Endpoint"].(map[string]any)["EndpointArn"].(string)

	dstRec := doDMS(t, h, "CreateEndpoint", map[string]any{
		"EndpointIdentifier": "state-dst",
		"EndpointType":       "TARGET",
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

// TestDescribeReplicationInstancesPagination verifies Marker/MaxRecords pagination.
func TestDescribeReplicationInstancesPagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *dms.Handler)
		name       string
		maxRecords int
		wantCount  int
		wantMarker bool
	}{
		{
			name: "first_page_limited",
			setup: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				for _, id := range []string{"inst-a", "inst-b", "inst-c"} {
					doDMS(t, h, "CreateReplicationInstance", map[string]any{
						"ReplicationInstanceIdentifier": id,
						"ReplicationInstanceClass":      "dms.t3.medium",
					})
				}
			},
			maxRecords: 2,
			wantCount:  2,
			wantMarker: true,
		},
		{
			name: "all_results_no_marker",
			setup: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				for _, id := range []string{"inst-x", "inst-y"} {
					doDMS(t, h, "CreateReplicationInstance", map[string]any{
						"ReplicationInstanceIdentifier": id,
						"ReplicationInstanceClass":      "dms.t3.medium",
					})
				}
			},
			maxRecords: 100,
			wantCount:  2,
			wantMarker: false,
		},
		{
			name: "zero_max_records_uses_default",
			setup: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				for _, id := range []string{"inst-p", "inst-q"} {
					doDMS(t, h, "CreateReplicationInstance", map[string]any{
						"ReplicationInstanceIdentifier": id,
						"ReplicationInstanceClass":      "dms.t3.medium",
					})
				}
			},
			maxRecords: 0,
			wantCount:  2,
			wantMarker: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestDMSHandler()

			if tt.setup != nil {
				tt.setup(t, h)
			}

			body := map[string]any{}
			if tt.maxRecords > 0 {
				body["MaxRecords"] = tt.maxRecords
			}

			rec := doDMS(t, h, "DescribeReplicationInstances", body)
			require.Equal(t, http.StatusOK, rec.Code)

			resp := parseJSON(t, rec)
			list, ok := resp["ReplicationInstances"].([]any)
			require.True(t, ok)
			assert.Len(t, list, tt.wantCount)

			_, hasMarker := resp["Marker"]
			assert.Equal(t, tt.wantMarker, hasMarker)
		})
	}
}

// TestDescribeReplicationInstancesContinuation verifies a two-page traversal.
func TestDescribeReplicationInstancesContinuation(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()

	for _, id := range []string{"inst-a", "inst-b", "inst-c"} {
		doDMS(t, h, "CreateReplicationInstance", map[string]any{
			"ReplicationInstanceIdentifier": id,
			"ReplicationInstanceClass":      "dms.t3.medium",
		})
	}

	// First page: 2 of 3.
	rec1 := doDMS(t, h, "DescribeReplicationInstances", map[string]any{"MaxRecords": 2})
	require.Equal(t, http.StatusOK, rec1.Code)
	resp1 := parseJSON(t, rec1)
	page1, ok := resp1["ReplicationInstances"].([]any)
	require.True(t, ok)
	assert.Len(t, page1, 2)

	marker, hasMarker := resp1["Marker"].(string)
	require.True(t, hasMarker, "expected Marker in first page response")
	require.NotEmpty(t, marker)

	// Second page: remaining 1.
	rec2 := doDMS(t, h, "DescribeReplicationInstances", map[string]any{
		"MaxRecords": 2,
		"Marker":     marker,
	})
	require.Equal(t, http.StatusOK, rec2.Code)
	resp2 := parseJSON(t, rec2)
	page2, ok := resp2["ReplicationInstances"].([]any)
	require.True(t, ok)
	assert.Len(t, page2, 1)

	_, stillHasMarker := resp2["Marker"]
	assert.False(t, stillHasMarker, "last page should have no Marker")

	// All identifiers collectively.
	ids := make([]string, 0, 3)
	for _, item := range append(page1, page2...) {
		ri := item.(map[string]any)
		ids = append(ids, ri["ReplicationInstanceIdentifier"].(string))
	}
	assert.ElementsMatch(t, []string{"inst-a", "inst-b", "inst-c"}, ids)
}

// TestDescribeEndpointsPagination verifies Marker/MaxRecords pagination.
func TestDescribeEndpointsPagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		count      int
		maxRecords int
		wantCount  int
		wantMarker bool
	}{
		{
			name:       "first_page_limited",
			count:      3,
			maxRecords: 2,
			wantCount:  2,
			wantMarker: true,
		},
		{
			name:       "all_results_no_marker",
			count:      2,
			maxRecords: 10,
			wantCount:  2,
			wantMarker: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestDMSHandler()

			for i := range tt.count {
				doDMS(t, h, "CreateEndpoint", map[string]any{
					"EndpointIdentifier": "ep-" + strconv.Itoa(i),
					"EndpointType":       "SOURCE",
					"EngineName":         "mysql",
				})
			}

			rec := doDMS(t, h, "DescribeEndpoints", map[string]any{"MaxRecords": tt.maxRecords})
			require.Equal(t, http.StatusOK, rec.Code)

			resp := parseJSON(t, rec)
			list, ok := resp["Endpoints"].([]any)
			require.True(t, ok)
			assert.Len(t, list, tt.wantCount)

			_, hasMarker := resp["Marker"]
			assert.Equal(t, tt.wantMarker, hasMarker)
		})
	}
}

// TestDescribeEndpointsContinuation verifies a two-page traversal.
func TestDescribeEndpointsContinuation(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()

	for i := range 3 {
		doDMS(t, h, "CreateEndpoint", map[string]any{
			"EndpointIdentifier": "ep-" + strconv.Itoa(i),
			"EndpointType":       "SOURCE",
			"EngineName":         "mysql",
		})
	}

	rec1 := doDMS(t, h, "DescribeEndpoints", map[string]any{"MaxRecords": 2})
	require.Equal(t, http.StatusOK, rec1.Code)
	resp1 := parseJSON(t, rec1)
	page1, ok := resp1["Endpoints"].([]any)
	require.True(t, ok)
	assert.Len(t, page1, 2)

	marker, hasMarker := resp1["Marker"].(string)
	require.True(t, hasMarker)
	require.NotEmpty(t, marker)

	rec2 := doDMS(t, h, "DescribeEndpoints", map[string]any{
		"MaxRecords": 2,
		"Marker":     marker,
	})
	require.Equal(t, http.StatusOK, rec2.Code)
	resp2 := parseJSON(t, rec2)
	page2, ok := resp2["Endpoints"].([]any)
	require.True(t, ok)
	assert.Len(t, page2, 1)

	_, stillHasMarker := resp2["Marker"]
	assert.False(t, stillHasMarker)
}

// TestDescribeReplicationTasksPagination verifies Marker/MaxRecords pagination.
func TestDescribeReplicationTasksPagination(t *testing.T) {
	t.Parallel()

	// Helper to create the prerequisite replication instance and endpoints.
	setupTaskEnv := func(t *testing.T, h *dms.Handler, n int) {
		t.Helper()

		instRec := doDMS(t, h, "CreateReplicationInstance", map[string]any{
			"ReplicationInstanceIdentifier": "pg-inst",
			"ReplicationInstanceClass":      "dms.t3.medium",
		})
		require.Equal(t, http.StatusOK, instRec.Code)
		instArn := parseJSON(t, instRec)["ReplicationInstance"].(map[string]any)["ReplicationInstanceArn"].(string)

		srcRec := doDMS(t, h, "CreateEndpoint", map[string]any{
			"EndpointIdentifier": "pg-src",
			"EndpointType":       "SOURCE",
			"EngineName":         "mysql",
		})
		require.Equal(t, http.StatusOK, srcRec.Code)
		srcArn := parseJSON(t, srcRec)["Endpoint"].(map[string]any)["EndpointArn"].(string)

		dstRec := doDMS(t, h, "CreateEndpoint", map[string]any{
			"EndpointIdentifier": "pg-dst",
			"EndpointType":       "TARGET",
			"EngineName":         "s3",
		})
		require.Equal(t, http.StatusOK, dstRec.Code)
		dstArn := parseJSON(t, dstRec)["Endpoint"].(map[string]any)["EndpointArn"].(string)

		for i := range n {
			doDMS(t, h, "CreateReplicationTask", map[string]any{
				"ReplicationTaskIdentifier": "task-" + strconv.Itoa(i),
				"SourceEndpointArn":         srcArn,
				"TargetEndpointArn":         dstArn,
				"ReplicationInstanceArn":    instArn,
				"MigrationType":             "full-load",
			})
		}
	}

	tests := []struct {
		name       string
		count      int
		maxRecords int
		wantCount  int
		wantMarker bool
	}{
		{
			name:       "first_page_limited",
			count:      3,
			maxRecords: 2,
			wantCount:  2,
			wantMarker: true,
		},
		{
			name:       "all_results_no_marker",
			count:      2,
			maxRecords: 10,
			wantCount:  2,
			wantMarker: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestDMSHandler()
			setupTaskEnv(t, h, tt.count)

			rec := doDMS(t, h, "DescribeReplicationTasks", map[string]any{"MaxRecords": tt.maxRecords})
			require.Equal(t, http.StatusOK, rec.Code)

			resp := parseJSON(t, rec)
			list, ok := resp["ReplicationTasks"].([]any)
			require.True(t, ok)
			assert.Len(t, list, tt.wantCount)

			_, hasMarker := resp["Marker"]
			assert.Equal(t, tt.wantMarker, hasMarker)
		})
	}
}

// TestDescribeReplicationTasksContinuation verifies a two-page traversal.
func TestDescribeReplicationTasksContinuation(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()

	instRec := doDMS(t, h, "CreateReplicationInstance", map[string]any{
		"ReplicationInstanceIdentifier": "cont-inst",
		"ReplicationInstanceClass":      "dms.t3.medium",
	})
	require.Equal(t, http.StatusOK, instRec.Code)
	instArn := parseJSON(t, instRec)["ReplicationInstance"].(map[string]any)["ReplicationInstanceArn"].(string)

	srcRec := doDMS(t, h, "CreateEndpoint", map[string]any{
		"EndpointIdentifier": "cont-src",
		"EndpointType":       "SOURCE",
		"EngineName":         "mysql",
	})
	require.Equal(t, http.StatusOK, srcRec.Code)
	srcArn := parseJSON(t, srcRec)["Endpoint"].(map[string]any)["EndpointArn"].(string)

	dstRec := doDMS(t, h, "CreateEndpoint", map[string]any{
		"EndpointIdentifier": "cont-dst",
		"EndpointType":       "TARGET",
		"EngineName":         "s3",
	})
	require.Equal(t, http.StatusOK, dstRec.Code)
	dstArn := parseJSON(t, dstRec)["Endpoint"].(map[string]any)["EndpointArn"].(string)

	for i := range 3 {
		doDMS(t, h, "CreateReplicationTask", map[string]any{
			"ReplicationTaskIdentifier": "task-" + strconv.Itoa(i),
			"SourceEndpointArn":         srcArn,
			"TargetEndpointArn":         dstArn,
			"ReplicationInstanceArn":    instArn,
			"MigrationType":             "full-load",
		})
	}

	rec1 := doDMS(t, h, "DescribeReplicationTasks", map[string]any{"MaxRecords": 2})
	require.Equal(t, http.StatusOK, rec1.Code)
	resp1 := parseJSON(t, rec1)
	page1, ok := resp1["ReplicationTasks"].([]any)
	require.True(t, ok)
	assert.Len(t, page1, 2)

	marker, hasMarker := resp1["Marker"].(string)
	require.True(t, hasMarker)
	require.NotEmpty(t, marker)

	rec2 := doDMS(t, h, "DescribeReplicationTasks", map[string]any{
		"MaxRecords": 2,
		"Marker":     marker,
	})
	require.Equal(t, http.StatusOK, rec2.Code)
	resp2 := parseJSON(t, rec2)
	page2, ok := resp2["ReplicationTasks"].([]any)
	require.True(t, ok)
	assert.Len(t, page2, 1)

	_, stillHasMarker := resp2["Marker"]
	assert.False(t, stillHasMarker)
}
