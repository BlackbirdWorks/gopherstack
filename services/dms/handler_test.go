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
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec)
				list, ok := resp["ReplicationInstances"].([]any)
				require.True(t, ok)
				assert.Empty(t, list)
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

// --- Tests for 10 new operations ---

func TestHandler_ApplyPendingMaintenanceAction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, h *dms.Handler)
		name string
	}{
		{
			name: "success",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				create := doDMS(t, h, "CreateReplicationInstance", map[string]any{
					"ReplicationInstanceIdentifier": "maint-inst",
					"ReplicationInstanceClass":      "dms.t3.medium",
				})
				require.Equal(t, http.StatusOK, create.Code)
				arn := parseJSON(t, create)["ReplicationInstance"].(map[string]any)["ReplicationInstanceArn"].(string)

				rec := doDMS(t, h, "ApplyPendingMaintenanceAction", map[string]any{
					"ReplicationInstanceArn": arn,
					"ApplyAction":            "os-upgrade",
					"OptInType":              "immediate",
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec)
				rp, ok := resp["ResourcePendingMaintenanceActions"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, arn, rp["ResourceIdentifier"])
			},
		},
		{
			name: "missing_arn",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				rec := doDMS(t, h, "ApplyPendingMaintenanceAction", map[string]any{
					"ApplyAction": "os-upgrade",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "not_found",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				rec := doDMS(t, h, "ApplyPendingMaintenanceAction", map[string]any{
					"ReplicationInstanceArn": "arn:aws:dms:us-east-1:123:rep:nonexistent",
					"ApplyAction":            "os-upgrade",
					"OptInType":              "immediate",
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

func TestHandler_BatchStartRecommendations(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	rec := doDMS(t, h, "BatchStartRecommendations", map[string]any{
		"Data": []any{},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	resp := parseJSON(t, rec)
	entries, ok := resp["ErrorEntries"].([]any)
	require.True(t, ok)
	assert.Empty(t, entries)
}

func TestHandler_CancelMetadataModelConversion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    map[string]any
		name     string
		wantCode int
	}{
		{
			name:     "success",
			wantCode: http.StatusOK,
			input: map[string]any{
				"MigrationProjectIdentifier": "proj-1",
				"RequestIdentifier":          "req-abc",
			},
		},
		{
			name:     "missing_project_identifier",
			wantCode: http.StatusBadRequest,
			input: map[string]any{
				"RequestIdentifier": "req-abc",
			},
		},
		{
			name:     "missing_request_identifier",
			wantCode: http.StatusBadRequest,
			input: map[string]any{
				"MigrationProjectIdentifier": "proj-1",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestDMSHandler()
			rec := doDMS(t, h, "CancelMetadataModelConversion", tt.input)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				resp := parseJSON(t, rec)
				assert.Equal(t, "req-abc", resp["RequestIdentifier"])
			}
		})
	}
}

func TestHandler_CancelMetadataModelCreation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    map[string]any
		name     string
		wantCode int
	}{
		{
			name:     "success",
			wantCode: http.StatusOK,
			input: map[string]any{
				"MigrationProjectIdentifier": "proj-1",
				"RequestIdentifier":          "req-xyz",
			},
		},
		{
			name:     "missing_project",
			wantCode: http.StatusBadRequest,
			input: map[string]any{
				"RequestIdentifier": "req-xyz",
			},
		},
		{
			name:     "missing_request_identifier",
			wantCode: http.StatusBadRequest,
			input: map[string]any{
				"MigrationProjectIdentifier": "proj-1",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestDMSHandler()
			rec := doDMS(t, h, "CancelMetadataModelCreation", tt.input)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				resp := parseJSON(t, rec)
				assert.Equal(t, "req-xyz", resp["RequestIdentifier"])
			}
		})
	}
}

func TestHandler_CancelReplicationTaskAssessmentRun(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    map[string]any
		name     string
		wantCode int
	}{
		{
			name:     "missing_arn",
			wantCode: http.StatusBadRequest,
			input:    map[string]any{},
		},
		{
			name:     "not_found",
			wantCode: http.StatusNotFound,
			input: map[string]any{
				"ReplicationTaskAssessmentRunArn": "arn:aws:dms:us-east-1:123:assessment-run:nonexistent",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestDMSHandler()
			rec := doDMS(t, h, "CancelReplicationTaskAssessmentRun", tt.input)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_CreateDataMigration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, h *dms.Handler)
		name string
	}{
		{
			name: "create_success",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				rec := doDMS(t, h, "CreateDataMigration", map[string]any{
					"DataMigrationName":          "my-migration",
					"MigrationProjectIdentifier": "proj-1",
					"DataMigrationType":          "full-load",
					"ServiceAccessRoleArn":       "arn:aws:iam::123456789012:role/dms-role",
					"NumberOfJobs":               2,
					"EnableCloudwatchLogs":       true,
					"Tags": []map[string]string{
						{"Key": "Env", "Value": "test"},
					},
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec)
				dm, ok := resp["DataMigration"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "my-migration", dm["DataMigrationName"])
				assert.Equal(t, "full-load", dm["DataMigrationType"])
				assert.Equal(t, "ready", dm["DataMigrationStatus"])
				assert.NotEmpty(t, dm["DataMigrationArn"])
			},
		},
		{
			name: "create_duplicate_conflict",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				doDMS(t, h, "CreateDataMigration", map[string]any{
					"DataMigrationName": "dup-migration",
					"DataMigrationType": "full-load",
				})
				rec := doDMS(t, h, "CreateDataMigration", map[string]any{
					"DataMigrationName": "dup-migration",
					"DataMigrationType": "full-load",
				})
				assert.Equal(t, http.StatusConflict, rec.Code)
			},
		},
		{
			name: "missing_name",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				rec := doDMS(t, h, "CreateDataMigration", map[string]any{
					"DataMigrationType": "full-load",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "missing_type",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				rec := doDMS(t, h, "CreateDataMigration", map[string]any{
					"DataMigrationName": "no-type-migration",
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

func TestHandler_CreateDataProvider(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, h *dms.Handler)
		name string
	}{
		{
			name: "create_success",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				rec := doDMS(t, h, "CreateDataProvider", map[string]any{
					"DataProviderName": "my-provider",
					"Engine":           "mysql",
					"Description":      "My MySQL provider",
					"Tags": []map[string]string{
						{"Key": "Team", "Value": "infra"},
					},
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec)
				dp, ok := resp["DataProvider"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "my-provider", dp["DataProviderName"])
				assert.Equal(t, "mysql", dp["Engine"])
				assert.Equal(t, "My MySQL provider", dp["Description"])
				assert.NotEmpty(t, dp["DataProviderArn"])
			},
		},
		{
			name: "create_duplicate_conflict",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				doDMS(t, h, "CreateDataProvider", map[string]any{
					"DataProviderName": "dup-provider",
					"Engine":           "postgres",
				})
				rec := doDMS(t, h, "CreateDataProvider", map[string]any{
					"DataProviderName": "dup-provider",
					"Engine":           "postgres",
				})
				assert.Equal(t, http.StatusConflict, rec.Code)
			},
		},
		{
			name: "missing_name",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				rec := doDMS(t, h, "CreateDataProvider", map[string]any{
					"Engine": "mysql",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "missing_engine",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				rec := doDMS(t, h, "CreateDataProvider", map[string]any{
					"DataProviderName": "no-engine",
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

func TestHandler_CreateEventSubscription(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, h *dms.Handler)
		name string
	}{
		{
			name: "create_success",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				rec := doDMS(t, h, "CreateEventSubscription", map[string]any{
					"SubscriptionName": "my-sub",
					"SnsTopicArn":      "arn:aws:sns:us-east-1:123:my-topic",
					"SourceType":       "replication-instance",
					"EventCategories":  []string{"creation", "deletion"},
					"Enabled":          true,
					"Tags": []map[string]string{
						{"Key": "Env", "Value": "prod"},
					},
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec)
				es, ok := resp["EventSubscription"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "my-sub", es["SubscriptionName"])
				assert.Equal(t, "arn:aws:sns:us-east-1:123:my-topic", es["SnsTopicArn"])
				assert.Equal(t, "active", es["Status"])
				assert.Equal(t, true, es["Enabled"])
			},
		},
		{
			name: "create_duplicate_conflict",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				doDMS(t, h, "CreateEventSubscription", map[string]any{
					"SubscriptionName": "dup-sub",
					"SnsTopicArn":      "arn:aws:sns:us-east-1:123:topic",
				})
				rec := doDMS(t, h, "CreateEventSubscription", map[string]any{
					"SubscriptionName": "dup-sub",
					"SnsTopicArn":      "arn:aws:sns:us-east-1:123:topic",
				})
				assert.Equal(t, http.StatusConflict, rec.Code)
			},
		},
		{
			name: "missing_subscription_name",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				rec := doDMS(t, h, "CreateEventSubscription", map[string]any{
					"SnsTopicArn": "arn:aws:sns:us-east-1:123:topic",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "missing_sns_topic_arn",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				rec := doDMS(t, h, "CreateEventSubscription", map[string]any{
					"SubscriptionName": "no-topic",
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

func TestHandler_CreateInstanceProfile(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, h *dms.Handler)
		name string
	}{
		{
			name: "create_success_named",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				rec := doDMS(t, h, "CreateInstanceProfile", map[string]any{
					"InstanceProfileName":   "my-profile",
					"AvailabilityZone":      "us-east-1a",
					"NetworkType":           "IPV4",
					"PubliclyAccessible":    false,
					"Description":           "Test profile",
					"SubnetGroupIdentifier": "subnet-group-1",
					"Tags": []map[string]string{
						{"Key": "Env", "Value": "staging"},
					},
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec)
				ip, ok := resp["InstanceProfile"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "my-profile", ip["InstanceProfileName"])
				assert.Equal(t, "us-east-1a", ip["AvailabilityZone"])
				assert.NotEmpty(t, ip["InstanceProfileArn"])
			},
		},
		{
			name: "create_success_no_name",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				rec := doDMS(t, h, "CreateInstanceProfile", map[string]any{
					"AvailabilityZone": "us-west-2a",
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec)
				ip, ok := resp["InstanceProfile"].(map[string]any)
				require.True(t, ok)
				assert.NotEmpty(t, ip["InstanceProfileArn"])
			},
		},
		{
			name: "create_duplicate_conflict",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				doDMS(t, h, "CreateInstanceProfile", map[string]any{
					"InstanceProfileName": "dup-profile",
				})
				rec := doDMS(t, h, "CreateInstanceProfile", map[string]any{
					"InstanceProfileName": "dup-profile",
				})
				assert.Equal(t, http.StatusConflict, rec.Code)
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

func TestHandler_NewOps_GetSupportedOperations(t *testing.T) {
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

func TestHandler_TagsOnNewResources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, h *dms.Handler)
		name string
	}{
		{
			name: "tags_on_data_migration",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				create := doDMS(t, h, "CreateDataMigration", map[string]any{
					"DataMigrationName": "tag-dm",
					"DataMigrationType": "full-load",
					"Tags": []map[string]string{
						{"Key": "Phase", "Value": "alpha"},
					},
				})
				require.Equal(t, http.StatusOK, create.Code)
				dmArn := parseJSON(t, create)["DataMigration"].(map[string]any)["DataMigrationArn"].(string)

				listRec := doDMS(t, h, "ListTagsForResource", map[string]any{
					"ResourceArn": dmArn,
				})
				assert.Equal(t, http.StatusOK, listRec.Code)
				tags := parseJSON(t, listRec)["TagList"].([]any)
				require.Len(t, tags, 1)
				assert.Equal(t, "Phase", tags[0].(map[string]any)["Key"])
			},
		},
		{
			name: "tags_on_data_provider",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				create := doDMS(t, h, "CreateDataProvider", map[string]any{
					"DataProviderName": "tag-dp",
					"Engine":           "oracle",
					"Tags": []map[string]string{
						{"Key": "Owner", "Value": "dba"},
					},
				})
				require.Equal(t, http.StatusOK, create.Code)
				dpArn := parseJSON(t, create)["DataProvider"].(map[string]any)["DataProviderArn"].(string)

				listRec := doDMS(t, h, "ListTagsForResource", map[string]any{
					"ResourceArn": dpArn,
				})
				assert.Equal(t, http.StatusOK, listRec.Code)
				tags := parseJSON(t, listRec)["TagList"].([]any)
				require.Len(t, tags, 1)
				assert.Equal(t, "Owner", tags[0].(map[string]any)["Key"])
			},
		},
		{
			name: "tags_on_instance_profile",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				create := doDMS(t, h, "CreateInstanceProfile", map[string]any{
					"InstanceProfileName": "tag-ip",
					"Tags": []map[string]string{
						{"Key": "Tier", "Value": "prod"},
					},
				})
				require.Equal(t, http.StatusOK, create.Code)
				ipArn := parseJSON(t, create)["InstanceProfile"].(map[string]any)["InstanceProfileArn"].(string)

				addRec := doDMS(t, h, "AddTagsToResource", map[string]any{
					"ResourceArn": ipArn,
					"Tags": []map[string]string{
						{"Key": "Extra", "Value": "value"},
					},
				})
				assert.Equal(t, http.StatusOK, addRec.Code)

				listRec := doDMS(t, h, "ListTagsForResource", map[string]any{
					"ResourceArn": ipArn,
				})
				assert.Equal(t, http.StatusOK, listRec.Code)
				tags := parseJSON(t, listRec)["TagList"].([]any)
				assert.Len(t, tags, 2)
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

// --- Refinement tests ---

func TestRefinement1_Reset(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	h.Backend.AddReplicationInstanceInternal("inst1", "dms.t3.medium")
	require.Equal(t, 1, h.Backend.ReplicationInstanceCount())

	h.Backend.Reset()
	assert.Equal(t, 0, h.Backend.ReplicationInstanceCount())
}

func TestRefinement1_HandlerReset(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	h.Backend.AddEndpointInternal("ep1", "source", "mysql")
	require.Equal(t, 1, h.Backend.EndpointCount())

	h.Reset()
	assert.Equal(t, 0, h.Backend.EndpointCount())
}

func TestRefinement1_ProviderInit_NilCtx(t *testing.T) {
	t.Parallel()

	p := dms.Provider{}
	_, err := p.Init(nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, dms.ErrNilAppContext)
}

func TestRefinement1_HandlerOpsPreBuilt(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	rec := doDMS(t, h, "CreateReplicationInstance", map[string]any{
		"ReplicationInstanceIdentifier": "pre-built-inst",
		"ReplicationInstanceClass":      "dms.t3.medium",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestRefinement1_SortedListTags(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	create := doDMS(t, h, "CreateReplicationInstance", map[string]any{
		"ReplicationInstanceIdentifier": "tagtest",
		"ReplicationInstanceClass":      "dms.t3.medium",
		"Tags": []map[string]any{
			{"Key": "zebra", "Value": "z"},
			{"Key": "alpha", "Value": "a"},
			{"Key": "middle", "Value": "m"},
		},
	})
	require.Equal(t, http.StatusOK, create.Code)
	arnStr := parseJSON(t, create)["ReplicationInstance"].(map[string]any)["ReplicationInstanceArn"].(string)

	listRec := doDMS(t, h, "ListTagsForResource", map[string]any{
		"ResourceArn": arnStr,
	})
	require.Equal(t, http.StatusOK, listRec.Code)

	tagList := parseJSON(t, listRec)["TagList"].([]any)
	require.Len(t, tagList, 3)
	assert.Equal(t, "alpha", tagList[0].(map[string]any)["Key"])
	assert.Equal(t, "middle", tagList[1].(map[string]any)["Key"])
	assert.Equal(t, "zebra", tagList[2].(map[string]any)["Key"])
}

func TestRefinement1_NonNilEventSubscriptionSlices(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	rec := doDMS(t, h, "CreateEventSubscription", map[string]any{
		"SubscriptionName": "sub1",
		"SnsTopicArn":      "arn:aws:sns:us-east-1:123:topic1",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	body := parseJSON(t, rec)
	es := body["EventSubscription"].(map[string]any)
	require.NotNil(t, es["SourceIdsList"])
	require.NotNil(t, es["EventCategories"])
}

func TestRefinement1_DescribeReturnsEmptyListOnFilterMiss(t *testing.T) {
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

func TestRefinement1_DescribeEndpointsFilterMiss(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	rec := doDMS(t, h, "DescribeEndpoints", map[string]any{
		"Filters": []map[string]any{
			{"Name": "endpoint-id", "Values": []string{"nonexistent"}},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	body := parseJSON(t, rec)
	endpoints := body["Endpoints"].([]any)
	assert.Empty(t, endpoints)
}

func TestRefinement1_DescribeTasksFilterMiss(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	rec := doDMS(t, h, "DescribeReplicationTasks", map[string]any{
		"Filters": []map[string]any{
			{"Name": "replication-task-id", "Values": []string{"nonexistent"}},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	body := parseJSON(t, rec)
	tasks := body["ReplicationTasks"].([]any)
	assert.Empty(t, tasks)
}

func TestRefinement1_SeedHelpers(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	h.Backend.AddReplicationInstanceInternal("seed-inst", "dms.t3.medium")
	h.Backend.AddEndpointInternal("seed-ep", "source", "mysql")
	h.Backend.AddReplicationTaskInternal("seed-task", "src-arn", "tgt-arn", "inst-arn", "full-load")

	assert.Equal(t, 1, h.Backend.ReplicationInstanceCount())
	assert.Equal(t, 1, h.Backend.EndpointCount())
	assert.Equal(t, 1, h.Backend.ReplicationTaskCount())
}

func TestRefinement1_ExportCountHelpers(t *testing.T) {
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

func TestRefinement1_PersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	h.Backend.AddReplicationInstanceInternal("persist-inst", "dms.t3.medium")
	h.Backend.AddEndpointInternal("persist-ep", "source", "mysql")

	snap := h.Snapshot()
	require.NotNil(t, snap)

	h2 := newTestDMSHandler()
	require.NoError(t, h2.Restore(snap))

	assert.Equal(t, 1, h2.Backend.ReplicationInstanceCount())
	assert.Equal(t, 1, h2.Backend.EndpointCount())
}

func TestRefinement1_MigrationTypeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		typeStr    string
		wantStatus int
	}{
		{name: "valid_full_load", typeStr: "full-load", wantStatus: http.StatusOK},
		{name: "valid_cdc", typeStr: "cdc", wantStatus: http.StatusOK},
		{name: "valid_full_load_and_cdc", typeStr: "full-load-and-cdc", wantStatus: http.StatusOK},
		{name: "invalid_type", typeStr: "unknown-type", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestDMSHandler()
			rec := doDMS(t, h, "CreateDataMigration", map[string]any{
				"DataMigrationName": "dm-" + tt.typeStr,
				"DataMigrationType": tt.typeStr,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestRefinement1_NetworkTypeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		networkType string
		wantStatus  int
	}{
		{name: "empty_network_type", networkType: "", wantStatus: http.StatusOK},
		{name: "ipv4", networkType: "IPV4", wantStatus: http.StatusOK},
		{name: "ipv6", networkType: "IPV6", wantStatus: http.StatusOK},
		{name: "dual", networkType: "DUAL", wantStatus: http.StatusOK},
		{name: "invalid", networkType: "INVALID", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestDMSHandler()
			body := map[string]any{
				"InstanceProfileName": "prof-" + tt.networkType,
			}
			if tt.networkType != "" {
				body["NetworkType"] = tt.networkType
			}
			rec := doDMS(t, h, "CreateInstanceProfile", body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestRefinement1_ValidationMappedTo400(t *testing.T) {
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

func TestRefinement1_MultipleResetCycle(t *testing.T) {
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

func TestRefinement1_ARNIndexedTagOps(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	createRec := doDMS(t, h, "CreateReplicationInstance", map[string]any{
		"ReplicationInstanceIdentifier": "arn-tag-inst",
		"ReplicationInstanceClass":      "dms.t3.medium",
	})
	require.Equal(t, http.StatusOK, createRec.Code)
	instARN := parseJSON(t, createRec)["ReplicationInstance"].(map[string]any)["ReplicationInstanceArn"].(string)

	addRec := doDMS(t, h, "AddTagsToResource", map[string]any{
		"ResourceArn": instARN,
		"Tags":        []map[string]any{{"Key": "team", "Value": "platform"}},
	})
	assert.Equal(t, http.StatusOK, addRec.Code)

	listRec := doDMS(t, h, "ListTagsForResource", map[string]any{
		"ResourceArn": instARN,
	})
	require.Equal(t, http.StatusOK, listRec.Code)
	tagList := parseJSON(t, listRec)["TagList"].([]any)
	require.Len(t, tagList, 1)
	assert.Equal(t, "team", tagList[0].(map[string]any)["Key"])
}

func TestRefinement1_FleetAdvisorCollectorTagsOnReset(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	h.Backend.AddFleetAdvisorCollectorInternal("col-reset-test")

	// Should not panic on reset even with collector tags.
	assert.NotPanics(t, func() { h.Backend.Reset() })
	assert.Equal(t, 0, h.Backend.FleetAdvisorCollectorCount())
}

func TestRefinement1_DataMigrationSeedHelper(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	h.Backend.AddDataMigrationInternal("seed-migration", "full-load")
	assert.Equal(t, 1, h.Backend.DataMigrationCount())
}

func TestRefinement1_EventSubscriptionSeedHelper(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	h.Backend.AddEventSubscriptionInternal("seed-sub", "arn:aws:sns:us-east-1:123:topic")
	assert.Equal(t, 1, h.Backend.EventSubscriptionCount())
}

func TestRefinement1_InstanceProfileSeedHelper(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	h.Backend.AddInstanceProfileInternal("seed-profile")
	assert.Equal(t, 1, h.Backend.InstanceProfileCount())
}
