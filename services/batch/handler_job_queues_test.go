package batch_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/batch"
)

func TestHandler_JobQueue_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *batch.Handler)
		name       string
		wantStatus int
		wantARN    bool
	}{
		{
			name:       "create_success",
			wantStatus: http.StatusOK,
			wantARN:    true,
		},
		{
			name: "create_duplicate",
			setup: func(t *testing.T, h *batch.Handler) {
				t.Helper()
				rec := post(t, h, "/v1/createjobqueue", map[string]any{
					"jobQueueName": "test-jq",
					"priority":     10,
					"state":        "ENABLED",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := post(t, h, "/v1/createjobqueue", map[string]any{
				"jobQueueName": "test-jq",
				"priority":     10,
				"state":        "ENABLED",
			})

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantARN {
				var out map[string]string
				mustUnmarshal(t, rec, &out)
				assert.Contains(t, out["jobQueueArn"], "test-jq")
				assert.Equal(t, "test-jq", out["jobQueueName"])
			}
		})
	}
}

func TestHandler_DescribeJobQueues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		filter     []string
		wantCount  int
		wantStatus int
	}{
		{name: "describe_all", filter: nil, wantCount: 2, wantStatus: http.StatusOK},
		{name: "describe_one", filter: []string{"jq-1"}, wantCount: 1, wantStatus: http.StatusOK},
		{name: "describe_missing", filter: []string{"no-such-queue"}, wantCount: 0, wantStatus: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			for _, name := range []string{"jq-1", "jq-2"} {
				rec := post(t, h, "/v1/createjobqueue", map[string]any{
					"jobQueueName": name,
					"priority":     1,
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			body := map[string]any{}
			if tt.filter != nil {
				body["jobQueues"] = tt.filter
			}

			rec := post(t, h, "/v1/describejobqueues", body)

			require.Equal(t, tt.wantStatus, rec.Code)

			var out map[string]any
			mustUnmarshal(t, rec, &out)

			list, ok := out["jobQueues"].([]any)
			require.True(t, ok)
			assert.Len(t, list, tt.wantCount)
		})
	}
}

func TestHandler_UpdateJobQueue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		priority   *int32
		name       string
		jq         string
		state      string
		wantStatus int
	}{
		{
			name:       "update_state",
			jq:         "test-jq",
			state:      "DISABLED",
			wantStatus: http.StatusOK,
		},
		{
			name: "update_priority",
			jq:   "test-jq",
			priority: func() *int32 {
				v := int32(20)

				return &v
			}(),
			wantStatus: http.StatusOK,
		},
		{
			name:       "not_found",
			jq:         "no-such-queue",
			state:      "DISABLED",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := post(t, h, "/v1/createjobqueue", map[string]any{
				"jobQueueName": "test-jq",
				"priority":     10,
				"state":        "ENABLED",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			body := map[string]any{"jobQueue": tt.jq}
			if tt.state != "" {
				body["state"] = tt.state
			}

			if tt.priority != nil {
				body["priority"] = *tt.priority
			}

			rec = post(t, h, "/v1/updatejobqueue", body)

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_DeleteJobQueue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		jq         string
		wantStatus int
	}{
		{name: "delete_success", jq: "test-jq", wantStatus: http.StatusOK},
		{name: "delete_not_found", jq: "missing", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := post(t, h, "/v1/createjobqueue", map[string]any{
				"jobQueueName": "test-jq",
				"priority":     1,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			if tt.jq == "test-jq" {
				rec = post(t, h, "/v1/updatejobqueue", map[string]any{
					"jobQueue": "test-jq",
					"state":    "DISABLED",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec = post(t, h, "/v1/deletejobqueue", map[string]any{
				"jobQueue": tt.jq,
			})

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// --- Job Definition tests ---

func TestHandler_JobQueueWithComputeEnvironmentOrder(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := post(t, h, "/v1/createjobqueue", map[string]any{
		"jobQueueName": "ordered-jq",
		"priority":     5,
		"state":        "ENABLED",
		"computeEnvironmentOrder": []map[string]any{
			{"computeEnvironment": "ce-1", "order": 1},
			{"computeEnvironment": "ce-2", "order": 2},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = post(t, h, "/v1/describejobqueues", map[string]any{
		"jobQueues": []string{"ordered-jq"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	mustUnmarshal(t, rec, &out)
	jqs := out["jobQueues"].([]any)
	require.Len(t, jqs, 1)

	jq := jqs[0].(map[string]any)
	ceOrder := jq["computeEnvironmentOrder"].([]any)
	assert.Len(t, ceOrder, 2)
}

func TestHandler_JobQueueByARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := post(t, h, "/v1/createjobqueue", map[string]any{
		"jobQueueName": "arn-lookup-jq",
		"priority":     1,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]string
	mustUnmarshal(t, rec, &out)
	jqARN := out["jobQueueArn"]

	// Update by ARN.
	rec = post(t, h, "/v1/updatejobqueue", map[string]any{
		"jobQueue": jqARN,
		"state":    "DISABLED",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Delete by ARN.
	rec = post(t, h, "/v1/deletejobqueue", map[string]any{
		"jobQueue": jqARN,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_DeleteJobQueue_CleansUpJobs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create compute environment and job queue.
	rec := post(t, h, "/v1/createcomputeenvironment", map[string]any{
		"computeEnvironmentName": "env1",
		"type":                   "MANAGED",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = post(t, h, "/v1/createjobqueue", map[string]any{
		"jobQueueName": "q1",
		"priority":     1,
		"computeEnvironmentOrder": []map[string]any{
			{"order": 1, "computeEnvironment": "env1"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = post(t, h, "/v1/registerjobdefinition", map[string]any{
		"jobDefinitionName": "jd1",
		"type":              "container",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Submit a job.
	rec = post(t, h, "/v1/submitjob", map[string]any{
		"jobName":       "job1",
		"jobQueue":      "q1",
		"jobDefinition": "jd1",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var submitResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &submitResp))
	jobID := submitResp["jobId"].(string)

	// Delete the queue — should also clean up associated jobs.
	rec = post(t, h, "/v1/updatejobqueue", map[string]any{
		"jobQueue": "q1",
		"state":    "DISABLED",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = post(t, h, "/v1/deletejobqueue", map[string]any{
		"jobQueue": "q1",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// The job should no longer be found.
	rec = post(t, h, "/v1/describejobs", map[string]any{
		"jobs": []string{jobID},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	jobs := descResp["jobs"].([]any)
	assert.Empty(t, jobs, "jobs should have been cleaned up when queue was deleted")
}

func TestHandler_GetJobQueueSnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		queueName   string
		wantStatus  int
		createQueue bool
	}{
		{
			name:        "valid_queue",
			wantStatus:  http.StatusOK,
			createQueue: true,
		},
		{
			name:       "missing_queue",
			queueName:  "",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "not_found",
			queueName:  "nonexistent-queue",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			queueName := tt.queueName

			if tt.createQueue {
				ceName := "ce-snap-" + tt.name
				rec := post(t, h, "/v1/createcomputeenvironment", map[string]any{
					"computeEnvironmentName": ceName,
					"type":                   "MANAGED",
					"state":                  "ENABLED",
				})
				require.Equal(t, http.StatusOK, rec.Code)

				queueName = "q-snap-" + tt.name
				rec = post(t, h, "/v1/createjobqueue", map[string]any{
					"jobQueueName": queueName,
					"priority":     1,
					"state":        "ENABLED",
					"computeEnvironmentOrder": []map[string]any{
						{"computeEnvironment": ceName, "order": 1},
					},
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := post(t, h, "/v1/getjobqueuesnapshot", map[string]any{"jobQueue": queueName})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var out map[string]any
				mustUnmarshal(t, rec, &out)
				assert.Contains(t, out, "frontOfQueue")
			}
		})
	}
}

// TestDescribeJobQueues_TagsPresentNoTags verifies that
// DescribeJobQueues always includes "tags": {} when a queue has no tags.
func TestDescribeJobQueues_TagsPresentNoTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := post(t, h, "/v1/createcomputeenvironment", map[string]any{
		"computeEnvironmentName": "ce-for-jq",
		"type":                   "MANAGED",
		"state":                  "ENABLED",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = post(t, h, "/v1/createjobqueue", map[string]any{
		"jobQueueName": "jq-notags",
		"priority":     1,
		"state":        "ENABLED",
		"computeEnvironmentOrder": []map[string]any{
			{"computeEnvironment": "ce-for-jq", "order": 1},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = post(t, h, "/v1/describejobqueues", map[string]any{
		"jobQueues": []string{"jq-notags"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	mustUnmarshal(t, rec, &out)
	items := out["jobQueues"].([]any)
	require.Len(t, items, 1)

	itemBytes, err := json.Marshal(items[0])
	require.NoError(t, err)
	assertTagsPresent(t, itemBytes)
}

// TestDescribeJobQueues_EmptyList verifies that DescribeJobQueues
// returns "jobQueues": [] not null.
func TestDescribeJobQueues_EmptyList(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := post(t, h, "/v1/describejobqueues", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var rawMap map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &rawMap))
	raw, ok := rawMap["jobQueues"]
	require.True(t, ok, "jobQueues key must be present")
	assert.Equal(t, "[]", string(raw), "jobQueues must be [] not null when empty")
}
