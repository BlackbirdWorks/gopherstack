package batch_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/batch"
)

func TestHandler_ConsumableResource_CRUD(t *testing.T) {
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
				rec := post(t, h, "/v1/createconsumableresource", map[string]any{
					"consumableResourceName": "test-cr",
					"totalQuantity":          10,
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

			rec := post(t, h, "/v1/createconsumableresource", map[string]any{
				"consumableResourceName": "test-cr",
				"totalQuantity":          10,
				"resourceType":           "REPLENISHABLE",
			})

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantARN {
				var out map[string]string
				mustUnmarshal(t, rec, &out)
				assert.Contains(t, out["consumableResourceArn"], "test-cr")
				assert.Equal(t, "test-cr", out["consumableResourceName"])
			}
		})
	}
}

func TestHandler_ConsumableResource_DescribeAndDelete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		resourceToUse    string
		wantDescStatus   int
		wantDeleteStatus int
		createFirst      bool
	}{
		{
			name:             "describe_and_delete_success",
			createFirst:      true,
			resourceToUse:    "my-cr",
			wantDescStatus:   http.StatusOK,
			wantDeleteStatus: http.StatusOK,
		},
		{
			name:             "describe_not_found",
			createFirst:      false,
			resourceToUse:    "missing-cr",
			wantDescStatus:   http.StatusBadRequest,
			wantDeleteStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.createFirst {
				rec := post(t, h, "/v1/createconsumableresource", map[string]any{
					"consumableResourceName": tt.resourceToUse,
					"totalQuantity":          5,
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			descRec := post(t, h, "/v1/describeconsumableresource", map[string]any{
				"consumableResource": tt.resourceToUse,
			})
			assert.Equal(t, tt.wantDescStatus, descRec.Code)

			if tt.wantDescStatus == http.StatusOK {
				var out map[string]any
				mustUnmarshal(t, descRec, &out)
				assert.Equal(t, tt.resourceToUse, out["consumableResourceName"])
				assert.NotEmpty(t, out["consumableResourceArn"])
			}

			delRec := post(t, h, "/v1/deleteconsumableresource", map[string]any{
				"consumableResource": tt.resourceToUse,
			})
			assert.Equal(t, tt.wantDeleteStatus, delRec.Code)
		})
	}
}

func TestHandler_ConsumableResource_DescribeByARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := post(t, h, "/v1/createconsumableresource", map[string]any{
		"consumableResourceName": "arn-cr",
		"totalQuantity":          20,
		"resourceType":           "NON_REPLENISHABLE",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createOut map[string]string
	mustUnmarshal(t, rec, &createOut)
	crARN := createOut["consumableResourceArn"]
	require.NotEmpty(t, crARN)

	descRec := post(t, h, "/v1/describeconsumableresource", map[string]any{
		"consumableResource": crARN,
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var descOut map[string]any
	mustUnmarshal(t, descRec, &descOut)
	assert.Equal(t, "arn-cr", descOut["consumableResourceName"])
	assert.Equal(t, "NON_REPLENISHABLE", descOut["resourceType"])
	assert.InDelta(t, float64(20), descOut["totalQuantity"], 0)
}

// --- SchedulingPolicy tests ---

func TestBatch_UpdateConsumableResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *batch.Handler) string
		name       string
		operation  string
		resource   string
		wantStatus int
		quantity   int64
	}{
		{
			name: "SET_operation",
			setup: func(t *testing.T, h *batch.Handler) string {
				t.Helper()
				rec := post(t, h, "/v1/createconsumableresource", map[string]any{
					"consumableResourceName": "cr-set",
					"totalQuantity":          int64(5),
				})
				require.Equal(t, http.StatusOK, rec.Code)

				return "cr-set"
			},
			operation:  "SET",
			quantity:   100,
			wantStatus: http.StatusOK,
		},
		{
			name: "ADD_operation",
			setup: func(t *testing.T, h *batch.Handler) string {
				t.Helper()
				rec := post(t, h, "/v1/createconsumableresource", map[string]any{
					"consumableResourceName": "cr-add",
					"totalQuantity":          int64(10),
				})
				require.Equal(t, http.StatusOK, rec.Code)

				return "cr-add"
			},
			operation:  "ADD",
			quantity:   5,
			wantStatus: http.StatusOK,
		},
		{
			name: "REMOVE_operation",
			setup: func(t *testing.T, h *batch.Handler) string {
				t.Helper()
				rec := post(t, h, "/v1/createconsumableresource", map[string]any{
					"consumableResourceName": "cr-remove",
					"totalQuantity":          int64(10),
				})
				require.Equal(t, http.StatusOK, rec.Code)

				return "cr-remove"
			},
			operation:  "REMOVE",
			quantity:   3,
			wantStatus: http.StatusOK,
		},
		{
			name:       "not_found",
			resource:   "nonexistent-cr",
			operation:  "SET",
			quantity:   10,
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "negative_quantity",
			setup: func(t *testing.T, h *batch.Handler) string {
				t.Helper()
				rec := post(t, h, "/v1/createconsumableresource", map[string]any{
					"consumableResourceName": "cr-neg",
					"totalQuantity":          int64(10),
				})
				require.Equal(t, http.StatusOK, rec.Code)

				return "cr-neg"
			},
			operation:  "SET",
			quantity:   -1,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_resource_field",
			resource:   "",
			operation:  "SET",
			quantity:   10,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			resource := tt.resource
			if tt.setup != nil {
				resource = tt.setup(t, h)
			}

			rec := post(t, h, "/v1/updateconsumableresource", map[string]any{
				"consumableResource": resource,
				"operation":          tt.operation,
				"quantity":           tt.quantity,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// --- ListConsumableResources tests ---

func TestBatch_ListConsumableResources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		resources []string
		wantCount int
	}{
		{
			name:      "empty",
			resources: nil,
			wantCount: 0,
		},
		{
			name:      "populated_sorted",
			resources: []string{"zzz-resource", "aaa-resource", "mmm-resource"},
			wantCount: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			for _, name := range tt.resources {
				rec := post(t, h, "/v1/createconsumableresource", map[string]any{
					"consumableResourceName": name,
					"totalQuantity":          int64(1),
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := post(t, h, "/v1/listconsumableresources", map[string]any{})
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			mustUnmarshal(t, rec, &out)

			items, _ := out["consumableResources"].([]any)
			assert.Len(t, items, tt.wantCount)

			if tt.wantCount > 1 {
				first := items[0].(map[string]any)["consumableResourceName"].(string)
				last := items[tt.wantCount-1].(map[string]any)["consumableResourceName"].(string)
				assert.Less(t, first, last, "list should be sorted by name")
			}
		})
	}
}

// --- DescribeSchedulingPolicies tests ---

func TestBatch_ResourceTypeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		resourceType string
		wantStatus   int
	}{
		{
			name:         "valid_replenishable",
			resourceType: "REPLENISHABLE",
			wantStatus:   http.StatusOK,
		},
		{
			name:         "valid_non_replenishable",
			resourceType: "NON_REPLENISHABLE",
			wantStatus:   http.StatusOK,
		},
		{
			name:         "invalid_resource_type",
			resourceType: "INVALID_TYPE",
			wantStatus:   http.StatusBadRequest,
		},
		{
			name:         "default_when_empty",
			resourceType: "",
			wantStatus:   http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := post(t, h, "/v1/createconsumableresource", map[string]any{
				"consumableResourceName": "cr-type-" + tt.name,
				"totalQuantity":          int64(1),
				"resourceType":           tt.resourceType,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// --- SchedulingPolicyNameIndex tests ---

func TestHandler_ListJobsByConsumableResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		consumableResource string
		submitJobs         int
		wantStatus         int
	}{
		{
			name:               "returns_all_jobs",
			consumableResource: "my-cr",
			submitJobs:         2,
			wantStatus:         http.StatusOK,
		},
		{
			name:               "empty_resource_name_still_works",
			consumableResource: "",
			submitJobs:         0,
			wantStatus:         http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.submitJobs > 0 {
				ceName := "ce-crl-" + tt.name
				qName := "q-crl-" + tt.name
				jdName := "jd-crl-" + tt.name

				rec := post(t, h, "/v1/createcomputeenvironment", map[string]any{
					"computeEnvironmentName": ceName, "type": "MANAGED", "state": "ENABLED",
				})
				require.Equal(t, http.StatusOK, rec.Code)

				rec = post(t, h, "/v1/createjobqueue", map[string]any{
					"jobQueueName": qName, "priority": 1, "state": "ENABLED",
					"computeEnvironmentOrder": []map[string]any{{"computeEnvironment": ceName, "order": 1}},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				rec = post(t, h, "/v1/registerjobdefinition", map[string]any{
					"jobDefinitionName": jdName, "type": "container",
				})
				require.Equal(t, http.StatusOK, rec.Code)

				for i := range tt.submitJobs {
					rec = post(t, h, "/v1/submitjob", map[string]any{
						"jobName": fmt.Sprintf("job-%d", i), "jobQueue": qName, "jobDefinition": jdName,
					})
					require.Equal(t, http.StatusOK, rec.Code)
				}
			}

			rec := post(t, h, "/v1/listjobsbyconsumableresource", map[string]any{
				"consumableResource": tt.consumableResource,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var out map[string]any
				mustUnmarshal(t, rec, &out)
				jobs := out["jobs"]
				if jobs != nil {
					assert.IsType(t, []any{}, jobs)
				}
			}
		})
	}
}

// TestHandler_ListJobsByConsumableResource_WireShape verifies the response
// matches aws-sdk-go-v2/service/batch/types.
// ListJobsByConsumableResourceSummary exactly: jobQueueArn (not jobQueue),
// jobStatus (not status), and quantity (the requested amount of the queried
// resource, extracted from the job's consumableResourceProperties) -- a
// narrower shape than the full Job this op previously (incorrectly)
// returned wholesale.
func TestHandler_ListJobsByConsumableResource_WireShape(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := post(t, h, "/v1/createcomputeenvironment", map[string]any{
		"computeEnvironmentName": "ce-crl-wire", "type": "MANAGED", "state": "ENABLED",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = post(t, h, "/v1/createjobqueue", map[string]any{
		"jobQueueName": "q-crl-wire", "priority": 1, "state": "ENABLED",
		"computeEnvironmentOrder": []map[string]any{{"computeEnvironment": "ce-crl-wire", "order": 1}},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = post(t, h, "/v1/registerjobdefinition", map[string]any{
		"jobDefinitionName": "jd-crl-wire", "type": "container",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = post(t, h, "/v1/submitjob", map[string]any{
		"jobName": "job-crl-wire", "jobQueue": "q-crl-wire", "jobDefinition": "jd-crl-wire",
		"consumableResourcePropertiesOverride": map[string]any{
			"consumableResourceList": []map[string]any{
				{"consumableResource": "gpu-pool", "quantity": 4},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = post(t, h, "/v1/listjobsbyconsumableresource", map[string]any{
		"consumableResource": "gpu-pool",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	mustUnmarshal(t, rec, &out)
	jobs := out["jobs"].([]any)
	require.Len(t, jobs, 1)

	job := jobs[0].(map[string]any)
	assert.Contains(t, job, "jobQueueArn")
	assert.NotContains(t, job, "jobQueue", "field must be jobQueueArn, not jobQueue")
	assert.Contains(t, job, "jobStatus")
	assert.NotContains(t, job, "status", "field must be jobStatus, not status")
	assert.InEpsilon(t, float64(4), job["quantity"].(float64), 0.001)
	assert.Equal(t, "job-crl-wire", job["jobName"])
}
