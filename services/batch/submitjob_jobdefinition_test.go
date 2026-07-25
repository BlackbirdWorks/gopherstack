package batch_test

import (
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/batch"
)

// TestConsumableResourceProperty_QuantityIsInt64 verifies
// ConsumableResourceProperty.Quantity is int64, matching
// aws-sdk-go-v2/service/batch/types.ConsumableResourceRequirement.Quantity
// (a Long) exactly. It was previously float64, which is wrong for the real
// API even though whole-number values happen to encode identically on the
// wire (see PARITY.md gaps).
func TestConsumableResourceProperty_QuantityIsInt64(t *testing.T) {
	t.Parallel()

	field, ok := reflect.TypeFor[batch.ConsumableResourceProperty]().FieldByName("Quantity")
	require.True(t, ok)
	assert.Equal(t, reflect.Int64, field.Type.Kind(), "Quantity must be int64, not float64")
}

// newHandlerWithQueueForSubmit creates a compute environment and an ENABLED
// job queue, returning the handler and queue name, for tests that focus on
// SubmitJob's jobDefinition resolution.
func newHandlerWithQueueForSubmit(t *testing.T) (*batch.Handler, string) {
	t.Helper()

	h := newTestHandler(t)
	rec := post(t, h, "/v1/createcomputeenvironment", map[string]any{
		"computeEnvironmentName": "ce-resolve",
		"type":                   "MANAGED",
		"state":                  "ENABLED",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = post(t, h, "/v1/createjobqueue", map[string]any{
		"jobQueueName": "q-resolve",
		"priority":     1,
		"state":        "ENABLED",
		"computeEnvironmentOrder": []map[string]any{
			{"computeEnvironment": "ce-resolve", "order": 1},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	return h, "q-resolve"
}

// describeJobDefinitionOf extracts the jobId from a SubmitJob response
// recorder, calls DescribeJobs, and returns the resolved jobDefinition.
// AWS Batch always echoes this back as the job definition's full ARN, never
// the caller's short reference (see backend.go's
// SubmitJob/lookupJobDefinitionForSubmit).
func describeJobDefinitionOf(t *testing.T, h *batch.Handler, submitRec *httptest.ResponseRecorder) string {
	t.Helper()

	var submitOut map[string]any
	mustUnmarshal(t, submitRec, &submitOut)
	jobID, _ := submitOut["jobId"].(string)
	require.NotEmpty(t, jobID)

	rec := post(t, h, "/v1/describejobs", map[string]any{"jobs": []string{jobID}})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	mustUnmarshal(t, rec, &out)
	jobs, _ := out["jobs"].([]any)
	require.Len(t, jobs, 1)

	job := jobs[0].(map[string]any)
	jobDef, _ := job["jobDefinition"].(string)

	return jobDef
}

// TestHandler_SubmitJob_UnknownJobDefinition verifies that SubmitJob rejects
// a jobDefinition reference that was never registered. AWS Batch validates
// the job definition exists (and is ACTIVE) before accepting the job;
// previously this emulator stored the raw string unchecked, letting jobs sit
// in a queue with no way for a real SDK client to detect the mistake.
func TestHandler_SubmitJob_UnknownJobDefinition(t *testing.T) {
	t.Parallel()

	h, queue := newHandlerWithQueueForSubmit(t)

	rec := post(t, h, "/v1/submitjob", map[string]any{
		"jobName":       "job1",
		"jobQueue":      queue,
		"jobDefinition": "does-not-exist",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var out map[string]string
	mustUnmarshal(t, rec, &out)
	assert.Equal(t, "ClientException", out["__type"])
}

// TestHandler_SubmitJob_JobDefinitionResolution exercises AWS Batch's
// documented jobDefinition resolution: a bare name resolves to the newest
// ACTIVE revision, an explicit "name:revision" must match exactly, and a
// revision that has been deregistered (INACTIVE) can no longer be submitted
// against, even by exact reference.
func TestHandler_SubmitJob_JobDefinitionResolution(t *testing.T) {
	t.Parallel()

	h, queue := newHandlerWithQueueForSubmit(t)

	// Register two revisions of the same job definition name.
	rec := post(t, h, "/v1/registerjobdefinition", map[string]any{
		"jobDefinitionName": "res-jd", "type": "container",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = post(t, h, "/v1/registerjobdefinition", map[string]any{
		"jobDefinitionName": "res-jd", "type": "container",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var rev2Out map[string]any
	mustUnmarshal(t, rec, &rev2Out)
	require.InDelta(t, float64(2), rev2Out["revision"], 0)

	// A bare name resolves to the latest ACTIVE revision (2).
	rec = post(t, h, "/v1/submitjob", map[string]any{
		"jobName": "job-bare", "jobQueue": queue, "jobDefinition": "res-jd",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, describeJobDefinitionOf(t, h, rec), "res-jd:2",
		"bare name should resolve to newest ACTIVE revision")

	// An explicit revision resolves to that exact revision, not the latest.
	rec = post(t, h, "/v1/submitjob", map[string]any{
		"jobName": "job-explicit", "jobQueue": queue, "jobDefinition": "res-jd:1",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, describeJobDefinitionOf(t, h, rec), "res-jd:1",
		"explicit revision must be honored exactly")

	// Deregister revision 2; bare name now resolves to revision 1.
	rec = post(t, h, "/v1/deregisterjobdefinition", map[string]any{"jobDefinition": "res-jd:2"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = post(t, h, "/v1/submitjob", map[string]any{
		"jobName": "job-after-deregister", "jobQueue": queue, "jobDefinition": "res-jd",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, describeJobDefinitionOf(t, h, rec), "res-jd:1",
		"bare name must skip the now-INACTIVE revision")

	// An explicit reference to the deregistered revision is rejected.
	rec = post(t, h, "/v1/submitjob", map[string]any{
		"jobName": "job-inactive", "jobQueue": queue, "jobDefinition": "res-jd:2",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code, "submitting against an INACTIVE revision must fail")
}

// TestHandler_DescribeJobDefinitions_ExplicitRevisionExactMatch verifies that
// DescribeJobDefinitions with a "name:revision" reference returns only that
// revision, not every revision sharing the name (the wire contract AWS
// documents for the jobDefinitions request parameter).
func TestHandler_DescribeJobDefinitions_ExplicitRevisionExactMatch(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for range 3 {
		rec := post(t, h, "/v1/registerjobdefinition", map[string]any{
			"jobDefinitionName": "multi-jd", "type": "container",
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// Exact "name:revision" returns exactly one job definition.
	rec := post(t, h, "/v1/describejobdefinitions", map[string]any{
		"jobDefinitions": []string{"multi-jd:2"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	mustUnmarshal(t, rec, &out)
	items, _ := out["jobDefinitions"].([]any)
	require.Len(t, items, 1, "explicit revision must return exactly one job definition")
	assert.InDelta(t, float64(2), items[0].(map[string]any)["revision"], 0)

	// A bare name still returns every revision.
	rec = post(t, h, "/v1/describejobdefinitions", map[string]any{
		"jobDefinitions": []string{"multi-jd"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	mustUnmarshal(t, rec, &out)
	items, _ = out["jobDefinitions"].([]any)
	assert.Len(t, items, 3, "bare name must still return every revision")
}

// TestHandler_RegisterJobDefinition_WireShapes verifies two response fields
// that must match aws-sdk-go-v2/service/batch/types.JobDefinition exactly:
// "timeout" is a nested {attemptDurationSeconds} object (never a flat
// "timeoutSeconds" integer), and "consumableResourceProperties" nests its
// list under "consumableResourceList" (never a bare array). A real SDK
// client's JSON deserializer looks for these exact shapes and silently drops
// anything else.
func TestHandler_RegisterJobDefinition_WireShapes(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := post(t, h, "/v1/registerjobdefinition", map[string]any{
		"jobDefinitionName": "wireshape-jd",
		"type":              "container",
		"timeout":           map[string]any{"attemptDurationSeconds": 60},
		"consumableResourceProperties": map[string]any{
			"consumableResourceList": []map[string]any{
				{"consumableResource": "cr1", "quantity": 5},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = post(t, h, "/v1/describejobdefinitions", map[string]any{
		"jobDefinitions": []string{"wireshape-jd"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var listWrapper struct {
		JobDefinitions []map[string]any `json:"jobDefinitions"`
	}
	mustUnmarshal(t, rec, &listWrapper)
	require.Len(t, listWrapper.JobDefinitions, 1)
	jd := listWrapper.JobDefinitions[0]

	timeout, ok := jd["timeout"].(map[string]any)
	require.True(t, ok, "timeout must be a nested object, not a flat timeoutSeconds field")
	assert.InDelta(t, float64(60), timeout["attemptDurationSeconds"], 0)
	assert.NotContains(t, jd, "timeoutSeconds", "timeoutSeconds is not part of the real AWS Batch wire shape")

	crp, ok := jd["consumableResourceProperties"].(map[string]any)
	require.True(t, ok, "consumableResourceProperties must be a nested object")
	list, ok := crp["consumableResourceList"].([]any)
	require.True(t, ok, "consumableResourceProperties must nest its list under consumableResourceList")
	require.Len(t, list, 1)
	assert.Equal(t, "cr1", list[0].(map[string]any)["consumableResource"])
}

// TestHandler_SubmitJob_PassesThroughOverrides verifies that SubmitJob
// request fields beyond the basics (arrayProperties, shareIdentifier,
// schedulingPriorityOverride, propagateTags, and the consumable-resource
// override) are stored and echoed back by DescribeJobs instead of being
// silently discarded.
func TestHandler_SubmitJob_PassesThroughOverrides(t *testing.T) {
	t.Parallel()

	h, queue := newHandlerWithQueueForSubmit(t)

	rec := post(t, h, "/v1/registerjobdefinition", map[string]any{
		"jobDefinitionName": "override-jd", "type": "container",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = post(t, h, "/v1/submitjob", map[string]any{
		"jobName":                    "job-override",
		"jobQueue":                   queue,
		"jobDefinition":              "override-jd",
		"arrayProperties":            map[string]any{"size": 3},
		"shareIdentifier":            "team-a",
		"schedulingPriorityOverride": 7,
		"propagateTags":              true,
		"consumableResourcePropertiesOverride": map[string]any{
			"consumableResourceList": []map[string]any{
				{"consumableResource": "cr-x", "quantity": 2},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var submitOut map[string]any
	mustUnmarshal(t, rec, &submitOut)
	jobID := submitOut["jobId"].(string)
	assert.NotEmpty(t, submitOut["jobArn"], "SubmitJob response must include jobArn")

	rec = post(t, h, "/v1/describejobs", map[string]any{"jobs": []string{jobID}})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	mustUnmarshal(t, rec, &out)
	jobs, _ := out["jobs"].([]any)
	require.Len(t, jobs, 1)
	job := jobs[0].(map[string]any)

	assert.Equal(t, "team-a", job["shareIdentifier"])
	assert.InDelta(t, float64(7), job["schedulingPriority"], 0)
	assert.Equal(t, true, job["propagateTags"])

	arrayProps, ok := job["arrayProperties"].(map[string]any)
	require.True(t, ok, "arrayProperties must be echoed back")
	assert.InDelta(t, float64(3), arrayProps["size"], 0)

	crp, ok := job["consumableResourceProperties"].(map[string]any)
	require.True(t, ok, "consumableResourceProperties override must be echoed back")
	list, ok := crp["consumableResourceList"].([]any)
	require.True(t, ok)
	require.Len(t, list, 1)
	assert.Equal(t, "cr-x", list[0].(map[string]any)["consumableResource"])
}
