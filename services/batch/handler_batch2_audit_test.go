package batch_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/batch"
)

// newAuditHandlerWithQueue returns a handler pre-loaded with a CE, job queue,
// and job definition named "audit-jd" for use in job-submission audit tests.
func newAuditHandlerWithQueue(t *testing.T, queueName string) *batch.Handler {
	t.Helper()

	h := newTestHandler(t)

	rec := post(t, h, "/v1/createcomputeenvironment", map[string]any{
		"computeEnvironmentName": "audit-ce",
		"type":                   "MANAGED",
		"state":                  "ENABLED",
	})
	require.Equal(t, http.StatusOK, rec.Code, "create CE")

	rec = post(t, h, "/v1/createjobqueue", map[string]any{
		"jobQueueName": queueName,
		"priority":     1,
		"state":        "ENABLED",
		"computeEnvironmentOrder": []map[string]any{
			{"computeEnvironment": "audit-ce", "order": 1},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, "create job queue")

	rec = post(t, h, "/v1/registerjobdefinition", map[string]any{
		"jobDefinitionName": "audit-jd",
		"type":              "container",
	})
	require.Equal(t, http.StatusOK, rec.Code, "register job definition")

	return h
}

// assertTagsPresent checks that raw JSON has a "tags" key containing a non-null
// object (possibly empty). This mirrors the AWS guarantee that describe responses
// always include "tags": {} even when no tags are set.
func assertTagsPresent(t *testing.T, raw []byte) {
	t.Helper()

	var rawMap map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &rawMap))

	tagBytes, hasTags := rawMap["tags"]
	assert.True(t, hasTags, "tags key must be present in describe response")

	if hasTags {
		var tags map[string]string
		require.NoError(t, json.Unmarshal(tagBytes, &tags), "tags must be a JSON object, not null")
		assert.NotNil(t, tags, "tags must be {} not null")
	}
}

// TestBatch2Audit_DescribeComputeEnvironments_TagsPresentNoTags verifies that
// DescribeComputeEnvironments always includes "tags": {} when a CE has no tags.
// AWS always returns tags:{} in this response.
func TestBatch2Audit_DescribeComputeEnvironments_TagsPresentNoTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := post(t, h, "/v1/createcomputeenvironment", map[string]any{
		"computeEnvironmentName": "ce-notags",
		"type":                   "MANAGED",
		"state":                  "ENABLED",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = post(t, h, "/v1/describecomputeenvironments", map[string]any{
		"computeEnvironments": []string{"ce-notags"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	mustUnmarshal(t, rec, &out)
	items := out["computeEnvironments"].([]any)
	require.Len(t, items, 1)

	itemBytes, err := json.Marshal(items[0])
	require.NoError(t, err)
	assertTagsPresent(t, itemBytes)
}

// TestBatch2Audit_DescribeComputeEnvironments_TagsRoundTrip verifies that tags
// set on a compute environment are returned in DescribeComputeEnvironments.
func TestBatch2Audit_DescribeComputeEnvironments_TagsRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := post(t, h, "/v1/createcomputeenvironment", map[string]any{
		"computeEnvironmentName": "ce-withtags",
		"type":                   "MANAGED",
		"state":                  "ENABLED",
		"tags":                   map[string]string{"owner": "ops"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = post(t, h, "/v1/describecomputeenvironments", map[string]any{
		"computeEnvironments": []string{"ce-withtags"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	mustUnmarshal(t, rec, &out)
	items := out["computeEnvironments"].([]any)
	require.Len(t, items, 1)
	tags := items[0].(map[string]any)["tags"].(map[string]any)
	assert.Equal(t, "ops", tags["owner"])
}

// TestBatch2Audit_DescribeJobQueues_TagsPresentNoTags verifies that
// DescribeJobQueues always includes "tags": {} when a queue has no tags.
func TestBatch2Audit_DescribeJobQueues_TagsPresentNoTags(t *testing.T) {
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

// TestBatch2Audit_DescribeJobDefinitions_TagsPresentNoTags verifies that
// DescribeJobDefinitions always includes "tags": {} when a job definition
// has no tags. AWS always returns tags:{} in this response.
func TestBatch2Audit_DescribeJobDefinitions_TagsPresentNoTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := post(t, h, "/v1/registerjobdefinition", map[string]any{
		"jobDefinitionName": "jd-notags",
		"type":              "container",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = post(t, h, "/v1/describejobdefinitions", map[string]any{
		"jobDefinitions": []string{"jd-notags"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	mustUnmarshal(t, rec, &out)
	items := out["jobDefinitions"].([]any)
	require.Len(t, items, 1)

	itemBytes, err := json.Marshal(items[0])
	require.NoError(t, err)
	assertTagsPresent(t, itemBytes)
}

// TestBatch2Audit_DescribeJobs_TagsPresentNoTags verifies that DescribeJobs
// always includes "tags": {} when a job was submitted without tags.
func TestBatch2Audit_DescribeJobs_TagsPresentNoTags(t *testing.T) {
	t.Parallel()

	h := newAuditHandlerWithQueue(t, "audit-q-notags")

	rec := post(t, h, "/v1/submitjob", map[string]any{
		"jobName":       "job-notags",
		"jobQueue":      "audit-q-notags",
		"jobDefinition": "audit-jd",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	var submitOut map[string]string
	mustUnmarshal(t, rec, &submitOut)

	rec = post(t, h, "/v1/describejobs", map[string]any{
		"jobs": []string{submitOut["jobId"]},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	mustUnmarshal(t, rec, &out)
	jobs := out["jobs"].([]any)
	require.Len(t, jobs, 1)

	itemBytes, err := json.Marshal(jobs[0])
	require.NoError(t, err)
	assertTagsPresent(t, itemBytes)
}

// TestBatch2Audit_DescribeJobs_TagsRoundTrip verifies that tags submitted
// with a job are returned in DescribeJobs.
func TestBatch2Audit_DescribeJobs_TagsRoundTrip(t *testing.T) {
	t.Parallel()

	h := newAuditHandlerWithQueue(t, "audit-q-tags")

	rec := post(t, h, "/v1/submitjob", map[string]any{
		"jobName":       "job-withtags",
		"jobQueue":      "audit-q-tags",
		"jobDefinition": "audit-jd",
		"tags":          map[string]string{"team": "infra", "env": "test"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	var submitOut map[string]string
	mustUnmarshal(t, rec, &submitOut)

	rec = post(t, h, "/v1/describejobs", map[string]any{
		"jobs": []string{submitOut["jobId"]},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	mustUnmarshal(t, rec, &out)
	jobs := out["jobs"].([]any)
	require.Len(t, jobs, 1)
	tags := jobs[0].(map[string]any)["tags"].(map[string]any)
	assert.Equal(t, "infra", tags["team"])
	assert.Equal(t, "test", tags["env"])
}

// TestBatch2Audit_DescribeComputeEnvironments_EmptyList verifies that
// DescribeComputeEnvironments returns "computeEnvironments": [] not null.
func TestBatch2Audit_DescribeComputeEnvironments_EmptyList(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := post(t, h, "/v1/describecomputeenvironments", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var rawMap map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &rawMap))
	raw, ok := rawMap["computeEnvironments"]
	require.True(t, ok, "computeEnvironments key must be present")
	assert.Equal(t, "[]", string(raw), "computeEnvironments must be [] not null when empty")
}

// TestBatch2Audit_DescribeJobQueues_EmptyList verifies that DescribeJobQueues
// returns "jobQueues": [] not null.
func TestBatch2Audit_DescribeJobQueues_EmptyList(t *testing.T) {
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

// TestBatch2Audit_DescribeJobDefinitions_EmptyList verifies that
// DescribeJobDefinitions returns "jobDefinitions": [] not null.
func TestBatch2Audit_DescribeJobDefinitions_EmptyList(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := post(t, h, "/v1/describejobdefinitions", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var rawMap map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &rawMap))
	raw, ok := rawMap["jobDefinitions"]
	require.True(t, ok, "jobDefinitions key must be present")
	assert.Equal(t, "[]", string(raw), "jobDefinitions must be [] not null when empty")
}

// TestBatch2Audit_DescribeJobs_EmptyList verifies that DescribeJobs returns
// "jobs": [] not null when no matching jobs are found.
func TestBatch2Audit_DescribeJobs_EmptyList(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := post(t, h, "/v1/describejobs", map[string]any{
		"jobs": []string{"nonexistent-job-id"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var rawMap map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &rawMap))
	raw, ok := rawMap["jobs"]
	require.True(t, ok, "jobs key must be present")
	assert.Equal(t, "[]", string(raw), "jobs must be [] not null when no matching jobs found")
}

// TestBatch2Audit_ListJobs_SummaryIncludesJobArn verifies that ListJobs returns
// jobArn in each jobSummary. AWS always populates jobArn on job summaries.
func TestBatch2Audit_ListJobs_SummaryIncludesJobArn(t *testing.T) {
	t.Parallel()

	h := newAuditHandlerWithQueue(t, "audit-q-arn")

	rec := post(t, h, "/v1/submitjob", map[string]any{
		"jobName":       "job-for-arn-check",
		"jobQueue":      "audit-q-arn",
		"jobDefinition": "audit-jd",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = post(t, h, "/v1/listjobs", map[string]any{"jobQueue": "audit-q-arn"})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	summaries, _ := out["jobSummaryList"].([]any)
	require.Len(t, summaries, 1)

	s := summaries[0].(map[string]any)
	arn, hasARN := s["jobArn"].(string)
	assert.True(t, hasARN && arn != "", "jobArn must be present and non-empty in ListJobs summary")
	assert.Contains(t, arn, "arn:aws:batch:", "jobArn must be a valid ARN")
}

// TestBatch2Audit_ListJobs_SummaryIncludesTimestamps verifies that ListJobs
// returns startedAt and stoppedAt in jobSummary entries when the job has
// transitioned through those states. AWS populates these fields.
func TestBatch2Audit_ListJobs_SummaryIncludesTimestamps(t *testing.T) {
	t.Parallel()

	h := newAuditHandlerWithQueue(t, "audit-q-ts")

	rec := post(t, h, "/v1/submitjob", map[string]any{
		"jobName":       "job-for-ts-check",
		"jobQueue":      "audit-q-ts",
		"jobDefinition": "audit-jd",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	var submitOut map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &submitOut))
	jobID := submitOut["jobId"]

	rec = post(t, h, "/v1/terminatejob", map[string]any{
		"jobId":  jobID,
		"reason": "test termination",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = post(t, h, "/v1/listjobs", map[string]any{
		"jobQueue":  "audit-q-ts",
		"jobStatus": "FAILED",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	summaries, _ := out["jobSummaryList"].([]any)
	require.Len(t, summaries, 1)

	s := summaries[0].(map[string]any)
	stoppedAt, hasStoppedAt := s["stoppedAt"].(float64)
	assert.True(t, hasStoppedAt && stoppedAt > 0, "stoppedAt must be present and positive after termination")
}

// TestBatch2Audit_ListJobs_InvalidJobStatus verifies that ListJobs returns
// HTTP 400 when an unrecognised jobStatus is provided.
// AWS Batch returns ClientException for invalid status values.
func TestBatch2Audit_ListJobs_InvalidJobStatus(t *testing.T) {
	t.Parallel()

	h := newAuditHandlerWithQueue(t, "audit-q-badstatus")

	rec := post(t, h, "/v1/listjobs", map[string]any{
		"jobQueue":  "audit-q-badstatus",
		"jobStatus": "INVALID_STATUS",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code, "invalid jobStatus must return 400")
}
