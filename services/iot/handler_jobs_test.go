package iot_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/iot"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBatch2_JobExecutions tests listing job executions.
func TestJobExecutions(t *testing.T) {
	t.Parallel()
	h := newIoTHandler(t)

	// Create a job first
	iotOK(t, h, http.MethodPost, "/jobs/test-job", map[string]any{
		"targets":  []any{"arn:aws:iot:us-east-1:000000000000:thing/my-thing"},
		"document": `{"action":"update"}`,
	})

	// Cancel an execution (creates it). Real AWS IoT paths this under
	// /things/{thingName}/jobs/{jobId}/cancel, not
	// /jobs/{jobId}/things/{thingName}/cancel -- see PARITY.md.
	iotOK(t, h, http.MethodPut, "/things/my-thing/jobs/test-job/cancel", nil)

	// ListJobExecutionsForJob
	out := iotOK(t, h, http.MethodGet, "/jobs/test-job/things", nil)
	execs, _ := out["executionSummaries"].([]any)
	if len(execs) != 1 {
		t.Errorf("expected 1 execution for job, got %d", len(execs))
	}

	summary, ok := execs[0].(map[string]any)
	require.True(t, ok)
	assert.Contains(t, summary, "thingArn", "real AWS nests thingArn, not thingName, in each summary")
	assert.NotContains(t, summary, "thingName")
	assert.Contains(t, summary, "jobExecutionSummary")

	// ListJobExecutionsForThing
	out2 := iotOK(t, h, http.MethodGet, "/things/my-thing/jobs", nil)
	execs2, _ := out2["executionSummaries"].([]any)
	if len(execs2) != 1 {
		t.Errorf("expected 1 execution for thing, got %d", len(execs2))
	}

	summary2, ok := execs2[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "test-job", summary2["jobId"])
	assert.Contains(t, summary2, "jobExecutionSummary")
}

// TestDescribeJob_WireShape verifies DescribeJob's response matches real AWS
// IoT: documentSource is a top-level field (not nested inside "job"), and
// the nested "job" object has no "document"/"documentSource"/"tags" fields
// at all -- aws-sdk-go-v2/service/iot/types.Job (v1.76.0) has none of the
// three. A previous version of this backend echoed all three back inside
// "job", which real AWS never does (document is only retrievable via
// GetJobDocument).
func TestDescribeJob_WireShape(t *testing.T) {
	t.Parallel()
	h := newIoTHandler(t)

	iotOK(t, h, http.MethodPost, "/jobs/wire-shape-job", map[string]any{
		"targets":        []any{"arn:aws:iot:us-east-1:000000000000:thing/my-thing"},
		"document":       `{"action":"update"}`,
		"documentSource": "s3://bucket/key",
	})

	out := iotOK(t, h, http.MethodGet, "/jobs/wire-shape-job", nil)
	assert.Equal(t, "s3://bucket/key", out["documentSource"], "documentSource must be a top-level field")

	job, ok := out["job"].(map[string]any)
	require.True(t, ok, "expected nested job object, got %v", out["job"])
	assert.Equal(t, "wire-shape-job", job["jobId"])
	assert.NotContains(t, job, "document", "real AWS Job has no document field")
	assert.NotContains(t, job, "documentSource", "real AWS Job has no documentSource field")
	assert.NotContains(t, job, "tags", "real AWS Job has no tags field")
}

func TestBackend_DescribeManagedJobTemplate(t *testing.T) {
	t.Parallel()

	b := iot.NewInMemoryBackend()

	tmpl, err := b.DescribeManagedJobTemplate("AWS-Reboot", "")
	require.NoError(t, err)
	assert.Equal(t, "AWS-Reboot", tmpl.TemplateName)
	assert.NotEmpty(t, tmpl.TemplateArn)
	assert.NotEmpty(t, tmpl.Document)
	assert.NotEmpty(t, tmpl.DocumentParameters)

	_, err = b.DescribeManagedJobTemplate("does-not-exist", "")
	require.ErrorIs(t, err, iot.ErrManagedJobTemplateNotFound)
}

func TestBackend_ListManagedJobTemplates(t *testing.T) {
	t.Parallel()

	b := iot.NewInMemoryBackend()

	all := b.ListManagedJobTemplates("")
	assert.GreaterOrEqual(t, len(all), 2)

	filtered := b.ListManagedJobTemplates("AWS-Reboot")
	require.Len(t, filtered, 1)
	assert.Equal(t, "AWS-Reboot", filtered[0].TemplateName)
}

func TestHandler_ManagedJobTemplate(t *testing.T) {
	t.Parallel()
	h := newIoTHandler(t)

	out := iotOK(t, h, http.MethodGet, "/managed-job-templates", nil)
	templates, _ := out["managedJobTemplates"].([]any)
	assert.NotEmpty(t, templates)

	out2 := iotOK(t, h, http.MethodGet, "/managed-job-templates/AWS-Reboot", nil)
	assert.Equal(t, "AWS-Reboot", out2["templateName"])

	iotExpectError(t, h, "/managed-job-templates/does-not-exist")
}

// TestNewOps_JobExecution tests job execution lifecycle.
func TestJobExecution(t *testing.T) {
	t.Parallel()
	h := newIoTHandlerBatch1(t)

	// Create job first.
	iotOK(t, h, http.MethodPost, "/jobs/exec-job", map[string]any{
		"targets":  []string{"arn:aws:iot:us-east-1:000000000000:thing/my-thing"},
		"document": `{}`,
	})

	// CancelJobExecution (creates execution if not exists). Real AWS IoT
	// paths Describe/Cancel/DeleteJobExecution under
	// /things/{thingName}/jobs/{jobId}[...], not
	// /jobs/{jobId}/things/{thingName} -- see PARITY.md.
	iotOK(t, h, http.MethodPut, "/things/my-thing/jobs/exec-job/cancel", nil)

	// DescribeJobExecution
	out := iotOK(t, h, http.MethodGet, "/things/my-thing/jobs/exec-job", nil)
	exec, ok := out["execution"].(map[string]any)
	if !ok {
		t.Fatalf("expected execution in response: %v", out)
	}
	if exec["jobId"] != "exec-job" {
		t.Errorf("exec jobId mismatch: %v", exec)
	}
	assert.Contains(t, exec, "thingArn", "real AWS's JobExecution has thingArn, not thingName")
	assert.NotContains(t, exec, "thingName")
	assert.EqualValues(t, "CANCELED", exec["status"])

	// DeleteJobExecution
	iotOK(t, h, http.MethodDelete, "/things/my-thing/jobs/exec-job/executionNumber/1", nil)
}

// TestJobExecution_RoutingAndStateGuards is a table-driven regression test
// covering two classes of previously-undiscovered bugs found while closing
// PARITY.md's job_and_jobtemplate gap:
//
//   - DescribeJobExecution/CancelJobExecution/DeleteJobExecution were routed
//     under /jobs/{jobId}/things/{thingName}[...], a path no real AWS SDK
//     client ever sends (real AWS uses /things/{thingName}/jobs/{jobId}[...],
//     confirmed against aws-sdk-go-v2/service/iot@v1.76.0's serializers.go
//     http bindings) -- all three ops were completely unreachable by a real
//     client.
//   - CancelJobExecution/DeleteJobExecution ignored force/expectedVersion/
//     statusDetails entirely; real AWS rejects canceling/deleting a
//     non-terminal execution without force=true (InvalidStateTransitionException),
//     and rejects a mismatched expectedVersion (VersionConflictException).
func TestJobExecution_RoutingAndStateGuards(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(t *testing.T, b *iot.InMemoryBackend)
		run   func(t *testing.T, h *iot.Handler)
		name  string
	}{
		{
			name: "old_jobId_things_thingName_path_no_longer_routes",
			run: func(t *testing.T, h *iot.Handler) {
				t.Helper()
				rec := iotRequest(t, h, http.MethodPut, "/jobs/old-job/things/my-thing/cancel", nil)
				assert.NotEqual(t, http.StatusOK, rec.Code,
					"the pre-fix path shape must not route to CancelJobExecution any more")
			},
		},
		{
			name: "describe_and_cancel_use_real_thing_scoped_path_with_correct_wire_shape",
			run: func(t *testing.T, h *iot.Handler) {
				t.Helper()

				iotOK(t, h, http.MethodPost, "/jobs/wire-job", map[string]any{
					"targets":  []any{"arn:aws:iot:us-east-1:000000000000:thing/wire-thing"},
					"document": `{}`,
				})
				iotOK(t, h, http.MethodPut, "/things/wire-thing/jobs/wire-job/cancel", nil)

				out := iotOK(t, h, http.MethodGet, "/things/wire-thing/jobs/wire-job", nil)
				exec, ok := out["execution"].(map[string]any)
				require.True(t, ok)
				assert.Contains(t, exec, "thingArn")
				assert.NotContains(t, exec, "thingName")
				assert.EqualValues(t, "CANCELED", exec["status"])
				assert.EqualValues(t, 1, exec["executionNumber"])
				assert.EqualValues(t, 1, exec["versionNumber"])
			},
		},
		{
			name: "cancel_in_progress_without_force_is_rejected",
			setup: func(t *testing.T, b *iot.InMemoryBackend) {
				t.Helper()
				b.AddJobExecutionInternal(&iot.JobExecution{
					JobID: "ip-job", ThingName: "ip-thing",
					Status: iot.JobExecInProgress, VersionNumber: 1,
				})
			},
			run: func(t *testing.T, h *iot.Handler) {
				t.Helper()
				rec := iotRequest(t, h, http.MethodPut, "/things/ip-thing/jobs/ip-job/cancel", nil)
				assert.Equal(t, http.StatusConflict, rec.Code)

				var body map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
				assert.Equal(t, "InvalidStateTransitionException", body["__type"])
			},
		},
		{
			name: "cancel_in_progress_with_force_succeeds",
			setup: func(t *testing.T, b *iot.InMemoryBackend) {
				t.Helper()
				b.AddJobExecutionInternal(&iot.JobExecution{
					JobID: "force-job", ThingName: "force-thing",
					Status: iot.JobExecInProgress, VersionNumber: 1,
				})
			},
			run: func(t *testing.T, h *iot.Handler) {
				t.Helper()
				iotOK(t, h, http.MethodPut, "/things/force-thing/jobs/force-job/cancel",
					map[string]any{"force": true})

				out := iotOK(t, h, http.MethodGet, "/things/force-thing/jobs/force-job", nil)
				exec, ok := out["execution"].(map[string]any)
				require.True(t, ok)
				assert.EqualValues(t, "CANCELED", exec["status"])
				assert.Equal(t, true, exec["forceCanceled"])
				assert.EqualValues(t, 2, exec["versionNumber"])
			},
		},
		{
			name: "cancel_expectedVersion_mismatch_returns_conflict",
			setup: func(t *testing.T, b *iot.InMemoryBackend) {
				t.Helper()
				b.AddJobExecutionInternal(&iot.JobExecution{
					JobID: "ver-job", ThingName: "ver-thing",
					Status: iot.JobExecQueued, VersionNumber: 5,
				})
			},
			run: func(t *testing.T, h *iot.Handler) {
				t.Helper()
				rec := iotRequest(t, h, http.MethodPut, "/things/ver-thing/jobs/ver-job/cancel",
					map[string]any{"expectedVersion": 1})
				assert.Equal(t, http.StatusConflict, rec.Code)

				var body map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
				assert.Equal(t, "VersionConflictException", body["__type"])
			},
		},
		{
			name: "delete_non_terminal_without_force_is_rejected",
			setup: func(t *testing.T, b *iot.InMemoryBackend) {
				t.Helper()
				b.AddJobExecutionInternal(&iot.JobExecution{
					JobID: "del-job", ThingName: "del-thing",
					Status: iot.JobExecQueued, VersionNumber: 1,
				})
			},
			run: func(t *testing.T, h *iot.Handler) {
				t.Helper()
				rec := iotRequest(t, h, http.MethodDelete,
					"/things/del-thing/jobs/del-job/executionNumber/1", nil)
				assert.Equal(t, http.StatusConflict, rec.Code)
			},
		},
		{
			name: "delete_non_terminal_with_force_succeeds",
			setup: func(t *testing.T, b *iot.InMemoryBackend) {
				t.Helper()
				b.AddJobExecutionInternal(&iot.JobExecution{
					JobID: "del-force-job", ThingName: "del-force-thing",
					Status: iot.JobExecInProgress, VersionNumber: 1,
				})
			},
			run: func(t *testing.T, h *iot.Handler) {
				t.Helper()
				iotOK(t, h, http.MethodDelete,
					"/things/del-force-thing/jobs/del-force-job/executionNumber/1?force=true", nil)
				iotExpectError(t, h, "/things/del-force-thing/jobs/del-force-job")
			},
		},
		{
			name: "listJobExecutions_use_nested_summary_wire_shape",
			run: func(t *testing.T, h *iot.Handler) {
				t.Helper()

				iotOK(t, h, http.MethodPost, "/jobs/list-job", map[string]any{
					"targets":  []any{"arn:aws:iot:us-east-1:000000000000:thing/list-thing"},
					"document": `{}`,
				})
				iotOK(t, h, http.MethodPut, "/things/list-thing/jobs/list-job/cancel", nil)

				forJob := iotOK(t, h, http.MethodGet, "/jobs/list-job/things", nil)
				jobSummaries, _ := forJob["executionSummaries"].([]any)
				require.Len(t, jobSummaries, 1)
				jobEntry, ok := jobSummaries[0].(map[string]any)
				require.True(t, ok)
				assert.Contains(t, jobEntry, "thingArn")
				assert.NotContains(t, jobEntry, "thingName")
				jobExecSummary, ok := jobEntry["jobExecutionSummary"].(map[string]any)
				require.True(t, ok)
				assert.EqualValues(t, "CANCELED", jobExecSummary["status"])

				forThing := iotOK(t, h, http.MethodGet, "/things/list-thing/jobs", nil)
				thingSummaries, _ := forThing["executionSummaries"].([]any)
				require.Len(t, thingSummaries, 1)
				thingEntry, ok := thingSummaries[0].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "list-job", thingEntry["jobId"])
				assert.Contains(t, thingEntry, "jobExecutionSummary")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iot.NewInMemoryBackend()
			h := iot.NewHandler(b, nil)

			if tt.setup != nil {
				tt.setup(t, b)
			}

			tt.run(t, h)
		})
	}
}

// TestNewOps_JobTemplate tests JobTemplate CRUD.
func TestJobTemplate(t *testing.T) {
	t.Parallel()
	h := newIoTHandlerBatch1(t)

	// CreateJobTemplate
	out := iotOK(t, h, http.MethodPost, "/job-templates/my-template", map[string]any{
		"description": "test template",
		"document":    `{"version":"1.0"}`,
	})
	if out["jobTemplateId"] != "my-template" {
		t.Errorf("jobTemplateId mismatch: %v", out)
	}

	// DescribeJobTemplate
	out2 := iotOK(t, h, http.MethodGet, "/job-templates/my-template", nil)
	if out2["jobTemplateId"] != "my-template" {
		t.Errorf("describe template mismatch: %v", out2)
	}

	// ListJobTemplates
	out3 := iotOK(t, h, http.MethodGet, "/job-templates", nil)
	templates, _ := out3["jobTemplates"].([]any)
	if len(templates) != 1 {
		t.Errorf("expected 1 template, got %d", len(templates))
	}

	// DeleteJobTemplate
	iotOK(t, h, http.MethodDelete, "/job-templates/my-template", nil)

	// Verify deletion
	iotExpectError(t, h, "/job-templates/my-template")
}
