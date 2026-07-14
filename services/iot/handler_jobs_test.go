package iot_test

import (
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

	// Cancel an execution (creates it)
	iotOK(t, h, http.MethodPut, "/jobs/test-job/things/my-thing/cancel", nil)

	// ListJobExecutionsForJob
	out := iotOK(t, h, http.MethodGet, "/jobs/test-job/things", nil)
	execs, _ := out["executionSummaries"].([]any)
	if len(execs) != 1 {
		t.Errorf("expected 1 execution for job, got %d", len(execs))
	}

	// ListJobExecutionsForThing
	out2 := iotOK(t, h, http.MethodGet, "/things/my-thing/jobs", nil)
	execs2, _ := out2["executionSummaries"].([]any)
	if len(execs2) != 1 {
		t.Errorf("expected 1 execution for thing, got %d", len(execs2))
	}
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

	// CancelJobExecution (creates execution if not exists).
	iotOK(t, h, http.MethodPut, "/jobs/exec-job/things/my-thing/cancel", nil)

	// DescribeJobExecution
	out := iotOK(t, h, http.MethodGet, "/jobs/exec-job/things/my-thing", nil)
	exec, ok := out["execution"].(map[string]any)
	if !ok {
		t.Fatalf("expected execution in response: %v", out)
	}
	if exec["jobId"] != "exec-job" {
		t.Errorf("exec jobId mismatch: %v", exec)
	}

	// DeleteJobExecution
	iotOK(t, h, http.MethodDelete, "/jobs/exec-job/things/my-thing", nil)
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
