package iot_test

import (
	"net/http"
	"testing"
)

// TestBatch2_AuditConfiguration tests audit configuration.
func TestAuditConfiguration(t *testing.T) {
	t.Parallel()
	h := newIoTHandler(t)

	// Describe empty
	out := iotOK(t, h, http.MethodGet, "/audit/configuration", nil)
	if out == nil {
		t.Error("expected audit configuration response")
	}

	// Update
	iotOK(t, h, http.MethodPatch, "/audit/configuration", map[string]any{
		"roleArn": "arn:aws:iam::000000000000:role/AuditRole",
		"auditCheckConfigurations": map[string]any{
			"DEVICE_CERTIFICATE_EXPIRING_CHECK": map[string]any{"enabled": true},
		},
	})

	// Start on-demand task
	out2 := iotOK(t, h, http.MethodPost, "/audit/tasks", map[string]any{
		"targetCheckNames": []any{"DEVICE_CERTIFICATE_EXPIRING_CHECK"},
	})
	taskID, _ := out2["taskId"].(string)
	if taskID == "" {
		t.Errorf("expected taskId: %v", out2)
	}

	// Describe task
	out3 := iotOK(t, h, http.MethodGet, "/audit/tasks/"+taskID, nil)
	if out3["taskId"] != taskID {
		t.Errorf("task describe mismatch: %v", out3)
	}

	// List tasks
	out4 := iotOK(t, h, http.MethodGet, "/audit/tasks", nil)
	tasks, _ := out4["tasks"].([]any)
	if len(tasks) != 1 {
		t.Errorf("expected 1 task, got %d", len(tasks))
	}
}

// TestBatch3_AuditSuppression tests audit suppression CRUD.
func TestAuditSuppression(t *testing.T) {
	t.Parallel()
	h, _ := newHandlerForBatch3Test(t)

	body := map[string]any{
		"checkName":            "CA_CERTIFICATE_EXPIRING_CHECK",
		"resourceIdentifier":   map[string]any{"caCertificateId": "abc123"},
		"suppressIndefinitely": true,
	}

	// Create
	iotOK(t, h, http.MethodPost, "/audit/suppressions", body)

	// Describe
	out := iotOK(t, h, http.MethodGet, "/audit/suppressions/describe", body)
	if out["checkName"] != "CA_CERTIFICATE_EXPIRING_CHECK" {
		t.Errorf("describe mismatch: %v", out)
	}

	// Update
	updateBody := map[string]any{
		"checkName":            "CA_CERTIFICATE_EXPIRING_CHECK",
		"resourceIdentifier":   map[string]any{"caCertificateId": "abc123"},
		"suppressIndefinitely": false,
		"description":          "updated",
	}
	iotOK(t, h, http.MethodPatch, "/audit/suppressions/update", updateBody)

	// List
	out2 := iotOK(t, h, http.MethodGet, "/audit/suppressions/list", nil)
	suppressions, _ := out2["suppressions"].([]any)
	if len(suppressions) != 1 {
		t.Errorf("expected 1 suppression, got %d", len(suppressions))
	}

	// Delete
	iotOK(t, h, http.MethodPost, "/audit/suppressions/delete", body)

	// List again - should be empty
	out3 := iotOK(t, h, http.MethodGet, "/audit/suppressions/list", nil)
	suppressions2, _ := out3["suppressions"].([]any)
	if len(suppressions2) != 0 {
		t.Errorf("expected 0 suppressions after delete, got %d", len(suppressions2))
	}
}

// TestBatch3_EventConfigurations tests DescribeEventConfigurations and UpdateEventConfigurations.
func TestEventConfigurations(t *testing.T) {
	t.Parallel()
	h, _ := newHandlerForBatch3Test(t)

	// Describe (empty)
	out := iotOK(t, h, http.MethodGet, "/event-configurations", nil)
	evts, _ := out["eventConfigurations"].(map[string]any)
	if evts == nil {
		t.Errorf("expected eventConfigurations map, got %v", out)
	}

	// Update
	iotOK(t, h, http.MethodPatch, "/event-configurations", map[string]any{
		"eventConfigurations": map[string]any{
			"THING": map[string]any{"Enabled": true},
		},
	})

	// Describe again
	out2 := iotOK(t, h, http.MethodGet, "/event-configurations", nil)
	evts2, _ := out2["eventConfigurations"].(map[string]any)
	thing, _ := evts2["THING"].(map[string]any)
	if thing["Enabled"] != true {
		t.Errorf("expected THING enabled=true, got %v", evts2)
	}
}

// TestBatch3_AuditFinding tests ListAuditFindings (empty list since we have no direct create endpoint).
func TestAuditFinding(t *testing.T) {
	t.Parallel()
	h, _ := newHandlerForBatch3Test(t)

	// List (empty)
	out := iotOK(t, h, http.MethodGet, "/audit/findings", nil)
	findings, _ := out["findings"].([]any)
	if findings == nil {
		t.Errorf("expected findings array, got %v", out)
	}
}

// TestNewOps_ScheduledAudit tests ScheduledAudit CRUD.
func TestScheduledAudit(t *testing.T) {
	t.Parallel()
	h := newIoTHandlerBatch1(t)

	// CreateScheduledAudit
	out := iotOK(t, h, http.MethodPost, "/audit/scheduledaudits/my-audit", map[string]any{
		"frequency":        "WEEKLY",
		"dayOfWeek":        "MON",
		"targetCheckNames": []string{"DEVICE_CERTIFICATE_EXPIRING_CHECK"},
	})
	if out["scheduledAuditArn"] == "" {
		t.Errorf("expected scheduledAuditArn: %v", out)
	}

	// DescribeScheduledAudit
	out2 := iotOK(t, h, http.MethodGet, "/audit/scheduledaudits/my-audit", nil)
	if out2["scheduledAuditName"] != "my-audit" {
		t.Errorf("describe mismatch: %v", out2)
	}

	// ListScheduledAudits
	out3 := iotOK(t, h, http.MethodGet, "/audit/scheduledaudits", nil)
	audits, _ := out3["scheduledAudits"].([]any)
	if len(audits) != 1 {
		t.Errorf("expected 1 audit, got %d", len(audits))
	}

	// UpdateScheduledAudit
	iotOK(t, h, http.MethodPatch, "/audit/scheduledaudits/my-audit", map[string]any{
		"frequency": "DAILY",
	})

	// DeleteScheduledAudit
	iotOK(t, h, http.MethodDelete, "/audit/scheduledaudits/my-audit", nil)

	iotExpectError(t, h, "/audit/scheduledaudits/my-audit")
}

// TestNewOps_MitigationAction tests MitigationAction CRUD.
func TestMitigationAction(t *testing.T) {
	t.Parallel()
	h := newIoTHandlerBatch1(t)

	// CreateMitigationAction
	out := iotOK(t, h, http.MethodPost, "/mitigationactions/actions/my-action", map[string]any{
		"roleArn": "arn:aws:iam::000000000000:role/MitigationRole",
		"actionParams": map[string]any{
			"updateDeviceCertificateParams": map[string]any{"action": "DEACTIVATE"},
		},
	})
	if out["actionArn"] == "" {
		t.Errorf("expected actionArn: %v", out)
	}

	// DescribeMitigationAction
	out2 := iotOK(t, h, http.MethodGet, "/mitigationactions/actions/my-action", nil)
	if out2["actionName"] != "my-action" {
		t.Errorf("describe mismatch: %v", out2)
	}

	// ListMitigationActions
	out3 := iotOK(t, h, http.MethodGet, "/mitigationactions/actions", nil)
	actions, _ := out3["actionIdentifiers"].([]any)
	if len(actions) != 1 {
		t.Errorf("expected 1 action, got %d", len(actions))
	}

	// UpdateMitigationAction
	iotOK(t, h, http.MethodPatch, "/mitigationactions/actions/my-action", map[string]any{
		"roleArn": "arn:aws:iam::000000000000:role/UpdatedRole",
	})

	// DeleteMitigationAction
	iotOK(t, h, http.MethodDelete, "/mitigationactions/actions/my-action", nil)

	iotExpectError(t, h, "/mitigationactions/actions/my-action")
}
