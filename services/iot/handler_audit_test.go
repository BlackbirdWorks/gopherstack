package iot_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestUpdateAccountAuditConfiguration_ChecksSurviveIndependentUpdates guards
// gopherstack-c8ge: AuditCheckConfigurations is a map[checkName]*AuditCheckConfig
// (types.UpdateAccountAuditConfigurationInput) that a real client only ever
// names the checks it's changing in. Enabling check B in a later call must
// not disable check A, which an earlier call enabled and this one never
// mentions.
func TestUpdateAccountAuditConfiguration_ChecksSurviveIndependentUpdates(t *testing.T) {
	t.Parallel()
	h := newIoTHandler(t)

	// Update A: enable one check.
	iotOK(t, h, http.MethodPatch, "/audit/configuration", map[string]any{
		"auditCheckConfigurations": map[string]any{
			"DEVICE_CERTIFICATE_EXPIRING_CHECK": map[string]any{"enabled": true},
		},
	})

	// Update B: enable a different check, without mentioning A's.
	iotOK(t, h, http.MethodPatch, "/audit/configuration", map[string]any{
		"auditCheckConfigurations": map[string]any{
			"CA_CERTIFICATE_EXPIRING_CHECK": map[string]any{"enabled": true},
		},
	})

	out := iotOK(t, h, http.MethodGet, "/audit/configuration", nil)
	checks, ok := out["auditCheckConfigurations"].(map[string]any)
	if !ok {
		t.Fatalf("expected auditCheckConfigurations map, got %#v", out["auditCheckConfigurations"])
	}

	aCheck, ok := checks["DEVICE_CERTIFICATE_EXPIRING_CHECK"].(map[string]any)
	if !ok {
		t.Fatalf("DEVICE_CERTIFICATE_EXPIRING_CHECK must survive an Update that never mentioned it, got %#v", checks)
	}
	if aCheck["enabled"] != true {
		t.Errorf("DEVICE_CERTIFICATE_EXPIRING_CHECK.enabled = %v, want true", aCheck["enabled"])
	}

	bCheck, ok := checks["CA_CERTIFICATE_EXPIRING_CHECK"].(map[string]any)
	if !ok {
		t.Fatalf("expected CA_CERTIFICATE_EXPIRING_CHECK to be present, got %#v", checks)
	}
	if bCheck["enabled"] != true {
		t.Errorf("CA_CERTIFICATE_EXPIRING_CHECK.enabled = %v, want true", bCheck["enabled"])
	}
}

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
//
// Real AWS IoT's ListAuditFindings is POST /audit/findings (its filter
// fields are carried in a JSON body), not GET -- confirmed against
// aws-sdk-go-v2/service/iot@v1.77.4's serializers.go http bindings. A
// previous version of this test (and gopherstack's routing) used GET, which
// no real client ever sends.
func TestAuditFinding(t *testing.T) {
	t.Parallel()
	h, _ := newHandlerForBatch3Test(t)

	// List (empty)
	out := iotOK(t, h, http.MethodPost, "/audit/findings", nil)
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

// TestUpdateAccountAuditConfiguration_ConfigurationFieldSurvives guards a
// previously-unmodeled member: real types.AuditCheckConfiguration
// (v1.77.4) has both "enabled" and "configuration" (map[string]string);
// this backend's AuditCheckConfig only ever had "enabled", so a real
// client's UpdateAccountAuditConfiguration call setting per-check
// configuration values had them silently dropped, and
// DescribeAccountAuditConfiguration could never surface them.
func TestUpdateAccountAuditConfiguration_ConfigurationFieldSurvives(t *testing.T) {
	t.Parallel()
	h := newIoTHandler(t)

	iotOK(t, h, http.MethodPatch, "/audit/configuration", map[string]any{
		"auditCheckConfigurations": map[string]any{
			"CA_CERTIFICATE_EXPIRING_CHECK": map[string]any{
				"enabled":       true,
				"configuration": map[string]any{"caCertificateMaxExpirationDays": "30"},
			},
		},
	})

	out := iotOK(t, h, http.MethodGet, "/audit/configuration", nil)
	checks, ok := out["auditCheckConfigurations"].(map[string]any)
	require.True(t, ok, "expected auditCheckConfigurations map, got %#v", out["auditCheckConfigurations"])

	check, ok := checks["CA_CERTIFICATE_EXPIRING_CHECK"].(map[string]any)
	require.True(t, ok, "expected check entry, got %#v", checks)

	assert.Equal(t, true, check["enabled"])
	cfg, ok := check["configuration"].(map[string]any)
	require.True(t, ok, "expected configuration map to survive round-trip, got %#v", check)
	assert.Equal(t, "30", cfg["caCertificateMaxExpirationDays"])
}

func TestScheduledAudit_ListFieldsAndDescribeWireShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		frequency  string
		dayOfMonth string
		dayOfWeek  string
	}{
		{name: "monthly", frequency: "MONTHLY", dayOfMonth: "15"},
		{name: "weekly", frequency: "WEEKLY", dayOfWeek: "MON"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newIoTHandler(t)

			body := map[string]any{
				"frequency": tt.frequency,
				"tags":      []map[string]any{{"Key": "env", "Value": "prod"}},
			}
			if tt.dayOfMonth != "" {
				body["dayOfMonth"] = tt.dayOfMonth
			}
			if tt.dayOfWeek != "" {
				body["dayOfWeek"] = tt.dayOfWeek
			}
			iotOK(t, h, http.MethodPost, "/audit/scheduledaudits/sched-"+tt.name, body)

			// DescribeScheduledAuditOutput (v1.77.4) has no "tags" member --
			// the internal Tags field (populated above, non-empty) must not
			// leak onto the wire.
			describeOut := iotOK(t, h, http.MethodGet, "/audit/scheduledaudits/sched-"+tt.name, nil)
			_, hasTags := describeOut["tags"]
			assert.False(t, hasTags, "DescribeScheduledAudit must not leak an internal tags field: %#v", describeOut)

			// ListScheduledAudits' ScheduledAuditMetadata (v1.77.4) has
			// dayOfMonth/dayOfWeek -- the backend already tracks both per
			// scheduled audit but never surfaced them here.
			listOut := iotOK(t, h, http.MethodGet, "/audit/scheduledaudits", nil)
			audits, ok := listOut["scheduledAudits"].([]any)
			require.True(t, ok)
			require.Len(t, audits, 1)

			entry, ok := audits[0].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, tt.dayOfMonth, entry["dayOfMonth"])
			assert.Equal(t, tt.dayOfWeek, entry["dayOfWeek"])
		})
	}
}
