package iot_test

import (
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/iot"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newDeviceDefenderTestHandler(t *testing.T) (*iot.Handler, *iot.InMemoryBackend) {
	t.Helper()
	b := iot.NewInMemoryBackend()
	h := iot.NewHandler(b, nil)

	return h, b
}

// TestDeviceDefender_AuditMitigationTaskLifecycle covers
// StartAuditMitigationActionsTask, DescribeAuditMitigationActionsTask,
// ListAuditMitigationActionsTasks, ListAuditMitigationActionsExecutions, and
// CancelAuditMitigationActionsTask.
func TestDeviceDefender_AuditMitigationTaskLifecycle(t *testing.T) {
	t.Parallel()
	h, b := newDeviceDefenderTestHandler(t)

	b.SeedAuditFinding(&iot.AuditFinding{
		FindingID: "finding-1",
		CheckName: "DEVICE_CERTIFICATE_EXPIRING_CHECK",
	})

	created := iotOK(t, h, http.MethodPost, "/mitigationactions/actions/update-ca-certs", map[string]any{
		"roleArn": "arn:aws:iam::000000000000:role/MitigationRole",
	})
	actionID, _ := created["actionId"].(string)
	if actionID == "" {
		t.Fatalf("expected actionId from CreateMitigationAction, got %v", created)
	}

	// StartAuditMitigationActionsTask
	startOut := iotOK(t, h, http.MethodPost, "/audit/mitigationactions/tasks/task-1", map[string]any{
		"target": map[string]any{
			"findingIds": []string{"finding-1"},
		},
		"auditCheckToActionsMapping": map[string]any{
			"DEVICE_CERTIFICATE_EXPIRING_CHECK": []string{"update-ca-certs"},
		},
	})
	if startOut["taskId"] != "task-1" {
		t.Fatalf("expected taskId=task-1, got %v", startOut)
	}

	// Starting the same task ID twice must fail (already exists).
	startAgain := iotRequest(t, h, http.MethodPost, "/audit/mitigationactions/tasks/task-1", map[string]any{
		"target":                     map[string]any{"findingIds": []string{"finding-1"}},
		"auditCheckToActionsMapping": map[string]any{"DEVICE_CERTIFICATE_EXPIRING_CHECK": []string{"update-ca-certs"}},
	})
	if startAgain.Code == http.StatusOK {
		t.Fatalf("expected error re-starting task-1, got 200: %s", startAgain.Body.String())
	}

	// Validation: missing target/mapping.
	invalid := iotRequest(t, h, http.MethodPost, "/audit/mitigationactions/tasks/task-invalid", map[string]any{})
	if invalid.Code == http.StatusOK {
		t.Fatalf("expected validation error for missing target, got 200")
	}

	// DescribeAuditMitigationActionsTask
	describeOut := iotOK(t, h, http.MethodGet, "/audit/mitigationactions/tasks/task-1", nil)
	if describeOut["taskStatus"] != "IN_PROGRESS" {
		t.Errorf("expected taskStatus=IN_PROGRESS, got %v", describeOut)
	}
	target, _ := describeOut["target"].(map[string]any)
	findingIDs, _ := target["findingIds"].([]any)
	if len(findingIDs) != 1 || findingIDs[0] != "finding-1" {
		t.Errorf("expected target.findingIds=[finding-1], got %v", target)
	}

	// Unknown task -> ResourceNotFoundException.
	iotExpectError(t, h, "/audit/mitigationactions/tasks/does-not-exist")

	// ListAuditMitigationActionsTasks
	listOut := iotOK(t, h, http.MethodGet, "/audit/mitigationactions/tasks", nil)
	tasks, _ := listOut["tasks"].([]any)
	if len(tasks) != 1 {
		t.Fatalf("expected 1 audit mitigation task, got %d: %v", len(tasks), listOut)
	}

	// ListAuditMitigationActionsExecutions
	execOut := iotOK(t, h, http.MethodGet, "/audit/mitigationactions/executions?taskId=task-1", nil)
	execs, _ := execOut["actionsExecutions"].([]any)
	if len(execs) != 1 {
		t.Fatalf("expected 1 execution, got %d: %v", len(execs), execOut)
	}
	exec, _ := execs[0].(map[string]any)
	if exec["findingId"] != "finding-1" || exec["actionName"] != "update-ca-certs" {
		t.Errorf("unexpected execution record: %v", exec)
	}
	if exec["actionId"] != actionID {
		t.Errorf("expected execution actionId=%s, got %v", actionID, exec["actionId"])
	}

	// CancelAuditMitigationActionsTask
	iotOK(t, h, http.MethodPut, "/audit/mitigationactions/tasks/task-1/cancel", nil)

	describeAfterCancel := iotOK(t, h, http.MethodGet, "/audit/mitigationactions/tasks/task-1", nil)
	if describeAfterCancel["taskStatus"] != "CANCELED" {
		t.Errorf("expected taskStatus=CANCELED after cancel, got %v", describeAfterCancel)
	}

	// Cancelling an unknown task ID returns ResourceNotFoundException,
	// matching real AWS IoT (see gopherstack-ep0r: this backend previously
	// let unknown task IDs succeed as a silent no-op).
	unknownRec := doRefRequest(t, h, http.MethodPut, "/audit/mitigationactions/tasks/unknown-task/cancel", nil, nil)
	if unknownRec.Code != http.StatusNotFound {
		t.Errorf("expected 404 ResourceNotFoundException cancelling unknown task, got %d: %s",
			unknownRec.Code, unknownRec.Body.String())
	}

	// Cancelling an already-CANCELED task returns InvalidRequestException
	// (not in progress).
	recanceled := doRefRequest(t, h, http.MethodPut, "/audit/mitigationactions/tasks/task-1/cancel", nil, nil)
	if recanceled.Code != http.StatusBadRequest {
		t.Errorf("expected 400 InvalidRequestException re-cancelling task-1, got %d: %s",
			recanceled.Code, recanceled.Body.String())
	}
}

// TestDeviceDefender_DetectMitigationTaskLifecycle covers
// StartDetectMitigationActionsTask, DescribeDetectMitigationActionsTask,
// ListDetectMitigationActionsTasks, ListDetectMitigationActionsExecutions, and
// CancelDetectMitigationActionsTask.
func TestDeviceDefender_DetectMitigationTaskLifecycle(t *testing.T) {
	t.Parallel()
	h, b := newDeviceDefenderTestHandler(t)

	if _, err := b.SeedActiveViolation(&iot.SeedActiveViolationInput{
		ViolationID:         "viol-1",
		ThingName:           "thing-1",
		SecurityProfileName: "profile-1",
	}); err != nil {
		t.Fatalf("SeedActiveViolation: %v", err)
	}

	startOut := iotOK(t, h, http.MethodPut, "/detect/mitigationactions/tasks/task-2", map[string]any{
		"target": map[string]any{
			"violationIds": []string{"viol-1"},
		},
		"actions": []string{"EnableIoTLogging"},
	})
	if startOut["taskId"] != "task-2" {
		t.Fatalf("expected taskId=task-2, got %v", startOut)
	}

	// Starting the same task ID twice must fail.
	startAgain := iotRequest(t, h, http.MethodPut, "/detect/mitigationactions/tasks/task-2", map[string]any{
		"target":  map[string]any{"violationIds": []string{"viol-1"}},
		"actions": []string{"EnableIoTLogging"},
	})
	if startAgain.Code == http.StatusOK {
		t.Fatalf("expected error re-starting task-2, got 200")
	}

	// Validation: missing actions.
	invalid := iotRequest(t, h, http.MethodPut, "/detect/mitigationactions/tasks/task-invalid", map[string]any{
		"target": map[string]any{"violationIds": []string{"viol-1"}},
	})
	if invalid.Code == http.StatusOK {
		t.Fatalf("expected validation error for missing actions, got 200")
	}

	describeOut := iotOK(t, h, http.MethodGet, "/detect/mitigationactions/tasks/task-2", nil)
	summary, _ := describeOut["taskSummary"].(map[string]any)
	if summary["taskStatus"] != "IN_PROGRESS" {
		t.Errorf("expected taskStatus=IN_PROGRESS, got %v", summary)
	}

	iotExpectError(t, h, "/detect/mitigationactions/tasks/does-not-exist")

	listOut := iotOK(t, h, http.MethodGet, "/detect/mitigationactions/tasks", nil)
	tasks, _ := listOut["tasks"].([]any)
	if len(tasks) != 1 {
		t.Fatalf("expected 1 detect mitigation task, got %d: %v", len(tasks), listOut)
	}

	execOut := iotOK(t, h, http.MethodGet, "/detect/mitigationactions/executions?taskId=task-2", nil)
	execs, _ := execOut["actionsExecutions"].([]any)
	if len(execs) != 1 {
		t.Fatalf("expected 1 execution, got %d: %v", len(execs), execOut)
	}
	exec, _ := execs[0].(map[string]any)
	if exec["violationId"] != "viol-1" || exec["thingName"] != "thing-1" {
		t.Errorf("unexpected execution record: %v", exec)
	}

	// CancelDetectMitigationActionsTask
	iotOK(t, h, http.MethodPut, "/detect/mitigationactions/tasks/task-2/cancel", nil)

	describeAfterCancel := iotOK(t, h, http.MethodGet, "/detect/mitigationactions/tasks/task-2", nil)
	summaryAfter, _ := describeAfterCancel["taskSummary"].(map[string]any)
	if summaryAfter["taskStatus"] != "CANCELED" {
		t.Errorf("expected taskStatus=CANCELED after cancel, got %v", summaryAfter)
	}

	// Unlike audit mitigation cancel, detect cancel is a brand-new op with no
	// legacy behavior to preserve: unknown task IDs are a real 404.
	cancelUnknown := iotRequest(t, h, http.MethodPut, "/detect/mitigationactions/tasks/unknown-task/cancel", nil)
	if cancelUnknown.Code == http.StatusOK {
		t.Fatalf("expected error cancelling unknown detect task, got 200")
	}
}

// TestDeviceDefender_Violations covers ListActiveViolations, ListViolationEvents,
// and PutVerificationStateOnViolation.
func TestDeviceDefender_Violations(t *testing.T) {
	t.Parallel()
	h, b := newDeviceDefenderTestHandler(t)

	if _, err := b.SeedActiveViolation(&iot.SeedActiveViolationInput{
		ViolationID:         "viol-a",
		ThingName:           "thing-a",
		SecurityProfileName: "profile-x",
		Behavior:            &iot.ViolationBehavior{Name: "ExcessiveMessages", Metric: "aws:num-messages-sent"},
	}); err != nil {
		t.Fatalf("SeedActiveViolation: %v", err)
	}
	if _, err := b.SeedActiveViolation(&iot.SeedActiveViolationInput{
		ViolationID:         "viol-b",
		ThingName:           "thing-b",
		SecurityProfileName: "profile-x",
	}); err != nil {
		t.Fatalf("SeedActiveViolation: %v", err)
	}

	// SeedActiveViolation requires thingName and securityProfileName.
	if _, err := b.SeedActiveViolation(&iot.SeedActiveViolationInput{ThingName: "orphan"}); err == nil {
		t.Fatalf("expected validation error seeding violation without securityProfileName")
	}

	all := iotOK(t, h, http.MethodGet, "/active-violations", nil)
	allViolations, _ := all["activeViolations"].([]any)
	if len(allViolations) != 2 {
		t.Fatalf("expected 2 active violations, got %d: %v", len(allViolations), all)
	}

	filtered := iotOK(t, h, http.MethodGet, "/active-violations?thingName=thing-a", nil)
	filteredViolations, _ := filtered["activeViolations"].([]any)
	if len(filteredViolations) != 1 {
		t.Fatalf("expected 1 active violation for thing-a, got %d", len(filteredViolations))
	}

	// PutVerificationStateOnViolation
	iotOK(t, h, http.MethodPost, "/violations/verification-state/viol-a", map[string]any{
		"verificationState":            "TRUE_POSITIVE",
		"verificationStateDescription": "confirmed by security team",
	})

	confirmed := iotOK(t, h, http.MethodGet, "/active-violations?verificationState=TRUE_POSITIVE", nil)
	confirmedViolations, _ := confirmed["activeViolations"].([]any)
	if len(confirmedViolations) != 1 {
		t.Fatalf("expected 1 TRUE_POSITIVE violation, got %d", len(confirmedViolations))
	}
	v, _ := confirmedViolations[0].(map[string]any)
	if v["violationId"] != "viol-a" || v["verificationStateDescription"] != "confirmed by security team" {
		t.Errorf("unexpected violation after verification update: %v", v)
	}

	// Unknown violation ID.
	putUnknown := iotRequest(t, h, http.MethodPost, "/violations/verification-state/unknown-violation", map[string]any{
		"verificationState": "BENIGN_POSITIVE",
	})
	if putUnknown.Code == http.StatusOK {
		t.Fatalf("expected error for unknown violation, got 200")
	}

	// Missing verificationState is a validation error.
	putMissingState := iotRequest(t, h, http.MethodPost, "/violations/verification-state/viol-a", map[string]any{})
	if putMissingState.Code == http.StatusOK {
		t.Fatalf("expected validation error for missing verificationState, got 200")
	}

	// ListViolationEvents: seeding each violation also records an event.
	events := iotOK(t, h, http.MethodGet, "/violation-events", nil)
	eventList, _ := events["violationEvents"].([]any)
	if len(eventList) != 2 {
		t.Fatalf("expected 2 violation events, got %d: %v", len(eventList), events)
	}

	eventsForThingA := iotOK(t, h, http.MethodGet, "/violation-events?thingName=thing-a", nil)
	eventsForThingAList, _ := eventsForThingA["violationEvents"].([]any)
	if len(eventsForThingAList) != 1 {
		t.Fatalf("expected 1 violation event for thing-a, got %d", len(eventsForThingAList))
	}
}

// TestDeviceDefender_AuditFindingRelatedResources covers
// ListRelatedResourcesForAuditFinding.
func TestDeviceDefender_AuditFindingRelatedResources(t *testing.T) {
	t.Parallel()
	h, b := newDeviceDefenderTestHandler(t)

	seeded := b.SeedAuditFinding(&iot.AuditFinding{
		CheckName: "CA_CERTIFICATE_EXPIRING_CHECK",
		RelatedResources: []map[string]any{
			{"resourceType": "CA_CERTIFICATE", "resourceIdentifier": map[string]any{"caCertificateId": "ca-1"}},
		},
	})
	if seeded.FindingID == "" {
		t.Fatalf("expected SeedAuditFinding to assign a findingId")
	}

	out := iotOK(t, h, http.MethodGet, "/audit/relatedResources?findingId="+seeded.FindingID, nil)
	resources, _ := out["relatedResources"].([]any)
	if len(resources) != 1 {
		t.Fatalf("expected 1 related resource, got %d: %v", len(resources), out)
	}
	resource, _ := resources[0].(map[string]any)
	if resource["resourceType"] != "CA_CERTIFICATE" {
		t.Errorf("unexpected related resource: %v", resource)
	}

	// Unknown finding.
	iotExpectError(t, h, "/audit/relatedResources?findingId=does-not-exist")
}

// TestDeviceDefender_DeleteAccountAuditConfiguration covers
// DeleteAccountAuditConfiguration (idempotent, including when unconfigured).
func TestDeviceDefender_DeleteAccountAuditConfiguration(t *testing.T) {
	t.Parallel()
	h, _ := newDeviceDefenderTestHandler(t)

	// Configure, then verify it is visible.
	iotOK(t, h, http.MethodPatch, "/audit/configuration", map[string]any{
		"roleArn": "arn:aws:iam::000000000000:role/AuditRole",
	})
	cfg := iotOK(t, h, http.MethodGet, "/audit/configuration", nil)
	if cfg["roleArn"] != "arn:aws:iam::000000000000:role/AuditRole" {
		t.Fatalf("expected configured roleArn, got %v", cfg)
	}

	// DeleteAccountAuditConfiguration clears it.
	iotOK(t, h, http.MethodDelete, "/audit/configuration", nil)
	cfgAfter := iotOK(t, h, http.MethodGet, "/audit/configuration", nil)
	if cfgAfter["roleArn"] != nil && cfgAfter["roleArn"] != "" {
		t.Errorf("expected cleared roleArn, got %v", cfgAfter)
	}

	// Deleting again (already unconfigured) is idempotent, not an error.
	iotOK(t, h, http.MethodDelete, "/audit/configuration", nil)
}

func TestGetBehaviorModelTrainingSummaries(t *testing.T) {
	t.Parallel()

	t.Run("round_trip", func(t *testing.T) {
		t.Parallel()

		h, b := newRefHandler()

		_, err := b.CreateSecurityProfile(&iot.CreateSecurityProfileInput{SecurityProfileName: "sp1"})
		require.NoError(t, err)

		b.AddBehaviorModelTrainingSummaryInternal("sp1", iot.BehaviorModelTrainingSummary{
			BehaviorName: "excessive-connects",
			ModelStatus:  "ACTIVE",
		})

		rec := doRefRequest(t, h, http.MethodGet,
			"/behavior-model-training/summaries?securityProfileName=sp1", nil, nil)
		require.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "excessive-connects")
	})

	t.Run("unknown_profile_404", func(t *testing.T) {
		t.Parallel()

		h, _ := newRefHandler()

		rec := doRefRequest(t, h, http.MethodGet,
			"/behavior-model-training/summaries?securityProfileName=no-such", nil, nil)
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})
}
