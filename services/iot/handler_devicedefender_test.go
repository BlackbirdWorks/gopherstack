package iot_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	iotsdk "github.com/aws/aws-sdk-go-v2/service/iot"
	iottypes "github.com/aws/aws-sdk-go-v2/service/iot/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iot"
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

// TestListActiveViolationsAndViolationEvents_SuppressedAlertsFilter covers
// ListActiveViolations/ListViolationEvents' listSuppressedAlerts filter, a
// directly-seedable flag standing in for the security-profile Behavior's
// SuppressAlerts setting; see device_defender.go's ActiveViolation.Suppressed
// doc comment.
func TestListActiveViolationsAndViolationEvents_SuppressedAlertsFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		listSuppressedAlerts *bool
		name                 string
		wantViolationIDs     []string
	}{
		{
			name:                 "unset_returns_both",
			listSuppressedAlerts: nil,
			wantViolationIDs:     []string{"viol-suppressed", "viol-unsuppressed"},
		},
		{
			name:                 "true_returns_only_suppressed",
			listSuppressedAlerts: aws.Bool(true),
			wantViolationIDs:     []string{"viol-suppressed"},
		},
		{
			name:                 "false_returns_only_unsuppressed",
			listSuppressedAlerts: aws.Bool(false),
			wantViolationIDs:     []string{"viol-unsuppressed"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, b := newIoTSDKClient(t)
			ctx := t.Context()

			_, err := b.SeedActiveViolation(&iot.SeedActiveViolationInput{
				ViolationID: "viol-suppressed", ThingName: "thing-x", SecurityProfileName: "profile-x",
				Suppressed: true,
			})
			require.NoError(t, err)
			_, err = b.SeedActiveViolation(&iot.SeedActiveViolationInput{
				ViolationID: "viol-unsuppressed", ThingName: "thing-x", SecurityProfileName: "profile-x",
				Suppressed: false,
			})
			require.NoError(t, err)

			activeOut, err := client.ListActiveViolations(ctx, &iotsdk.ListActiveViolationsInput{
				ListSuppressedAlerts: tt.listSuppressedAlerts,
			})
			require.NoError(t, err)

			gotActiveIDs := make([]string, 0, len(activeOut.ActiveViolations))
			for _, v := range activeOut.ActiveViolations {
				gotActiveIDs = append(gotActiveIDs, *v.ViolationId)
			}
			assert.ElementsMatch(t, tt.wantViolationIDs, gotActiveIDs)

			eventsOut, err := client.ListViolationEvents(ctx, &iotsdk.ListViolationEventsInput{
				StartTime:            aws.Time(time.Unix(0, 0)),
				EndTime:              aws.Time(time.Now().Add(time.Hour)),
				ListSuppressedAlerts: tt.listSuppressedAlerts,
			})
			require.NoError(t, err)

			gotEventIDs := make([]string, 0, len(eventsOut.ViolationEvents))
			for _, e := range eventsOut.ViolationEvents {
				gotEventIDs = append(gotEventIDs, *e.ViolationId)
			}
			assert.ElementsMatch(t, tt.wantViolationIDs, gotEventIDs)
		})
	}
}

// TestListActiveViolationsAndViolationEvents_BehaviorCriteriaTypeFilter
// covers ListActiveViolations/ListViolationEvents' behaviorCriteriaType
// filter, resolved live against each violation's owning security profile's
// persisted Behaviors via securityProfileBehaviorCriteriaTypeLocked
// (types.BehaviorCriteriaType).
func TestListActiveViolationsAndViolationEvents_BehaviorCriteriaTypeFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		filter           iottypes.BehaviorCriteriaType
		name             string
		wantViolationIDs []string
	}{
		{
			name:             "unset_returns_all",
			filter:           "",
			wantViolationIDs: []string{"viol-static", "viol-statistical", "viol-ml"},
		},
		{
			name:             "static",
			filter:           iottypes.BehaviorCriteriaTypeStatic,
			wantViolationIDs: []string{"viol-static"},
		},
		{
			name:             "statistical",
			filter:           iottypes.BehaviorCriteriaTypeStatistical,
			wantViolationIDs: []string{"viol-statistical"},
		},
		{
			name:             "machine_learning",
			filter:           iottypes.BehaviorCriteriaTypeMachineLearning,
			wantViolationIDs: []string{"viol-ml"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, b := newIoTSDKClient(t)
			ctx := t.Context()

			_, err := b.CreateSecurityProfile(&iot.CreateSecurityProfileInput{
				SecurityProfileName: "profile-x",
				Behaviors: []iot.SecurityProfileBehavior{
					{
						Name:   "static-behavior",
						Metric: "aws:num-connections",
						Criteria: &iot.SecurityProfileBehaviorCriteria{
							ComparisonOperator: "greater-than",
						},
					},
					{
						Name:   "statistical-behavior",
						Metric: "aws:message-byte-size",
						Criteria: &iot.SecurityProfileBehaviorCriteria{
							StatisticalThreshold: &iot.SecurityProfileStatisticalThreshold{Statistic: "p90"},
						},
					},
					{
						Name:   "ml-behavior",
						Metric: "aws:num-messages-sent",
						Criteria: &iot.SecurityProfileBehaviorCriteria{
							MlDetectionConfig: &iot.SecurityProfileMLDetectionConfig{ConfidenceLevel: "MEDIUM"},
						},
					},
				},
			})
			require.NoError(t, err)

			_, err = b.SeedActiveViolation(&iot.SeedActiveViolationInput{
				ViolationID: "viol-static", ThingName: "thing-x", SecurityProfileName: "profile-x",
				Behavior: &iot.ViolationBehavior{Name: "static-behavior", Metric: "aws:num-connections"},
			})
			require.NoError(t, err)
			_, err = b.SeedActiveViolation(&iot.SeedActiveViolationInput{
				ViolationID: "viol-statistical", ThingName: "thing-x", SecurityProfileName: "profile-x",
				Behavior: &iot.ViolationBehavior{Name: "statistical-behavior", Metric: "aws:message-byte-size"},
			})
			require.NoError(t, err)
			_, err = b.SeedActiveViolation(&iot.SeedActiveViolationInput{
				ViolationID: "viol-ml", ThingName: "thing-x", SecurityProfileName: "profile-x",
				Behavior: &iot.ViolationBehavior{Name: "ml-behavior", Metric: "aws:num-messages-sent"},
			})
			require.NoError(t, err)

			activeOut, err := client.ListActiveViolations(ctx, &iotsdk.ListActiveViolationsInput{
				BehaviorCriteriaType: tt.filter,
			})
			require.NoError(t, err)

			gotActiveIDs := make([]string, 0, len(activeOut.ActiveViolations))
			for _, v := range activeOut.ActiveViolations {
				gotActiveIDs = append(gotActiveIDs, *v.ViolationId)
			}
			assert.ElementsMatch(t, tt.wantViolationIDs, gotActiveIDs)

			eventsOut, err := client.ListViolationEvents(ctx, &iotsdk.ListViolationEventsInput{
				StartTime:            aws.Time(time.Unix(0, 0)),
				EndTime:              aws.Time(time.Now().Add(time.Hour)),
				BehaviorCriteriaType: tt.filter,
			})
			require.NoError(t, err)

			gotEventIDs := make([]string, 0, len(eventsOut.ViolationEvents))
			for _, e := range eventsOut.ViolationEvents {
				gotEventIDs = append(gotEventIDs, *e.ViolationId)
			}
			assert.ElementsMatch(t, tt.wantViolationIDs, gotEventIDs)
		})
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

// TestDeviceDefender_AuditFinding_WireFieldsAndFilters covers AuditFinding's
// isSuppressed/reasonForNonComplianceCode/reasonForNonCompliance/taskStartTime
// wire fields (types.AuditFinding, v1.77.4) and ListAuditFindings'
// checkName/taskId/listSuppressedFindings filters via the real POST
// /audit/findings route.
func TestDeviceDefender_AuditFinding_WireFieldsAndFilters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		listBody map[string]any
		check    func(t *testing.T, findings []map[string]any)
		name     string
	}{
		{
			name: "wire_fields_surfaced",
			check: func(t *testing.T, findings []map[string]any) {
				t.Helper()
				require.Len(t, findings, 2)

				byCheck := map[string]map[string]any{}
				for _, f := range findings {
					byCheck[f["checkName"].(string)] = f
				}

				suppressed := byCheck["SUPPRESSED_CHECK"]
				assert.Equal(t, true, suppressed["isSuppressed"])
				assert.Equal(t, "AUDIT_TASK", suppressed["reasonForNonComplianceCode"])
				assert.Equal(t, "sample reason", suppressed["reasonForNonCompliance"])
				assert.NotEmpty(t, suppressed["taskStartTime"],
					"taskStartTime should be auto-derived from the referenced AuditTask")

				plain := byCheck["PLAIN_CHECK"]
				assert.Nil(t, plain["isSuppressed"])
			},
		},
		{
			name:     "filter_by_checkName",
			listBody: map[string]any{"checkName": "SUPPRESSED_CHECK"},
			check: func(t *testing.T, findings []map[string]any) {
				t.Helper()
				require.Len(t, findings, 1)
				assert.Equal(t, "SUPPRESSED_CHECK", findings[0]["checkName"])
			},
		},
		{
			name:     "filter_by_listSuppressedFindings_true",
			listBody: map[string]any{"listSuppressedFindings": true},
			check: func(t *testing.T, findings []map[string]any) {
				t.Helper()
				require.Len(t, findings, 1)
				assert.Equal(t, true, findings[0]["isSuppressed"])
			},
		},
		{
			name:     "filter_by_listSuppressedFindings_false",
			listBody: map[string]any{"listSuppressedFindings": false},
			check: func(t *testing.T, findings []map[string]any) {
				t.Helper()
				require.Len(t, findings, 1)
				assert.Equal(t, "PLAIN_CHECK", findings[0]["checkName"])
			},
		},
		{
			name:     "filter_by_unknown_checkName_returns_empty",
			listBody: map[string]any{"checkName": "NO_SUCH_CHECK"},
			check: func(t *testing.T, findings []map[string]any) {
				t.Helper()
				assert.Empty(t, findings)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newDeviceDefenderTestHandler(t)

			taskID, err := b.StartOnDemandAuditTask([]string{"SUPPRESSED_CHECK"})
			require.NoError(t, err)

			b.SeedAuditFinding(&iot.AuditFinding{
				CheckName:                  "SUPPRESSED_CHECK",
				TaskID:                     taskID,
				IsSuppressed:               true,
				ReasonForNonComplianceCode: "AUDIT_TASK",
				ReasonForNonCompliance:     "sample reason",
			})
			b.SeedAuditFinding(&iot.AuditFinding{
				CheckName: "PLAIN_CHECK",
			})

			out := iotOK(t, h, http.MethodPost, "/audit/findings", tt.listBody)
			raw, _ := out["findings"].([]any)

			findings := make([]map[string]any, len(raw))
			for i, f := range raw {
				findings[i], _ = f.(map[string]any)
			}

			tt.check(t, findings)
		})
	}
}

// TestListAuditFindings_ResourceIdentifierFilter covers
// ListAuditFindings.resourceIdentifier: every field SET on the filter must
// be present and equal on the finding's own identifier (audit.go's
// ResourceIdentifier).
func TestListAuditFindings_ResourceIdentifierFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		filter        *iottypes.ResourceIdentifier
		wantFindingID string // "" means expect zero matches
		name          string
	}{
		{
			name:          "matches_by_deviceCertificateId",
			filter:        &iottypes.ResourceIdentifier{DeviceCertificateId: aws.String("cert-1")},
			wantFindingID: "cert-finding",
		},
		{
			name:          "mismatched_deviceCertificateId_returns_empty",
			filter:        &iottypes.ResourceIdentifier{DeviceCertificateId: aws.String("cert-does-not-exist")},
			wantFindingID: "",
		},
		{
			name: "matches_by_policyVersionIdentifier_policyName_alone",
			filter: &iottypes.ResourceIdentifier{
				PolicyVersionIdentifier: &iottypes.PolicyVersionIdentifier{
					PolicyName: aws.String("overly-permissive-policy"),
				},
			},
			wantFindingID: "policy-finding",
		},
		{
			name: "policyVersionIdentifier_with_wrong_versionId_does_not_match",
			filter: &iottypes.ResourceIdentifier{
				PolicyVersionIdentifier: &iottypes.PolicyVersionIdentifier{
					PolicyName:      aws.String("overly-permissive-policy"),
					PolicyVersionId: aws.String("not-the-real-version"),
				},
			},
			wantFindingID: "",
		},
		{
			name: "does_not_match_a_finding_with_no_resourceIdentifier_at_all",
			filter: &iottypes.ResourceIdentifier{
				IamRoleArn: aws.String("arn:aws:iam::000000000000:role/AnyRole"),
			},
			wantFindingID: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, b := newIoTSDKClient(t)

			b.SeedAuditFinding(&iot.AuditFinding{
				FindingID: "cert-finding",
				CheckName: "DEVICE_CERTIFICATE_EXPIRING_CHECK",
				NonCompliantResource: &iot.NonCompliantResource{
					ResourceType:       "CLIENT_CERT",
					ResourceIdentifier: &iot.ResourceIdentifier{DeviceCertificateID: "cert-1"},
				},
			})
			b.SeedAuditFinding(&iot.AuditFinding{
				FindingID: "policy-finding",
				CheckName: "IOT_POLICY_OVERLY_PERMISSIVE_CHECK",
				NonCompliantResource: &iot.NonCompliantResource{
					ResourceType: "IOT_POLICY",
					ResourceIdentifier: &iot.ResourceIdentifier{
						PolicyVersionIdentifier: &iot.PolicyVersionIdentifier{
							PolicyName:      "overly-permissive-policy",
							PolicyVersionID: "2",
						},
					},
				},
			})
			b.SeedAuditFinding(&iot.AuditFinding{
				FindingID: "bare-finding",
				CheckName: "CONFORMANCE_CHECK",
			})

			out, err := client.ListAuditFindings(t.Context(), &iotsdk.ListAuditFindingsInput{
				ResourceIdentifier: tt.filter,
			})
			require.NoError(t, err)

			var gotIDs []string
			for _, f := range out.Findings {
				gotIDs = append(gotIDs, *f.FindingId)
			}

			if tt.wantFindingID == "" {
				assert.Empty(t, gotIDs)

				return
			}
			assert.Equal(t, []string{tt.wantFindingID}, gotIDs)
		})
	}
}

// TestStartAuditMitigationActionsTask_TargetResolution covers
// auditMitigationFindingIDs' (device_defender.go) target resolution:
// auditTaskId and auditCheckToReasonCodeFilter combine as AND, and an empty
// reason-code list for a check means "any reason code for that check".
func TestStartAuditMitigationActionsTask_TargetResolution(t *testing.T) {
	t.Parallel()

	const actionsMapping = "checkA"

	tests := []struct {
		target         *iottypes.AuditMitigationActionsTaskTarget
		name           string
		wantFindingIDs []string
	}{
		{
			name: "reasonCodeFilter_matches_only_the_listed_reason_code",
			target: &iottypes.AuditMitigationActionsTaskTarget{
				AuditCheckToReasonCodeFilter: map[string][]string{actionsMapping: {"R1"}},
			},
			wantFindingIDs: []string{"finding-r1"},
		},
		{
			name: "reasonCodeFilter_empty_list_matches_any_reason_code_for_that_check",
			target: &iottypes.AuditMitigationActionsTaskTarget{
				AuditCheckToReasonCodeFilter: map[string][]string{actionsMapping: {}},
			},
			wantFindingIDs: []string{"finding-r1", "finding-r2"},
		},
		{
			name: "auditTaskId_and_reasonCodeFilter_combine_as_AND_not_first_match_wins",
			target: &iottypes.AuditMitigationActionsTaskTarget{
				AuditTaskId:                  aws.String("task-other"),
				AuditCheckToReasonCodeFilter: map[string][]string{actionsMapping: {"R1"}},
			},
			// finding-r1 is checkA/R1 but belongs to task-1, not task-other:
			// the combined filter must reject it, not accept it just because
			// auditTaskId used to win outright.
			wantFindingIDs: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, b := newIoTSDKClient(t)

			b.SeedAuditFinding(&iot.AuditFinding{
				FindingID:                  "finding-r1",
				CheckName:                  actionsMapping,
				TaskID:                     "task-1",
				ReasonForNonComplianceCode: "R1",
			})
			b.SeedAuditFinding(&iot.AuditFinding{
				FindingID:                  "finding-r2",
				CheckName:                  actionsMapping,
				TaskID:                     "task-1",
				ReasonForNonComplianceCode: "R2",
			})

			_, err := client.CreateMitigationAction(t.Context(), &iotsdk.CreateMitigationActionInput{
				ActionName: aws.String("update-ca-certs"),
				RoleArn:    aws.String("arn:aws:iam::000000000000:role/MitigationRole"),
				ActionParams: &iottypes.MitigationActionParams{
					UpdateCACertificateParams: &iottypes.UpdateCACertificateParams{
						Action: iottypes.CACertificateUpdateActionDeactivate,
					},
				},
			})
			require.NoError(t, err)

			_, err = client.StartAuditMitigationActionsTask(t.Context(), &iotsdk.StartAuditMitigationActionsTaskInput{
				TaskId: aws.String("resolution-task"),
				Target: tt.target,
				AuditCheckToActionsMapping: map[string][]string{
					actionsMapping: {"update-ca-certs"},
				},
			})
			require.NoError(t, err)

			// Real ListAuditMitigationActionsExecutionsInput marks BOTH taskId
			// and findingId as required client-side fields (confirmed by the
			// generated SDK client itself rejecting a call that omits
			// either) -- so, unlike gopherstack's own handler (which treats
			// findingId as an optional filter), a real caller must query per
			// candidate finding rather than list a task's executions in one
			// call.
			var gotFindingIDs []string
			for _, findingID := range []string{"finding-r1", "finding-r2"} {
				execOut, execErr := client.ListAuditMitigationActionsExecutions(
					t.Context(),
					&iotsdk.ListAuditMitigationActionsExecutionsInput{
						TaskId:    aws.String("resolution-task"),
						FindingId: aws.String(findingID),
					},
				)
				require.NoError(t, execErr)
				if len(execOut.ActionsExecutions) > 0 {
					gotFindingIDs = append(gotFindingIDs, findingID)
				}
			}
			assert.ElementsMatch(t, tt.wantFindingIDs, gotFindingIDs)
		})
	}
}

// TestDetectMitigationActionsTaskSummary_WireShape covers
// DetectMitigationActionsTaskSummary: wire field is "actionsDefinition" (full
// MitigationAction objects), not "actions" (bare names); Describe and List
// share the same rich type; violationEventOccurrenceRange round-trips.
func TestDetectMitigationActionsTaskSummary_WireShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fetch func(t *testing.T, ctx context.Context, client *iotsdk.Client) *iottypes.DetectMitigationActionsTaskSummary
		name  string
	}{
		{
			name: "describeDetectMitigationActionsTask",
			fetch: func(t *testing.T, ctx context.Context, client *iotsdk.Client) *iottypes.DetectMitigationActionsTaskSummary {
				t.Helper()

				out, err := client.DescribeDetectMitigationActionsTask(
					ctx,
					&iotsdk.DescribeDetectMitigationActionsTaskInput{
						TaskId: aws.String("wire-shape-task"),
					},
				)
				require.NoError(t, err)

				return out.TaskSummary
			},
		},
		{
			name: "listDetectMitigationActionsTasks_returns_the_same_shape_as_describe",
			fetch: func(t *testing.T, ctx context.Context, client *iotsdk.Client) *iottypes.DetectMitigationActionsTaskSummary {
				t.Helper()

				out, err := client.ListDetectMitigationActionsTasks(ctx, &iotsdk.ListDetectMitigationActionsTasksInput{
					StartTime: aws.Time(time.Unix(0, 0)),
					EndTime:   aws.Time(time.Now().Add(time.Hour)),
				})
				require.NoError(t, err)
				require.Len(t, out.Tasks, 1)

				return &out.Tasks[0]
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, b := newIoTSDKClient(t)
			ctx := t.Context()

			_, err := b.SeedActiveViolation(&iot.SeedActiveViolationInput{
				ViolationID: "wire-viol", ThingName: "wire-thing", SecurityProfileName: "wire-profile",
			})
			require.NoError(t, err)

			_, err = client.CreateMitigationAction(ctx, &iotsdk.CreateMitigationActionInput{
				ActionName: aws.String("enable-logging"),
				RoleArn:    aws.String("arn:aws:iam::000000000000:role/MitigationRole"),
				ActionParams: &iottypes.MitigationActionParams{
					EnableIoTLoggingParams: &iottypes.EnableIoTLoggingParams{
						LogLevel:          iottypes.LogLevelInfo,
						RoleArnForLogging: aws.String("arn:aws:iam::000000000000:role/LoggingRole"),
					},
				},
			})
			require.NoError(t, err)

			_, err = client.StartDetectMitigationActionsTask(ctx, &iotsdk.StartDetectMitigationActionsTaskInput{
				TaskId:  aws.String("wire-shape-task"),
				Target:  &iottypes.DetectMitigationActionsTaskTarget{ViolationIds: []string{"wire-viol"}},
				Actions: []string{"enable-logging"},
				ViolationEventOccurrenceRange: &iottypes.ViolationEventOccurrenceRange{
					StartTime: aws.Time(time.Unix(1000, 0)),
					EndTime:   aws.Time(time.Unix(2000, 0)),
				},
			})
			require.NoError(t, err)

			summary := tt.fetch(t, ctx, client)

			require.NotNil(t, summary)
			require.Len(t, summary.ActionsDefinition, 1,
				`actionsDefinition must be populated, not the invented "actions" field`)
			assert.Equal(t, "enable-logging", *summary.ActionsDefinition[0].Name)
			assert.Equal(t, "arn:aws:iam::000000000000:role/MitigationRole", *summary.ActionsDefinition[0].RoleArn)
			require.NotNil(t, summary.ActionsDefinition[0].ActionParams)
			require.NotNil(t, summary.ActionsDefinition[0].ActionParams.EnableIoTLoggingParams)
			assert.Equal(t,
				iottypes.LogLevelInfo,
				summary.ActionsDefinition[0].ActionParams.EnableIoTLoggingParams.LogLevel,
			)

			require.NotNil(t, summary.ViolationEventOccurrenceRange)
			assert.Equal(t, int64(1000), summary.ViolationEventOccurrenceRange.StartTime.Unix())
			assert.Equal(t, int64(2000), summary.ViolationEventOccurrenceRange.EndTime.Unix())

			// DetectMitigationActionExecution.executionStartDate was
			// field-diffed against
			// awsRestjson1_deserializeDocumentDetectMitigationActionExecution
			// and found wire-keyed "executionStartTime" instead of the real
			// "executionStartDate" -- a real client's deserializer would
			// never have found it. Confirmed fixed via the real client here.
			execOut, err := client.ListDetectMitigationActionsExecutions(
				ctx,
				&iotsdk.ListDetectMitigationActionsExecutionsInput{
					TaskId: aws.String("wire-shape-task"),
				},
			)
			require.NoError(t, err)
			require.Len(t, execOut.ActionsExecutions, 1)
			require.NotNil(t, execOut.ActionsExecutions[0].ExecutionStartDate,
				`executionStartDate must be populated, not the invented "executionStartTime" field`)
		})
	}
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
