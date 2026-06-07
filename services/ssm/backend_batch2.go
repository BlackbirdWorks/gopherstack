package ssm

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/google/uuid"
)

var (
	ErrResourceDataSyncNotFound    = errors.New("ResourceDataSyncNotFoundException")
	ErrAutomationExecutionNotFound = errors.New("AutomationExecutionNotFoundException")
	ErrExecutionPreviewNotFound    = errors.New("ExecutionPreviewNotFoundException")
	ErrResourcePolicyNotFound      = errors.New("ResourcePolicyInvalidRequest")
	ErrResourceDataSyncExists      = errors.New("ResourceDataSyncAlreadyExistsException")
)

const (
	automationStatusInProgress = "InProgress"
	automationStatusStopped    = "Stopped"
	calendarStateOpen          = "OPEN"
	policyIDPrefix             = "pol-"
	previewIDPrefix            = "ep-"
	connectionStatusConnected  = "connected"
	settingStatusCustomized    = "Customized"
	settingStatusDefault       = "Default"
	platformTypeLinux          = "Linux"
	mwExecutionScheduleHours   = 24
)

// --- ResourceDataSync ---

// CreateResourceDataSync stores a new resource data sync configuration.
func (b *InMemoryBackend) CreateResourceDataSync(input *CreateResourceDataSyncInput) (*StubOutput, error) {
	b.mu.Lock("CreateResourceDataSync")
	defer b.mu.Unlock()

	syncName := input.SyncName
	if syncName == "" {
		syncName = "default-sync"
	}

	if _, exists := b.resourceDataSyncs[syncName]; exists {
		return nil, ErrResourceDataSyncExists
	}

	b.resourceDataSyncs[syncName] = &ResourceDataSync{
		SyncName:        syncName,
		SyncType:        input.SyncType,
		LastStatus:      "InProgress",
		SyncCreatedTime: time.Now().UTC(),
		LastSyncTime:    time.Now().UTC(),
	}

	return &StubOutput{}, nil
}

// DeleteResourceDataSync removes a resource data sync by name.
func (b *InMemoryBackend) DeleteResourceDataSync(input *DeleteResourceDataSyncInput) (*StubOutput, error) {
	b.mu.Lock("DeleteResourceDataSync")
	defer b.mu.Unlock()

	if input.SyncName == "" {
		return &StubOutput{}, nil
	}

	if _, exists := b.resourceDataSyncs[input.SyncName]; !exists {
		return nil, fmt.Errorf("%w: %q", ErrResourceDataSyncNotFound, input.SyncName)
	}

	delete(b.resourceDataSyncs, input.SyncName)

	return &StubOutput{}, nil
}

// ListResourceDataSync returns all resource data syncs.
func (b *InMemoryBackend) ListResourceDataSync(_ *ListResourceDataSyncInput) (*ListResourceDataSyncOutputFull, error) {
	b.mu.RLock("ListResourceDataSync")
	defer b.mu.RUnlock()

	items := make([]ResourceDataSync, 0, len(b.resourceDataSyncs))
	for _, s := range b.resourceDataSyncs {
		items = append(items, *s)
	}

	sort.Slice(items, func(i, k int) bool {
		return items[i].SyncName < items[k].SyncName
	})

	return &ListResourceDataSyncOutputFull{ResourceDataSyncItems: items}, nil
}

// UpdateResourceDataSync updates an existing resource data sync.
func (b *InMemoryBackend) UpdateResourceDataSync(input *UpdateResourceDataSyncInput) (*StubOutput, error) {
	b.mu.Lock("UpdateResourceDataSync")
	defer b.mu.Unlock()

	if input.SyncName == "" {
		return &StubOutput{}, nil
	}

	if sync, exists := b.resourceDataSyncs[input.SyncName]; exists {
		sync.LastSyncTime = time.Now().UTC()
	}

	return &StubOutput{}, nil
}

// --- Activation lifecycle ---

// DeregisterManagedInstance removes the activation associated with a managed instance ID.
// The InstanceID field is treated as the ActivationID in this in-memory implementation.
func (b *InMemoryBackend) DeregisterManagedInstance(input *DeregisterManagedInstanceInput) (*StubOutput, error) {
	b.mu.Lock("DeregisterManagedInstance")
	defer b.mu.Unlock()

	// Try to match by ActivationID (InstanceID in the request maps to ActivationID here).
	if _, exists := b.activations[input.InstanceID]; exists {
		delete(b.activations, input.InstanceID)
		delete(b.miscResourceTags, input.InstanceID)
	}

	return &StubOutput{}, nil
}

// UpdateManagedInstanceRole updates the IAM role for a managed instance's activation.
func (b *InMemoryBackend) UpdateManagedInstanceRole(input *UpdateManagedInstanceRoleInput) (*StubOutput, error) {
	b.mu.Lock("UpdateManagedInstanceRole")
	defer b.mu.Unlock()

	if act, exists := b.activations[input.InstanceID]; exists {
		act.IamRole = input.IamRole
		b.activations[input.InstanceID] = act
	}

	return &StubOutput{}, nil
}

// --- Session Manager ---

// DescribeSessions returns sessions from the in-memory store.
func (b *InMemoryBackend) DescribeSessions(input *DescribeSessionsInput) (*DescribeSessionsOutputFull, error) {
	b.mu.RLock("DescribeSessions")
	defer b.mu.RUnlock()

	list := make([]Session, 0, len(b.sessions))
	for _, s := range b.sessions {
		if input.State == "" || s.Status == input.State {
			list = append(list, s)
		}
	}

	sort.Slice(list, func(i, k int) bool {
		return list[i].SessionID < list[k].SessionID
	})

	return &DescribeSessionsOutputFull{Sessions: list}, nil
}

// GetConnectionStatus returns the connection status of a target session.
func (b *InMemoryBackend) GetConnectionStatus(input *GetConnectionStatusInput) (*GetConnectionStatusOutputFull, error) {
	b.mu.RLock("GetConnectionStatus")
	defer b.mu.RUnlock()

	target := input.Target
	status := "notConnected"

	for _, s := range b.sessions {
		if s.Target == target && s.Status == sessionStatusConnected {
			status = connectionStatusConnected

			break
		}
	}

	return &GetConnectionStatusOutputFull{Target: target, Status: status}, nil
}

// GetAccessToken returns a mock access token for an active session.
func (b *InMemoryBackend) GetAccessToken(input *GetAccessTokenInput) (*GetAccessTokenOutputFull, error) {
	b.mu.RLock("GetAccessToken")
	defer b.mu.RUnlock()

	_ = input

	return &GetAccessTokenOutputFull{
		TokenValue: "gph-mock-access-token-" + uuid.NewString(),
	}, nil
}

// ResumeSession resumes a disconnected session.
func (b *InMemoryBackend) ResumeSession(input *ResumeSessionInput) (*ResumeSessionOutputFull, error) {
	b.mu.Lock("ResumeSession")
	defer b.mu.Unlock()

	sess, exists := b.sessions[input.SessionID]
	if !exists {
		return &ResumeSessionOutputFull{SessionID: input.SessionID, StreamURL: ""}, nil
	}

	sess.Status = sessionStatusConnected
	b.sessions[input.SessionID] = sess

	return &ResumeSessionOutputFull{
		SessionID:  sess.SessionID,
		StreamURL:  "wss://gopherstack.mock/" + sess.SessionID,
		TokenValue: "gph-resume-token-" + uuid.NewString(),
	}, nil
}

// StartAccessRequest creates an access request record.
func (b *InMemoryBackend) StartAccessRequest(input *StartAccessRequestInput) (*StartAccessRequestOutputFull, error) {
	b.mu.Lock("StartAccessRequest")
	defer b.mu.Unlock()

	_ = input

	return &StartAccessRequestOutputFull{
		AccessRequestID: "ar-" + uuid.NewString(),
	}, nil
}

// --- Service Settings ---

// GetServiceSetting returns the value for a service setting.
func (b *InMemoryBackend) GetServiceSetting(input *GetServiceSettingInput) (*GetServiceSettingOutputFull, error) {
	b.mu.RLock("GetServiceSetting")
	defer b.mu.RUnlock()

	if s, exists := b.serviceSettings[input.SettingID]; exists {
		return &GetServiceSettingOutputFull{ServiceSetting: s}, nil
	}

	return &GetServiceSettingOutputFull{ServiceSetting: &ServiceSetting{
		SettingID:    input.SettingID,
		SettingValue: "",
		Status:       settingStatusDefault,
	}}, nil
}

// UpdateServiceSetting stores a custom value for a service setting.
func (b *InMemoryBackend) UpdateServiceSetting(input *UpdateServiceSettingInput) (*StubOutput, error) {
	b.mu.Lock("UpdateServiceSetting")
	defer b.mu.Unlock()

	b.serviceSettings[input.SettingID] = &ServiceSetting{
		SettingID:    input.SettingID,
		SettingValue: input.SettingValue,
		Status:       settingStatusCustomized,
	}

	return &StubOutput{}, nil
}

// ResetServiceSetting removes any custom value for a service setting.
func (b *InMemoryBackend) ResetServiceSetting(input *ResetServiceSettingInput) (*ResetServiceSettingOutputFull, error) {
	b.mu.Lock("ResetServiceSetting")
	defer b.mu.Unlock()

	delete(b.serviceSettings, input.SettingID)

	return &ResetServiceSettingOutputFull{ServiceSetting: &ServiceSetting{
		SettingID: input.SettingID,
		Status:    settingStatusDefault,
	}}, nil
}

// --- Resource Policies ---

// GetResourcePolicies returns policies attached to a resource.
func (b *InMemoryBackend) GetResourcePolicies(input *GetResourcePoliciesInput) (*GetResourcePoliciesOutputFull, error) {
	b.mu.RLock("GetResourcePolicies")
	defer b.mu.RUnlock()

	policies := b.resourcePolicies[input.ResourceARN]
	list := make([]ResourcePolicy, 0, len(policies))

	for _, p := range policies {
		list = append(list, *p)
	}

	return &GetResourcePoliciesOutputFull{Policies: list}, nil
}

// PutResourcePolicy attaches a policy to a resource.
func (b *InMemoryBackend) PutResourcePolicy(input *PutResourcePolicyInput) (*PutResourcePolicyOutputFull, error) {
	b.mu.Lock("PutResourcePolicy")
	defer b.mu.Unlock()

	policyID := policyIDPrefix + uuid.NewString()
	policy := &ResourcePolicy{
		PolicyID:   policyID,
		PolicyHash: uuid.NewString(),
		Policy:     input.Policy,
	}
	b.resourcePolicies[input.ResourceARN] = append(b.resourcePolicies[input.ResourceARN], policy)

	return &PutResourcePolicyOutputFull{PolicyID: policyID, PolicyHash: policy.PolicyHash}, nil
}

// DeleteResourcePolicy removes a policy from a resource.
func (b *InMemoryBackend) DeleteResourcePolicy(input *DeleteResourcePolicyInput) (*StubOutput, error) {
	b.mu.Lock("DeleteResourcePolicy")
	defer b.mu.Unlock()

	if input.ResourceARN == "" {
		return &StubOutput{}, nil
	}

	existing := b.resourcePolicies[input.ResourceARN]
	updated := existing[:0]

	for _, p := range existing {
		if p.PolicyID != input.PolicyID {
			updated = append(updated, p)
		}
	}

	b.resourcePolicies[input.ResourceARN] = updated

	return &StubOutput{}, nil
}

// --- Parameter Labels ---

// LabelParameterVersion adds labels to a specific parameter version.
// When ParameterVersion is 0, labels are applied to the latest version.
func (b *InMemoryBackend) LabelParameterVersion(
	input *LabelParameterVersionInput,
) (*LabelParameterVersionOutputFull, error) {
	b.mu.Lock("LabelParameterVersion")
	defer b.mu.Unlock()

	if input.Name == "" {
		return &LabelParameterVersionOutputFull{
			InvalidLabels: []string{},
			AddedLabels:   input.Labels,
		}, nil
	}

	param, exists := b.parameters[input.Name]
	if !exists {
		return nil, fmt.Errorf("%w: %q", ErrParameterNotFound, input.Name)
	}

	version := input.ParameterVersion
	if version == 0 {
		version = param.Version
	}

	if b.parameterLabels[input.Name] == nil {
		b.parameterLabels[input.Name] = make(map[int64][]string)
	}

	b.parameterLabels[input.Name][version] = appendUniqueLabels(
		b.parameterLabels[input.Name][version], input.Labels,
	)

	return &LabelParameterVersionOutputFull{
		InvalidLabels: []string{},
		AddedLabels:   input.Labels,
	}, nil
}

// UnlabelParameterVersion removes labels from a specific parameter version.
// When ParameterVersion is 0, labels are removed from the latest version.
func (b *InMemoryBackend) UnlabelParameterVersion(
	input *UnlabelParameterVersionInput,
) (*UnlabelParameterVersionOutputFull, error) {
	b.mu.Lock("UnlabelParameterVersion")
	defer b.mu.Unlock()

	if input.Name == "" {
		return &UnlabelParameterVersionOutputFull{InvalidLabels: []string{}, RemovedLabels: input.Labels}, nil
	}

	version := input.ParameterVersion
	if version == 0 {
		if param, exists := b.parameters[input.Name]; exists {
			version = param.Version
		}
	}

	removedSet := make(map[string]bool, len(input.Labels))
	for _, l := range input.Labels {
		removedSet[l] = true
	}

	existing := b.parameterLabels[input.Name][version]
	kept := make([]string, 0, len(existing))

	for _, l := range existing {
		if !removedSet[l] {
			kept = append(kept, l)
		}
	}

	if b.parameterLabels[input.Name] != nil {
		b.parameterLabels[input.Name][version] = kept
	}

	return &UnlabelParameterVersionOutputFull{
		InvalidLabels: []string{},
		RemovedLabels: input.Labels,
	}, nil
}

func appendUniqueLabels(existing, newLabels []string) []string {
	seen := make(map[string]bool, len(existing))

	for _, l := range existing {
		seen[l] = true
	}

	for _, l := range newLabels {
		if !seen[l] {
			existing = append(existing, l)
			seen[l] = true
		}
	}

	return existing
}

// --- Automation Execution ---

// StartAutomationExecution creates a new automation execution.
func (b *InMemoryBackend) StartAutomationExecution(
	input *StartAutomationExecutionInput,
) (*StartAutomationExecutionOutputFull, error) {
	b.mu.Lock("StartAutomationExecution")
	defer b.mu.Unlock()

	execID := "auto-" + uuid.NewString()

	mode := input.Mode
	if mode == "" {
		mode = "Auto"
	}

	exec := &AutomationExecution{
		AutomationExecutionID: execID,
		DocumentName:          input.DocumentName,
		DocumentVersion:       input.DocumentVersion,
		Parameters:            input.Parameters,
		Status:                automationStatusInProgress,
		StartTime:             time.Now().UTC(),
		ExecutionType:         "Standard",
		Mode:                  mode,
	}
	b.automationExecutions[execID] = exec

	return &StartAutomationExecutionOutputFull{AutomationExecutionID: execID}, nil
}

// GetAutomationExecution returns an automation execution by ID.
func (b *InMemoryBackend) GetAutomationExecution(
	input *GetAutomationExecutionInput,
) (*GetAutomationExecutionOutputFull, error) {
	b.mu.RLock("GetAutomationExecution")
	defer b.mu.RUnlock()

	exec, exists := b.automationExecutions[input.AutomationExecutionID]
	if !exists {
		return nil, fmt.Errorf("%w: %q", ErrAutomationExecutionNotFound, input.AutomationExecutionID)
	}

	cp := *exec

	return &GetAutomationExecutionOutputFull{AutomationExecution: &cp}, nil
}

// DescribeAutomationExecutions returns all automation executions.
func (b *InMemoryBackend) DescribeAutomationExecutions(
	_ *DescribeAutomationExecutionsInput,
) (*DescribeAutomationExecutionsOutputFull, error) {
	b.mu.RLock("DescribeAutomationExecutions")
	defer b.mu.RUnlock()

	list := make([]AutomationExecution, 0, len(b.automationExecutions))
	for _, exec := range b.automationExecutions {
		list = append(list, *exec)
	}

	sort.Slice(list, func(i, k int) bool {
		return list[i].StartTime.Before(list[k].StartTime)
	})

	return &DescribeAutomationExecutionsOutputFull{AutomationExecutionMetadataList: list}, nil
}

// StopAutomationExecution marks an automation execution as stopped.
func (b *InMemoryBackend) StopAutomationExecution(input *StopAutomationExecutionInput) (*StubOutput, error) {
	b.mu.Lock("StopAutomationExecution")
	defer b.mu.Unlock()

	if exec, exists := b.automationExecutions[input.AutomationExecutionID]; exists {
		exec.Status = automationStatusStopped
		now := time.Now().UTC()
		exec.EndTime = &now
	}

	return &StubOutput{}, nil
}

// SendAutomationSignal sends a signal to an automation execution.
// Approve/Reject signals update the execution status accordingly.
func (b *InMemoryBackend) SendAutomationSignal(input *SendAutomationSignalInput) (*StubOutput, error) {
	b.mu.Lock("SendAutomationSignal")
	defer b.mu.Unlock()

	exec, exists := b.automationExecutions[input.AutomationExecutionID]
	if !exists {
		return &StubOutput{}, nil
	}

	switch input.SignalType {
	case "Approve":
		exec.Status = "Approved"
	case "Reject":
		exec.Status = "Rejected"
	case "StopStep":
		exec.Status = automationStatusStopped
	}

	return &StubOutput{}, nil
}

// DescribeAutomationStepExecutions returns step executions for an automation.
func (b *InMemoryBackend) DescribeAutomationStepExecutions(
	input *DescribeAutomationStepExecutionsInput,
) (*DescribeAutomationStepExecutionsOutputFull, error) {
	b.mu.RLock("DescribeAutomationStepExecutions")
	defer b.mu.RUnlock()

	exec, exists := b.automationExecutions[input.AutomationExecutionID]
	if !exists {
		return &DescribeAutomationStepExecutionsOutputFull{StepExecutions: []AutomationStepExec{}}, nil
	}

	return &DescribeAutomationStepExecutionsOutputFull{StepExecutions: exec.Steps}, nil
}

// StartChangeRequestExecution creates a change request automation execution.
func (b *InMemoryBackend) StartChangeRequestExecution(
	input *StartChangeRequestExecutionInput,
) (*StartChangeRequestExecutionOutputFull, error) {
	b.mu.Lock("StartChangeRequestExecution")
	defer b.mu.Unlock()

	execID := "auto-cr-" + uuid.NewString()
	exec := &AutomationExecution{
		AutomationExecutionID: execID,
		DocumentName:          input.DocumentName,
		Status:                automationStatusInProgress,
		StartTime:             time.Now().UTC(),
		ExecutionType:         "ChangeRequest",
	}
	b.automationExecutions[execID] = exec

	return &StartChangeRequestExecutionOutputFull{AutomationExecutionID: execID}, nil
}

// --- Execution Preview ---

// StartExecutionPreview creates an execution preview.
func (b *InMemoryBackend) StartExecutionPreview(
	input *StartExecutionPreviewInput,
) (*StartExecutionPreviewOutputFull, error) {
	b.mu.Lock("StartExecutionPreview")
	defer b.mu.Unlock()

	previewID := previewIDPrefix + uuid.NewString()
	b.executionPreviews[previewID] = &ExecutionPreview{
		ExecutionPreviewID: previewID,
		Status:             "Running",
		DocumentName:       input.DocumentName,
	}

	return &StartExecutionPreviewOutputFull{ExecutionPreviewID: previewID}, nil
}

// GetExecutionPreview returns an execution preview by ID.
func (b *InMemoryBackend) GetExecutionPreview(input *GetExecutionPreviewInput) (*GetExecutionPreviewOutputFull, error) {
	b.mu.RLock("GetExecutionPreview")
	defer b.mu.RUnlock()

	preview, exists := b.executionPreviews[input.ExecutionPreviewID]
	if !exists {
		return &GetExecutionPreviewOutputFull{ExecutionPreviewID: input.ExecutionPreviewID, Status: "Running"}, nil
	}

	cp := *preview

	return &GetExecutionPreviewOutputFull{
		ExecutionPreviewID: preview.ExecutionPreviewID,
		Status:             preview.Status,
		ExecutionPreview:   &cp,
	}, nil
}

// --- Calendar State ---

// GetCalendarState returns the current state of an SSM Change Calendar.
// When CalendarNames is provided, each name is looked up as a ChangeCalendar document.
// Non-existent names result in an error. The returned state is OPEN unless a
// ChangeCalendar document explicitly has a Closed state in its content.
func (b *InMemoryBackend) GetCalendarState(input *GetCalendarStateInput) (*GetCalendarStateOutputFull, error) {
	if len(input.CalendarNames) == 0 {
		return &GetCalendarStateOutputFull{State: calendarStateOpen}, nil
	}

	b.mu.RLock("GetCalendarState")
	defer b.mu.RUnlock()

	for _, name := range input.CalendarNames {
		doc, exists := b.documents[name]
		if !exists {
			return nil, fmt.Errorf("%w: calendar document %q not found", ErrDocumentNotFound, name)
		}

		if doc.DocumentType != "ChangeCalendar" {
			return nil, fmt.Errorf("%w: document %q is not a ChangeCalendar document", ErrValidationException, name)
		}
	}

	return &GetCalendarStateOutputFull{State: calendarStateOpen}, nil
}

// --- OpsItem summary / OpsMetadata list ---

// GetOpsSummary returns a summary count of ops items.
func (b *InMemoryBackend) GetOpsSummary(_ *GetOpsSummaryInput) (*GetOpsSummaryOutputFull, error) {
	b.mu.RLock("GetOpsSummary")
	defer b.mu.RUnlock()

	return &GetOpsSummaryOutputFull{
		Entities: []OpsSummaryEntity{
			{
				ID: "AWS:OpsItem",
				Data: map[string]OpsSummaryValue{
					"Count": {Count: len(b.opsItems), Unit: "Count"},
				},
			},
		},
	}, nil
}

// ListOpsMetadata returns all ops metadata entries.
func (b *InMemoryBackend) ListOpsMetadata(_ *ListOpsMetadataInput) (*ListOpsMetadataOutputFull, error) {
	b.mu.RLock("ListOpsMetadata")
	defer b.mu.RUnlock()

	list := make([]OpsMetadata, 0, len(b.opsMetadata))
	for _, m := range b.opsMetadata {
		list = append(list, m)
	}

	sort.Slice(list, func(i, k int) bool {
		return list[i].OpsMetadataArn < list[k].OpsMetadataArn
	})

	return &ListOpsMetadataOutputFull{OpsMetadataList: list}, nil
}

// --- Association operations ---

// UpdateAssociationStatus updates the status of an association.
func (b *InMemoryBackend) UpdateAssociationStatus(
	input *UpdateAssociationStatusInput,
) (*UpdateAssociationStatusOutputFull, error) {
	b.mu.Lock("UpdateAssociationStatus")
	defer b.mu.Unlock()

	for id, assoc := range b.associations {
		if assoc.InstanceID == input.InstanceID && assoc.Name == input.Name {
			if assoc.Overview == nil {
				assoc.Overview = &AssociationOverview{}
			}

			assoc.Overview.Status = input.AssociationStatus.Name
			b.associations[id] = assoc

			return &UpdateAssociationStatusOutputFull{AssociationDescription: assoc}, nil
		}
	}

	return nil, fmt.Errorf("%w: instance %q / name %q", ErrAssociationNotFound, input.InstanceID, input.Name)
}

// StartAssociationsOnce triggers a one-time run of the given associations.
func (b *InMemoryBackend) StartAssociationsOnce(input *StartAssociationsOnceInput) (*StubOutput, error) {
	b.mu.Lock("StartAssociationsOnce")
	defer b.mu.Unlock()

	now := time.Now().UTC()

	for _, assocID := range input.AssociationIDs {
		if assoc, exists := b.associations[assocID]; exists {
			assoc.LastUpdateAssociationDate = float64(now.Unix())
			b.associations[assocID] = assoc
		}
	}

	return &StubOutput{}, nil
}

// ListAssociationVersions returns the version history of an association.
func (b *InMemoryBackend) ListAssociationVersions(
	input *ListAssociationVersionsInput,
) (*ListAssociationVersionsOutputFull, error) {
	b.mu.RLock("ListAssociationVersions")
	defer b.mu.RUnlock()

	assoc, exists := b.associations[input.AssociationID]
	if !exists {
		return &ListAssociationVersionsOutputFull{AssociationVersions: []Association{}}, nil
	}

	return &ListAssociationVersionsOutputFull{
		AssociationVersions: []Association{assoc},
	}, nil
}

// DescribeAssociationExecutions returns execution history for an association.
func (b *InMemoryBackend) DescribeAssociationExecutions(
	input *DescribeAssociationExecutionsInput,
) (*DescribeAssociationExecutionsOutputFull, error) {
	b.mu.RLock("DescribeAssociationExecutions")
	defer b.mu.RUnlock()

	assoc, exists := b.associations[input.AssociationID]
	if !exists {
		return &DescribeAssociationExecutionsOutputFull{AssociationExecutions: []AssociationExecution{}}, nil
	}

	status := commandStatusSuccess
	if assoc.Overview != nil {
		status = assoc.Overview.Status
	}

	exec := AssociationExecution{
		AssociationID: assoc.AssociationID,
		ExecutionID:   uuid.NewString(),
		Status:        status,
		ExecutionDate: time.Unix(int64(assoc.LastUpdateAssociationDate), 0).UTC(),
	}

	return &DescribeAssociationExecutionsOutputFull{
		AssociationExecutions: []AssociationExecution{exec},
	}, nil
}

// DescribeAssociationExecutionTargets returns targets for an association execution.
func (b *InMemoryBackend) DescribeAssociationExecutionTargets(
	input *DescribeAssociationExecutionTargetsInput,
) (*DescribeAssociationExecutionTargetsOutputFull, error) {
	b.mu.RLock("DescribeAssociationExecutionTargets")
	defer b.mu.RUnlock()

	_ = input

	return &DescribeAssociationExecutionTargetsOutputFull{
		AssociationExecutionTargets: []AssociationExecutionTarget{},
	}, nil
}

// --- Maintenance Window Executions ---

// DescribeMaintenanceWindowExecutions returns execution records for a window.
func (b *InMemoryBackend) DescribeMaintenanceWindowExecutions(
	input *DescribeMaintenanceWindowExecutionsInput,
) (*DescribeMaintenanceWindowExecutionsOutputFull, error) {
	b.mu.RLock("DescribeMaintenanceWindowExecutions")
	defer b.mu.RUnlock()

	_ = input

	return &DescribeMaintenanceWindowExecutionsOutputFull{
		WindowExecutions: []MaintenanceWindowExecution{},
	}, nil
}

// DescribeMaintenanceWindowExecutionTasks returns task executions for a window execution.
func (b *InMemoryBackend) DescribeMaintenanceWindowExecutionTasks(
	input *DescribeMaintenanceWindowExecutionTasksInput,
) (*DescribeMaintenanceWindowExecutionTasksOutputFull, error) {
	b.mu.RLock("DescribeMaintenanceWindowExecutionTasks")
	defer b.mu.RUnlock()

	_ = input

	return &DescribeMaintenanceWindowExecutionTasksOutputFull{
		WindowExecutionTaskIdentities: []MaintenanceWindowExecutionTask{},
	}, nil
}

// DescribeMaintenanceWindowExecutionTaskInvocations returns invocations for a task.
func (b *InMemoryBackend) DescribeMaintenanceWindowExecutionTaskInvocations(
	input *DescribeMaintenanceWindowExecutionTaskInvocationsInput,
) (*DescribeMaintenanceWindowExecutionTaskInvocationsOutputFull, error) {
	b.mu.RLock("DescribeMaintenanceWindowExecutionTaskInvocations")
	defer b.mu.RUnlock()

	_ = input

	return &DescribeMaintenanceWindowExecutionTaskInvocationsOutputFull{
		WindowExecutionTaskInvocationIdentities: []MaintenanceWindowExecutionTaskInvocation{},
	}, nil
}

// DescribeMaintenanceWindowSchedule returns the upcoming schedule for a window.
func (b *InMemoryBackend) DescribeMaintenanceWindowSchedule(
	input *DescribeMaintenanceWindowScheduleInput,
) (*DescribeMaintenanceWindowScheduleOutputFull, error) {
	b.mu.RLock("DescribeMaintenanceWindowSchedule")
	defer b.mu.RUnlock()

	win, exists := b.maintenanceWindows[input.WindowID]
	if !exists {
		return &DescribeMaintenanceWindowScheduleOutputFull{
			ScheduledWindowExecutions: []ScheduledWindowExecution{},
		}, nil
	}

	return &DescribeMaintenanceWindowScheduleOutputFull{
		ScheduledWindowExecutions: []ScheduledWindowExecution{
			{
				WindowID:      win.WindowID,
				Name:          win.Name,
				ExecutionTime: time.Now().UTC().Add(mwExecutionScheduleHours * time.Hour).Format(time.RFC3339),
			},
		},
	}, nil
}

// GetMaintenanceWindowExecution returns a specific window execution.
func (b *InMemoryBackend) GetMaintenanceWindowExecution(
	input *GetMaintenanceWindowExecutionInput,
) (*GetMaintenanceWindowExecutionOutputFull, error) {
	b.mu.RLock("GetMaintenanceWindowExecution")
	defer b.mu.RUnlock()

	return &GetMaintenanceWindowExecutionOutputFull{
		WindowID:          input.WindowID,
		WindowExecutionID: input.WindowExecutionID,
		Status:            commandStatusSuccess,
	}, nil
}

// GetMaintenanceWindowExecutionTask returns a specific task within a window execution.
func (b *InMemoryBackend) GetMaintenanceWindowExecutionTask(
	input *GetMaintenanceWindowExecutionTaskInput,
) (*GetMaintenanceWindowExecutionTaskOutputFull, error) {
	b.mu.RLock("GetMaintenanceWindowExecutionTask")
	defer b.mu.RUnlock()

	_ = input

	return &GetMaintenanceWindowExecutionTaskOutputFull{
		Status: commandStatusSuccess,
	}, nil
}

// GetMaintenanceWindowExecutionTaskInvocation returns a specific task invocation.
func (b *InMemoryBackend) GetMaintenanceWindowExecutionTaskInvocation(
	input *GetMaintenanceWindowExecutionTaskInvocationInput,
) (*GetMaintenanceWindowExecutionTaskInvocationOutputFull, error) {
	b.mu.RLock("GetMaintenanceWindowExecutionTaskInvocation")
	defer b.mu.RUnlock()

	_ = input

	return &GetMaintenanceWindowExecutionTaskInvocationOutputFull{
		Status: commandStatusSuccess,
	}, nil
}

// --- Nodes ---

// ListNodes returns managed nodes derived from the activations store.
func (b *InMemoryBackend) ListNodes(_ *ListNodesInput) (*ListNodesOutputFull, error) {
	b.mu.RLock("ListNodes")
	defer b.mu.RUnlock()

	nodes := make([]NodeInfo, 0, len(b.activations))
	for _, act := range b.activations {
		nodes = append(nodes, NodeInfo{
			InstanceID:       act.ActivationID,
			PlatformType:     platformTypeLinux,
			AgentVersion:     "3.0.0",
			RegistrationDate: time.Unix(int64(act.CreatedDate), 0).UTC(),
		})
	}

	sort.Slice(nodes, func(i, k int) bool {
		return nodes[i].InstanceID < nodes[k].InstanceID
	})

	return &ListNodesOutputFull{Nodes: nodes}, nil
}

// ListNodesSummary returns a summary of managed nodes.
func (b *InMemoryBackend) ListNodesSummary(_ *ListNodesSummaryInput) (*ListNodesSummaryOutputFull, error) {
	b.mu.RLock("ListNodesSummary")
	defer b.mu.RUnlock()

	return &ListNodesSummaryOutputFull{
		Summary: []map[string]string{
			{"NodeCount": strconv.Itoa(len(b.activations))},
		},
	}, nil
}

// --- Instance associations ---

// DescribeEffectiveInstanceAssociations returns associations targeting an instance.
func (b *InMemoryBackend) DescribeEffectiveInstanceAssociations(
	input *DescribeEffectiveInstanceAssociationsInput,
) (*DescribeEffectiveInstanceAssociationsOutputFull, error) {
	b.mu.RLock("DescribeEffectiveInstanceAssociations")
	defer b.mu.RUnlock()

	var result []InstanceAssociationInfo

	for _, assoc := range b.associations {
		if assoc.InstanceID == input.InstanceID {
			result = append(result, InstanceAssociationInfo{
				AssociationID:      assoc.AssociationID,
				Name:               assoc.Name,
				DocumentVersion:    assoc.DocumentVersion,
				AssociationVersion: "1",
			})
		}
	}

	if result == nil {
		result = []InstanceAssociationInfo{}
	}

	return &DescribeEffectiveInstanceAssociationsOutputFull{Associations: result}, nil
}

// DescribeInstanceAssociationsStatus returns status of associations on an instance.
func (b *InMemoryBackend) DescribeInstanceAssociationsStatus(
	input *DescribeInstanceAssociationsStatusInput,
) (*DescribeInstanceAssociationsStatusOutputFull, error) {
	b.mu.RLock("DescribeInstanceAssociationsStatus")
	defer b.mu.RUnlock()

	var result []InstanceAssociationStatusInfo

	for _, assoc := range b.associations {
		if assoc.InstanceID == input.InstanceID {
			status := commandStatusSuccess
			if assoc.Overview != nil {
				status = assoc.Overview.Status
			}

			result = append(result, InstanceAssociationStatusInfo{
				AssociationID: assoc.AssociationID,
				Name:          assoc.Name,
				Status:        status,
				ExecutionDate: time.Unix(int64(assoc.LastUpdateAssociationDate), 0).UTC(),
			})
		}
	}

	if result == nil {
		result = []InstanceAssociationStatusInfo{}
	}

	return &DescribeInstanceAssociationsStatusOutputFull{InstanceAssociationStatusInfos: result}, nil
}

// DescribeInstanceInformation returns information about managed instances from activations.
func (b *InMemoryBackend) DescribeInstanceInformation(
	_ *DescribeInstanceInformationInput,
) (*DescribeInstanceInformationOutputFull, error) {
	b.mu.RLock("DescribeInstanceInformation")
	defer b.mu.RUnlock()

	list := make([]InstanceInformation, 0, len(b.activations))
	for _, act := range b.activations {
		list = append(list, InstanceInformation{
			InstanceID:       act.ActivationID,
			PingStatus:       "Online",
			AgentVersion:     "3.0.0",
			PlatformType:     platformTypeLinux,
			RegistrationDate: time.Unix(int64(act.CreatedDate), 0).UTC(),
		})
	}

	return &DescribeInstanceInformationOutputFull{InstanceInformationList: list}, nil
}

// DescribeInstancePatchStates returns patch compliance state for instances.
func (b *InMemoryBackend) DescribeInstancePatchStates(
	_ *DescribeInstancePatchStatesInput,
) (*DescribeInstancePatchStatesOutputFull, error) {
	b.mu.RLock("DescribeInstancePatchStates")
	defer b.mu.RUnlock()

	return &DescribeInstancePatchStatesOutputFull{
		InstancePatchStates: []InstancePatchState{},
	}, nil
}
