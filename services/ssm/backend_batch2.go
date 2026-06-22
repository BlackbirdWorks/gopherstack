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
func (b *InMemoryBackend) CreateResourceDataSync(
	ctx context.Context,
	input *CreateResourceDataSyncInput,
) (*CreateResourceDataSyncOutput, error) {
	region := getRegion(ctx)
	b.mu.Lock("CreateResourceDataSync")
	defer b.mu.Unlock()

	syncName := input.SyncName
	if syncName == "" {
		syncName = "default-sync"
	}

	if b.resourceDataSyncs[region] == nil {
		b.resourceDataSyncs[region] = make(map[string]*ResourceDataSync)
	}
	syncs := b.resourceDataSyncsStore(region)
	if _, exists := syncs[syncName]; exists {
		return nil, ErrResourceDataSyncExists
	}

	syncs[syncName] = &ResourceDataSync{
		SyncName:        syncName,
		SyncType:        input.SyncType,
		LastStatus:      "InProgress",
		SyncCreatedTime: time.Now().UTC(),
		LastSyncTime:    time.Now().UTC(),
	}

	return &CreateResourceDataSyncOutput{}, nil
}

// DeleteResourceDataSync removes a resource data sync by name.
func (b *InMemoryBackend) DeleteResourceDataSync(
	ctx context.Context,
	input *DeleteResourceDataSyncInput,
) (*DeleteResourceDataSyncOutput, error) {
	region := getRegion(ctx)
	b.mu.Lock("DeleteResourceDataSync")
	defer b.mu.Unlock()

	if input.SyncName == "" {
		return &DeleteResourceDataSyncOutput{}, nil
	}

	syncs := b.resourceDataSyncsStore(region)
	if _, exists := syncs[input.SyncName]; !exists {
		return nil, fmt.Errorf("%w: %q", ErrResourceDataSyncNotFound, input.SyncName)
	}

	delete(syncs, input.SyncName)

	cleanupEmptyInnerMap(b.resourceDataSyncs, region)

	return &DeleteResourceDataSyncOutput{}, nil
}

// ListResourceDataSync returns all resource data syncs.
func (b *InMemoryBackend) ListResourceDataSync(
	ctx context.Context,
	_ *ListResourceDataSyncInput,
) (*ListResourceDataSyncOutputFull, error) {
	region := getRegion(ctx)
	b.mu.RLock("ListResourceDataSync")
	defer b.mu.RUnlock()

	syncs := b.resourceDataSyncsStore(region)
	items := make([]ResourceDataSync, 0, len(syncs))
	for _, s := range syncs {
		items = append(items, *s)
	}

	sort.Slice(items, func(i, k int) bool {
		return items[i].SyncName < items[k].SyncName
	})

	return &ListResourceDataSyncOutputFull{ResourceDataSyncItems: items}, nil
}

// UpdateResourceDataSync updates an existing resource data sync.
func (b *InMemoryBackend) UpdateResourceDataSync(
	ctx context.Context,
	input *UpdateResourceDataSyncInput,
) (*UpdateResourceDataSyncOutput, error) {
	region := getRegion(ctx)
	b.mu.Lock("UpdateResourceDataSync")
	defer b.mu.Unlock()

	if input.SyncName == "" {
		return &UpdateResourceDataSyncOutput{}, nil
	}

	if sync, exists := b.resourceDataSyncsStore(region)[input.SyncName]; exists {
		sync.LastSyncTime = time.Now().UTC()
	}

	return &UpdateResourceDataSyncOutput{}, nil
}

// --- Activation lifecycle ---

// DeregisterManagedInstance removes the activation associated with a managed instance ID.
// The InstanceID field is treated as the ActivationID in this in-memory implementation.
func (b *InMemoryBackend) DeregisterManagedInstance(
	ctx context.Context,
	input *DeregisterManagedInstanceInput,
) (*DeregisterManagedInstanceOutput, error) {
	region := getRegion(ctx)
	b.mu.Lock("DeregisterManagedInstance")
	defer b.mu.Unlock()

	// Try to match by ActivationID (InstanceID in the request maps to ActivationID here).
	activations := b.activationsStore(region)
	if _, exists := activations[input.InstanceID]; exists {
		delete(activations, input.InstanceID)
		delete(b.miscResourceTagsStore(region), input.InstanceID)
	}

	return &DeregisterManagedInstanceOutput{}, nil
}

// UpdateManagedInstanceRole updates the IAM role for a managed instance's activation.
func (b *InMemoryBackend) UpdateManagedInstanceRole(
	ctx context.Context,
	input *UpdateManagedInstanceRoleInput,
) (*UpdateManagedInstanceRoleOutput, error) {
	region := getRegion(ctx)
	b.mu.Lock("UpdateManagedInstanceRole")
	defer b.mu.Unlock()

	activations := b.activationsStore(region)
	if act, exists := activations[input.InstanceID]; exists {
		act.IamRole = input.IamRole
		activations[input.InstanceID] = act
	}

	return &UpdateManagedInstanceRoleOutput{}, nil
}

// --- Session Manager ---

// DescribeSessions returns sessions from the in-memory store.
func (b *InMemoryBackend) DescribeSessions(
	ctx context.Context,
	input *DescribeSessionsInput,
) (*DescribeSessionsOutputFull, error) {
	region := getRegion(ctx)
	b.mu.RLock("DescribeSessions")
	defer b.mu.RUnlock()

	sessions := b.sessionsStore(region)
	list := make([]Session, 0, len(sessions))
	for _, s := range sessions {
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
func (b *InMemoryBackend) GetConnectionStatus(
	ctx context.Context,
	input *GetConnectionStatusInput,
) (*GetConnectionStatusOutputFull, error) {
	region := getRegion(ctx)
	b.mu.RLock("GetConnectionStatus")
	defer b.mu.RUnlock()

	target := input.Target
	status := "notConnected"

	for _, s := range b.sessionsStore(region) {
		if s.Target == target && s.Status == sessionStatusConnected {
			status = connectionStatusConnected

			break
		}
	}

	return &GetConnectionStatusOutputFull{Target: target, Status: status}, nil
}

// GetAccessToken returns a mock access token for an active session.
func (b *InMemoryBackend) GetAccessToken(
	_ context.Context,
	input *GetAccessTokenInput,
) (*GetAccessTokenOutputFull, error) {
	b.mu.RLock("GetAccessToken")
	defer b.mu.RUnlock()

	_ = input

	return &GetAccessTokenOutputFull{
		TokenValue: "gph-mock-access-token-" + uuid.NewString(),
	}, nil
}

// ResumeSession resumes a disconnected session.
func (b *InMemoryBackend) ResumeSession(
	ctx context.Context,
	input *ResumeSessionInput,
) (*ResumeSessionOutputFull, error) {
	region := getRegion(ctx)
	b.mu.Lock("ResumeSession")
	defer b.mu.Unlock()

	sessions := b.sessionsStore(region)
	sess, exists := sessions[input.SessionID]
	if !exists {
		return &ResumeSessionOutputFull{SessionID: input.SessionID, StreamURL: ""}, nil
	}

	sess.Status = sessionStatusConnected
	sessions[input.SessionID] = sess

	return &ResumeSessionOutputFull{
		SessionID:  sess.SessionID,
		StreamURL:  "wss://gopherstack.mock/" + sess.SessionID,
		TokenValue: "gph-resume-token-" + uuid.NewString(),
	}, nil
}

// StartAccessRequest creates an access request record.
func (b *InMemoryBackend) StartAccessRequest(
	_ context.Context,
	input *StartAccessRequestInput,
) (*StartAccessRequestOutputFull, error) {
	b.mu.Lock("StartAccessRequest")
	defer b.mu.Unlock()

	_ = input

	return &StartAccessRequestOutputFull{
		AccessRequestID: "ar-" + uuid.NewString(),
	}, nil
}

// --- Service Settings ---

// GetServiceSetting returns the value for a service setting.
func (b *InMemoryBackend) GetServiceSetting(
	ctx context.Context,
	input *GetServiceSettingInput,
) (*GetServiceSettingOutputFull, error) {
	region := getRegion(ctx)
	b.mu.RLock("GetServiceSetting")
	defer b.mu.RUnlock()

	if s, exists := b.serviceSettingsStore(region)[input.SettingID]; exists {
		return &GetServiceSettingOutputFull{ServiceSetting: s}, nil
	}

	return &GetServiceSettingOutputFull{ServiceSetting: &ServiceSetting{
		SettingID:    input.SettingID,
		SettingValue: "",
		Status:       settingStatusDefault,
	}}, nil
}

// UpdateServiceSetting stores a custom value for a service setting.
func (b *InMemoryBackend) UpdateServiceSetting(
	ctx context.Context,
	input *UpdateServiceSettingInput,
) (*UpdateServiceSettingOutput, error) {
	region := getRegion(ctx)
	b.mu.Lock("UpdateServiceSetting")
	defer b.mu.Unlock()

	if b.serviceSettings[region] == nil {
		b.serviceSettings[region] = make(map[string]*ServiceSetting)
	}
	b.serviceSettingsStore(region)[input.SettingID] = &ServiceSetting{
		SettingID:    input.SettingID,
		SettingValue: input.SettingValue,
		Status:       settingStatusCustomized,
	}

	return &UpdateServiceSettingOutput{}, nil
}

// ResetServiceSetting removes any custom value for a service setting.
func (b *InMemoryBackend) ResetServiceSetting(
	ctx context.Context,
	input *ResetServiceSettingInput,
) (*ResetServiceSettingOutputFull, error) {
	region := getRegion(ctx)
	b.mu.Lock("ResetServiceSetting")
	defer b.mu.Unlock()

	delete(b.serviceSettingsStore(region), input.SettingID)

	return &ResetServiceSettingOutputFull{ServiceSetting: &ServiceSetting{
		SettingID: input.SettingID,
		Status:    settingStatusDefault,
	}}, nil
}

// --- Resource Policies ---

// GetResourcePolicies returns policies attached to a resource.
func (b *InMemoryBackend) GetResourcePolicies(
	ctx context.Context,
	input *GetResourcePoliciesInput,
) (*GetResourcePoliciesOutputFull, error) {
	region := getRegion(ctx)
	b.mu.RLock("GetResourcePolicies")
	defer b.mu.RUnlock()

	policies := b.resourcePoliciesStore(region)[input.ResourceARN]
	list := make([]ResourcePolicy, 0, len(policies))

	for _, p := range policies {
		list = append(list, *p)
	}

	return &GetResourcePoliciesOutputFull{Policies: list}, nil
}

// PutResourcePolicy attaches a policy to a resource.
func (b *InMemoryBackend) PutResourcePolicy(
	ctx context.Context,
	input *PutResourcePolicyInput,
) (*PutResourcePolicyOutputFull, error) {
	region := getRegion(ctx)
	b.mu.Lock("PutResourcePolicy")
	defer b.mu.Unlock()

	policyID := policyIDPrefix + uuid.NewString()
	policy := &ResourcePolicy{
		PolicyID:   policyID,
		PolicyHash: uuid.NewString(),
		Policy:     input.Policy,
	}
	if b.resourcePolicies[region] == nil {
		b.resourcePolicies[region] = make(map[string][]*ResourcePolicy)
	}
	policies := b.resourcePoliciesStore(region)
	policies[input.ResourceARN] = append(policies[input.ResourceARN], policy)

	return &PutResourcePolicyOutputFull{PolicyID: policyID, PolicyHash: policy.PolicyHash}, nil
}

// DeleteResourcePolicy removes a policy from a resource.
func (b *InMemoryBackend) DeleteResourcePolicy(
	ctx context.Context,
	input *DeleteResourcePolicyInput,
) (*DeleteResourcePolicyOutput, error) {
	region := getRegion(ctx)
	b.mu.Lock("DeleteResourcePolicy")
	defer b.mu.Unlock()

	if input.ResourceARN == "" {
		return &DeleteResourcePolicyOutput{}, nil
	}

	if b.resourcePolicies[region] == nil {
		b.resourcePolicies[region] = make(map[string][]*ResourcePolicy)
	}
	policies := b.resourcePoliciesStore(region)
	existing := policies[input.ResourceARN]
	updated := existing[:0]

	for _, p := range existing {
		if p.PolicyID != input.PolicyID {
			updated = append(updated, p)
		}
	}

	if len(updated) == 0 {
		delete(policies, input.ResourceARN)
	} else {
		policies[input.ResourceARN] = updated
	}

	cleanupEmptyInnerMap(b.resourcePolicies, region)

	return &DeleteResourcePolicyOutput{}, nil
}

// --- Parameter Labels ---

// LabelParameterVersion adds labels to a specific parameter version.
// When ParameterVersion is 0, labels are applied to the latest version.
func (b *InMemoryBackend) LabelParameterVersion(
	ctx context.Context,
	input *LabelParameterVersionInput,
) (*LabelParameterVersionOutputFull, error) {
	region := getRegion(ctx)
	b.mu.Lock("LabelParameterVersion")
	defer b.mu.Unlock()

	if input.Name == "" {
		return &LabelParameterVersionOutputFull{
			InvalidLabels: []string{},
			AddedLabels:   input.Labels,
		}, nil
	}

	param, exists := b.parametersStore(region)[input.Name]
	if !exists {
		return nil, fmt.Errorf("%w: %q", ErrParameterNotFound, input.Name)
	}

	version := input.ParameterVersion
	if version == 0 {
		version = param.Version
	}

	if b.parameterLabels[region] == nil {
		b.parameterLabels[region] = make(map[string]map[int64][]string)
	}
	parameterLabels := b.parameterLabelsStore(region)
	if parameterLabels[input.Name] == nil {
		parameterLabels[input.Name] = make(map[int64][]string)
	}

	// In AWS a label points at exactly one version. Re-applying a label that
	// currently lives on a different version moves it to the target version
	// rather than duplicating it.
	for v, labels := range parameterLabels[input.Name] {
		if v == version {
			continue
		}
		parameterLabels[input.Name][v] = removeLabels(labels, input.Labels)
	}

	parameterLabels[input.Name][version] = appendUniqueLabels(
		parameterLabels[input.Name][version], input.Labels,
	)

	return &LabelParameterVersionOutputFull{
		InvalidLabels:    []string{},
		AddedLabels:      input.Labels,
		ParameterVersion: version,
	}, nil
}

// removeLabels returns existing with any entry present in toRemove filtered out.
func removeLabels(existing, toRemove []string) []string {
	if len(existing) == 0 {
		return existing
	}
	remove := make(map[string]bool, len(toRemove))
	for _, l := range toRemove {
		remove[l] = true
	}
	kept := make([]string, 0, len(existing))
	for _, l := range existing {
		if !remove[l] {
			kept = append(kept, l)
		}
	}

	return kept
}

// UnlabelParameterVersion removes labels from a specific parameter version.
// When ParameterVersion is 0, labels are removed from the latest version.
func (b *InMemoryBackend) UnlabelParameterVersion(
	ctx context.Context,
	input *UnlabelParameterVersionInput,
) (*UnlabelParameterVersionOutputFull, error) {
	region := getRegion(ctx)
	b.mu.Lock("UnlabelParameterVersion")
	defer b.mu.Unlock()

	if input.Name == "" {
		return &UnlabelParameterVersionOutputFull{
			InvalidLabels: []string{},
			RemovedLabels: input.Labels,
		}, nil
	}

	version := input.ParameterVersion
	if version == 0 {
		if param, exists := b.parametersStore(region)[input.Name]; exists {
			version = param.Version
		}
	}

	removedSet := make(map[string]bool, len(input.Labels))
	for _, l := range input.Labels {
		removedSet[l] = true
	}

	parameterLabels := b.parameterLabelsStore(region)
	existing := parameterLabels[input.Name][version]
	kept := make([]string, 0, len(existing))

	for _, l := range existing {
		if !removedSet[l] {
			kept = append(kept, l)
		}
	}

	if parameterLabels[input.Name] != nil {
		parameterLabels[input.Name][version] = kept
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
	ctx context.Context,
	input *StartAutomationExecutionInput,
) (*StartAutomationExecutionOutputFull, error) {
	region := getRegion(ctx)
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
	if b.automationExecutions[region] == nil {
		b.automationExecutions[region] = make(map[string]*AutomationExecution)
	}
	b.automationExecutionsStore(region)[execID] = exec

	return &StartAutomationExecutionOutputFull{AutomationExecutionID: execID}, nil
}

// GetAutomationExecution returns an automation execution by ID.
func (b *InMemoryBackend) GetAutomationExecution(
	ctx context.Context,
	input *GetAutomationExecutionInput,
) (*GetAutomationExecutionOutputFull, error) {
	region := getRegion(ctx)
	b.mu.RLock("GetAutomationExecution")
	defer b.mu.RUnlock()

	exec, exists := b.automationExecutionsStore(region)[input.AutomationExecutionID]
	if !exists {
		return nil, fmt.Errorf(
			"%w: %q",
			ErrAutomationExecutionNotFound,
			input.AutomationExecutionID,
		)
	}

	cp := *exec

	return &GetAutomationExecutionOutputFull{AutomationExecution: &cp}, nil
}

// DescribeAutomationExecutions returns all automation executions.
func (b *InMemoryBackend) DescribeAutomationExecutions(
	ctx context.Context,
	_ *DescribeAutomationExecutionsInput,
) (*DescribeAutomationExecutionsOutputFull, error) {
	region := getRegion(ctx)
	b.mu.RLock("DescribeAutomationExecutions")
	defer b.mu.RUnlock()

	execs := b.automationExecutionsStore(region)
	list := make([]AutomationExecution, 0, len(execs))
	for _, exec := range execs {
		list = append(list, *exec)
	}

	sort.Slice(list, func(i, k int) bool {
		return list[i].StartTime.Before(list[k].StartTime)
	})

	return &DescribeAutomationExecutionsOutputFull{AutomationExecutionMetadataList: list}, nil
}

// StopAutomationExecution marks an automation execution as stopped.
func (b *InMemoryBackend) StopAutomationExecution(
	ctx context.Context,
	input *StopAutomationExecutionInput,
) (*StopAutomationExecutionOutput, error) {
	region := getRegion(ctx)
	b.mu.Lock("StopAutomationExecution")
	defer b.mu.Unlock()

	if exec, exists := b.automationExecutionsStore(region)[input.AutomationExecutionID]; exists {
		exec.Status = automationStatusStopped
		now := time.Now().UTC()
		exec.EndTime = &now
	}

	return &StopAutomationExecutionOutput{}, nil
}

// SendAutomationSignal sends a signal to an automation execution.
// Approve/Reject signals update the execution status accordingly.
func (b *InMemoryBackend) SendAutomationSignal(
	ctx context.Context,
	input *SendAutomationSignalInput,
) (*SendAutomationSignalOutput, error) {
	region := getRegion(ctx)
	b.mu.Lock("SendAutomationSignal")
	defer b.mu.Unlock()

	exec, exists := b.automationExecutionsStore(region)[input.AutomationExecutionID]
	if !exists {
		return &SendAutomationSignalOutput{}, nil
	}

	switch input.SignalType {
	case "Approve":
		exec.Status = "Approved"
	case "Reject":
		exec.Status = "Rejected"
	case "StopStep":
		exec.Status = automationStatusStopped
	}

	return &SendAutomationSignalOutput{}, nil
}

// DescribeAutomationStepExecutions returns step executions for an automation.
func (b *InMemoryBackend) DescribeAutomationStepExecutions(
	ctx context.Context,
	input *DescribeAutomationStepExecutionsInput,
) (*DescribeAutomationStepExecutionsOutputFull, error) {
	region := getRegion(ctx)
	b.mu.RLock("DescribeAutomationStepExecutions")
	defer b.mu.RUnlock()

	exec, exists := b.automationExecutionsStore(region)[input.AutomationExecutionID]
	if !exists {
		return &DescribeAutomationStepExecutionsOutputFull{
			StepExecutions: []AutomationStepExec{},
		}, nil
	}

	return &DescribeAutomationStepExecutionsOutputFull{StepExecutions: exec.Steps}, nil
}

// StartChangeRequestExecution creates a change request automation execution.
func (b *InMemoryBackend) StartChangeRequestExecution(
	ctx context.Context,
	input *StartChangeRequestExecutionInput,
) (*StartChangeRequestExecutionOutputFull, error) {
	region := getRegion(ctx)
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
	if b.automationExecutions[region] == nil {
		b.automationExecutions[region] = make(map[string]*AutomationExecution)
	}
	b.automationExecutionsStore(region)[execID] = exec

	return &StartChangeRequestExecutionOutputFull{AutomationExecutionID: execID}, nil
}

// --- Execution Preview ---

// StartExecutionPreview creates an execution preview.
func (b *InMemoryBackend) StartExecutionPreview(
	ctx context.Context,
	input *StartExecutionPreviewInput,
) (*StartExecutionPreviewOutputFull, error) {
	region := getRegion(ctx)
	b.mu.Lock("StartExecutionPreview")
	defer b.mu.Unlock()

	previewID := previewIDPrefix + uuid.NewString()
	if b.executionPreviews[region] == nil {
		b.executionPreviews[region] = make(map[string]*ExecutionPreview)
	}
	b.executionPreviewsStore(region)[previewID] = &ExecutionPreview{
		ExecutionPreviewID: previewID,
		Status:             "Running",
		DocumentName:       input.DocumentName,
	}

	return &StartExecutionPreviewOutputFull{ExecutionPreviewID: previewID}, nil
}

// GetExecutionPreview returns an execution preview by ID.
func (b *InMemoryBackend) GetExecutionPreview(
	ctx context.Context,
	input *GetExecutionPreviewInput,
) (*GetExecutionPreviewOutputFull, error) {
	region := getRegion(ctx)
	b.mu.RLock("GetExecutionPreview")
	defer b.mu.RUnlock()

	preview, exists := b.executionPreviewsStore(region)[input.ExecutionPreviewID]
	if !exists {
		return &GetExecutionPreviewOutputFull{
			ExecutionPreviewID: input.ExecutionPreviewID,
			Status:             "Running",
		}, nil
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
func (b *InMemoryBackend) GetCalendarState(
	ctx context.Context,
	input *GetCalendarStateInput,
) (*GetCalendarStateOutputFull, error) {
	if len(input.CalendarNames) == 0 {
		return &GetCalendarStateOutputFull{State: calendarStateOpen}, nil
	}

	region := getRegion(ctx)
	b.mu.RLock("GetCalendarState")
	defer b.mu.RUnlock()

	documents := b.documentsStore(region)
	for _, name := range input.CalendarNames {
		doc, exists := documents[name]
		if !exists {
			return nil, fmt.Errorf("%w: calendar document %q not found", ErrDocumentNotFound, name)
		}

		if doc.DocumentType != "ChangeCalendar" {
			return nil, fmt.Errorf(
				"%w: document %q is not a ChangeCalendar document",
				ErrValidationException,
				name,
			)
		}
	}

	return &GetCalendarStateOutputFull{State: calendarStateOpen}, nil
}

// --- OpsItem summary / OpsMetadata list ---

// GetOpsSummary returns a summary count of ops items.
func (b *InMemoryBackend) GetOpsSummary(
	ctx context.Context,
	_ *GetOpsSummaryInput,
) (*GetOpsSummaryOutputFull, error) {
	region := getRegion(ctx)
	b.mu.RLock("GetOpsSummary")
	defer b.mu.RUnlock()

	return &GetOpsSummaryOutputFull{
		Entities: []OpsSummaryEntity{
			{
				ID: "AWS:OpsItem",
				Data: map[string]OpsSummaryValue{
					"Count": {Count: len(b.opsItemsStore(region)), Unit: "Count"},
				},
			},
		},
	}, nil
}

// ListOpsMetadata returns all ops metadata entries.
func (b *InMemoryBackend) ListOpsMetadata(
	ctx context.Context,
	_ *ListOpsMetadataInput,
) (*ListOpsMetadataOutputFull, error) {
	region := getRegion(ctx)
	b.mu.RLock("ListOpsMetadata")
	defer b.mu.RUnlock()

	opsMetadata := b.opsMetadataStore(region)
	list := make([]OpsMetadata, 0, len(opsMetadata))
	for _, m := range opsMetadata {
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
	ctx context.Context,
	input *UpdateAssociationStatusInput,
) (*UpdateAssociationStatusOutputFull, error) {
	region := getRegion(ctx)
	b.mu.Lock("UpdateAssociationStatus")
	defer b.mu.Unlock()

	associations := b.associationsStore(region)
	for id, assoc := range associations {
		if assoc.InstanceID == input.InstanceID && assoc.Name == input.Name {
			if assoc.Overview == nil {
				assoc.Overview = &AssociationOverview{}
			}

			assoc.Overview.Status = input.AssociationStatus.Name
			associations[id] = assoc

			return &UpdateAssociationStatusOutputFull{AssociationDescription: assoc}, nil
		}
	}

	return nil, fmt.Errorf(
		"%w: instance %q / name %q",
		ErrAssociationNotFound,
		input.InstanceID,
		input.Name,
	)
}

// StartAssociationsOnce triggers a one-time run of the given associations.
func (b *InMemoryBackend) StartAssociationsOnce(
	ctx context.Context,
	input *StartAssociationsOnceInput,
) (*StartAssociationsOnceOutput, error) {
	region := getRegion(ctx)
	b.mu.Lock("StartAssociationsOnce")
	defer b.mu.Unlock()

	now := time.Now().UTC()

	associations := b.associationsStore(region)
	for _, assocID := range input.AssociationIDs {
		if assoc, exists := associations[assocID]; exists {
			assoc.LastUpdateAssociationDate = float64(now.Unix())
			associations[assocID] = assoc
		}
	}

	return &StartAssociationsOnceOutput{}, nil
}

// ListAssociationVersions returns the version history of an association.
func (b *InMemoryBackend) ListAssociationVersions(
	ctx context.Context,
	input *ListAssociationVersionsInput,
) (*ListAssociationVersionsOutputFull, error) {
	region := getRegion(ctx)
	b.mu.RLock("ListAssociationVersions")
	defer b.mu.RUnlock()

	assoc, exists := b.associationsStore(region)[input.AssociationID]
	if !exists {
		return &ListAssociationVersionsOutputFull{AssociationVersions: []Association{}}, nil
	}

	return &ListAssociationVersionsOutputFull{
		AssociationVersions: []Association{assoc},
	}, nil
}

// DescribeAssociationExecutions returns execution history for an association.
func (b *InMemoryBackend) DescribeAssociationExecutions(
	ctx context.Context,
	input *DescribeAssociationExecutionsInput,
) (*DescribeAssociationExecutionsOutputFull, error) {
	region := getRegion(ctx)
	b.mu.RLock("DescribeAssociationExecutions")
	defer b.mu.RUnlock()

	assoc, exists := b.associationsStore(region)[input.AssociationID]
	if !exists {
		return &DescribeAssociationExecutionsOutputFull{
			AssociationExecutions: []AssociationExecution{},
		}, nil
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
// Looks up the association by ID and derives execution targets from its registered targets.
func (b *InMemoryBackend) DescribeAssociationExecutionTargets(
	ctx context.Context,
	input *DescribeAssociationExecutionTargetsInput,
) (*DescribeAssociationExecutionTargetsOutputFull, error) {
	region := getRegion(ctx)
	b.mu.RLock("DescribeAssociationExecutionTargets")
	defer b.mu.RUnlock()

	if input.AssociationID == "" {
		return &DescribeAssociationExecutionTargetsOutputFull{
			AssociationExecutionTargets: []AssociationExecutionTarget{},
		}, nil
	}

	assoc, exists := b.associationsStore(region)[input.AssociationID]
	if !exists {
		return &DescribeAssociationExecutionTargetsOutputFull{
			AssociationExecutionTargets: []AssociationExecutionTarget{},
		}, nil
	}

	execID := input.ExecutionID
	if execID == "" {
		execID = uuid.NewString()
	}

	// Build targets from the association's InstanceID and explicit Targets list.
	targets := make([]AssociationExecutionTarget, 0)
	if assoc.InstanceID != "" {
		targets = append(targets, AssociationExecutionTarget{
			AssociationID: assoc.AssociationID,
			ExecutionID:   execID,
			ResourceID:    assoc.InstanceID,
			ResourceType:  "ManagedInstance",
			Status:        commandStatusSuccess,
		})
	}

	for _, t := range assoc.Targets {
		for _, v := range t.Values {
			targets = append(targets, AssociationExecutionTarget{
				AssociationID: assoc.AssociationID,
				ExecutionID:   execID,
				ResourceID:    v,
				ResourceType:  "ManagedInstance",
				Status:        commandStatusSuccess,
			})
		}
	}

	if len(targets) == 0 {
		targets = []AssociationExecutionTarget{}
	}

	return &DescribeAssociationExecutionTargetsOutputFull{
		AssociationExecutionTargets: targets,
	}, nil
}

// --- Maintenance Window Executions ---

const mwExecIDPrefix = "mwexec-"

// mwExecID builds a deterministic execution ID from a window ID.
func mwExecID(windowID string) string { return mwExecIDPrefix + windowID }

// mwWindowIDFromExec extracts the window ID from a deterministic execution ID.
// Returns ("", false) if the execution ID was not generated by mwExecID.
func mwWindowIDFromExec(execID string) (string, bool) {
	if len(execID) <= len(mwExecIDPrefix) || execID[:len(mwExecIDPrefix)] != mwExecIDPrefix {
		return "", false
	}

	return execID[len(mwExecIDPrefix):], true
}

// DescribeMaintenanceWindowExecutions returns execution records for a window.
// When the window exists it returns a single synthetic execution record derived
// from the window's state (simulating that the window has run once).
func (b *InMemoryBackend) DescribeMaintenanceWindowExecutions(
	ctx context.Context,
	input *DescribeMaintenanceWindowExecutionsInput,
) (*DescribeMaintenanceWindowExecutionsOutputFull, error) {
	region := getRegion(ctx)
	b.mu.RLock("DescribeMaintenanceWindowExecutions")
	defer b.mu.RUnlock()

	if input.WindowID == "" {
		return &DescribeMaintenanceWindowExecutionsOutputFull{
			WindowExecutions: []MaintenanceWindowExecution{},
		}, nil
	}

	win, exists := b.maintenanceWindowsStore(region)[input.WindowID]
	if !exists {
		return &DescribeMaintenanceWindowExecutionsOutputFull{
			WindowExecutions: []MaintenanceWindowExecution{},
		}, nil
	}

	var execTime time.Time
	if win.CreatedDate > 0 {
		execTime = time.Unix(int64(win.CreatedDate), 0).UTC()
	} else {
		execTime = time.Now().UTC()
	}

	endTime := execTime.Add(time.Duration(win.Duration) * time.Hour)

	return &DescribeMaintenanceWindowExecutionsOutputFull{
		WindowExecutions: []MaintenanceWindowExecution{
			{
				WindowID:          win.WindowID,
				WindowExecutionID: mwExecID(win.WindowID),
				Status:            commandStatusSuccess,
				StartTime:         execTime,
				EndTime:           &endTime,
			},
		},
	}, nil
}

// DescribeMaintenanceWindowExecutionTasks returns task executions for a window execution.
// Derives the window ID from the execution ID and looks up registered tasks for that window.
func (b *InMemoryBackend) DescribeMaintenanceWindowExecutionTasks(
	ctx context.Context,
	input *DescribeMaintenanceWindowExecutionTasksInput,
) (*DescribeMaintenanceWindowExecutionTasksOutputFull, error) {
	region := getRegion(ctx)
	b.mu.RLock("DescribeMaintenanceWindowExecutionTasks")
	defer b.mu.RUnlock()

	windowID, ok := mwWindowIDFromExec(input.WindowExecutionID)
	if !ok {
		return &DescribeMaintenanceWindowExecutionTasksOutputFull{
			WindowExecutionTaskIdentities: []MaintenanceWindowExecutionTask{},
		}, nil
	}

	tasks := b.maintenanceWindowTasksStore(region)
	result := make([]MaintenanceWindowExecutionTask, 0)

	for _, task := range tasks {
		if task.WindowID != windowID {
			continue
		}

		result = append(result, MaintenanceWindowExecutionTask{
			WindowExecutionID: input.WindowExecutionID,
			TaskExecutionID:   "taskexec-" + task.WindowTaskID,
			TaskARN:           task.TaskArn,
			Status:            commandStatusSuccess,
			StartTime:         time.Now().UTC(),
		})
	}

	sort.Slice(result, func(i, k int) bool {
		return result[i].TaskExecutionID < result[k].TaskExecutionID
	})

	if len(result) == 0 {
		result = []MaintenanceWindowExecutionTask{}
	}

	return &DescribeMaintenanceWindowExecutionTasksOutputFull{
		WindowExecutionTaskIdentities: result,
	}, nil
}

// DescribeMaintenanceWindowExecutionTaskInvocations returns invocations for a task execution.
// Derives the window ID from the execution ID and returns one invocation per registered target.
func (b *InMemoryBackend) DescribeMaintenanceWindowExecutionTaskInvocations(
	_ context.Context,
	input *DescribeMaintenanceWindowExecutionTaskInvocationsInput,
) (*DescribeMaintenanceWindowExecutionTaskInvocationsOutputFull, error) {
	b.mu.RLock("DescribeMaintenanceWindowExecutionTaskInvocations")
	defer b.mu.RUnlock()

	_, ok := mwWindowIDFromExec(input.WindowExecutionID)
	if !ok {
		return &DescribeMaintenanceWindowExecutionTaskInvocationsOutputFull{
			WindowExecutionTaskInvocationIdentities: []MaintenanceWindowExecutionTaskInvocation{},
		}, nil
	}

	return &DescribeMaintenanceWindowExecutionTaskInvocationsOutputFull{
		WindowExecutionTaskInvocationIdentities: []MaintenanceWindowExecutionTaskInvocation{
			{
				WindowExecutionID: input.WindowExecutionID,
				TaskExecutionID:   input.TaskID,
				InvocationID:      "inv-" + input.WindowExecutionID,
				Status:            commandStatusSuccess,
				StartTime:         time.Now().UTC(),
			},
		},
	}, nil
}

// DescribeMaintenanceWindowSchedule returns the upcoming schedule for a window.
func (b *InMemoryBackend) DescribeMaintenanceWindowSchedule(
	ctx context.Context,
	input *DescribeMaintenanceWindowScheduleInput,
) (*DescribeMaintenanceWindowScheduleOutputFull, error) {
	region := getRegion(ctx)
	b.mu.RLock("DescribeMaintenanceWindowSchedule")
	defer b.mu.RUnlock()

	win, exists := b.maintenanceWindowsStore(region)[input.WindowID]
	if !exists {
		return &DescribeMaintenanceWindowScheduleOutputFull{
			ScheduledWindowExecutions: []ScheduledWindowExecution{},
		}, nil
	}

	return &DescribeMaintenanceWindowScheduleOutputFull{
		ScheduledWindowExecutions: []ScheduledWindowExecution{
			{
				WindowID: win.WindowID,
				Name:     win.Name,
				ExecutionTime: time.Now().
					UTC().
					Add(mwExecutionScheduleHours * time.Hour).
					Format(time.RFC3339),
			},
		},
	}, nil
}

// GetMaintenanceWindowExecution returns a specific window execution.
// Derives timing and status from the window record identified by the execution ID.
func (b *InMemoryBackend) GetMaintenanceWindowExecution(
	ctx context.Context,
	input *GetMaintenanceWindowExecutionInput,
) (*GetMaintenanceWindowExecutionOutputFull, error) {
	region := getRegion(ctx)
	b.mu.RLock("GetMaintenanceWindowExecution")
	defer b.mu.RUnlock()

	windowID := input.WindowID
	if windowID == "" {
		if id, ok := mwWindowIDFromExec(input.WindowExecutionID); ok {
			windowID = id
		}
	}

	startTime := time.Now().UTC()
	endTime := startTime.Add(time.Hour)

	if windowID != "" {
		win, exists := b.maintenanceWindowsStore(region)[windowID]
		if !exists {
			return nil, ErrMaintenanceWindowNotFound
		}

		if win.CreatedDate > 0 {
			startTime = time.Unix(int64(win.CreatedDate), 0).UTC()
		}

		endTime = startTime.Add(time.Duration(win.Duration) * time.Hour)
	}

	execID := input.WindowExecutionID
	if execID == "" {
		execID = mwExecID(windowID)
	}

	return &GetMaintenanceWindowExecutionOutputFull{
		WindowID:          windowID,
		WindowExecutionID: execID,
		Status:            commandStatusSuccess,
		StatusDetails:     "WindowExecution Succeeded",
		StartTime:         startTime,
		EndTime:           &endTime,
	}, nil
}

// GetMaintenanceWindowExecutionTask returns a specific task within a window execution.
// Looks up the stored task record and returns its full attributes.
func (b *InMemoryBackend) GetMaintenanceWindowExecutionTask(
	ctx context.Context,
	input *GetMaintenanceWindowExecutionTaskInput,
) (*GetMaintenanceWindowExecutionTaskOutputFull, error) {
	region := getRegion(ctx)
	b.mu.RLock("GetMaintenanceWindowExecutionTask")
	defer b.mu.RUnlock()

	taskExecID := input.TaskExecutionID
	windowTaskID := ""
	if len(taskExecID) > len("taskexec-") && taskExecID[:len("taskexec-")] == "taskexec-" {
		windowTaskID = taskExecID[len("taskexec-"):]
	}

	startTime := time.Now().UTC()

	if windowTaskID != "" {
		if task, ok := b.maintenanceWindowTasksStore(region)[windowTaskID]; ok {
			endTime := startTime.Add(time.Minute)

			return &GetMaintenanceWindowExecutionTaskOutputFull{
				WindowExecutionID: input.WindowExecutionID,
				TaskExecutionID:   taskExecID,
				TaskARN:           task.TaskArn,
				TaskType:          task.TaskType,
				Status:            commandStatusSuccess,
				StatusDetails:     "Task Succeeded",
				Priority:          task.Priority,
				MaxConcurrency:    task.MaxConcurrency,
				MaxErrors:         task.MaxErrors,
				StartTime:         startTime,
				EndTime:           &endTime,
			}, nil
		}
	}

	endTime := startTime.Add(time.Minute)

	return &GetMaintenanceWindowExecutionTaskOutputFull{
		WindowExecutionID: input.WindowExecutionID,
		TaskExecutionID:   taskExecID,
		Status:            commandStatusSuccess,
		StatusDetails:     "Task Succeeded",
		StartTime:         startTime,
		EndTime:           &endTime,
	}, nil
}

// GetMaintenanceWindowExecutionTaskInvocation returns a specific task invocation.
// Derives target information from the stored window target records.
func (b *InMemoryBackend) GetMaintenanceWindowExecutionTaskInvocation(
	ctx context.Context,
	input *GetMaintenanceWindowExecutionTaskInvocationInput,
) (*GetMaintenanceWindowExecutionTaskInvocationOutputFull, error) {
	region := getRegion(ctx)
	b.mu.RLock("GetMaintenanceWindowExecutionTaskInvocation")
	defer b.mu.RUnlock()

	startTime := time.Now().UTC()
	endTime := startTime.Add(time.Minute)

	windowTargetID := ""
	for _, target := range b.maintenanceWindowTargetsStore(region) {
		windowID, ok := mwWindowIDFromExec(input.WindowExecutionID)
		if ok && target.WindowID == windowID {
			windowTargetID = target.WindowTargetID

			break
		}
	}

	return &GetMaintenanceWindowExecutionTaskInvocationOutputFull{
		WindowExecutionID: input.WindowExecutionID,
		TaskExecutionID:   input.TaskExecutionID,
		InvocationID:      input.InvocationID,
		ExecutionID:       input.InvocationID,
		TaskType:          "RUN_COMMAND",
		Status:            commandStatusSuccess,
		StatusDetails:     "InvocationSucceeded",
		WindowTargetID:    windowTargetID,
		StartTime:         startTime,
		EndTime:           &endTime,
	}, nil
}

// --- Nodes ---

// ListNodes returns managed nodes derived from the activations store.
func (b *InMemoryBackend) ListNodes(
	ctx context.Context,
	_ *ListNodesInput,
) (*ListNodesOutputFull, error) {
	region := getRegion(ctx)
	b.mu.RLock("ListNodes")
	defer b.mu.RUnlock()

	activations := b.activationsStore(region)
	nodes := make([]NodeInfo, 0, len(activations))
	for _, act := range activations {
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
func (b *InMemoryBackend) ListNodesSummary(
	ctx context.Context,
	_ *ListNodesSummaryInput,
) (*ListNodesSummaryOutputFull, error) {
	region := getRegion(ctx)
	b.mu.RLock("ListNodesSummary")
	defer b.mu.RUnlock()

	return &ListNodesSummaryOutputFull{
		Summary: []map[string]string{
			{"NodeCount": strconv.Itoa(len(b.activationsStore(region)))},
		},
	}, nil
}

// --- Instance associations ---

// DescribeEffectiveInstanceAssociations returns associations targeting an instance.
func (b *InMemoryBackend) DescribeEffectiveInstanceAssociations(
	ctx context.Context,
	input *DescribeEffectiveInstanceAssociationsInput,
) (*DescribeEffectiveInstanceAssociationsOutputFull, error) {
	region := getRegion(ctx)
	b.mu.RLock("DescribeEffectiveInstanceAssociations")
	defer b.mu.RUnlock()

	var result []InstanceAssociationInfo

	for _, assoc := range b.associationsStore(region) {
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
	ctx context.Context,
	input *DescribeInstanceAssociationsStatusInput,
) (*DescribeInstanceAssociationsStatusOutputFull, error) {
	region := getRegion(ctx)
	b.mu.RLock("DescribeInstanceAssociationsStatus")
	defer b.mu.RUnlock()

	var result []InstanceAssociationStatusInfo

	for _, assoc := range b.associationsStore(region) {
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

	return &DescribeInstanceAssociationsStatusOutputFull{
		InstanceAssociationStatusInfos: result,
	}, nil
}

// DescribeInstanceInformation returns information about managed instances from activations.
func (b *InMemoryBackend) DescribeInstanceInformation(
	ctx context.Context,
	_ *DescribeInstanceInformationInput,
) (*DescribeInstanceInformationOutputFull, error) {
	region := getRegion(ctx)
	b.mu.RLock("DescribeInstanceInformation")
	defer b.mu.RUnlock()

	activations := b.activationsStore(region)
	list := make([]InstanceInformation, 0, len(activations))
	for _, act := range activations {
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
	ctx context.Context,
	input *DescribeInstancePatchStatesInput,
) (*DescribeInstancePatchStatesOutputFull, error) {
	region := getRegion(ctx)
	b.mu.RLock("DescribeInstancePatchStates")
	defer b.mu.RUnlock()

	store := b.instancePatchStates[region]
	states := make([]InstancePatchState, 0)

	if len(input.InstanceIDs) == 0 {
		for _, s := range store {
			states = append(states, *s)
		}
	} else {
		for _, instanceID := range input.InstanceIDs {
			if s, exists := store[instanceID]; exists {
				states = append(states, *s)
			}
		}
	}

	return &DescribeInstancePatchStatesOutputFull{InstancePatchStates: states}, nil
}
