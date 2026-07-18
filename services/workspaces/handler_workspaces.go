package workspaces

import (
	"context"
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// buildWorkspacesOps returns the map of core WorkSpace lifecycle operations.
func (h *Handler) buildWorkspacesOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateWorkspaces":   service.WrapOp(h.handleCreateWorkspaces),
		"DescribeWorkspaces": service.WrapOp(h.handleDescribeWorkspaces),
		"DescribeWorkspacesConnectionStatus": service.WrapOp(
			h.handleDescribeWorkspacesConnectionStatus,
		),
		"ModifyWorkspaceProperties": service.WrapOp(h.handleModifyWorkspaceProperties),
		"ModifyWorkspaceState":      service.WrapOp(h.handleModifyWorkspaceState),
		"RebootWorkspaces":          service.WrapOp(h.handleRebootWorkspaces),
		"RebuildWorkspaces":         service.WrapOp(h.handleRebuildWorkspaces),
		"StartWorkspaces":           service.WrapOp(h.handleStartWorkspaces),
		"StopWorkspaces":            service.WrapOp(h.handleStopWorkspaces),
		"TerminateWorkspaces":       service.WrapOp(h.handleTerminateWorkspaces),
		"MigrateWorkspace":          service.WrapOp(h.handleMigrateWorkspace),
		"RestoreWorkspace":          service.WrapOp(h.handleRestoreWorkspace),
		"CreateStandbyWorkspaces":   service.WrapOp(h.handleCreateStandbyWorkspaces),
	}
}

// workspacePropertiesResp is the JSON shape for WorkspaceProperties in responses.
type workspacePropertiesResp struct {
	ComputeTypeName                     string `json:"ComputeTypeName,omitempty"`
	RunningMode                         string `json:"RunningMode,omitempty"`
	RootVolumeSizeGib                   int32  `json:"RootVolumeSizeGib,omitempty"`
	RunningModeAutoStopTimeoutInMinutes int32  `json:"RunningModeAutoStopTimeoutInMinutes,omitempty"`
	UserVolumeSizeGib                   int32  `json:"UserVolumeSizeGib,omitempty"`
}

// --- CreateWorkspaces ---

type createWorkspacesInput struct {
	Workspaces []createWorkspaceSpec `json:"Workspaces"`
}

type createWorkspaceSpec struct {
	WorkspaceProperties         *createWorkspaceProps `json:"WorkspaceProperties,omitempty"`
	UserName                    string                `json:"UserName"`
	DirectoryID                 string                `json:"DirectoryId"`
	BundleID                    string                `json:"BundleId"`
	SubnetID                    string                `json:"SubnetId"`
	VolumeEncryptionKey         string                `json:"VolumeEncryptionKey"`
	Tags                        []tagItem             `json:"Tags"`
	UserVolumeEncryptionEnabled bool                  `json:"UserVolumeEncryptionEnabled"` //nolint:tagliatelle // JSON
	RootVolumeEncryptionEnabled bool                  `json:"RootVolumeEncryptionEnabled"` //nolint:tagliatelle // JSON
}

type createWorkspaceProps struct {
	ComputeTypeName                     string `json:"ComputeTypeName"`
	RunningMode                         string `json:"RunningMode"`
	RootVolumeSizeGib                   int32  `json:"RootVolumeSizeGib"`
	RunningModeAutoStopTimeoutInMinutes int32  `json:"RunningModeAutoStopTimeoutInMinutes"`
	UserVolumeSizeGib                   int32  `json:"UserVolumeSizeGib"`
}

type createWorkspacesOutput struct {
	FailedRequests  []failedCreateWorkspaceItem `json:"FailedRequests"`
	PendingRequests []pendingWorkspace          `json:"PendingRequests"`
}

// workspaceRequestResp echoes the WorkspaceRequest that failed to create, matching
// the real WorkspaceRequest shape (a distinct, narrower shape than Workspace/pendingWorkspace:
// no WorkspaceId/State/SubnetId, since the WorkSpace was never created).
type workspaceRequestResp struct {
	WorkspaceProperties         *workspacePropertiesResp `json:"WorkspaceProperties,omitempty"`
	BundleID                    string                   `json:"BundleId,omitempty"`
	DirectoryID                 string                   `json:"DirectoryId,omitempty"`
	UserName                    string                   `json:"UserName,omitempty"`
	VolumeEncryptionKey         string                   `json:"VolumeEncryptionKey,omitempty"`
	Tags                        []tagItem                `json:"Tags,omitempty"`
	UserVolumeEncryptionEnabled bool                     `json:"UserVolumeEncryptionEnabled,omitempty"`
	RootVolumeEncryptionEnabled bool                     `json:"RootVolumeEncryptionEnabled,omitempty"`
}

// failedCreateWorkspaceItem is the JSON representation of a FailedCreateWorkspaceRequest.
type failedCreateWorkspaceItem struct {
	WorkspaceRequest *workspaceRequestResp `json:"WorkspaceRequest,omitempty"`
	ErrorCode        string                `json:"ErrorCode,omitempty"`
	ErrorMessage     string                `json:"ErrorMessage,omitempty"`
}

func specToWorkspaceRequestResp(spec createWorkspaceSpec) *workspaceRequestResp {
	tags := make([]tagItem, len(spec.Tags))
	copy(tags, spec.Tags)

	var props *workspacePropertiesResp
	if spec.WorkspaceProperties != nil {
		props = &workspacePropertiesResp{
			ComputeTypeName:                     spec.WorkspaceProperties.ComputeTypeName,
			RunningMode:                         spec.WorkspaceProperties.RunningMode,
			RootVolumeSizeGib:                   spec.WorkspaceProperties.RootVolumeSizeGib,
			RunningModeAutoStopTimeoutInMinutes: spec.WorkspaceProperties.RunningModeAutoStopTimeoutInMinutes,
			UserVolumeSizeGib:                   spec.WorkspaceProperties.UserVolumeSizeGib,
		}
	}

	return &workspaceRequestResp{
		BundleID:                    spec.BundleID,
		DirectoryID:                 spec.DirectoryID,
		UserName:                    spec.UserName,
		VolumeEncryptionKey:         spec.VolumeEncryptionKey,
		UserVolumeEncryptionEnabled: spec.UserVolumeEncryptionEnabled,
		RootVolumeEncryptionEnabled: spec.RootVolumeEncryptionEnabled,
		Tags:                        tags,
		WorkspaceProperties:         props,
	}
}

// classifyCreateError maps a backend error to the (ErrorCode, ErrorMessage) pair
// reported in a FailedCreateWorkspaceRequest item.
func classifyCreateError(err error) (string, string) {
	switch {
	case errors.Is(err, awserr.ErrNotFound):
		return errResourceNotFound, err.Error()
	case errors.Is(err, awserr.ErrInvalidParameter):
		return errInvalidParameterValues, err.Error()
	default:
		return "InternalServerException", err.Error()
	}
}

type pendingWorkspace struct {
	WorkspaceProperties         *workspacePropertiesResp `json:"WorkspaceProperties,omitempty"`
	WorkspaceID                 string                   `json:"WorkspaceId"`
	DirectoryID                 string                   `json:"DirectoryId"`
	UserName                    string                   `json:"UserName"`
	BundleID                    string                   `json:"BundleId"`
	SubnetID                    string                   `json:"SubnetId,omitempty"`
	VolumeEncryptionKey         string                   `json:"VolumeEncryptionKey,omitempty"`
	State                       string                   `json:"State"`
	UserVolumeEncryptionEnabled bool                     `json:"UserVolumeEncryptionEnabled,omitempty"`
	RootVolumeEncryptionEnabled bool                     `json:"RootVolumeEncryptionEnabled,omitempty"`
}

func validateCreateWorkspacesInput(req *createWorkspacesInput) error {
	if len(req.Workspaces) == 0 {
		return awserr.New("Workspaces list must not be empty", awserr.ErrInvalidParameter)
	}

	if len(req.Workspaces) > maxWorkspacesPerCreate {
		return awserr.Newf(
			"too many workspaces: maximum is %d per request",
			awserr.ErrInvalidParameter, maxWorkspacesPerCreate)
	}

	for i, spec := range req.Workspaces {
		switch {
		case spec.UserName == "":
			return awserr.Newf(
				"workspace[%d]: UserName is required", awserr.ErrInvalidParameter, i)
		case spec.DirectoryID == "":
			return awserr.Newf(
				"workspace[%d]: DirectoryId is required", awserr.ErrInvalidParameter, i)
		case spec.BundleID == "":
			return awserr.Newf(
				"workspace[%d]: BundleId is required", awserr.ErrInvalidParameter, i)
		case len(spec.Tags) > maxTagsPerResource:
			return awserr.Newf(
				"workspace[%d]: too many tags (%d); maximum is %d",
				awserr.ErrInvalidParameter,
				i,
				len(spec.Tags),
				maxTagsPerResource,
			)
		}
	}

	return nil
}

func specToCreationSpec(spec createWorkspaceSpec) *WorkspaceCreationSpec {
	tags := make(map[string]string, len(spec.Tags))
	for _, t := range spec.Tags {
		tags[t.Key] = t.Value
	}

	var props *WorkspaceProperties
	if spec.WorkspaceProperties != nil {
		props = &WorkspaceProperties{
			ComputeTypeName:                     spec.WorkspaceProperties.ComputeTypeName,
			RunningMode:                         spec.WorkspaceProperties.RunningMode,
			RootVolumeSizeGib:                   spec.WorkspaceProperties.RootVolumeSizeGib,
			RunningModeAutoStopTimeoutInMinutes: spec.WorkspaceProperties.RunningModeAutoStopTimeoutInMinutes,
			UserVolumeSizeGib:                   spec.WorkspaceProperties.UserVolumeSizeGib,
		}
	}

	return &WorkspaceCreationSpec{
		UserName:                    spec.UserName,
		DirectoryID:                 spec.DirectoryID,
		BundleID:                    spec.BundleID,
		SubnetID:                    spec.SubnetID,
		VolumeEncryptionKey:         spec.VolumeEncryptionKey,
		UserVolumeEncryptionEnabled: spec.UserVolumeEncryptionEnabled,
		RootVolumeEncryptionEnabled: spec.RootVolumeEncryptionEnabled,
		Tags:                        tags,
		Properties:                  props,
	}
}

func wsToPending(ws *Workspace) pendingWorkspace {
	pw := pendingWorkspace{
		WorkspaceID:                 ws.WorkspaceID,
		DirectoryID:                 ws.DirectoryID,
		UserName:                    ws.UserName,
		BundleID:                    ws.BundleID,
		SubnetID:                    ws.SubnetID,
		VolumeEncryptionKey:         ws.VolumeEncryptionKey,
		UserVolumeEncryptionEnabled: ws.UserVolumeEncryptionEnabled,
		RootVolumeEncryptionEnabled: ws.RootVolumeEncryptionEnabled,
		State:                       ws.State,
	}

	if ws.Properties != nil {
		pw.WorkspaceProperties = &workspacePropertiesResp{
			ComputeTypeName:                     ws.Properties.ComputeTypeName,
			RunningMode:                         ws.Properties.RunningMode,
			RootVolumeSizeGib:                   ws.Properties.RootVolumeSizeGib,
			RunningModeAutoStopTimeoutInMinutes: ws.Properties.RunningModeAutoStopTimeoutInMinutes,
			UserVolumeSizeGib:                   ws.Properties.UserVolumeSizeGib,
		}
	}

	return pw
}

func (h *Handler) handleCreateWorkspaces(
	ctx context.Context,
	req *createWorkspacesInput,
) (*createWorkspacesOutput, error) {
	if err := validateCreateWorkspacesInput(req); err != nil {
		return nil, err
	}

	pending := make([]pendingWorkspace, 0, len(req.Workspaces))
	failed := make([]failedCreateWorkspaceItem, 0)

	// Per AWS, CreateWorkspaces is a partial-failure batch operation: a runtime
	// failure for one WorkspaceRequest (e.g. an unregistered DirectoryId) must not
	// abort the rest of the batch. Only request-shape validation (handled above by
	// validateCreateWorkspacesInput) fails the whole call.
	for _, spec := range req.Workspaces {
		ws, err := h.Backend.CreateWorkspace(ctx, specToCreationSpec(spec))
		if err != nil {
			code, message := classifyCreateError(err)
			failed = append(failed, failedCreateWorkspaceItem{
				WorkspaceRequest: specToWorkspaceRequestResp(spec),
				ErrorCode:        code,
				ErrorMessage:     message,
			})

			continue
		}

		pending = append(pending, wsToPending(ws))
	}

	return &createWorkspacesOutput{
		FailedRequests:  failed,
		PendingRequests: pending,
	}, nil
}

// --- DescribeWorkspaces ---

type describeWorkspacesInput struct {
	DirectoryID  string   `json:"DirectoryId"`
	UserName     string   `json:"UserName"`
	BundleID     string   `json:"BundleId"`
	NextToken    string   `json:"NextToken"`
	WorkspaceIDs []string `json:"WorkspaceIds"`
	Limit        int32    `json:"Limit"`
}

type describeWorkspacesOutput struct {
	NextToken  string          `json:"NextToken,omitempty"`
	Workspaces []workspaceResp `json:"Workspaces"`
}

type workspaceResp struct {
	WorkspaceProperties         *workspacePropertiesResp `json:"WorkspaceProperties,omitempty"`
	Tags                        map[string]string        `json:"Tags,omitempty"`
	WorkspaceID                 string                   `json:"WorkspaceId"`
	DirectoryID                 string                   `json:"DirectoryId"`
	UserName                    string                   `json:"UserName"`
	BundleID                    string                   `json:"BundleId"`
	SubnetID                    string                   `json:"SubnetId,omitempty"`
	VolumeEncryptionKey         string                   `json:"VolumeEncryptionKey,omitempty"`
	ComputerName                string                   `json:"ComputerName,omitempty"`
	ErrorCode                   string                   `json:"ErrorCode,omitempty"`
	ErrorMessage                string                   `json:"ErrorMessage,omitempty"`
	State                       string                   `json:"State"`
	UserVolumeEncryptionEnabled bool                     `json:"UserVolumeEncryptionEnabled,omitempty"`
	RootVolumeEncryptionEnabled bool                     `json:"RootVolumeEncryptionEnabled,omitempty"`
}

func (h *Handler) handleDescribeWorkspaces(
	ctx context.Context,
	req *describeWorkspacesInput,
) (*describeWorkspacesOutput, error) {
	var directoryIDs, userIDs, bundleIDs []string
	if req.DirectoryID != "" {
		directoryIDs = []string{req.DirectoryID}
	}

	if req.UserName != "" {
		userIDs = []string{req.UserName}
	}

	if req.BundleID != "" {
		bundleIDs = []string{req.BundleID}
	}

	wsList, nextToken, err := h.Backend.DescribeWorkspaces(
		ctx, req.WorkspaceIDs, directoryIDs, userIDs, bundleIDs, req.Limit, req.NextToken,
	)
	if err != nil {
		return nil, err
	}

	items := make([]workspaceResp, 0, len(wsList))
	for _, ws := range wsList {
		items = append(items, toWorkspaceResp(ws))
	}

	return &describeWorkspacesOutput{Workspaces: items, NextToken: nextToken}, nil
}

func toWorkspaceResp(ws *Workspace) workspaceResp {
	item := workspaceResp{
		WorkspaceID:                 ws.WorkspaceID,
		DirectoryID:                 ws.DirectoryID,
		UserName:                    ws.UserName,
		BundleID:                    ws.BundleID,
		State:                       ws.State,
		SubnetID:                    ws.SubnetID,
		VolumeEncryptionKey:         ws.VolumeEncryptionKey,
		UserVolumeEncryptionEnabled: ws.UserVolumeEncryptionEnabled,
		RootVolumeEncryptionEnabled: ws.RootVolumeEncryptionEnabled,
		ComputerName:                ws.ComputerName,
		ErrorCode:                   ws.ErrorCode,
		ErrorMessage:                ws.ErrorMessage,
		Tags:                        ws.Tags,
	}

	if ws.Properties != nil {
		item.WorkspaceProperties = &workspacePropertiesResp{
			ComputeTypeName:                     ws.Properties.ComputeTypeName,
			RunningMode:                         ws.Properties.RunningMode,
			RootVolumeSizeGib:                   ws.Properties.RootVolumeSizeGib,
			RunningModeAutoStopTimeoutInMinutes: ws.Properties.RunningModeAutoStopTimeoutInMinutes,
			UserVolumeSizeGib:                   ws.Properties.UserVolumeSizeGib,
		}
	}

	return item
}

// --- DescribeWorkspacesConnectionStatus ---

type describeConnectionStatusInput struct {
	WorkspaceIDs []string `json:"WorkspaceIds"`
}

type describeConnectionStatusOutput struct {
	WorkspacesConnectionStatus []connStatusResp `json:"WorkspacesConnectionStatus"`
}

type connStatusResp struct {
	WorkspaceID     string `json:"WorkspaceId"`
	ConnectionState string `json:"ConnectionState"`
}

func (h *Handler) handleDescribeWorkspacesConnectionStatus(
	_ context.Context, req *describeConnectionStatusInput,
) (*describeConnectionStatusOutput, error) {
	statuses, err := h.Backend.GetWorkspacesConnectionStatus(req.WorkspaceIDs)
	if err != nil {
		return nil, err
	}

	items := make([]connStatusResp, 0, len(statuses))
	for _, s := range statuses {
		items = append(
			items,
			connStatusResp{WorkspaceID: s.WorkspaceID, ConnectionState: s.ConnectionState},
		)
	}

	return &describeConnectionStatusOutput{WorkspacesConnectionStatus: items}, nil
}

// --- ModifyWorkspaceProperties ---

type modifyPropertiesInput struct {
	WorkspaceID         string `json:"WorkspaceId"`
	WorkspaceProperties struct {
		ComputeTypeName                     string `json:"ComputeTypeName"`
		RunningMode                         string `json:"RunningMode"`
		RootVolumeSizeGib                   int32  `json:"RootVolumeSizeGib"`
		RunningModeAutoStopTimeoutInMinutes int32  `json:"RunningModeAutoStopTimeoutInMinutes"`
		UserVolumeSizeGib                   int32  `json:"UserVolumeSizeGib"`
	} `json:"WorkspaceProperties"`
}

func (h *Handler) handleModifyWorkspaceProperties(
	_ context.Context,
	req *modifyPropertiesInput,
) (*emptyOutput, error) {
	props := WorkspaceProperties{
		ComputeTypeName:                     req.WorkspaceProperties.ComputeTypeName,
		RunningMode:                         req.WorkspaceProperties.RunningMode,
		RootVolumeSizeGib:                   req.WorkspaceProperties.RootVolumeSizeGib,
		RunningModeAutoStopTimeoutInMinutes: req.WorkspaceProperties.RunningModeAutoStopTimeoutInMinutes,
		UserVolumeSizeGib:                   req.WorkspaceProperties.UserVolumeSizeGib,
	}

	return &emptyOutput{}, h.Backend.ModifyWorkspaceProperties(req.WorkspaceID, props)
}

// --- ModifyWorkspaceState ---

type modifyStateInput struct {
	WorkspaceID    string `json:"WorkspaceId"`
	WorkspaceState string `json:"WorkspaceState"`
}

func (h *Handler) handleModifyWorkspaceState(
	_ context.Context,
	req *modifyStateInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.ModifyWorkspaceState(req.WorkspaceID, req.WorkspaceState)
}

// --- Bulk workspace operations ---

type workspaceIDItem struct {
	WorkspaceID string `json:"WorkspaceId"`
}

type bulkOutput struct {
	FailedRequests []failedRequestItem `json:"FailedRequests"`
}

type rebootInput struct {
	RebootWorkspaceRequests []workspaceIDItem `json:"RebootWorkspaceRequests"`
}

func (h *Handler) handleRebootWorkspaces(_ context.Context, req *rebootInput) (*bulkOutput, error) {
	failures, err := h.Backend.RebootWorkspaces(extractIDs(req.RebootWorkspaceRequests))
	if err != nil {
		return nil, err
	}

	return &bulkOutput{FailedRequests: marshalFailedRequests(failures)}, nil
}

type rebuildInput struct {
	RebuildWorkspaceRequests []workspaceIDItem `json:"RebuildWorkspaceRequests"`
}

func (h *Handler) handleRebuildWorkspaces(
	_ context.Context,
	req *rebuildInput,
) (*bulkOutput, error) {
	failures, err := h.Backend.RebuildWorkspaces(extractIDs(req.RebuildWorkspaceRequests))
	if err != nil {
		return nil, err
	}

	return &bulkOutput{FailedRequests: marshalFailedRequests(failures)}, nil
}

type startInput struct {
	StartWorkspaceRequests []workspaceIDItem `json:"StartWorkspaceRequests"`
}

func (h *Handler) handleStartWorkspaces(_ context.Context, req *startInput) (*bulkOutput, error) {
	failures, err := h.Backend.StartWorkspaces(extractIDs(req.StartWorkspaceRequests))
	if err != nil {
		return nil, err
	}

	return &bulkOutput{FailedRequests: marshalFailedRequests(failures)}, nil
}

type stopInput struct {
	StopWorkspaceRequests []workspaceIDItem `json:"StopWorkspaceRequests"`
}

func (h *Handler) handleStopWorkspaces(_ context.Context, req *stopInput) (*bulkOutput, error) {
	failures, err := h.Backend.StopWorkspaces(extractIDs(req.StopWorkspaceRequests))
	if err != nil {
		return nil, err
	}

	return &bulkOutput{FailedRequests: marshalFailedRequests(failures)}, nil
}

type terminateInput struct {
	TerminateWorkspaceRequests []workspaceIDItem `json:"TerminateWorkspaceRequests"`
}

func (h *Handler) handleTerminateWorkspaces(
	_ context.Context,
	req *terminateInput,
) (*bulkOutput, error) {
	failures, err := h.Backend.TerminateWorkspaces(extractIDs(req.TerminateWorkspaceRequests))
	if err != nil {
		return nil, err
	}

	return &bulkOutput{FailedRequests: marshalFailedRequests(failures)}, nil
}

// failedRequestItem is the JSON representation of a FailedRequest.
type failedRequestItem struct {
	WorkspaceID  string `json:"WorkspaceId"`
	ErrorCode    string `json:"ErrorCode"`
	ErrorMessage string `json:"ErrorMessage"`
}

func extractIDs(reqs []workspaceIDItem) []string {
	ids := make([]string, 0, len(reqs))
	for _, r := range reqs {
		ids = append(ids, r.WorkspaceID)
	}

	return ids
}

func marshalFailedRequests(failures []FailedRequest) []failedRequestItem {
	if len(failures) == 0 {
		return []failedRequestItem{}
	}

	items := make([]failedRequestItem, 0, len(failures))
	for _, f := range failures {
		items = append(items, failedRequestItem(f))
	}

	return items
}

// --- Workspace-level ops ---

type migrateWorkspaceInput struct {
	SourceWorkspaceId string `json:"SourceWorkspaceId"` //nolint:revive,staticcheck // existing issue.
	BundleId          string `json:"BundleId"`          //nolint:revive,staticcheck // existing issue.
}

type migrateWorkspaceOutput struct {
	SourceWorkspaceId string `json:"SourceWorkspaceId"` //nolint:revive,staticcheck // existing issue.
	TargetWorkspaceId string `json:"TargetWorkspaceId"` //nolint:revive,staticcheck // existing issue.
}

func (h *Handler) handleMigrateWorkspace(
	_ context.Context, req *migrateWorkspaceInput,
) (*migrateWorkspaceOutput, error) {
	srcID, tgtID, err := h.Backend.MigrateWorkspace(req.SourceWorkspaceId, req.BundleId)
	if err != nil {
		return nil, err
	}

	return &migrateWorkspaceOutput{SourceWorkspaceId: srcID, TargetWorkspaceId: tgtID}, nil
}

type restoreWorkspaceInput struct {
	WorkspaceId string `json:"WorkspaceId"` //nolint:revive,staticcheck // existing issue.
}

func (h *Handler) handleRestoreWorkspace(
	_ context.Context,
	req *restoreWorkspaceInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.RestoreWorkspace(req.WorkspaceId)
}

type standbyWorkspaceSpec struct {
	PrimaryWorkspaceId  string `json:"PrimaryWorkspaceId"` //nolint:revive,staticcheck // existing issue.
	DirectoryId         string `json:"DirectoryId"`        //nolint:revive,staticcheck // existing issue.
	VolumeEncryptionKey string `json:"VolumeEncryptionKey"`
}

type createStandbyWorkspacesInput struct {
	PrimaryRegion     string                 `json:"PrimaryRegion"`
	StandbyWorkspaces []standbyWorkspaceSpec `json:"StandbyWorkspaces"`
}

type createStandbyWorkspacesOutput struct {
	FailedStandbyRequests  []any               `json:"FailedStandbyRequests"`
	PendingStandbyRequests []map[string]string `json:"PendingStandbyRequests"`
}

func (h *Handler) handleCreateStandbyWorkspaces(
	_ context.Context, req *createStandbyWorkspacesInput,
) (*createStandbyWorkspacesOutput, error) {
	specs := make([]map[string]string, 0, len(req.StandbyWorkspaces))
	for _, s := range req.StandbyWorkspaces {
		specs = append(specs, map[string]string{
			"DirectoryId": s.DirectoryId,
			"UserName":    "",
			"BundleId":    "",
		})
	}

	failed, pending, err := h.Backend.CreateStandbyWorkspaces(req.PrimaryRegion, specs)
	if err != nil {
		return nil, err
	}

	failedOut := make([]any, 0, len(failed))
	for _, f := range failed {
		failedOut = append(failedOut, f)
	}

	return &createStandbyWorkspacesOutput{
		FailedStandbyRequests:  failedOut,
		PendingStandbyRequests: pending,
	}, nil
}
