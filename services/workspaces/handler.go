package workspaces

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	matchPriority = service.PriorityHeaderExact
	targetPrefix  = "WorkspacesService."
	contentType   = "application/x-amz-json-1.1"
)

// Handler handles WorkSpaces HTTP requests.
type Handler struct {
	Backend StorageBackend
	ops     map[string]service.JSONOpFunc
}

// NewHandler constructs a new Handler.
func NewHandler(b StorageBackend) *Handler {
	h := &Handler{Backend: b}
	h.ops = h.buildOps()

	return h
}

// Name returns the service name.
func (h *Handler) Name() string { return "WorkSpaces" }

// Reset resets the backend.
func (h *Handler) Reset() { h.Backend.Reset() }

// GetSupportedOperations returns the list of supported operations.
func (h *Handler) GetSupportedOperations() []string {
	ops := make([]string, 0, len(h.ops))
	for k := range h.ops {
		ops = append(ops, k)
	}

	return ops
}

// RouteMatcher returns a matcher that accepts WorkSpaces target header requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), targetPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return matchPriority }

// ExtractOperation extracts the operation name from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	return strings.TrimPrefix(c.Request().Header.Get("X-Amz-Target"), targetPrefix)
}

// ExtractResource extracts the resource identifier from the request.
func (h *Handler) ExtractResource(c *echo.Context) string {
	return h.ExtractOperation(c)
}

// Handler returns the Echo handler function.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return service.HandleTarget(
			c, logger.Load(c.Request().Context()), h.Name(), contentType,
			h.GetSupportedOperations(), h.dispatch, h.handleError,
		)
	}
}

func (h *Handler) dispatch(ctx context.Context, action string, body []byte) ([]byte, error) {
	fn, ok := h.ops[action]
	if !ok {
		return nil, newUnknownOpError(action)
	}

	result, err := fn(ctx, body)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleError(_ context.Context, c *echo.Context, _ string, err error) error {
	switch {
	case errors.Is(err, awserr.ErrNotFound):
		return c.JSON(http.StatusNotFound, errBody(errResourceNotFound, err.Error()))
	case errors.Is(err, awserr.ErrInvalidParameter):
		return c.JSON(http.StatusBadRequest, errBody(errInvalidParameterValues, err.Error()))
	default:
		return c.JSON(
			http.StatusInternalServerError,
			errBody("InternalServerException", err.Error()),
		)
	}
}

func (h *Handler) buildOps() map[string]service.JSONOpFunc {
	base := map[string]service.JSONOpFunc{
		"CreateWorkspaces":   service.WrapOp(h.handleCreateWorkspaces),
		"DescribeWorkspaces": service.WrapOp(h.handleDescribeWorkspaces),
		"DescribeWorkspacesConnectionStatus": service.WrapOp(
			h.handleDescribeWorkspacesConnectionStatus,
		),
		"ModifyWorkspaceProperties":    service.WrapOp(h.handleModifyWorkspaceProperties),
		"ModifyWorkspaceState":         service.WrapOp(h.handleModifyWorkspaceState),
		"RebootWorkspaces":             service.WrapOp(h.handleRebootWorkspaces),
		"RebuildWorkspaces":            service.WrapOp(h.handleRebuildWorkspaces),
		"StartWorkspaces":              service.WrapOp(h.handleStartWorkspaces),
		"StopWorkspaces":               service.WrapOp(h.handleStopWorkspaces),
		"TerminateWorkspaces":          service.WrapOp(h.handleTerminateWorkspaces),
		"CreateTags":                   service.WrapOp(h.handleCreateTags),
		"DeleteTags":                   service.WrapOp(h.handleDeleteTags),
		"DescribeTags":                 service.WrapOp(h.handleDescribeTags),
		"DescribeWorkspaceBundles":     service.WrapOp(h.handleDescribeWorkspaceBundles),
		"DescribeWorkspaceDirectories": service.WrapOp(h.handleDescribeWorkspaceDirectories),
	}

	maps.Copy(base, h.buildAppendixAOps())

	return base
}

// tagItem represents a key/value tag pair in AWS API requests.
type tagItem struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
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

type emptyOutput struct{}

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

// --- Tags ---

type createTagsInput struct {
	ResourceID string    `json:"ResourceId"`
	Tags       []tagItem `json:"Tags"`
}

func (h *Handler) handleCreateTags(_ context.Context, req *createTagsInput) (*emptyOutput, error) {
	if req.ResourceID == "" {
		return nil, awserr.New("ResourceId is required", awserr.ErrInvalidParameter)
	}

	tags := make(map[string]string, len(req.Tags))
	for _, t := range req.Tags {
		tags[t.Key] = t.Value
	}

	return &emptyOutput{}, h.Backend.CreateTags(req.ResourceID, tags)
}

type deleteTagsInput struct {
	ResourceID string   `json:"ResourceId"`
	TagKeys    []string `json:"TagKeys"`
}

func (h *Handler) handleDeleteTags(_ context.Context, req *deleteTagsInput) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.DeleteTags(req.ResourceID, req.TagKeys)
}

type describeTagsInput struct {
	ResourceID string `json:"ResourceId"`
}

type describeTagsOutput struct {
	TagList []tagItem `json:"TagList"`
}

func (h *Handler) handleDescribeTags(
	_ context.Context,
	req *describeTagsInput,
) (*describeTagsOutput, error) {
	tags, err := h.Backend.DescribeTags(req.ResourceID)
	if err != nil {
		return nil, err
	}

	items := make([]tagItem, 0, len(tags))
	for k, v := range tags {
		items = append(items, tagItem{Key: k, Value: v})
	}

	return &describeTagsOutput{TagList: items}, nil
}

// --- DescribeWorkspaceBundles ---

type describeBundlesInput struct {
	Owner     string   `json:"Owner"`
	NextToken string   `json:"NextToken"`
	BundleIDs []string `json:"BundleIds"`
}

type describeBundlesOutput struct {
	NextToken string       `json:"NextToken,omitempty"`
	Bundles   []bundleResp `json:"Bundles"`
}

type bundleComputeTypeResp struct {
	Name string `json:"Name,omitempty"`
}

type bundleStorageResp struct {
	Capacity int32 `json:"Capacity,omitempty"`
}

type bundleResp struct {
	BundleID    string                `json:"BundleId"`
	Name        string                `json:"Name"`
	Owner       string                `json:"Owner"`
	Description string                `json:"Description"`
	ImageID     string                `json:"ImageId,omitempty"`
	ComputeType bundleComputeTypeResp `json:"ComputeType"`
	UserStorage bundleStorageResp     `json:"UserStorage"`
	RootStorage bundleStorageResp     `json:"RootStorage"`
}

func (h *Handler) handleDescribeWorkspaceBundles(
	ctx context.Context,
	req *describeBundlesInput,
) (*describeBundlesOutput, error) {
	bundles, nextToken, err := h.Backend.DescribeWorkspaceBundles(
		ctx,
		req.BundleIDs,
		req.Owner,
		req.NextToken,
	)
	if err != nil {
		return nil, err
	}

	items := make([]bundleResp, 0, len(bundles))
	for _, bun := range bundles {
		items = append(items, bundleResp{
			BundleID:    bun.BundleID,
			Name:        bun.Name,
			Owner:       bun.Owner,
			Description: bun.Description,
			ImageID:     bun.ImageID,
			ComputeType: bundleComputeTypeResp{Name: bun.ComputeType.Name},
			UserStorage: bundleStorageResp{Capacity: bun.UserStorage.Capacity},
			RootStorage: bundleStorageResp{Capacity: bun.RootStorage.Capacity},
		})
	}

	return &describeBundlesOutput{Bundles: items, NextToken: nextToken}, nil
}

// --- DescribeWorkspaceDirectories ---

type describeDirectoriesInput struct {
	NextToken    string   `json:"NextToken"`
	DirectoryIDs []string `json:"DirectoryIds"`
}

type describeDirectoriesOutput struct {
	NextToken   string    `json:"NextToken,omitempty"`
	Directories []dirResp `json:"Directories"`
}

type dirResp struct {
	DirectoryID   string   `json:"DirectoryId"`
	DirectoryName string   `json:"DirectoryName,omitempty"`
	DirectoryType string   `json:"DirectoryType,omitempty"`
	Alias         string   `json:"Alias,omitempty"`
	State         string   `json:"State"`
	SubnetIds     []string `json:"SubnetIds,omitempty"` //nolint:revive // AWS API uses SubnetIds capitalization
}

func (h *Handler) handleDescribeWorkspaceDirectories(
	ctx context.Context, req *describeDirectoriesInput,
) (*describeDirectoriesOutput, error) {
	dirs, nextToken, err := h.Backend.DescribeWorkspaceDirectories(
		ctx,
		req.DirectoryIDs,
		req.NextToken,
	)
	if err != nil {
		return nil, err
	}

	items := make([]dirResp, 0, len(dirs))
	for _, d := range dirs {
		items = append(items, dirResp{
			DirectoryID:   d.DirectoryID,
			DirectoryName: d.DirectoryName,
			DirectoryType: d.DirectoryType,
			Alias:         d.Alias,
			State:         d.State,
			SubnetIds:     d.SubnetIDs,
		})
	}

	return &describeDirectoriesOutput{Directories: items, NextToken: nextToken}, nil
}

// --- Helpers ---

func errBody(code, msg string) map[string]string {
	return map[string]string{"__type": code, "message": msg}
}

type unknownOpError struct{ op string }

func (e *unknownOpError) Error() string { return "unknown operation: " + e.op }

func newUnknownOpError(op string) error { return &unknownOpError{op: op} }

func extractIDs(reqs []workspaceIDItem) []string {
	ids := make([]string, 0, len(reqs))
	for _, r := range reqs {
		ids = append(ids, r.WorkspaceID)
	}

	return ids
}

// failedRequestItem is the JSON representation of a FailedRequest.
type failedRequestItem struct {
	WorkspaceID  string `json:"WorkspaceId"`
	ErrorCode    string `json:"ErrorCode"`
	ErrorMessage string `json:"ErrorMessage"`
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
