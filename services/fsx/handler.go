package fsx

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	fsxTargetPrefix  = "AWSSimbaAPIService_v20180301."
	matchPriority    = service.PriorityHeaderExact
	bodyReadBufBytes = 4096

	opCreateFileSystem           = "CreateFileSystem"
	opCreateFileSystemFromBackup = "CreateFileSystemFromBackup"
	opDescribeFileSystems        = "DescribeFileSystems"
	opDeleteFileSystem           = "DeleteFileSystem"
	opUpdateFileSystem           = "UpdateFileSystem"
	opCreateBackup               = "CreateBackup"
	opDescribeBackups            = "DescribeBackups"
	opDeleteBackup               = "DeleteBackup"
	opCopyBackup                 = "CopyBackup"
	opTagResource                = "TagResource"
	opUntagResource              = "UntagResource"
	opListTagsForResource        = "ListTagsForResource"

	opAssociateFileSystemAliases    = "AssociateFileSystemAliases"
	opDisassociateFileSystemAliases = "DisassociateFileSystemAliases"
	opDescribeFileSystemAliases     = "DescribeFileSystemAliases"

	opCreateDataRepositoryAssociation = "CreateDataRepositoryAssociation"
	opDeleteDataRepositoryAssociation = "DeleteDataRepositoryAssociation"
	opDescribeDataRepositoryAssocs    = "DescribeDataRepositoryAssociations"
	opUpdateDataRepositoryAssociation = "UpdateDataRepositoryAssociation"

	opCancelDataRepositoryTask    = "CancelDataRepositoryTask"
	opCreateDataRepositoryTask    = "CreateDataRepositoryTask"
	opDescribeDataRepositoryTasks = "DescribeDataRepositoryTasks"

	opCreateFileCache    = "CreateFileCache"
	opDeleteFileCache    = "DeleteFileCache"
	opDescribeFileCaches = "DescribeFileCaches"
	opUpdateFileCache    = "UpdateFileCache"

	opCreateSnapshot              = "CreateSnapshot"
	opDeleteSnapshot              = "DeleteSnapshot"
	opDescribeSnapshots           = "DescribeSnapshots"
	opUpdateSnapshot              = "UpdateSnapshot"
	opCopySnapshotAndUpdateVolume = "CopySnapshotAndUpdateVolume"

	opCreateStorageVirtualMachine    = "CreateStorageVirtualMachine"
	opDeleteStorageVirtualMachine    = "DeleteStorageVirtualMachine"
	opDescribeStorageVirtualMachines = "DescribeStorageVirtualMachines"
	opUpdateStorageVirtualMachine    = "UpdateStorageVirtualMachine"

	opCreateVolume              = "CreateVolume"
	opCreateVolumeFromBackup    = "CreateVolumeFromBackup"
	opDeleteVolume              = "DeleteVolume"
	opDescribeVolumes           = "DescribeVolumes"
	opRestoreVolumeFromSnapshot = "RestoreVolumeFromSnapshot"
	opUpdateVolume              = "UpdateVolume"

	opCreateAndAttachS3AccessPoint     = "CreateAndAttachS3AccessPoint"
	opDetachAndDeleteS3AccessPoint     = "DetachAndDeleteS3AccessPoint"
	opDescribeS3AccessPointAttachments = "DescribeS3AccessPointAttachments"

	opDescribeSharedVpcConfiguration = "DescribeSharedVpcConfiguration"
	opUpdateSharedVpcConfiguration   = "UpdateSharedVpcConfiguration"

	opReleaseFileSystemNfsV3Locks     = "ReleaseFileSystemNfsV3Locks"
	opStartMisconfiguredStateRecovery = "StartMisconfiguredStateRecovery"
)

var errUnknownOperation = errors.New("UnsupportedOperation")

// Handler handles FSx HTTP requests.
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
func (h *Handler) Name() string { return "FSx" }

// Reset resets the backend.
func (h *Handler) Reset() { h.Backend.Reset() }

// GetSupportedOperations returns the list of supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		opCreateFileSystem,
		opCreateFileSystemFromBackup,
		opDescribeFileSystems,
		opDeleteFileSystem,
		opUpdateFileSystem,
		opCreateBackup,
		opDescribeBackups,
		opDeleteBackup,
		opCopyBackup,
		opTagResource,
		opUntagResource,
		opListTagsForResource,
		opAssociateFileSystemAliases,
		opDisassociateFileSystemAliases,
		opDescribeFileSystemAliases,
		opCreateDataRepositoryAssociation,
		opDeleteDataRepositoryAssociation,
		opDescribeDataRepositoryAssocs,
		opUpdateDataRepositoryAssociation,
		opCancelDataRepositoryTask,
		opCreateDataRepositoryTask,
		opDescribeDataRepositoryTasks,
		opCreateFileCache,
		opDeleteFileCache,
		opDescribeFileCaches,
		opUpdateFileCache,
		opCreateSnapshot,
		opDeleteSnapshot,
		opDescribeSnapshots,
		opUpdateSnapshot,
		opCopySnapshotAndUpdateVolume,
		opCreateStorageVirtualMachine,
		opDeleteStorageVirtualMachine,
		opDescribeStorageVirtualMachines,
		opUpdateStorageVirtualMachine,
		opCreateVolume,
		opCreateVolumeFromBackup,
		opDeleteVolume,
		opDescribeVolumes,
		opRestoreVolumeFromSnapshot,
		opUpdateVolume,
		opCreateAndAttachS3AccessPoint,
		opDetachAndDeleteS3AccessPoint,
		opDescribeS3AccessPointAttachments,
		opDescribeSharedVpcConfiguration,
		opUpdateSharedVpcConfiguration,
		opReleaseFileSystemNfsV3Locks,
		opStartMisconfiguredStateRecovery,
	}
}

// RouteMatcher returns a matcher that accepts FSx requests by X-Amz-Target.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		target := c.Request().Header.Get("X-Amz-Target")

		return strings.HasPrefix(target, fsxTargetPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return matchPriority }

// ExtractOperation extracts the FSx operation from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")

	return strings.TrimPrefix(target, fsxTargetPrefix)
}

// ExtractResource extracts a resource identifier from the request body.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := c.Request().GetBody()
	if err != nil || body == nil {
		return ""
	}

	var req struct {
		FileSystemID string `json:"FileSystemId"`
		BackupID     string `json:"BackupId"`
		ResourceARN  string `json:"ResourceARN"`
	}

	buf := make([]byte, bodyReadBufBytes)
	n, _ := body.Read(buf)
	_ = json.Unmarshal(buf[:n], &req)

	switch {
	case req.ResourceARN != "":
		return req.ResourceARN
	case req.FileSystemID != "":
		return req.FileSystemID
	case req.BackupID != "":
		return req.BackupID
	}

	return ""
}

// Handler returns the echo handler func.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"FSx", "application/x-amz-json-1.1",
			h.GetSupportedOperations(),
			h.dispatch,
			h.handleError,
		)
	}
}

func (h *Handler) buildOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		opCreateFileSystem:           service.WrapOp(h.handleCreateFileSystem),
		opCreateFileSystemFromBackup: service.WrapOp(h.handleCreateFileSystemFromBackup),
		opDescribeFileSystems:        service.WrapOp(h.handleDescribeFileSystems),
		opDeleteFileSystem:           service.WrapOp(h.handleDeleteFileSystem),
		opUpdateFileSystem:           service.WrapOp(h.handleUpdateFileSystem),
		opCreateBackup:               service.WrapOp(h.handleCreateBackup),
		opDescribeBackups:            service.WrapOp(h.handleDescribeBackups),
		opDeleteBackup:               service.WrapOp(h.handleDeleteBackup),
		opCopyBackup:                 service.WrapOp(h.handleCopyBackup),
		opTagResource:                service.WrapOp(h.handleTagResource),
		opUntagResource:              service.WrapOp(h.handleUntagResource),
		opListTagsForResource:        service.WrapOp(h.handleListTagsForResource),

		opAssociateFileSystemAliases:    service.WrapOp(h.handleAssociateFileSystemAliases),
		opDisassociateFileSystemAliases: service.WrapOp(h.handleDisassociateFileSystemAliases),
		opDescribeFileSystemAliases:     service.WrapOp(h.handleDescribeFileSystemAliases),

		opCreateDataRepositoryAssociation: service.WrapOp(h.handleCreateDataRepositoryAssociation),
		opDeleteDataRepositoryAssociation: service.WrapOp(h.handleDeleteDataRepositoryAssociation),
		opDescribeDataRepositoryAssocs:    service.WrapOp(h.handleDescribeDataRepositoryAssociations),
		opUpdateDataRepositoryAssociation: service.WrapOp(h.handleUpdateDataRepositoryAssociation),

		opCancelDataRepositoryTask:    service.WrapOp(h.handleCancelDataRepositoryTask),
		opCreateDataRepositoryTask:    service.WrapOp(h.handleCreateDataRepositoryTask),
		opDescribeDataRepositoryTasks: service.WrapOp(h.handleDescribeDataRepositoryTasks),

		opCreateFileCache:    service.WrapOp(h.handleCreateFileCache),
		opDeleteFileCache:    service.WrapOp(h.handleDeleteFileCache),
		opDescribeFileCaches: service.WrapOp(h.handleDescribeFileCaches),
		opUpdateFileCache:    service.WrapOp(h.handleUpdateFileCache),

		opCreateSnapshot:              service.WrapOp(h.handleCreateSnapshot),
		opDeleteSnapshot:              service.WrapOp(h.handleDeleteSnapshot),
		opDescribeSnapshots:           service.WrapOp(h.handleDescribeSnapshots),
		opUpdateSnapshot:              service.WrapOp(h.handleUpdateSnapshot),
		opCopySnapshotAndUpdateVolume: service.WrapOp(h.handleCopySnapshotAndUpdateVolume),

		opCreateStorageVirtualMachine:    service.WrapOp(h.handleCreateStorageVirtualMachine),
		opDeleteStorageVirtualMachine:    service.WrapOp(h.handleDeleteStorageVirtualMachine),
		opDescribeStorageVirtualMachines: service.WrapOp(h.handleDescribeStorageVirtualMachines),
		opUpdateStorageVirtualMachine:    service.WrapOp(h.handleUpdateStorageVirtualMachine),

		opCreateVolume:              service.WrapOp(h.handleCreateVolume),
		opCreateVolumeFromBackup:    service.WrapOp(h.handleCreateVolumeFromBackup),
		opDeleteVolume:              service.WrapOp(h.handleDeleteVolume),
		opDescribeVolumes:           service.WrapOp(h.handleDescribeVolumes),
		opRestoreVolumeFromSnapshot: service.WrapOp(h.handleRestoreVolumeFromSnapshot),
		opUpdateVolume:              service.WrapOp(h.handleUpdateVolume),

		opCreateAndAttachS3AccessPoint:     service.WrapOp(h.handleCreateAndAttachS3AccessPoint),
		opDetachAndDeleteS3AccessPoint:     service.WrapOp(h.handleDetachAndDeleteS3AccessPoint),
		opDescribeS3AccessPointAttachments: service.WrapOp(h.handleDescribeS3AccessPointAttachments),

		opDescribeSharedVpcConfiguration: service.WrapOp(h.handleDescribeSharedVpcConfiguration),
		opUpdateSharedVpcConfiguration:   service.WrapOp(h.handleUpdateSharedVpcConfiguration),

		opReleaseFileSystemNfsV3Locks:     service.WrapOp(h.handleReleaseFileSystemNfsV3Locks),
		opStartMisconfiguredStateRecovery: service.WrapOp(h.handleStartMisconfiguredStateRecovery),
	}
}

func (h *Handler) dispatch(ctx context.Context, action string, body []byte) ([]byte, error) {
	fn, ok := h.ops[action]
	if !ok {
		return nil, fmt.Errorf("%w: %s", errUnknownOperation, action)
	}

	result, err := fn(ctx, body)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleError(_ context.Context, c *echo.Context, _ string, err error) error {
	switch {
	case errors.Is(err, ErrTagLimitExceeded):
		return c.JSON(http.StatusBadRequest, errorResponse("ServiceLimitExceeded", err.Error()))
	case errors.Is(err, ErrTagInvalid):
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequest", err.Error()))
	case errors.Is(err, ErrBackupNotFound):
		return c.JSON(http.StatusBadRequest, errorResponse("BackupNotFound", err.Error()))
	case errors.Is(err, ErrSnapshotNotFound):
		return c.JSON(http.StatusBadRequest, errorResponse("SnapshotNotFound", err.Error()))
	case errors.Is(err, ErrStorageVirtualMachineNotFound):
		return c.JSON(http.StatusBadRequest, errorResponse("StorageVirtualMachineNotFound", err.Error()))
	case errors.Is(err, ErrVolumeNotFound):
		return c.JSON(http.StatusBadRequest, errorResponse("VolumeNotFound", err.Error()))
	case errors.Is(err, ErrFileCacheNotFound):
		return c.JSON(http.StatusBadRequest, errorResponse("FileCacheNotFound", err.Error()))
	case errors.Is(err, ErrDataRepositoryAssociationNotFound):
		return c.JSON(http.StatusBadRequest, errorResponse("DataRepositoryAssociationNotFound", err.Error()))
	case errors.Is(err, ErrDataRepositoryTaskNotFound):
		return c.JSON(http.StatusBadRequest, errorResponse("DataRepositoryTaskNotFound", err.Error()))
	case errors.Is(err, ErrS3AccessPointNotFound):
		return c.JSON(http.StatusBadRequest, errorResponse("InvalidRequest", err.Error()))
	case errors.Is(err, awserr.ErrNotFound):
		return c.JSON(http.StatusBadRequest, errorResponse("FileSystemNotFound", err.Error()))
	case errors.Is(err, awserr.ErrInvalidParameter):
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationError", err.Error()))
	case errors.Is(err, errUnknownOperation):
		return c.JSON(http.StatusBadRequest, errorResponse("UnsupportedOperation", err.Error()))
	default:
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", err.Error()))
	}
}

func errorResponse(code, msg string) map[string]string {
	return map[string]string{"__type": code, "message": msg}
}

// --- CreateFileSystem ---

type createFileSystemOutput struct {
	FileSystem *FileSystem `json:"FileSystem"`
}

func (h *Handler) handleCreateFileSystem(
	_ context.Context,
	in *createFileSystemInput,
) (*createFileSystemOutput, error) {
	fs, err := h.Backend.CreateFileSystem(in)
	if err != nil {
		return nil, err
	}

	return &createFileSystemOutput{FileSystem: fs}, nil
}

// --- CreateFileSystemFromBackup ---

type createFileSystemFromBackupOutput struct {
	FileSystem *FileSystem `json:"FileSystem"`
}

func (h *Handler) handleCreateFileSystemFromBackup(
	_ context.Context,
	in *createFileSystemFromBackupInput,
) (*createFileSystemFromBackupOutput, error) {
	fs, err := h.Backend.CreateFileSystemFromBackup(in)
	if err != nil {
		return nil, err
	}

	return &createFileSystemFromBackupOutput{FileSystem: fs}, nil
}

// --- DescribeFileSystems ---

type describeFileSystemsInput struct {
	NextToken     string   `json:"NextToken,omitempty"`
	FileSystemIDs []string `json:"FileSystemIds,omitempty"`
	MaxResults    int32    `json:"MaxResults,omitempty"`
}

type describeFileSystemsOutput struct {
	NextToken   string        `json:"NextToken,omitempty"`
	FileSystems []*FileSystem `json:"FileSystems"`
}

func (h *Handler) handleDescribeFileSystems(
	_ context.Context,
	in *describeFileSystemsInput,
) (*describeFileSystemsOutput, error) {
	fss, next, err := h.Backend.DescribeFileSystems(in.FileSystemIDs, in.MaxResults, in.NextToken)
	if err != nil {
		return nil, err
	}

	return &describeFileSystemsOutput{FileSystems: fss, NextToken: next}, nil
}

// --- DeleteFileSystem ---

type deleteFileSystemInput struct {
	FileSystemID string `json:"FileSystemId"`
}

type deleteFileSystemOutput struct {
	FileSystemID string `json:"FileSystemId"`
	Lifecycle    string `json:"Lifecycle"`
}

func (h *Handler) handleDeleteFileSystem(
	_ context.Context,
	in *deleteFileSystemInput,
) (*deleteFileSystemOutput, error) {
	if err := h.Backend.DeleteFileSystem(in.FileSystemID); err != nil {
		return nil, err
	}

	return &deleteFileSystemOutput{FileSystemID: in.FileSystemID, Lifecycle: lifecycleDeleting}, nil
}

// --- UpdateFileSystem ---

type updateFileSystemOutput struct {
	FileSystem *FileSystem `json:"FileSystem"`
}

func (h *Handler) handleUpdateFileSystem(
	_ context.Context,
	in *updateFileSystemInput,
) (*updateFileSystemOutput, error) {
	fs, err := h.Backend.UpdateFileSystem(in)
	if err != nil {
		return nil, err
	}

	return &updateFileSystemOutput{FileSystem: fs}, nil
}

// --- CreateBackup ---

type createBackupOutput struct {
	Backup *Backup `json:"Backup"`
}

func (h *Handler) handleCreateBackup(_ context.Context, in *createBackupInput) (*createBackupOutput, error) {
	bk, err := h.Backend.CreateBackup(in)
	if err != nil {
		return nil, err
	}

	return &createBackupOutput{Backup: bk}, nil
}

// --- DescribeBackups ---

type describeBackupsInput struct {
	NextToken  string   `json:"NextToken,omitempty"`
	BackupIDs  []string `json:"BackupIds,omitempty"`
	MaxResults int32    `json:"MaxResults,omitempty"`
}

type describeBackupsOutput struct {
	NextToken string    `json:"NextToken,omitempty"`
	Backups   []*Backup `json:"Backups"`
}

func (h *Handler) handleDescribeBackups(_ context.Context, in *describeBackupsInput) (*describeBackupsOutput, error) {
	bks, next, err := h.Backend.DescribeBackups(in.BackupIDs, in.MaxResults, in.NextToken)
	if err != nil {
		return nil, err
	}

	return &describeBackupsOutput{Backups: bks, NextToken: next}, nil
}

// --- DeleteBackup ---

type deleteBackupInput struct {
	BackupID string `json:"BackupId"`
}

type deleteBackupOutput struct {
	BackupID  string `json:"BackupId"`
	Lifecycle string `json:"Lifecycle"`
}

func (h *Handler) handleDeleteBackup(_ context.Context, in *deleteBackupInput) (*deleteBackupOutput, error) {
	if err := h.Backend.DeleteBackup(in.BackupID); err != nil {
		return nil, err
	}

	return &deleteBackupOutput{BackupID: in.BackupID, Lifecycle: lifecycleDeleted}, nil
}

// --- TagResource ---

type tagResourceInput struct {
	ResourceARN string `json:"ResourceARN"`
	Tags        []Tag  `json:"Tags"`
}

type tagResourceOutput struct{}

func (h *Handler) handleTagResource(_ context.Context, in *tagResourceInput) (*tagResourceOutput, error) {
	if err := h.Backend.TagResource(in.ResourceARN, in.Tags); err != nil {
		return nil, err
	}

	return &tagResourceOutput{}, nil
}

// --- UntagResource ---

type untagResourceInput struct {
	ResourceARN string   `json:"ResourceARN"`
	TagKeys     []string `json:"TagKeys"`
}

type untagResourceOutput struct{}

func (h *Handler) handleUntagResource(_ context.Context, in *untagResourceInput) (*untagResourceOutput, error) {
	if err := h.Backend.UntagResource(in.ResourceARN, in.TagKeys); err != nil {
		return nil, err
	}

	return &untagResourceOutput{}, nil
}

// --- ListTagsForResource ---

type listTagsForResourceInput struct {
	ResourceARN string `json:"ResourceARN"`
}

type listTagsForResourceOutput struct {
	Tags []Tag `json:"Tags"`
}

func (h *Handler) handleListTagsForResource(
	_ context.Context,
	in *listTagsForResourceInput,
) (*listTagsForResourceOutput, error) {
	tags, err := h.Backend.ListTagsForResource(in.ResourceARN)
	if err != nil {
		return nil, err
	}

	return &listTagsForResourceOutput{Tags: tags}, nil
}

// --- CopyBackup ---

type copyBackupHandlerOutput struct {
	Backup *Backup `json:"Backup"`
}

func (h *Handler) handleCopyBackup(_ context.Context, in *copyBackupInput) (*copyBackupHandlerOutput, error) {
	bk, err := h.Backend.CopyBackup(in)
	if err != nil {
		return nil, err
	}

	return &copyBackupHandlerOutput{Backup: bk}, nil
}

// --- AssociateFileSystemAliases ---

type associateFileSystemAliasesInput struct {
	FileSystemID string   `json:"FileSystemId"`
	Aliases      []string `json:"Aliases"`
}

type associateFileSystemAliasesOutput struct {
	Aliases []FileSystemAlias `json:"Aliases"`
}

func (h *Handler) handleAssociateFileSystemAliases(
	_ context.Context,
	in *associateFileSystemAliasesInput,
) (*associateFileSystemAliasesOutput, error) {
	aliases, err := h.Backend.AssociateFileSystemAliases(in.FileSystemID, in.Aliases)
	if err != nil {
		return nil, err
	}

	return &associateFileSystemAliasesOutput{Aliases: aliases}, nil
}

// --- DisassociateFileSystemAliases ---

type disassociateFileSystemAliasesInput struct {
	FileSystemID string   `json:"FileSystemId"`
	Aliases      []string `json:"Aliases"`
}

type disassociateFileSystemAliasesOutput struct {
	Aliases []FileSystemAlias `json:"Aliases"`
}

func (h *Handler) handleDisassociateFileSystemAliases(
	_ context.Context,
	in *disassociateFileSystemAliasesInput,
) (*disassociateFileSystemAliasesOutput, error) {
	aliases, err := h.Backend.DisassociateFileSystemAliases(in.FileSystemID, in.Aliases)
	if err != nil {
		return nil, err
	}

	return &disassociateFileSystemAliasesOutput{Aliases: aliases}, nil
}

// --- DescribeFileSystemAliases ---

type describeFileSystemAliasesInput struct {
	FileSystemID string `json:"FileSystemId"`
	MaxResults   int32  `json:"MaxResults,omitempty"`
	NextToken    string `json:"NextToken,omitempty"`
}

type describeFileSystemAliasesOutput struct {
	Aliases   []FileSystemAlias `json:"Aliases"`
	NextToken string            `json:"NextToken,omitempty"`
}

func (h *Handler) handleDescribeFileSystemAliases(
	_ context.Context,
	in *describeFileSystemAliasesInput,
) (*describeFileSystemAliasesOutput, error) {
	aliases, next, err := h.Backend.DescribeFileSystemAliases(in.FileSystemID, in.MaxResults, in.NextToken)
	if err != nil {
		return nil, err
	}

	return &describeFileSystemAliasesOutput{Aliases: aliases, NextToken: next}, nil
}

// --- CreateDataRepositoryAssociation ---

type createDataRepositoryAssociationOutput struct {
	Association *DataRepositoryAssociation `json:"Association"`
}

func (h *Handler) handleCreateDataRepositoryAssociation(
	_ context.Context,
	in *createDataRepositoryAssociationInput,
) (*createDataRepositoryAssociationOutput, error) {
	a, err := h.Backend.CreateDataRepositoryAssociation(in)
	if err != nil {
		return nil, err
	}

	return &createDataRepositoryAssociationOutput{Association: a}, nil
}

// --- DeleteDataRepositoryAssociation ---

type deleteDataRepositoryAssociationInput struct {
	AssociationID string `json:"AssociationId"`
}

type deleteDataRepositoryAssociationOutput struct {
	AssociationID string `json:"AssociationId"`
	Lifecycle     string `json:"Lifecycle"`
}

func (h *Handler) handleDeleteDataRepositoryAssociation(
	_ context.Context,
	in *deleteDataRepositoryAssociationInput,
) (*deleteDataRepositoryAssociationOutput, error) {
	if err := h.Backend.DeleteDataRepositoryAssociation(in.AssociationID); err != nil {
		return nil, err
	}

	return &deleteDataRepositoryAssociationOutput{AssociationID: in.AssociationID, Lifecycle: lifecycleDeleting}, nil
}

// --- DescribeDataRepositoryAssociations ---

type describeDataRepositoryAssociationsInput struct {
	AssociationIDs []string `json:"AssociationIds,omitempty"`
	MaxResults     int32    `json:"MaxResults,omitempty"`
	NextToken      string   `json:"NextToken,omitempty"`
}

type describeDataRepositoryAssociationsOutput struct {
	Associations []*DataRepositoryAssociation `json:"Associations"`
	NextToken    string                       `json:"NextToken,omitempty"`
}

func (h *Handler) handleDescribeDataRepositoryAssociations(
	_ context.Context,
	in *describeDataRepositoryAssociationsInput,
) (*describeDataRepositoryAssociationsOutput, error) {
	assocs, next, err := h.Backend.DescribeDataRepositoryAssociations(in.AssociationIDs, in.MaxResults, in.NextToken)
	if err != nil {
		return nil, err
	}

	return &describeDataRepositoryAssociationsOutput{Associations: assocs, NextToken: next}, nil
}

// --- UpdateDataRepositoryAssociation ---

type updateDataRepositoryAssociationOutput struct {
	Association *DataRepositoryAssociation `json:"Association"`
}

func (h *Handler) handleUpdateDataRepositoryAssociation(
	_ context.Context,
	in *updateDataRepositoryAssociationInput,
) (*updateDataRepositoryAssociationOutput, error) {
	a, err := h.Backend.UpdateDataRepositoryAssociation(in)
	if err != nil {
		return nil, err
	}

	return &updateDataRepositoryAssociationOutput{Association: a}, nil
}

// --- CancelDataRepositoryTask ---

type cancelDataRepositoryTaskInput struct {
	TaskID string `json:"TaskId"`
}

type cancelDataRepositoryTaskOutput struct {
	TaskID    string `json:"TaskId"`
	Lifecycle string `json:"Lifecycle"`
}

func (h *Handler) handleCancelDataRepositoryTask(
	_ context.Context,
	in *cancelDataRepositoryTaskInput,
) (*cancelDataRepositoryTaskOutput, error) {
	if err := h.Backend.CancelDataRepositoryTask(in.TaskID); err != nil {
		return nil, err
	}

	return &cancelDataRepositoryTaskOutput{TaskID: in.TaskID, Lifecycle: "CANCELING"}, nil
}

// --- CreateDataRepositoryTask ---

type createDataRepositoryTaskOutput struct {
	DataRepositoryTask *DataRepositoryTask `json:"DataRepositoryTask"`
}

func (h *Handler) handleCreateDataRepositoryTask(
	_ context.Context,
	in *createDataRepositoryTaskInput,
) (*createDataRepositoryTaskOutput, error) {
	t, err := h.Backend.CreateDataRepositoryTask(in)
	if err != nil {
		return nil, err
	}

	return &createDataRepositoryTaskOutput{DataRepositoryTask: t}, nil
}

// --- DescribeDataRepositoryTasks ---

type describeDataRepositoryTasksInput struct {
	TaskIDs    []string `json:"TaskIds,omitempty"`
	MaxResults int32    `json:"MaxResults,omitempty"`
	NextToken  string   `json:"NextToken,omitempty"`
}

type describeDataRepositoryTasksOutput struct {
	DataRepositoryTasks []*DataRepositoryTask `json:"DataRepositoryTasks"`
	NextToken           string                `json:"NextToken,omitempty"`
}

func (h *Handler) handleDescribeDataRepositoryTasks(
	_ context.Context,
	in *describeDataRepositoryTasksInput,
) (*describeDataRepositoryTasksOutput, error) {
	tasks, next, err := h.Backend.DescribeDataRepositoryTasks(in.TaskIDs, in.MaxResults, in.NextToken)
	if err != nil {
		return nil, err
	}

	return &describeDataRepositoryTasksOutput{DataRepositoryTasks: tasks, NextToken: next}, nil
}

// --- CreateFileCache ---

type createFileCacheOutput struct {
	FileCache *FileCache `json:"FileCache"`
}

func (h *Handler) handleCreateFileCache(
	_ context.Context,
	in *createFileCacheInput,
) (*createFileCacheOutput, error) {
	c, err := h.Backend.CreateFileCache(in)
	if err != nil {
		return nil, err
	}

	return &createFileCacheOutput{FileCache: c}, nil
}

// --- DeleteFileCache ---

type deleteFileCacheInput struct {
	FileCacheID string `json:"FileCacheId"`
}

type deleteFileCacheOutput struct {
	FileCacheID string `json:"FileCacheId"`
	Lifecycle   string `json:"Lifecycle"`
}

func (h *Handler) handleDeleteFileCache(
	_ context.Context,
	in *deleteFileCacheInput,
) (*deleteFileCacheOutput, error) {
	if err := h.Backend.DeleteFileCache(in.FileCacheID); err != nil {
		return nil, err
	}

	return &deleteFileCacheOutput{FileCacheID: in.FileCacheID, Lifecycle: lifecycleDeleting}, nil
}

// --- DescribeFileCaches ---

type describeFileCachesInput struct {
	FileCacheIDs []string `json:"FileCacheIds,omitempty"`
	MaxResults   int32    `json:"MaxResults,omitempty"`
	NextToken    string   `json:"NextToken,omitempty"`
}

type describeFileCachesOutput struct {
	FileCaches []*FileCache `json:"FileCaches"`
	NextToken  string       `json:"NextToken,omitempty"`
}

func (h *Handler) handleDescribeFileCaches(
	_ context.Context,
	in *describeFileCachesInput,
) (*describeFileCachesOutput, error) {
	caches, next, err := h.Backend.DescribeFileCaches(in.FileCacheIDs, in.MaxResults, in.NextToken)
	if err != nil {
		return nil, err
	}

	return &describeFileCachesOutput{FileCaches: caches, NextToken: next}, nil
}

// --- UpdateFileCache ---

type updateFileCacheOutput struct {
	FileCache *FileCache `json:"FileCache"`
}

func (h *Handler) handleUpdateFileCache(
	_ context.Context,
	in *updateFileCacheInput,
) (*updateFileCacheOutput, error) {
	c, err := h.Backend.UpdateFileCache(in)
	if err != nil {
		return nil, err
	}

	return &updateFileCacheOutput{FileCache: c}, nil
}

// --- CreateSnapshot ---

type createSnapshotOutput struct {
	Snapshot *Snapshot `json:"Snapshot"`
}

func (h *Handler) handleCreateSnapshot(
	_ context.Context,
	in *createSnapshotInput,
) (*createSnapshotOutput, error) {
	s, err := h.Backend.CreateSnapshot(in)
	if err != nil {
		return nil, err
	}

	return &createSnapshotOutput{Snapshot: s}, nil
}

// --- DeleteSnapshot ---

type deleteSnapshotInput struct {
	SnapshotID string `json:"SnapshotId"`
}

type deleteSnapshotOutput struct {
	SnapshotID string `json:"SnapshotId"`
	Lifecycle  string `json:"Lifecycle"`
}

func (h *Handler) handleDeleteSnapshot(
	_ context.Context,
	in *deleteSnapshotInput,
) (*deleteSnapshotOutput, error) {
	if err := h.Backend.DeleteSnapshot(in.SnapshotID); err != nil {
		return nil, err
	}

	return &deleteSnapshotOutput{SnapshotID: in.SnapshotID, Lifecycle: lifecycleDeleting}, nil
}

// --- DescribeSnapshots ---

type describeSnapshotsInput struct {
	SnapshotIDs []string `json:"SnapshotIds,omitempty"`
	MaxResults  int32    `json:"MaxResults,omitempty"`
	NextToken   string   `json:"NextToken,omitempty"`
}

type describeSnapshotsOutput struct {
	Snapshots []*Snapshot `json:"Snapshots"`
	NextToken string      `json:"NextToken,omitempty"`
}

func (h *Handler) handleDescribeSnapshots(
	_ context.Context,
	in *describeSnapshotsInput,
) (*describeSnapshotsOutput, error) {
	snaps, next, err := h.Backend.DescribeSnapshots(in.SnapshotIDs, in.MaxResults, in.NextToken)
	if err != nil {
		return nil, err
	}

	return &describeSnapshotsOutput{Snapshots: snaps, NextToken: next}, nil
}

// --- UpdateSnapshot ---

type updateSnapshotOutput struct {
	Snapshot *Snapshot `json:"Snapshot"`
}

func (h *Handler) handleUpdateSnapshot(
	_ context.Context,
	in *updateSnapshotInput,
) (*updateSnapshotOutput, error) {
	s, err := h.Backend.UpdateSnapshot(in)
	if err != nil {
		return nil, err
	}

	return &updateSnapshotOutput{Snapshot: s}, nil
}

// --- CopySnapshotAndUpdateVolume ---

type copySnapshotAndUpdateVolumeOutput struct {
	Volume *Volume `json:"Volume"`
}

func (h *Handler) handleCopySnapshotAndUpdateVolume(
	_ context.Context,
	in *copySnapshotAndUpdateVolumeInput,
) (*copySnapshotAndUpdateVolumeOutput, error) {
	v, err := h.Backend.CopySnapshotAndUpdateVolume(in)
	if err != nil {
		return nil, err
	}

	return &copySnapshotAndUpdateVolumeOutput{Volume: v}, nil
}

// --- CreateStorageVirtualMachine ---

type createStorageVirtualMachineOutput struct {
	StorageVirtualMachine *StorageVirtualMachine `json:"StorageVirtualMachine"`
}

func (h *Handler) handleCreateStorageVirtualMachine(
	_ context.Context,
	in *createStorageVirtualMachineInput,
) (*createStorageVirtualMachineOutput, error) {
	svm, err := h.Backend.CreateStorageVirtualMachine(in)
	if err != nil {
		return nil, err
	}

	return &createStorageVirtualMachineOutput{StorageVirtualMachine: svm}, nil
}

// --- DeleteStorageVirtualMachine ---

type deleteStorageVirtualMachineInput struct {
	StorageVirtualMachineID string `json:"StorageVirtualMachineId"`
}

type deleteStorageVirtualMachineOutput struct {
	StorageVirtualMachineID string `json:"StorageVirtualMachineId"`
	Lifecycle               string `json:"Lifecycle"`
}

func (h *Handler) handleDeleteStorageVirtualMachine(
	_ context.Context,
	in *deleteStorageVirtualMachineInput,
) (*deleteStorageVirtualMachineOutput, error) {
	if err := h.Backend.DeleteStorageVirtualMachine(in.StorageVirtualMachineID); err != nil {
		return nil, err
	}

	return &deleteStorageVirtualMachineOutput{
		StorageVirtualMachineID: in.StorageVirtualMachineID,
		Lifecycle:               lifecycleDeleting,
	}, nil
}

// --- DescribeStorageVirtualMachines ---

type describeStorageVirtualMachinesInput struct {
	StorageVirtualMachineIDs []string `json:"StorageVirtualMachineIds,omitempty"`
	MaxResults               int32    `json:"MaxResults,omitempty"`
	NextToken                string   `json:"NextToken,omitempty"`
}

type describeStorageVirtualMachinesOutput struct {
	StorageVirtualMachines []*StorageVirtualMachine `json:"StorageVirtualMachines"`
	NextToken              string                   `json:"NextToken,omitempty"`
}

func (h *Handler) handleDescribeStorageVirtualMachines(
	_ context.Context,
	in *describeStorageVirtualMachinesInput,
) (*describeStorageVirtualMachinesOutput, error) {
	svms, next, err := h.Backend.DescribeStorageVirtualMachines(
		in.StorageVirtualMachineIDs, in.MaxResults, in.NextToken,
	)
	if err != nil {
		return nil, err
	}

	return &describeStorageVirtualMachinesOutput{StorageVirtualMachines: svms, NextToken: next}, nil
}

// --- UpdateStorageVirtualMachine ---

type updateStorageVirtualMachineOutput struct {
	StorageVirtualMachine *StorageVirtualMachine `json:"StorageVirtualMachine"`
}

func (h *Handler) handleUpdateStorageVirtualMachine(
	_ context.Context,
	in *updateStorageVirtualMachineInput,
) (*updateStorageVirtualMachineOutput, error) {
	svm, err := h.Backend.UpdateStorageVirtualMachine(in)
	if err != nil {
		return nil, err
	}

	return &updateStorageVirtualMachineOutput{StorageVirtualMachine: svm}, nil
}

// --- CreateVolume ---

type createVolumeOutput struct {
	Volume *Volume `json:"Volume"`
}

func (h *Handler) handleCreateVolume(_ context.Context, in *createVolumeInput) (*createVolumeOutput, error) {
	v, err := h.Backend.CreateVolume(in)
	if err != nil {
		return nil, err
	}

	return &createVolumeOutput{Volume: v}, nil
}

// --- CreateVolumeFromBackup ---

type createVolumeFromBackupOutput struct {
	Volume *Volume `json:"Volume"`
}

func (h *Handler) handleCreateVolumeFromBackup(
	_ context.Context,
	in *createVolumeFromBackupInput,
) (*createVolumeFromBackupOutput, error) {
	v, err := h.Backend.CreateVolumeFromBackup(in)
	if err != nil {
		return nil, err
	}

	return &createVolumeFromBackupOutput{Volume: v}, nil
}

// --- DeleteVolume ---

type deleteVolumeInput struct {
	VolumeID string `json:"VolumeId"`
}

type deleteVolumeOutput struct {
	VolumeID  string `json:"VolumeId"`
	Lifecycle string `json:"Lifecycle"`
}

func (h *Handler) handleDeleteVolume(_ context.Context, in *deleteVolumeInput) (*deleteVolumeOutput, error) {
	if err := h.Backend.DeleteVolume(in.VolumeID); err != nil {
		return nil, err
	}

	return &deleteVolumeOutput{VolumeID: in.VolumeID, Lifecycle: lifecycleDeleting}, nil
}

// --- DescribeVolumes ---

type describeVolumesInput struct {
	VolumeIDs  []string `json:"VolumeIds,omitempty"`
	MaxResults int32    `json:"MaxResults,omitempty"`
	NextToken  string   `json:"NextToken,omitempty"`
}

type describeVolumesOutput struct {
	Volumes   []*Volume `json:"Volumes"`
	NextToken string    `json:"NextToken,omitempty"`
}

func (h *Handler) handleDescribeVolumes(_ context.Context, in *describeVolumesInput) (*describeVolumesOutput, error) {
	vols, next, err := h.Backend.DescribeVolumes(in.VolumeIDs, in.MaxResults, in.NextToken)
	if err != nil {
		return nil, err
	}

	return &describeVolumesOutput{Volumes: vols, NextToken: next}, nil
}

// --- RestoreVolumeFromSnapshot ---

type restoreVolumeFromSnapshotOutput struct {
	Volume *Volume `json:"Volume"`
}

func (h *Handler) handleRestoreVolumeFromSnapshot(
	_ context.Context,
	in *restoreVolumeFromSnapshotInput,
) (*restoreVolumeFromSnapshotOutput, error) {
	v, err := h.Backend.RestoreVolumeFromSnapshot(in)
	if err != nil {
		return nil, err
	}

	return &restoreVolumeFromSnapshotOutput{Volume: v}, nil
}

// --- UpdateVolume ---

type updateVolumeOutput struct {
	Volume *Volume `json:"Volume"`
}

func (h *Handler) handleUpdateVolume(_ context.Context, in *updateVolumeInput) (*updateVolumeOutput, error) {
	v, err := h.Backend.UpdateVolume(in)
	if err != nil {
		return nil, err
	}

	return &updateVolumeOutput{Volume: v}, nil
}

// --- CreateAndAttachS3AccessPoint ---

type createAndAttachS3AccessPointOutput struct {
	S3AccessPoint *S3AccessPoint `json:"S3AccessPoint"`
}

func (h *Handler) handleCreateAndAttachS3AccessPoint(
	_ context.Context,
	in *createAndAttachS3AccessPointInput,
) (*createAndAttachS3AccessPointOutput, error) {
	ap, err := h.Backend.CreateAndAttachS3AccessPoint(in)
	if err != nil {
		return nil, err
	}

	return &createAndAttachS3AccessPointOutput{S3AccessPoint: ap}, nil
}

// --- DetachAndDeleteS3AccessPoint ---

type detachAndDeleteS3AccessPointInput struct {
	Name         string `json:"Name"`
	FileSystemID string `json:"FileSystemId"`
}

type detachAndDeleteS3AccessPointOutput struct{}

func (h *Handler) handleDetachAndDeleteS3AccessPoint(
	_ context.Context,
	in *detachAndDeleteS3AccessPointInput,
) (*detachAndDeleteS3AccessPointOutput, error) {
	if err := h.Backend.DetachAndDeleteS3AccessPoint(in.Name, in.FileSystemID); err != nil {
		return nil, err
	}

	return &detachAndDeleteS3AccessPointOutput{}, nil
}

// --- DescribeS3AccessPointAttachments ---

type describeS3AccessPointAttachmentsInput struct {
	Names      []string `json:"Names,omitempty"`
	MaxResults int32    `json:"MaxResults,omitempty"`
	NextToken  string   `json:"NextToken,omitempty"`
}

type describeS3AccessPointAttachmentsOutput struct {
	S3AccessPoints []*S3AccessPoint `json:"S3AccessPoints"`
	NextToken      string           `json:"NextToken,omitempty"`
}

func (h *Handler) handleDescribeS3AccessPointAttachments(
	_ context.Context,
	in *describeS3AccessPointAttachmentsInput,
) (*describeS3AccessPointAttachmentsOutput, error) {
	aps, next, err := h.Backend.DescribeS3AccessPointAttachments(in.Names, in.MaxResults, in.NextToken)
	if err != nil {
		return nil, err
	}

	return &describeS3AccessPointAttachmentsOutput{S3AccessPoints: aps, NextToken: next}, nil
}

// --- DescribeSharedVpcConfiguration ---

type describeSharedVpcConfigurationInput struct{}

type describeSharedVpcConfigurationOutput struct {
	EnableSharedVpcOnFileSystemCreation string `json:"EnableSharedVpcOnFileSystemCreation"`
}

func (h *Handler) handleDescribeSharedVpcConfiguration(
	_ context.Context,
	_ *describeSharedVpcConfigurationInput,
) (*describeSharedVpcConfigurationOutput, error) {
	cfg, err := h.Backend.DescribeSharedVpcConfiguration()
	if err != nil {
		return nil, err
	}

	return &describeSharedVpcConfigurationOutput{
		EnableSharedVpcOnFileSystemCreation: cfg.EnableSharedVpcOnFileSystemCreation,
	}, nil
}

// --- UpdateSharedVpcConfiguration ---

type updateSharedVpcConfigurationOutput struct {
	EnableSharedVpcOnFileSystemCreation string `json:"EnableSharedVpcOnFileSystemCreation"`
}

func (h *Handler) handleUpdateSharedVpcConfiguration(
	_ context.Context,
	in *updateSharedVpcConfigurationInput,
) (*updateSharedVpcConfigurationOutput, error) {
	cfg, err := h.Backend.UpdateSharedVpcConfiguration(in)
	if err != nil {
		return nil, err
	}

	return &updateSharedVpcConfigurationOutput{
		EnableSharedVpcOnFileSystemCreation: cfg.EnableSharedVpcOnFileSystemCreation,
	}, nil
}

// --- ReleaseFileSystemNfsV3Locks ---

type releaseFileSystemNfsV3LocksInput struct {
	FileSystemID string `json:"FileSystemId"`
}

type releaseFileSystemNfsV3LocksOutput struct {
	FileSystem *FileSystem `json:"FileSystem"`
}

func (h *Handler) handleReleaseFileSystemNfsV3Locks(
	_ context.Context,
	in *releaseFileSystemNfsV3LocksInput,
) (*releaseFileSystemNfsV3LocksOutput, error) {
	if err := h.Backend.ReleaseFileSystemNfsV3Locks(in.FileSystemID); err != nil {
		return nil, err
	}

	fss, _, err := h.Backend.DescribeFileSystems([]string{in.FileSystemID}, 1, "")
	if err != nil {
		return nil, err
	}

	var fs *FileSystem
	if len(fss) > 0 {
		fs = fss[0]
	}

	return &releaseFileSystemNfsV3LocksOutput{FileSystem: fs}, nil
}

// --- StartMisconfiguredStateRecovery ---

type startMisconfiguredStateRecoveryInput struct {
	FileSystemID string `json:"FileSystemId"`
}

type startMisconfiguredStateRecoveryOutput struct {
	FileSystem *FileSystem `json:"FileSystem"`
}

func (h *Handler) handleStartMisconfiguredStateRecovery(
	_ context.Context,
	in *startMisconfiguredStateRecoveryInput,
) (*startMisconfiguredStateRecoveryOutput, error) {
	if err := h.Backend.StartMisconfiguredStateRecovery(in.FileSystemID); err != nil {
		return nil, err
	}

	fss, _, err := h.Backend.DescribeFileSystems([]string{in.FileSystemID}, 1, "")
	if err != nil {
		return nil, err
	}

	var fs *FileSystem
	if len(fss) > 0 {
		fs = fss[0]
	}

	return &startMisconfiguredStateRecoveryOutput{FileSystem: fs}, nil
}
