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
	opTagResource                = "TagResource"
	opUntagResource              = "UntagResource"
	opListTagsForResource        = "ListTagsForResource"
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
		opTagResource,
		opUntagResource,
		opListTagsForResource,
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
		opTagResource:                service.WrapOp(h.handleTagResource),
		opUntagResource:              service.WrapOp(h.handleUntagResource),
		opListTagsForResource:        service.WrapOp(h.handleListTagsForResource),
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

type deleteFileSystemOutput struct{}

func (h *Handler) handleDeleteFileSystem(
	_ context.Context,
	in *deleteFileSystemInput,
) (*deleteFileSystemOutput, error) {
	if err := h.Backend.DeleteFileSystem(in.FileSystemID); err != nil {
		return nil, err
	}

	return &deleteFileSystemOutput{}, nil
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

type deleteBackupOutput struct{}

func (h *Handler) handleDeleteBackup(_ context.Context, in *deleteBackupInput) (*deleteBackupOutput, error) {
	if err := h.Backend.DeleteBackup(in.BackupID); err != nil {
		return nil, err
	}

	return &deleteBackupOutput{}, nil
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
