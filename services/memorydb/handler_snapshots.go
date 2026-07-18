package memorydb

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

func (h *Handler) handleCreateSnapshot(ctx context.Context, c *echo.Context, body []byte) error {
	var req createSnapshotRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.SnapshotName == "" {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "SnapshotName is required")
	}

	if req.ClusterName == "" {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "ClusterName is required")
	}

	if err := validateTagEntries(req.Tags); err != nil {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", err.Error())
	}

	s, err := h.Backend.CreateSnapshot(ctx, &req)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, createSnapshotResponse{Snapshot: toSnapshotObject(s)})
}

func (h *Handler) handleCopySnapshot(ctx context.Context, c *echo.Context, body []byte) error {
	var req copySnapshotRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.SourceSnapshotName == "" {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "SourceSnapshotName is required")
	}

	if req.TargetSnapshotName == "" && req.TargetBucket == "" {
		return writeError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValueException",
			"TargetSnapshotName or TargetBucket is required",
		)
	}

	if err := validateTagEntries(req.Tags); err != nil {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", err.Error())
	}

	s, err := h.Backend.CopySnapshot(ctx, &req)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, copySnapshotResponse{Snapshot: toSnapshotObject(s)})
}

func (h *Handler) handleDeleteSnapshot(ctx context.Context, c *echo.Context, body []byte) error {
	var req deleteSnapshotRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.SnapshotName == "" {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "SnapshotName is required")
	}

	s, err := h.Backend.DeleteSnapshot(ctx, req.SnapshotName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, deleteSnapshotResponse{Snapshot: toSnapshotObject(s)})
}

func (h *Handler) handleDescribeSnapshots(ctx context.Context, c *echo.Context, body []byte) error {
	var req describeSnapshotRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	snapshots, err := h.Backend.DescribeSnapshots(ctx, req.SnapshotName, req.ClusterName, req.SnapshotType, req.Source)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	snapshots, nextToken := paginateItems(
		snapshots,
		req.NextToken,
		req.MaxResults,
		func(s *Snapshot) string { return s.Name },
	)

	objs := make([]snapshotObject, 0, len(snapshots))

	for _, s := range snapshots {
		objs = append(objs, toSnapshotObject(s))
	}

	return c.JSON(http.StatusOK, describeSnapshotResponse{Snapshots: objs, NextToken: nextToken})
}

func (h *Handler) handleExportSnapshot(ctx context.Context, c *echo.Context, body []byte) error {
	var req exportSnapshotRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.SnapshotName == "" {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "SnapshotName is required")
	}

	s, err := h.Backend.ExportSnapshot(ctx, &req)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, exportSnapshotResponse{Snapshot: toSnapshotObject(s)})
}

// -- EngineVersion handlers ------------------------------------------------------

// toSnapshotObject converts a Snapshot to its JSON representation.
func toSnapshotObject(s *Snapshot) snapshotObject {
	var clusterConfig *snapshotClusterConfig
	if s.ClusterConfiguration.Name != "" {
		cfg := s.ClusterConfiguration
		clusterConfig = &cfg
	}

	return snapshotObject{
		Name:                 s.Name,
		ARN:                  s.ARN,
		ClusterConfiguration: clusterConfig,
		Status:               s.Status,
		KmsKeyID:             s.KmsKeyID,
		SnapshotType:         s.SnapshotType,
		Source:               s.Source,
		CreatedAt:            awstime.Epoch(s.CreatedAt),
	}
}
