package kinesisanalyticsv2

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

type createSnapshotInput struct {
	ApplicationName string `json:"ApplicationName"`
	SnapshotName    string `json:"SnapshotName"`
}

type describeSnapshotInput struct {
	ApplicationName string `json:"ApplicationName"`
	SnapshotName    string `json:"SnapshotName"`
}

type describeSnapshotOutput struct {
	SnapshotDetails snapshotDetail `json:"SnapshotDetails"`
}

type listSnapshotsInput struct {
	ApplicationName string `json:"ApplicationName"`
	NextToken       string `json:"NextToken,omitempty"`
}

type listSnapshotsOutput struct {
	NextToken         string           `json:"NextToken,omitempty"`
	SnapshotSummaries []snapshotDetail `json:"SnapshotSummaries"`
}

type deleteSnapshotInput struct {
	ApplicationName           string      `json:"ApplicationName"`
	SnapshotName              string      `json:"SnapshotName"`
	SnapshotCreationTimestamp json.Number `json:"SnapshotCreationTimestamp,omitempty"`
}

func (h *Handler) handleCreateApplicationSnapshot(ctx context.Context, c *echo.Context, body []byte) error {
	var in createSnapshotInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	snap, err := h.Backend.CreateApplicationSnapshot(ctx, in.ApplicationName, in.SnapshotName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, struct {
		SnapshotDetails snapshotDetail `json:"SnapshotDetails"`
	}{SnapshotDetails: toSnapshotDetail(snap)})
}

func (h *Handler) handleDescribeApplicationSnapshot(ctx context.Context, c *echo.Context, body []byte) error {
	var in describeSnapshotInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	snap, err := h.Backend.DescribeApplicationSnapshot(ctx, in.ApplicationName, in.SnapshotName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, describeSnapshotOutput{SnapshotDetails: toSnapshotDetail(snap)})
}

func (h *Handler) handleListApplicationSnapshots(ctx context.Context, c *echo.Context, body []byte) error {
	var in listSnapshotsInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	snaps, outToken, err := h.Backend.ListApplicationSnapshots(ctx, in.ApplicationName, in.NextToken)
	if err != nil {
		return h.handleError(c, err)
	}

	details := make([]snapshotDetail, 0, len(snaps))
	for _, s := range snaps {
		details = append(details, toSnapshotDetail(s))
	}

	return c.JSON(http.StatusOK, listSnapshotsOutput{SnapshotSummaries: details, NextToken: outToken})
}

func (h *Handler) handleDeleteApplicationSnapshot(ctx context.Context, c *echo.Context, body []byte) error {
	var in deleteSnapshotInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	if err := h.Backend.DeleteApplicationSnapshot(ctx, in.ApplicationName, in.SnapshotName); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}
