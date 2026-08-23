package fsx

import (
	"context"
	"time"
)

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
	NextToken   string   `json:"NextToken,omitempty"`
	SnapshotIDs []string `json:"SnapshotIds,omitempty"`
	MaxResults  int32    `json:"MaxResults,omitempty"`
}

type describeSnapshotsOutput struct {
	NextToken string      `json:"NextToken,omitempty"`
	Snapshots []*Snapshot `json:"Snapshots"`
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
	Lifecycle             string                 `json:"Lifecycle"`
	VolumeID              string                 `json:"VolumeId"`
	AdministrativeActions []AdministrativeAction `json:"AdministrativeActions,omitempty"`
}

func (h *Handler) handleCopySnapshotAndUpdateVolume(
	_ context.Context,
	in *copySnapshotAndUpdateVolumeInput,
) (*copySnapshotAndUpdateVolumeOutput, error) {
	v, err := h.Backend.CopySnapshotAndUpdateVolume(in)
	if err != nil {
		return nil, err
	}

	return &copySnapshotAndUpdateVolumeOutput{
		Lifecycle: v.Lifecycle,
		VolumeID:  v.VolumeID,
		AdministrativeActions: []AdministrativeAction{{
			AdministrativeActionType: administrativeActionTypeVolumeUpdateWithSnapshot,
			Status:                   administrativeActionStatusCompleted,
			RequestTime:              epochTime(time.Now().UTC()),
			TargetVolumeValues:       v,
		}},
	}, nil
}
