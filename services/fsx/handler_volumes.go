package fsx

import (
	"context"
	"time"
)

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
	NextToken  string   `json:"NextToken,omitempty"`
	VolumeIDs  []string `json:"VolumeIds,omitempty"`
	MaxResults int32    `json:"MaxResults,omitempty"`
}

type describeVolumesOutput struct {
	NextToken string    `json:"NextToken,omitempty"`
	Volumes   []*Volume `json:"Volumes"`
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
	Lifecycle             string                 `json:"Lifecycle"`
	VolumeID              string                 `json:"VolumeId"`
	AdministrativeActions []AdministrativeAction `json:"AdministrativeActions,omitempty"`
}

func (h *Handler) handleRestoreVolumeFromSnapshot(
	_ context.Context,
	in *restoreVolumeFromSnapshotInput,
) (*restoreVolumeFromSnapshotOutput, error) {
	v, err := h.Backend.RestoreVolumeFromSnapshot(in)
	if err != nil {
		return nil, err
	}

	return &restoreVolumeFromSnapshotOutput{
		Lifecycle: v.Lifecycle,
		VolumeID:  v.VolumeID,
		AdministrativeActions: []AdministrativeAction{{
			AdministrativeActionType: administrativeActionTypeVolumeRestore,
			Status:                   administrativeActionStatusCompleted,
			RequestTime:              epochTime(time.Now().UTC()),
			TargetVolumeValues:       v,
		}},
	}, nil
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
