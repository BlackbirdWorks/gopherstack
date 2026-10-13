package fsx

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/regionpeers"
)

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
	NextToken  string       `json:"NextToken,omitempty"`
	BackupIDs  []string     `json:"BackupIds,omitempty"`
	Filters    []wireFilter `json:"Filters,omitempty"`
	MaxResults int32        `json:"MaxResults,omitempty"`
}

type describeBackupsOutput struct {
	NextToken string    `json:"NextToken,omitempty"`
	Backups   []*Backup `json:"Backups"`
}

func (h *Handler) handleDescribeBackups(_ context.Context, in *describeBackupsInput) (*describeBackupsOutput, error) {
	bks, next, err := h.Backend.DescribeBackups(in.BackupIDs, in.Filters, in.MaxResults, in.NextToken)
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

// --- CopyBackup ---

type copyBackupHandlerOutput struct {
	Backup *Backup `json:"Backup"`
}

func (h *Handler) handleCopyBackup(_ context.Context, in *copyBackupInput) (*copyBackupHandlerOutput, error) {
	if in.SourceRegion != "" && in.SourceRegion != h.Backend.Region() {
		if !regionpeers.ValidRegion(in.SourceRegion) {
			return nil, fmt.Errorf("%w: SourceRegion %q is not a valid region", ErrValidation, in.SourceRegion)
		}

		src, err := h.regionRoot().BackendFor(in.SourceRegion).cloneBackup(in.SourceBackupID)
		if err != nil {
			return nil, err
		}

		in.sourceBackup = src
	}

	bk, err := h.Backend.CopyBackup(in)
	if err != nil {
		return nil, err
	}

	return &copyBackupHandlerOutput{Backup: bk}, nil
}
