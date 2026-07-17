package fsx

import "context"

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
	NextToken    string `json:"NextToken,omitempty"`
	MaxResults   int32  `json:"MaxResults,omitempty"`
}

type describeFileSystemAliasesOutput struct {
	NextToken string            `json:"NextToken,omitempty"`
	Aliases   []FileSystemAlias `json:"Aliases"`
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
