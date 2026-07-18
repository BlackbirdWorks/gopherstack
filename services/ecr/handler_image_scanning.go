package ecr

import (
	"context"
)

// batchGetRepositoryScanningConfigurationInput is the request body for BatchGetRepositoryScanningConfiguration.
type batchGetRepositoryScanningConfigurationInput struct {
	RepositoryNames []string `json:"repositoryNames"`
}

type batchGetRepositoryScanningConfigurationOutput struct {
	ScanningConfigurations []RepositoryScanningConfiguration        `json:"scanningConfigurations"`
	Failures               []RepositoryScanningConfigurationFailure `json:"failures"`
}

func (h *Handler) handleBatchGetRepositoryScanningConfiguration(
	ctx context.Context,
	in *batchGetRepositoryScanningConfigurationInput,
) (*batchGetRepositoryScanningConfigurationOutput, error) {
	configs, failures, err := h.Backend.BatchGetRepositoryScanningConfiguration(
		ctx,
		in.RepositoryNames,
	)
	if err != nil {
		return nil, err
	}

	if configs == nil {
		configs = []RepositoryScanningConfiguration{}
	}

	if failures == nil {
		failures = []RepositoryScanningConfigurationFailure{}
	}

	return &batchGetRepositoryScanningConfigurationOutput{
		ScanningConfigurations: configs,
		Failures:               failures,
	}, nil
}

// imageInput is a request body shared by several image-scoped operations
// (StartImageScan, DescribeImageSigningStatus, DescribeImageReplicationStatus).
type imageInput struct {
	ImageID        ImageIdentifier `json:"imageId"`
	RepositoryName string          `json:"repositoryName"`
	RegistryID     string          `json:"registryId,omitempty"`
}

type describeImageScanFindingsOutput struct {
	ImageID           ImageIdentifier          `json:"imageId"`
	ImageScanFindings *ImageScanFindingsResult `json:"imageScanFindings"`
	ImageScanStatus   scanStatusView           `json:"imageScanStatus"`
	RegistryID        string                   `json:"registryId"`
	RepositoryName    string                   `json:"repositoryName"`
	NextToken         string                   `json:"nextToken,omitempty"`
}

type scanStatusView struct {
	Description string `json:"description,omitempty"`
	Status      string `json:"status"`
}

type describeImageScanFindingsInput struct {
	ImageID        ImageIdentifier `json:"imageId"`
	RepositoryName string          `json:"repositoryName"`
	RegistryID     string          `json:"registryId,omitempty"`
	NextToken      string          `json:"nextToken,omitempty"`
	MaxResults     int             `json:"maxResults,omitempty"`
}

func (h *Handler) handleDescribeImageScanFindings(
	ctx context.Context,
	in *describeImageScanFindingsInput,
) (*describeImageScanFindingsOutput, error) {
	findings, nextToken, err := h.Backend.DescribeImageScanFindings(
		ctx,
		in.RepositoryName,
		in.ImageID,
		in.MaxResults,
		in.NextToken,
	)
	if err != nil {
		return nil, err
	}

	return &describeImageScanFindingsOutput{
		ImageID:           findings.ImageID,
		ImageScanFindings: findings,
		ImageScanStatus: scanStatusView{
			Description: findings.Description,
			Status:      findings.Status,
		},
		RegistryID:     findings.RegistryID,
		RepositoryName: findings.RepositoryName,
		NextToken:      nextToken,
	}, nil
}

type startImageScanOutput struct {
	ImageID         ImageIdentifier `json:"imageId"`
	ImageScanStatus scanStatusView  `json:"imageScanStatus"`
	RegistryID      string          `json:"registryId"`
	RepositoryName  string          `json:"repositoryName"`
}

func (h *Handler) handleStartImageScan(
	ctx context.Context,
	in *imageInput,
) (*startImageScanOutput, error) {
	result, err := h.Backend.StartImageScan(ctx, in.RepositoryName, in.ImageID)
	if err != nil {
		return nil, err
	}

	return &startImageScanOutput{
		ImageID: result.ImageID,
		ImageScanStatus: scanStatusView{
			Description: result.Description,
			Status:      result.Status,
		},
		RegistryID:     result.RegistryID,
		RepositoryName: result.RepositoryName,
	}, nil
}

type putImageScanningConfigurationInput struct {
	RepositoryName             string                         `json:"repositoryName"`
	RegistryID                 string                         `json:"registryId,omitempty"`
	ImageScanningConfiguration imageScanningConfigurationView `json:"imageScanningConfiguration"`
}

type putImageScanningConfigurationOutput struct {
	RegistryID                 string                         `json:"registryId,omitempty"`
	RepositoryName             string                         `json:"repositoryName"`
	ImageScanningConfiguration imageScanningConfigurationView `json:"imageScanningConfiguration"`
}

func (h *Handler) handlePutImageScanningConfiguration(
	ctx context.Context,
	in *putImageScanningConfigurationInput,
) (*putImageScanningConfigurationOutput, error) {
	cfg, err := h.Backend.PutImageScanningConfiguration(
		ctx,
		in.RepositoryName,
		in.ImageScanningConfiguration.ScanOnPush,
	)
	if err != nil {
		return nil, err
	}

	return &putImageScanningConfigurationOutput{
		ImageScanningConfiguration: imageScanningConfigurationView{ScanOnPush: cfg.ScanOnPush},
		RepositoryName:             cfg.RepositoryName,
	}, nil
}
