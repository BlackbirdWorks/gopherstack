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
	ImageID           ImageIdentifier        `json:"imageId"`
	ImageScanFindings *imageScanFindingsView `json:"imageScanFindings"`
	ImageScanStatus   scanStatusView         `json:"imageScanStatus"`
	RegistryID        string                 `json:"registryId"`
	RepositoryName    string                 `json:"repositoryName"`
	NextToken         string                 `json:"nextToken,omitempty"`
}

// imageScanFindingsView is the JSON representation of DescribeImageScanFindings'
// nested "imageScanFindings" object (real AWS type: ImageScanFindings). Unlike
// ImageScanFindingsResult — this package's internal domain struct, which also
// carries imageId/repositoryName/registryId/status/description for other
// callers — the real nested ImageScanFindings shape has ONLY these five
// fields; those other five belong solely at DescribeImageScanFindingsOutput's
// own top level (confirmed against awsAwsjson11_deserializeDocumentImageScanFindings,
// which has no case for any of them).
type imageScanFindingsView struct {
	FindingSeverityCounts        map[string]int32           `json:"findingSeverityCounts,omitempty"`
	Findings                     []ImageScanFinding         `json:"findings,omitempty"`
	EnhancedFindings             []EnhancedImageScanFinding `json:"enhancedFindings,omitempty"`
	ImageScanCompletedAt         float64                    `json:"imageScanCompletedAt"`
	VulnerabilitySourceUpdatedAt float64                    `json:"vulnerabilitySourceUpdatedAt,omitempty"`
}

func toImageScanFindingsView(r *ImageScanFindingsResult) *imageScanFindingsView {
	if r == nil {
		return nil
	}

	return &imageScanFindingsView{
		FindingSeverityCounts:        r.FindingSeverityCounts,
		Findings:                     r.Findings,
		EnhancedFindings:             r.EnhancedFindings,
		ImageScanCompletedAt:         r.ImageScanCompletedAt,
		VulnerabilitySourceUpdatedAt: r.VulnerabilitySourceUpdatedAt,
	}
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
		ImageScanFindings: toImageScanFindingsView(findings),
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
		RegistryID:                 h.Backend.AccountID(),
	}, nil
}
