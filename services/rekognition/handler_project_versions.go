package rekognition

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

func (h *Handler) projectVersionOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateProjectVersion":    service.WrapOp(h.handleCreateProjectVersion),
		"DeleteProjectVersion":    service.WrapOp(h.handleDeleteProjectVersion),
		"DescribeProjectVersions": service.WrapOp(h.handleDescribeProjectVersions),
		"CopyProjectVersion":      service.WrapOp(h.handleCopyProjectVersion),
		"StartProjectVersion":     service.WrapOp(h.handleStartProjectVersion),
		"StopProjectVersion":      service.WrapOp(h.handleStopProjectVersion),
	}
}

// =============================================================================
// Project Versions
// =============================================================================

// outputConfigWire mirrors AWS's types.OutputConfig exactly (verified
// against aws-sdk-go-v2/service/rekognition's awsAwsjson11 serializer/
// deserializer for CreateProjectVersionInput.OutputConfig /
// ProjectVersionDescription.OutputConfig).
type outputConfigWire struct {
	S3Bucket    string `json:"S3Bucket,omitempty"`
	S3KeyPrefix string `json:"S3KeyPrefix,omitempty"`
}

// customizationFeatureContentModerationConfigWire mirrors
// types.CustomizationFeatureContentModerationConfig (types.go:495).
type customizationFeatureContentModerationConfigWire struct {
	ConfidenceThreshold *float32 `json:"ConfidenceThreshold,omitempty"`
}

// customizationFeatureConfigWire mirrors types.CustomizationFeatureConfig
// (types.go:486) -- a 2-level struct with a single member and no unions, so
// (unlike TrainingData/TestingData) it is modeled and echoed verbatim.
type customizationFeatureConfigWire struct {
	ContentModeration *customizationFeatureContentModerationConfigWire `json:"ContentModeration,omitempty"`
}

type createProjectVersionReq struct {
	OutputConfig       *outputConfigWire               `json:"OutputConfig"`
	FeatureConfig      *customizationFeatureConfigWire `json:"FeatureConfig"`
	Tags               map[string]string               `json:"Tags"`
	ProjectArn         string                          `json:"ProjectArn"`
	VersionName        string                          `json:"VersionName"`
	KmsKeyID           string                          `json:"KmsKeyId"`
	VersionDescription string                          `json:"VersionDescription"`
	TrainingData       json.RawMessage                 `json:"TrainingData"`
	TestingData        json.RawMessage                 `json:"TestingData"`
}

type createProjectVersionResp struct {
	ProjectVersionArn string `json:"ProjectVersionArn"`
}

func (h *Handler) handleCreateProjectVersion(
	_ context.Context, req *createProjectVersionReq,
) (*createProjectVersionResp, error) {
	if req.ProjectArn == "" {
		return nil, fmt.Errorf("%w: ProjectArn is required", ErrValidation)
	}

	if req.VersionName == "" {
		return nil, fmt.Errorf("%w: VersionName is required", ErrValidation)
	}

	// OutputConfig is a required CreateProjectVersionInput member (verified
	// against validateOpCreateProjectVersionInput, validators.go).
	if req.OutputConfig == nil {
		return nil, fmt.Errorf("%w: OutputConfig is required", ErrValidation)
	}

	// "If you specify TrainingData you must also specify TestingData"
	// (api_op_CreateProjectVersion.go doc comment) -- both external-manifest
	// fields are opaque here (see CreateProjectVersionParams doc), but this
	// cross-field requirement is still cheaply enforceable without modeling
	// their contents.
	if (len(req.TrainingData) > 0) != (len(req.TestingData) > 0) {
		return nil, fmt.Errorf("%w: TrainingData and TestingData must both be specified or both omitted", ErrValidation)
	}

	params := CreateProjectVersionParams{
		KmsKeyID:                req.KmsKeyID,
		VersionDescription:      req.VersionDescription,
		OutputConfigS3Bucket:    req.OutputConfig.S3Bucket,
		OutputConfigS3KeyPrefix: req.OutputConfig.S3KeyPrefix,
	}

	if req.FeatureConfig != nil && req.FeatureConfig.ContentModeration != nil {
		params.FeatureConfigContentModConfidenceThresh = req.FeatureConfig.ContentModeration.ConfidenceThreshold
	}

	v, err := h.Backend.CreateProjectVersion(req.ProjectArn, req.VersionName, params, req.Tags)
	if err != nil {
		return nil, err
	}

	return &createProjectVersionResp{ProjectVersionArn: v.ProjectVersionARN}, nil
}

type deleteProjectVersionReq struct {
	ProjectVersionArn string `json:"ProjectVersionArn"`
}

type deleteProjectVersionResp struct {
	Status string `json:"Status"`
}

func (h *Handler) handleDeleteProjectVersion(
	_ context.Context, req *deleteProjectVersionReq,
) (*deleteProjectVersionResp, error) {
	if req.ProjectVersionArn == "" {
		return nil, fmt.Errorf("%w: ProjectVersionArn is required", ErrValidation)
	}

	if err := h.Backend.DeleteProjectVersion(req.ProjectVersionArn); err != nil {
		return nil, err
	}

	return &deleteProjectVersionResp{Status: "DELETING"}, nil
}

type describeProjectVersionsReq struct {
	ProjectArn   string   `json:"ProjectArn"`
	NextToken    string   `json:"NextToken"`
	VersionNames []string `json:"VersionNames"`
	MaxResults   int32    `json:"MaxResults"`
}

type projectVersionDescription struct {
	OutputConfig            *outputConfigWire               `json:"OutputConfig,omitempty"`
	FeatureConfig           *customizationFeatureConfigWire `json:"FeatureConfig,omitempty"`
	ProjectVersionArn       string                          `json:"ProjectVersionArn"`
	Status                  string                          `json:"Status"`
	StatusMessage           string                          `json:"StatusMessage,omitempty"`
	KmsKeyID                string                          `json:"KmsKeyId,omitempty"`
	VersionDescription      string                          `json:"VersionDescription,omitempty"`
	SourceProjectVersionArn string                          `json:"SourceProjectVersionArn,omitempty"`
	CreationTimestamp       float64                         `json:"CreationTimestamp"`
	MinInferenceUnits       int32                           `json:"MinInferenceUnits,omitempty"`
	MaxInferenceUnits       int32                           `json:"MaxInferenceUnits,omitempty"`
}

// projectVersionOutputConfigFromDomain renders v's OutputConfig fields as
// the wire shape, or nil if CreateProjectVersion was never given one
// (OutputConfig is optional on the response even though it's a required
// CreateProjectVersionInput member -- see api_op_DescribeProjectVersions.go).
func projectVersionOutputConfigFromDomain(v *ProjectVersion) *outputConfigWire {
	if v.OutputConfigS3Bucket == "" && v.OutputConfigS3KeyPrefix == "" {
		return nil
	}

	return &outputConfigWire{S3Bucket: v.OutputConfigS3Bucket, S3KeyPrefix: v.OutputConfigS3KeyPrefix}
}

// projectVersionFeatureConfigFromDomain renders v's FeatureConfig field as
// the wire shape, or nil if CreateProjectVersion was never given one.
func projectVersionFeatureConfigFromDomain(v *ProjectVersion) *customizationFeatureConfigWire {
	if v.FeatureConfigContentModConfidenceThresh == nil {
		return nil
	}

	return &customizationFeatureConfigWire{
		ContentModeration: &customizationFeatureContentModerationConfigWire{
			ConfidenceThreshold: v.FeatureConfigContentModConfidenceThresh,
		},
	}
}

type describeProjectVersionsResp struct {
	NextToken                  string                      `json:"NextToken,omitempty"`
	ProjectVersionDescriptions []projectVersionDescription `json:"ProjectVersionDescriptions"`
}

func (h *Handler) handleDescribeProjectVersions(
	_ context.Context, req *describeProjectVersionsReq,
) (*describeProjectVersionsResp, error) {
	versions, nextToken, err := h.Backend.DescribeProjectVersions(
		req.ProjectArn, req.VersionNames, req.MaxResults, req.NextToken,
	)
	if err != nil {
		return nil, err
	}

	descriptions := make([]projectVersionDescription, 0, len(versions))
	for _, v := range versions {
		descriptions = append(descriptions, projectVersionDescription{
			ProjectVersionArn:       v.ProjectVersionARN,
			Status:                  v.Status,
			StatusMessage:           v.StatusMessage,
			KmsKeyID:                v.KmsKeyID,
			VersionDescription:      v.VersionDescription,
			SourceProjectVersionArn: v.SourceProjectVersionARN,
			CreationTimestamp:       epochSeconds(v.CreationTimestamp),
			OutputConfig:            projectVersionOutputConfigFromDomain(v),
			FeatureConfig:           projectVersionFeatureConfigFromDomain(v),
			MinInferenceUnits:       v.MinInferenceUnits,
			MaxInferenceUnits:       v.MaxInferenceUnits,
		})
	}

	return &describeProjectVersionsResp{
		ProjectVersionDescriptions: descriptions,
		NextToken:                  nextToken,
	}, nil
}

type copyProjectVersionReq struct {
	OutputConfig            *outputConfigWire `json:"OutputConfig"`
	SourceProjectArn        string            `json:"SourceProjectArn"`
	SourceProjectVersionArn string            `json:"SourceProjectVersionArn"`
	DestinationProjectArn   string            `json:"DestinationProjectArn"`
	VersionName             string            `json:"VersionName"`
}

type copyProjectVersionResp struct {
	ProjectVersionArn string `json:"ProjectVersionArn"`
}

func (h *Handler) handleCopyProjectVersion(
	_ context.Context, req *copyProjectVersionReq,
) (*copyProjectVersionResp, error) {
	if req.SourceProjectArn == "" {
		return nil, fmt.Errorf("%w: SourceProjectArn is required", ErrValidation)
	}

	if req.SourceProjectVersionArn == "" {
		return nil, fmt.Errorf("%w: SourceProjectVersionArn is required", ErrValidation)
	}

	if req.DestinationProjectArn == "" {
		return nil, fmt.Errorf("%w: DestinationProjectArn is required", ErrValidation)
	}

	if req.VersionName == "" {
		return nil, fmt.Errorf("%w: VersionName is required", ErrValidation)
	}

	// OutputConfig is a required CopyProjectVersionInput member (verified
	// against validateOpCopyProjectVersionInput, validators.go).
	if req.OutputConfig == nil {
		return nil, fmt.Errorf("%w: OutputConfig is required", ErrValidation)
	}

	params := CopyProjectVersionParams{
		SourceProjectARN:        req.SourceProjectArn,
		OutputConfigS3Bucket:    req.OutputConfig.S3Bucket,
		OutputConfigS3KeyPrefix: req.OutputConfig.S3KeyPrefix,
	}

	v, err := h.Backend.CopyProjectVersion(
		req.SourceProjectVersionArn, req.DestinationProjectArn, req.VersionName, params,
	)
	if err != nil {
		return nil, err
	}

	return &copyProjectVersionResp{ProjectVersionArn: v.ProjectVersionARN}, nil
}

type startProjectVersionReq struct {
	ProjectVersionArn string `json:"ProjectVersionArn"`
	MinInferenceUnits int32  `json:"MinInferenceUnits"`
	MaxInferenceUnits int32  `json:"MaxInferenceUnits"`
}

type startProjectVersionResp struct {
	Status string `json:"Status"`
}

func (h *Handler) handleStartProjectVersion(
	_ context.Context, req *startProjectVersionReq,
) (*startProjectVersionResp, error) {
	if req.ProjectVersionArn == "" {
		return nil, fmt.Errorf("%w: ProjectVersionArn is required", ErrValidation)
	}

	err := h.Backend.StartProjectVersion(req.ProjectVersionArn, req.MinInferenceUnits, req.MaxInferenceUnits)
	if err != nil {
		return nil, err
	}

	return &startProjectVersionResp{Status: "RUNNING"}, nil
}

type stopProjectVersionReq struct {
	ProjectVersionArn string `json:"ProjectVersionArn"`
}

type stopProjectVersionResp struct {
	Status string `json:"Status"`
}

func (h *Handler) handleStopProjectVersion(
	_ context.Context, req *stopProjectVersionReq,
) (*stopProjectVersionResp, error) {
	if req.ProjectVersionArn == "" {
		return nil, fmt.Errorf("%w: ProjectVersionArn is required", ErrValidation)
	}

	if err := h.Backend.StopProjectVersion(req.ProjectVersionArn); err != nil {
		return nil, err
	}

	return &stopProjectVersionResp{Status: "STOPPED"}, nil
}
