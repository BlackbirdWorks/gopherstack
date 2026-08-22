package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// ---------------------------------------------------------------------------
// ProcessingJob handlers (#12)
// ---------------------------------------------------------------------------

type processingAppSpecRequest struct {
	ImageURI            string   `json:"ImageUri"`
	ContainerArguments  []string `json:"ContainerArguments,omitempty"`
	ContainerEntrypoint []string `json:"ContainerEntrypoint,omitempty"`
}

type processingClusterConfigRequest struct {
	InstanceType   string `json:"InstanceType"`
	VolumeKmsKeyID string `json:"VolumeKmsKeyId,omitempty"`
	InstanceCount  int32  `json:"InstanceCount"`
	VolumeSizeInGB int32  `json:"VolumeSizeInGB"`
}

type processingResourcesRequest struct {
	ClusterConfig processingClusterConfigRequest `json:"ClusterConfig"`
}

type processingS3InputRequest struct {
	S3Uri                  string `json:"S3Uri"`
	LocalPath              string `json:"LocalPath"`
	S3DataType             string `json:"S3DataType,omitempty"`
	S3InputMode            string `json:"S3InputMode,omitempty"`
	S3DataDistributionType string `json:"S3DataDistributionType,omitempty"`
	S3CompressionType      string `json:"S3CompressionType,omitempty"`
}

type processingInputRequest struct {
	S3Input    *processingS3InputRequest `json:"S3Input,omitempty"`
	InputName  string                    `json:"InputName"`
	AppManaged bool                      `json:"AppManaged,omitempty"`
}

type processingS3OutputRequest struct {
	S3Uri        string `json:"S3Uri"`
	LocalPath    string `json:"LocalPath"`
	S3UploadMode string `json:"S3UploadMode,omitempty"`
}

type processingOutputRequest struct {
	S3Output   *processingS3OutputRequest `json:"S3Output,omitempty"`
	OutputName string                     `json:"OutputName"`
	AppManaged bool                       `json:"AppManaged,omitempty"`
}

type processingOutputConfigRequest struct {
	KmsKeyID string                    `json:"KmsKeyId,omitempty"`
	Outputs  []processingOutputRequest `json:"Outputs,omitempty"`
}

// processingNetworkConfigRequest mirrors types.NetworkConfig
// (api_op_CreateProcessingJob.go's NetworkConfig field) -- previously this
// handler decoded a top-level "VpcConfig" key that does not exist anywhere
// on CreateProcessingJobInput; the real VPC settings nest under
// NetworkConfig.VpcConfig instead, so every real client's VPC-isolated
// processing job silently lost its network settings.
type processingNetworkConfigRequest struct {
	VpcConfig                             *VpcConfig `json:"VpcConfig,omitempty"`
	EnableInterContainerTrafficEncryption *bool      `json:"EnableInterContainerTrafficEncryption,omitempty"`
	EnableNetworkIsolation                *bool      `json:"EnableNetworkIsolation,omitempty"`
}

// toBackend converts r to its backend type, or returns nil for a nil
// receiver -- lets callers write req.NetworkConfig.toBackend() unconditionally.
func (r *processingNetworkConfigRequest) toBackend() *ProcessingNetworkConfig {
	if r == nil {
		return nil
	}

	return &ProcessingNetworkConfig{
		VpcConfig:                             r.VpcConfig,
		EnableInterContainerTrafficEncryption: r.EnableInterContainerTrafficEncryption,
		EnableNetworkIsolation:                r.EnableNetworkIsolation,
	}
}

type processingExperimentConfigRequest struct {
	ExperimentName            string `json:"ExperimentName,omitempty"`
	RunName                   string `json:"RunName,omitempty"`
	TrialComponentDisplayName string `json:"TrialComponentDisplayName,omitempty"`
	TrialName                 string `json:"TrialName,omitempty"`
}

func (r *processingExperimentConfigRequest) toBackend() *ProcessingExperimentConfig {
	if r == nil {
		return nil
	}

	return &ProcessingExperimentConfig{
		ExperimentName:            r.ExperimentName,
		RunName:                   r.RunName,
		TrialComponentDisplayName: r.TrialComponentDisplayName,
		TrialName:                 r.TrialName,
	}
}

type processingStoppingConditionRequest struct {
	MaxRuntimeInSeconds int32 `json:"MaxRuntimeInSeconds"`
}

func (r *processingStoppingConditionRequest) toBackend() *ProcessingStoppingCondition {
	if r == nil {
		return nil
	}

	return &ProcessingStoppingCondition{MaxRuntimeInSeconds: r.MaxRuntimeInSeconds}
}

// processingInputsFromRequest converts wire-level processing inputs to their
// backend type.
func processingInputsFromRequest(reqInputs []processingInputRequest) []ProcessingInput {
	inputs := make([]ProcessingInput, len(reqInputs))

	for i, inp := range reqInputs {
		pi := ProcessingInput{InputName: inp.InputName, AppManaged: inp.AppManaged}
		if inp.S3Input != nil {
			pi.S3Input = &ProcessingS3Input{
				S3Uri:                  inp.S3Input.S3Uri,
				LocalPath:              inp.S3Input.LocalPath,
				S3DataType:             inp.S3Input.S3DataType,
				S3InputMode:            inp.S3Input.S3InputMode,
				S3DataDistributionType: inp.S3Input.S3DataDistributionType,
				S3CompressionType:      inp.S3Input.S3CompressionType,
			}
		}
		inputs[i] = pi
	}

	return inputs
}

// processingOutputsFromRequest converts wire-level processing outputs to
// their backend type.
func processingOutputsFromRequest(reqOutputs []processingOutputRequest) []ProcessingOutput {
	outputs := make([]ProcessingOutput, len(reqOutputs))

	for i, out := range reqOutputs {
		po := ProcessingOutput{OutputName: out.OutputName, AppManaged: out.AppManaged}
		if out.S3Output != nil {
			po.S3Output = &ProcessingS3Output{
				S3Uri:        out.S3Output.S3Uri,
				LocalPath:    out.S3Output.LocalPath,
				S3UploadMode: out.S3Output.S3UploadMode,
			}
		}
		outputs[i] = po
	}

	return outputs
}

type createProcessingJobRequest struct {
	NetworkConfig          *processingNetworkConfigRequest     `json:"NetworkConfig,omitempty"`
	ExperimentConfig       *processingExperimentConfigRequest  `json:"ExperimentConfig,omitempty"`
	StoppingCondition      *processingStoppingConditionRequest `json:"StoppingCondition,omitempty"`
	Environment            map[string]string                   `json:"Environment,omitempty"`
	AppSpecification       processingAppSpecRequest            `json:"AppSpecification"`
	ProcessingResources    processingResourcesRequest          `json:"ProcessingResources"`
	ProcessingOutputConfig processingOutputConfigRequest       `json:"ProcessingOutputConfig"`
	ProcessingJobName      string                              `json:"ProcessingJobName"`
	RoleArn                string                              `json:"RoleArn,omitempty"`
	ProcessingInputs       []processingInputRequest            `json:"ProcessingInputs,omitempty"`
	Tags                   []tagObject                         `json:"Tags"`
}

func (h *Handler) handleCreateProcessingJob(ctx context.Context, body []byte) ([]byte, error) {
	var req createProcessingJobRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if req.ProcessingJobName == "" {
		return nil, fmt.Errorf("%w: ProcessingJobName is required", errInvalidRequest)
	}

	if req.RoleArn == "" {
		return nil, fmt.Errorf("%w: RoleArn is required", errInvalidRequest)
	}

	if req.AppSpecification.ImageURI == "" {
		return nil, fmt.Errorf("%w: AppSpecification.ImageUri is required", errInvalidRequest)
	}

	if req.ProcessingResources.ClusterConfig.InstanceType == "" {
		return nil, fmt.Errorf("%w: ProcessingResources.ClusterConfig.InstanceType is required", errInvalidRequest)
	}

	pj, err := h.Backend.CreateProcessingJob(ctx, ProcessingJob{
		ProcessingJobName: req.ProcessingJobName,
		RoleArn:           req.RoleArn,
		AppSpecification: ProcessingAppSpec{
			ImageURI:            req.AppSpecification.ImageURI,
			ContainerArguments:  req.AppSpecification.ContainerArguments,
			ContainerEntrypoint: req.AppSpecification.ContainerEntrypoint,
		},
		ProcessingResources: ProcessingResources{
			ClusterConfig: ProcessingClusterConfig{
				InstanceType:   req.ProcessingResources.ClusterConfig.InstanceType,
				InstanceCount:  req.ProcessingResources.ClusterConfig.InstanceCount,
				VolumeSizeInGB: req.ProcessingResources.ClusterConfig.VolumeSizeInGB,
				VolumeKmsKeyID: req.ProcessingResources.ClusterConfig.VolumeKmsKeyID,
			},
		},
		ProcessingInputs: processingInputsFromRequest(req.ProcessingInputs),
		ProcessingOutputConfig: ProcessingOutputConfig{
			Outputs:  processingOutputsFromRequest(req.ProcessingOutputConfig.Outputs),
			KmsKeyID: req.ProcessingOutputConfig.KmsKeyID,
		},
		NetworkConfig:     req.NetworkConfig.toBackend(),
		ExperimentConfig:  req.ExperimentConfig.toBackend(),
		StoppingCondition: req.StoppingCondition.toBackend(),
		Environment:       req.Environment,
		Tags:              fromTagObjects(req.Tags),
	})
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(
		ctx,
		"sagemaker: created processing job",
		"name",
		pj.ProcessingJobName,
		"arn",
		pj.ProcessingJobArn,
	)

	return json.Marshal(map[string]string{keyProcessingJobArn: pj.ProcessingJobArn})
}

type describeProcessingJobRequest struct {
	ProcessingJobName string `json:"ProcessingJobName"`
}

func (h *Handler) handleDescribeProcessingJob(ctx context.Context, body []byte) ([]byte, error) {
	var req describeProcessingJobRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if req.ProcessingJobName == "" {
		return nil, fmt.Errorf("%w: ProcessingJobName is required", errInvalidRequest)
	}

	pj, err := h.Backend.DescribeProcessingJob(ctx, req.ProcessingJobName)
	if err != nil {
		return nil, err
	}

	resp := map[string]any{
		"ProcessingJobName":      pj.ProcessingJobName,
		keyProcessingJobArn:      pj.ProcessingJobArn,
		"ProcessingJobStatus":    pj.ProcessingJobStatus,
		keyRoleArn:               pj.RoleArn,
		"AppSpecification":       pj.AppSpecification,
		"ProcessingResources":    pj.ProcessingResources,
		"ProcessingInputs":       pj.ProcessingInputs,
		"ProcessingOutputConfig": pj.ProcessingOutputConfig,
		keyCreationTime:          epochSeconds(pj.CreationTime),
		keyLastModifiedTime:      epochSeconds(pj.LastModifiedTime),
	}
	if pj.ProcessingStartTime != nil {
		resp["ProcessingStartTime"] = epochSeconds(*pj.ProcessingStartTime)
	}
	if pj.ProcessingEndTime != nil {
		resp["ProcessingEndTime"] = epochSeconds(*pj.ProcessingEndTime)
	}
	if pj.FailureReason != "" {
		resp["FailureReason"] = pj.FailureReason
	}
	if pj.NetworkConfig != nil {
		resp["NetworkConfig"] = pj.NetworkConfig
	}
	if pj.ExperimentConfig != nil {
		resp["ExperimentConfig"] = pj.ExperimentConfig
	}
	if pj.StoppingCondition != nil {
		resp["StoppingCondition"] = pj.StoppingCondition
	}
	if len(pj.Environment) > 0 {
		resp["Environment"] = pj.Environment
	}

	return json.Marshal(resp)
}

type stopProcessingJobRequest struct {
	ProcessingJobName string `json:"ProcessingJobName"`
}

func (h *Handler) handleStopProcessingJob(ctx context.Context, body []byte) error {
	var req stopProcessingJobRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if req.ProcessingJobName == "" {
		return fmt.Errorf("%w: ProcessingJobName is required", errInvalidRequest)
	}

	if err := h.Backend.StopProcessingJob(ctx, req.ProcessingJobName); err != nil {
		return err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "sagemaker: stopped processing job", "name", req.ProcessingJobName)

	return nil
}

type deleteProcessingJobRequest struct {
	ProcessingJobName string `json:"ProcessingJobName"`
}

func (h *Handler) handleDeleteProcessingJob(ctx context.Context, body []byte) error {
	var req deleteProcessingJobRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if req.ProcessingJobName == "" {
		return fmt.Errorf("%w: ProcessingJobName is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteProcessingJob(ctx, req.ProcessingJobName); err != nil {
		return err
	}

	logger.Load(ctx).InfoContext(ctx, "sagemaker: deleted processing job", "name", req.ProcessingJobName)

	return nil
}

type processingJobSummary struct {
	ProcessingJobName   string  `json:"ProcessingJobName"`
	ProcessingJobArn    string  `json:"ProcessingJobArn"`
	ProcessingJobStatus string  `json:"ProcessingJobStatus"`
	CreationTime        float64 `json:"CreationTime"`
	LastModifiedTime    float64 `json:"LastModifiedTime"`
}

type listProcessingJobsRequest struct {
	CreationTimeAfter      *float64 `json:"CreationTimeAfter,omitempty"`
	CreationTimeBefore     *float64 `json:"CreationTimeBefore,omitempty"`
	LastModifiedTimeAfter  *float64 `json:"LastModifiedTimeAfter,omitempty"`
	LastModifiedTimeBefore *float64 `json:"LastModifiedTimeBefore,omitempty"`
	NextToken              string   `json:"NextToken"`
	StatusEquals           string   `json:"StatusEquals"`
	NameContains           string   `json:"NameContains,omitempty"`
	SortBy                 string   `json:"SortBy,omitempty"`
	SortOrder              string   `json:"SortOrder,omitempty"`
	MaxResults             int32    `json:"MaxResults"`
}

func (h *Handler) handleListProcessingJobs(ctx context.Context, body []byte) ([]byte, error) {
	var req listProcessingJobsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	jobs, nextToken := h.Backend.ListProcessingJobs(ctx, req.NextToken, ListProcessingJobsFilter{
		CreationTimeAfter:      epochPtr(req.CreationTimeAfter),
		CreationTimeBefore:     epochPtr(req.CreationTimeBefore),
		LastModifiedTimeAfter:  epochPtr(req.LastModifiedTimeAfter),
		LastModifiedTimeBefore: epochPtr(req.LastModifiedTimeBefore),
		StatusEquals:           req.StatusEquals,
		NameContains:           req.NameContains,
		SortBy:                 req.SortBy,
		SortOrder:              req.SortOrder,
		MaxResults:             req.MaxResults,
	})
	summaries := make([]processingJobSummary, 0, len(jobs))
	for _, pj := range jobs {
		summaries = append(summaries, processingJobSummary{
			ProcessingJobName:   pj.ProcessingJobName,
			ProcessingJobArn:    pj.ProcessingJobArn,
			ProcessingJobStatus: pj.ProcessingJobStatus,
			CreationTime:        epochSeconds(pj.CreationTime),
			LastModifiedTime:    epochSeconds(pj.LastModifiedTime),
		})
	}

	resp := map[string]any{"ProcessingJobSummaries": summaries}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return json.Marshal(resp)
}
