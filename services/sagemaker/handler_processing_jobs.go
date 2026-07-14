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

type createProcessingJobRequest struct {
	VpcConfig              *VpcConfig                    `json:"VpcConfig,omitempty"`
	Environment            map[string]string             `json:"Environment,omitempty"`
	AppSpecification       processingAppSpecRequest      `json:"AppSpecification"`
	ProcessingResources    processingResourcesRequest    `json:"ProcessingResources"`
	ProcessingOutputConfig processingOutputConfigRequest `json:"ProcessingOutputConfig"`
	ProcessingJobName      string                        `json:"ProcessingJobName"`
	RoleArn                string                        `json:"RoleArn,omitempty"`
	ProcessingInputs       []processingInputRequest      `json:"ProcessingInputs,omitempty"`
	Tags                   []tagObject                   `json:"Tags"`
}

func (h *Handler) handleCreateProcessingJob(ctx context.Context, body []byte) ([]byte, error) {
	var req createProcessingJobRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if req.ProcessingJobName == "" {
		return nil, fmt.Errorf("%w: ProcessingJobName is required", errInvalidRequest)
	}

	inputs := make([]ProcessingInput, len(req.ProcessingInputs))
	for i, inp := range req.ProcessingInputs {
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

	outputs := make([]ProcessingOutput, len(req.ProcessingOutputConfig.Outputs))
	for i, out := range req.ProcessingOutputConfig.Outputs {
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
		ProcessingInputs: inputs,
		ProcessingOutputConfig: ProcessingOutputConfig{
			Outputs:  outputs,
			KmsKeyID: req.ProcessingOutputConfig.KmsKeyID,
		},
		VpcConfig:   req.VpcConfig,
		Environment: req.Environment,
		Tags:        fromTagObjects(req.Tags),
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

func (h *Handler) handleDescribeProcessingJob(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ProcessingJobName string `json:"ProcessingJobName"`
	}
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

	return json.Marshal(resp)
}

func (h *Handler) handleStopProcessingJob(ctx context.Context, body []byte) error {
	var req struct {
		ProcessingJobName string `json:"ProcessingJobName"`
	}
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

func (h *Handler) handleDeleteProcessingJob(ctx context.Context, body []byte) error {
	var req struct {
		ProcessingJobName string `json:"ProcessingJobName"`
	}
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

func (h *Handler) handleListProcessingJobs(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		NextToken    string `json:"NextToken"`
		StatusEquals string `json:"StatusEquals"`
		MaxResults   int32  `json:"MaxResults"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	jobs, nextToken := h.Backend.ListProcessingJobs(ctx, req.NextToken, req.StatusEquals, req.MaxResults)
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
