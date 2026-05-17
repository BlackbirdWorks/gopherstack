package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// ---------------------------------------------------------------------------
// NotebookInstanceLifecycleConfig handlers (#3)
// ---------------------------------------------------------------------------

type lifecycleHookRequest struct {
	Content string `json:"Content,omitempty"`
}

type createNotebookLifecycleConfigRequest struct {
	OnCreate                            []lifecycleHookRequest `json:"OnCreate,omitempty"`
	OnStart                             []lifecycleHookRequest `json:"OnStart,omitempty"`
	NotebookInstanceLifecycleConfigName string                 `json:"NotebookInstanceLifecycleConfigName"`
}

func (h *Handler) handleCreateNotebookInstanceLifecycleConfig(
	ctx context.Context,
	body []byte,
) ([]byte, error) {
	var req createNotebookLifecycleConfigRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.NotebookInstanceLifecycleConfigName == "" {
		return nil, fmt.Errorf(
			"%w: NotebookInstanceLifecycleConfigName is required",
			errInvalidRequest,
		)
	}

	onCreate := make([]NotebookLifecycleHook, len(req.OnCreate))
	for i, h := range req.OnCreate {
		onCreate[i] = NotebookLifecycleHook{Content: h.Content}
	}
	onStart := make([]NotebookLifecycleHook, len(req.OnStart))
	for i, h := range req.OnStart {
		onStart[i] = NotebookLifecycleHook{Content: h.Content}
	}

	lc, err := h.Backend.CreateNotebookInstanceLifecycleConfig(
		req.NotebookInstanceLifecycleConfigName,
		onCreate,
		onStart,
	)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "sagemaker: created notebook lifecycle config", "name", lc.Name)

	return json.Marshal(map[string]string{keyNotebookLifecycleConfigArn: lc.ARN})
}

func (h *Handler) handleDescribeNotebookInstanceLifecycleConfig(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		NotebookInstanceLifecycleConfigName string `json:"NotebookInstanceLifecycleConfigName"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if req.NotebookInstanceLifecycleConfigName == "" {
		return nil, fmt.Errorf(
			"%w: NotebookInstanceLifecycleConfigName is required",
			errInvalidRequest,
		)
	}

	lc, err := h.Backend.DescribeNotebookInstanceLifecycleConfig(
		req.NotebookInstanceLifecycleConfigName,
	)
	if err != nil {
		return nil, err
	}

	onCreate := make([]map[string]string, len(lc.OnCreate))
	for i, hook := range lc.OnCreate {
		onCreate[i] = map[string]string{"Content": hook.Content}
	}
	onStart := make([]map[string]string, len(lc.OnStart))
	for i, hook := range lc.OnStart {
		onStart[i] = map[string]string{"Content": hook.Content}
	}

	return json.Marshal(map[string]any{
		"NotebookInstanceLifecycleConfigName": lc.Name,
		keyNotebookLifecycleConfigArn:         lc.ARN,
		"OnCreate":                            onCreate,
		"OnStart":                             onStart,
		keyCreationTime:                       epochSeconds(lc.CreationTime),
		keyLastModifiedTime:                   epochSeconds(lc.LastModifiedTime),
	})
}

func (h *Handler) handleUpdateNotebookInstanceLifecycleConfig(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		OnCreate                            []lifecycleHookRequest `json:"OnCreate,omitempty"`
		OnStart                             []lifecycleHookRequest `json:"OnStart,omitempty"`
		NotebookInstanceLifecycleConfigName string                 `json:"NotebookInstanceLifecycleConfigName"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if req.NotebookInstanceLifecycleConfigName == "" {
		return nil, fmt.Errorf(
			"%w: NotebookInstanceLifecycleConfigName is required",
			errInvalidRequest,
		)
	}

	var onCreate []NotebookLifecycleHook
	if req.OnCreate != nil {
		onCreate = make([]NotebookLifecycleHook, len(req.OnCreate))
		for i, h := range req.OnCreate {
			onCreate[i] = NotebookLifecycleHook{Content: h.Content}
		}
	}
	var onStart []NotebookLifecycleHook
	if req.OnStart != nil {
		onStart = make([]NotebookLifecycleHook, len(req.OnStart))
		for i, h := range req.OnStart {
			onStart[i] = NotebookLifecycleHook{Content: h.Content}
		}
	}

	_, err := h.Backend.UpdateNotebookInstanceLifecycleConfig(
		req.NotebookInstanceLifecycleConfigName,
		onCreate,
		onStart,
	)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{})
}

func (h *Handler) handleDeleteNotebookInstanceLifecycleConfig(
	ctx context.Context,
	body []byte,
) error {
	var req struct {
		NotebookInstanceLifecycleConfigName string `json:"NotebookInstanceLifecycleConfigName"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if req.NotebookInstanceLifecycleConfigName == "" {
		return fmt.Errorf("%w: NotebookInstanceLifecycleConfigName is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteNotebookInstanceLifecycleConfig(req.NotebookInstanceLifecycleConfigName); err != nil {
		return err
	}

	log := logger.Load(ctx)
	log.InfoContext(
		ctx,
		"sagemaker: deleted notebook lifecycle config",
		"name",
		req.NotebookInstanceLifecycleConfigName,
	)

	return nil
}

type notebookLifecycleSummary struct {
	NotebookInstanceLifecycleConfigName string  `json:"NotebookInstanceLifecycleConfigName"`
	NotebookInstanceLifecycleConfigArn  string  `json:"NotebookInstanceLifecycleConfigArn"`
	CreationTime                        float64 `json:"CreationTime"`
	LastModifiedTime                    float64 `json:"LastModifiedTime"`
}

func (h *Handler) handleListNotebookInstanceLifecycleConfigs(body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	configs, nextToken := h.Backend.ListNotebookInstanceLifecycleConfigs(req.NextToken)
	summaries := make([]notebookLifecycleSummary, 0, len(configs))
	for _, lc := range configs {
		summaries = append(summaries, notebookLifecycleSummary{
			NotebookInstanceLifecycleConfigName: lc.Name,
			NotebookInstanceLifecycleConfigArn:  lc.ARN,
			CreationTime:                        epochSeconds(lc.CreationTime),
			LastModifiedTime:                    epochSeconds(lc.LastModifiedTime),
		})
	}

	resp := map[string]any{"NotebookInstanceLifecycleConfigs": summaries}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return json.Marshal(resp)
}

// ---------------------------------------------------------------------------
// Pipeline ops handlers (#29) — wire real backend to stub-bypassed ops
// ---------------------------------------------------------------------------

func (h *Handler) handleRetryPipelineExecution(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		PipelineExecutionArn string `json:"PipelineExecutionArn"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if req.PipelineExecutionArn == "" {
		return nil, fmt.Errorf("%w: PipelineExecutionArn is required", errInvalidRequest)
	}

	exec, err := h.Backend.RetryPipelineExecution(req.PipelineExecutionArn)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "sagemaker: retried pipeline execution", "arn", exec.PipelineExecutionArn)

	return json.Marshal(map[string]string{keyPipelineExecutionArn: exec.PipelineExecutionArn})
}

func (h *Handler) handleStopPipelineExecution(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		PipelineExecutionArn string `json:"PipelineExecutionArn"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if req.PipelineExecutionArn == "" {
		return nil, fmt.Errorf("%w: PipelineExecutionArn is required", errInvalidRequest)
	}

	exec, err := h.Backend.StopPipelineExecution(req.PipelineExecutionArn)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "sagemaker: stopped pipeline execution", "arn", exec.PipelineExecutionArn)

	return json.Marshal(map[string]string{keyPipelineExecutionArn: exec.PipelineExecutionArn})
}

// handleSendPipelineExecutionStepSuccess handles the AWS callback token API.
// AWS uses CallbackToken (opaque) rather than PipelineExecutionArn+StepName.
// We use the token as both the step key and (by convention) accept execArn if provided.
func (h *Handler) handleSendPipelineExecutionStepSuccess(
	ctx context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		PipelineExecutionArn string `json:"PipelineExecutionArn"`
		CallbackToken        string `json:"CallbackToken"`
		StepName             string `json:"StepName"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	execArn := req.PipelineExecutionArn
	if execArn == "" {
		execArn = req.CallbackToken
	}
	stepName := req.StepName
	if stepName == "" {
		stepName = "callback-step"
	}

	// Lenient: if execArn not found, return success (callback token may reference
	// executions from before this session).
	_ = h.Backend.SendPipelineExecutionStepSuccess(execArn, stepName)

	log := logger.Load(ctx)
	log.InfoContext(ctx, "sagemaker: sent pipeline step success", "token", execArn)

	return json.Marshal(map[string]string{keyPipelineExecutionArn: execArn})
}

// handleSendPipelineExecutionStepFailure handles the AWS callback token API.
func (h *Handler) handleSendPipelineExecutionStepFailure(
	ctx context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		PipelineExecutionArn string `json:"PipelineExecutionArn"`
		CallbackToken        string `json:"CallbackToken"`
		StepName             string `json:"StepName"`
		FailureReason        string `json:"FailureReason,omitempty"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	execArn := req.PipelineExecutionArn
	if execArn == "" {
		execArn = req.CallbackToken
	}
	stepName := req.StepName
	if stepName == "" {
		stepName = "callback-step"
	}

	// Lenient: tolerate missing execArn (stale callback token).
	_ = h.Backend.SendPipelineExecutionStepFailure(execArn, stepName, req.FailureReason)

	log := logger.Load(ctx)
	log.InfoContext(ctx, "sagemaker: sent pipeline step failure", "token", execArn)

	return json.Marshal(map[string]string{keyPipelineExecutionArn: execArn})
}

type pipelineExecStepSummary struct {
	StepName      string  `json:"StepName"`
	StepType      string  `json:"StepType,omitempty"`
	StepStatus    string  `json:"StepStatus"`
	FailureReason string  `json:"FailureReason,omitempty"`
	StartTime     float64 `json:"StartTime,omitempty"`
	EndTime       float64 `json:"EndTime,omitempty"`
}

func (h *Handler) handleListPipelineExecutionSteps(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		PipelineExecutionArn string `json:"PipelineExecutionArn"`
		NextToken            string `json:"NextToken"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if req.PipelineExecutionArn == "" {
		return nil, fmt.Errorf("%w: PipelineExecutionArn is required", errInvalidRequest)
	}

	steps, nextToken := h.Backend.ListPipelineExecutionSteps(
		req.PipelineExecutionArn,
		req.NextToken,
	)
	summaries := make([]pipelineExecStepSummary, 0, len(steps))
	for _, s := range steps {
		sum := pipelineExecStepSummary{
			StepName:      s.StepName,
			StepType:      s.StepType,
			StepStatus:    s.StepStatus,
			FailureReason: s.FailureReason,
			StartTime:     epochSeconds(s.StartTime),
		}
		if !s.EndTime.IsZero() {
			sum.EndTime = epochSeconds(s.EndTime)
		}
		summaries = append(summaries, sum)
	}

	resp := map[string]any{"PipelineExecutionSteps": summaries}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return json.Marshal(resp)
}

// ---------------------------------------------------------------------------
// ProcessingJob handlers (#12)
// ---------------------------------------------------------------------------

type processingAppSpecRequest struct {
	ContainerArguments  []string `json:"ContainerArguments,omitempty"`
	ContainerEntrypoint []string `json:"ContainerEntrypoint,omitempty"`
	ImageUri            string   `json:"ImageUri"`
}

type processingClusterConfigRequest struct {
	InstanceType   string `json:"InstanceType"`
	InstanceCount  int32  `json:"InstanceCount"`
	VolumeSizeInGB int32  `json:"VolumeSizeInGB"`
	VolumeKmsKeyId string `json:"VolumeKmsKeyId,omitempty"`
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
	InputName  string                    `json:"InputName"`
	S3Input    *processingS3InputRequest `json:"S3Input,omitempty"`
	AppManaged bool                      `json:"AppManaged,omitempty"`
}

type processingS3OutputRequest struct {
	S3Uri        string `json:"S3Uri"`
	LocalPath    string `json:"LocalPath"`
	S3UploadMode string `json:"S3UploadMode,omitempty"`
}

type processingOutputRequest struct {
	OutputName string                     `json:"OutputName"`
	S3Output   *processingS3OutputRequest `json:"S3Output,omitempty"`
	AppManaged bool                       `json:"AppManaged,omitempty"`
}

type processingOutputConfigRequest struct {
	Outputs  []processingOutputRequest `json:"Outputs,omitempty"`
	KmsKeyId string                    `json:"KmsKeyId,omitempty"`
}

type createProcessingJobRequest struct {
	AppSpecification       processingAppSpecRequest      `json:"AppSpecification"`
	ProcessingResources    processingResourcesRequest    `json:"ProcessingResources"`
	ProcessingInputs       []processingInputRequest      `json:"ProcessingInputs,omitempty"`
	ProcessingOutputConfig processingOutputConfigRequest `json:"ProcessingOutputConfig,omitempty"`
	VpcConfig              *VpcConfig                    `json:"VpcConfig,omitempty"`
	Environment            map[string]string             `json:"Environment,omitempty"`
	Tags                   []tagObject                   `json:"Tags"`
	ProcessingJobName      string                        `json:"ProcessingJobName"`
	RoleArn                string                        `json:"RoleArn,omitempty"`
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

	pj, err := h.Backend.CreateProcessingJob(ProcessingJob{
		ProcessingJobName: req.ProcessingJobName,
		RoleArn:           req.RoleArn,
		AppSpecification: ProcessingAppSpec{
			ImageUri:            req.AppSpecification.ImageUri,
			ContainerArguments:  req.AppSpecification.ContainerArguments,
			ContainerEntrypoint: req.AppSpecification.ContainerEntrypoint,
		},
		ProcessingResources: ProcessingResources{
			ClusterConfig: ProcessingClusterConfig{
				InstanceType:   req.ProcessingResources.ClusterConfig.InstanceType,
				InstanceCount:  req.ProcessingResources.ClusterConfig.InstanceCount,
				VolumeSizeInGB: req.ProcessingResources.ClusterConfig.VolumeSizeInGB,
				VolumeKmsKeyId: req.ProcessingResources.ClusterConfig.VolumeKmsKeyId,
			},
		},
		ProcessingInputs: inputs,
		ProcessingOutputConfig: ProcessingOutputConfig{
			Outputs:  outputs,
			KmsKeyId: req.ProcessingOutputConfig.KmsKeyId,
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

func (h *Handler) handleDescribeProcessingJob(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		ProcessingJobName string `json:"ProcessingJobName"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if req.ProcessingJobName == "" {
		return nil, fmt.Errorf("%w: ProcessingJobName is required", errInvalidRequest)
	}

	pj, err := h.Backend.DescribeProcessingJob(req.ProcessingJobName)
	if err != nil {
		return nil, err
	}

	resp := map[string]any{
		"ProcessingJobName":      pj.ProcessingJobName,
		keyProcessingJobArn:      pj.ProcessingJobArn,
		"ProcessingJobStatus":    pj.ProcessingJobStatus,
		"RoleArn":                pj.RoleArn,
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

	if err := h.Backend.StopProcessingJob(req.ProcessingJobName); err != nil {
		return err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "sagemaker: stopped processing job", "name", req.ProcessingJobName)

	return nil
}

type processingJobSummary struct {
	ProcessingJobName   string  `json:"ProcessingJobName"`
	ProcessingJobArn    string  `json:"ProcessingJobArn"`
	ProcessingJobStatus string  `json:"ProcessingJobStatus"`
	CreationTime        float64 `json:"CreationTime"`
	LastModifiedTime    float64 `json:"LastModifiedTime"`
}

func (h *Handler) handleListProcessingJobs(body []byte) ([]byte, error) {
	var req struct {
		NextToken    string `json:"NextToken"`
		StatusEquals string `json:"StatusEquals"`
		MaxResults   int32  `json:"MaxResults"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	jobs, nextToken := h.Backend.ListProcessingJobs(req.NextToken, req.StatusEquals, req.MaxResults)
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

// ---------------------------------------------------------------------------
// Expanded CreateEndpoint handler (uses FSM)
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateEndpointFSM(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		EndpointName       string      `json:"EndpointName"`
		EndpointConfigName string      `json:"EndpointConfigName"`
		Tags               []tagObject `json:"Tags"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if req.EndpointName == "" {
		return nil, fmt.Errorf("%w: EndpointName is required", errInvalidRequest)
	}
	if req.EndpointConfigName == "" {
		return nil, fmt.Errorf("%w: EndpointConfigName is required", errInvalidRequest)
	}

	tags := fromTagObjects(req.Tags)
	ep, err := h.Backend.CreateEndpointFSM(req.EndpointName, req.EndpointConfigName, tags)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(
		ctx,
		"sagemaker: created endpoint (FSM)",
		"name",
		ep.EndpointName,
		"arn",
		ep.EndpointArn,
	)

	return json.Marshal(map[string]string{keyEndpointArn: ep.EndpointArn})
}

// handleUpdateEndpointFSM replaces the basic UpdateEndpoint handler with one
// that properly drives Updating → InService via the lifecycle simulator.
func (h *Handler) handleUpdateEndpointFSM(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		EndpointName       string `json:"EndpointName"`
		EndpointConfigName string `json:"EndpointConfigName"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if req.EndpointName == "" {
		return nil, fmt.Errorf("%w: EndpointName is required", errInvalidRequest)
	}

	ep, err := h.Backend.UpdateEndpointFSM(req.EndpointName, req.EndpointConfigName)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "sagemaker: updated endpoint (FSM)", "name", ep.EndpointName)

	return json.Marshal(map[string]string{keyEndpointArn: ep.EndpointArn})
}

// ---------------------------------------------------------------------------
// UpdateEndpointWeightsAndCapacities (proper implementation, gap #10)
// ---------------------------------------------------------------------------

func (h *Handler) handleUpdateEndpointWeightsAndCapacitiesFull(
	ctx context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		DesiredWeightsAndCapacities []DesiredWeightAndCapacity `json:"DesiredWeightsAndCapacities"`
		EndpointName                string                     `json:"EndpointName"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if req.EndpointName == "" {
		return nil, fmt.Errorf("%w: EndpointName is required", errInvalidRequest)
	}

	ep, err := h.Backend.UpdateEndpointWeightsAndCapacitiesFull(
		req.EndpointName,
		req.DesiredWeightsAndCapacities,
	)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(
		ctx,
		"sagemaker: updated endpoint weights and capacities",
		"name",
		req.EndpointName,
	)

	return json.Marshal(map[string]string{keyEndpointArn: ep.EndpointArn})
}

// ---------------------------------------------------------------------------
// Expanded CreateNotebookInstance (uses FSM + full fields, gaps #1, #2)
// ---------------------------------------------------------------------------

type createNotebookInstanceFullRequest struct {
	AdditionalCodeRepositories []string    `json:"AdditionalCodeRepositories,omitempty"`
	AcceleratorTypes           []string    `json:"AcceleratorTypes,omitempty"`
	SecurityGroupIds           []string    `json:"SecurityGroupIds,omitempty"`
	Tags                       []tagObject `json:"Tags"`
	NotebookInstanceName       string      `json:"NotebookInstanceName"`
	InstanceType               string      `json:"InstanceType"`
	RoleArn                    string      `json:"RoleArn"`
	SubnetId                   string      `json:"SubnetId,omitempty"`
	KmsKeyId                   string      `json:"KmsKeyId,omitempty"`
	LifecycleConfigName        string      `json:"NotebookInstanceLifecycleConfigName,omitempty"`
	DefaultCodeRepository      string      `json:"DefaultCodeRepository,omitempty"`
	PlatformIdentifier         string      `json:"PlatformIdentifier,omitempty"`
	DirectInternetAccess       string      `json:"DirectInternetAccess,omitempty"`
	RootAccess                 string      `json:"RootAccess,omitempty"`
	VolumeSizeInGB             int32       `json:"VolumeSizeInGB,omitempty"`
}

func (h *Handler) handleCreateNotebookInstanceFull(
	ctx context.Context,
	body []byte,
) ([]byte, error) {
	var req createNotebookInstanceFullRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	nb, err := h.Backend.CreateNotebookInstanceFSM(NotebookInstanceOptions{
		Name:                       req.NotebookInstanceName,
		InstanceType:               req.InstanceType,
		RoleArn:                    req.RoleArn,
		SubnetId:                   req.SubnetId,
		SecurityGroupIds:           req.SecurityGroupIds,
		KmsKeyId:                   req.KmsKeyId,
		LifecycleConfigName:        req.LifecycleConfigName,
		DirectInternetAccess:       req.DirectInternetAccess,
		RootAccess:                 req.RootAccess,
		AcceleratorTypes:           req.AcceleratorTypes,
		AdditionalCodeRepositories: req.AdditionalCodeRepositories,
		DefaultCodeRepository:      req.DefaultCodeRepository,
		VolumeSizeInGB:             req.VolumeSizeInGB,
		PlatformIdentifier:         req.PlatformIdentifier,
		Tags:                       fromTagObjects(req.Tags),
	})
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(
		ctx,
		"sagemaker: created notebook instance (full)",
		"name",
		nb.NotebookInstanceName,
		"arn",
		nb.NotebookInstanceArn,
	)

	return json.Marshal(map[string]string{"NotebookInstanceArn": nb.NotebookInstanceArn})
}

// handleStartNotebookInstanceFSM wraps StartNotebookInstanceFSM with proper status validation.
func (h *Handler) handleStartNotebookInstanceFSM(ctx context.Context, body []byte) error {
	var req struct {
		NotebookInstanceName string `json:"NotebookInstanceName"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if req.NotebookInstanceName == "" {
		return fmt.Errorf("%w: NotebookInstanceName is required", errInvalidRequest)
	}

	if err := h.Backend.StartNotebookInstanceFSM(req.NotebookInstanceName); err != nil {
		return err
	}

	log := logger.Load(ctx)
	log.InfoContext(
		ctx,
		"sagemaker: started notebook instance (FSM)",
		"name",
		req.NotebookInstanceName,
	)

	return nil
}

// handleStopNotebookInstanceFSM wraps StopNotebookInstanceFSM with proper status validation.
func (h *Handler) handleStopNotebookInstanceFSM(ctx context.Context, body []byte) error {
	var req struct {
		NotebookInstanceName string `json:"NotebookInstanceName"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if req.NotebookInstanceName == "" {
		return fmt.Errorf("%w: NotebookInstanceName is required", errInvalidRequest)
	}

	if err := h.Backend.StopNotebookInstanceFSM(req.NotebookInstanceName); err != nil {
		return err
	}

	log := logger.Load(ctx)
	log.InfoContext(
		ctx,
		"sagemaker: stopped notebook instance (FSM)",
		"name",
		req.NotebookInstanceName,
	)

	return nil
}

// handleDescribeNotebookInstanceFull returns all notebook fields.
func (h *Handler) handleDescribeNotebookInstanceFull(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		NotebookInstanceName string `json:"NotebookInstanceName"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if req.NotebookInstanceName == "" {
		return nil, fmt.Errorf("%w: NotebookInstanceName is required", errInvalidRequest)
	}

	nb, err := h.Backend.DescribeNotebookInstance(req.NotebookInstanceName)
	if err != nil {
		return nil, err
	}

	resp := map[string]any{
		"NotebookInstanceName":   nb.NotebookInstanceName,
		"NotebookInstanceArn":    nb.NotebookInstanceArn,
		"NotebookInstanceStatus": nb.NotebookInstanceStatus,
		"InstanceType":           nb.InstanceType,
		"RoleArn":                nb.RoleArn,
		keyCreationTime:          epochSeconds(nb.CreationTime),
		keyLastModifiedTime:      epochSeconds(nb.LastModifiedTime),
	}

	// Optional fields — only include if set.
	if nb.SubnetId != "" {
		resp["SubnetId"] = nb.SubnetId
	}
	if nb.KmsKeyId != "" {
		resp["KmsKeyId"] = nb.KmsKeyId
	}
	if nb.LifecycleConfigName != "" {
		resp["NotebookInstanceLifecycleConfigName"] = nb.LifecycleConfigName
	}
	if nb.DirectInternetAccess != "" {
		resp["DirectInternetAccess"] = nb.DirectInternetAccess
	}
	if nb.RootAccess != "" {
		resp["RootAccess"] = nb.RootAccess
	}
	if nb.DefaultCodeRepository != "" {
		resp["DefaultCodeRepository"] = nb.DefaultCodeRepository
	}
	if nb.PlatformIdentifier != "" {
		resp["PlatformIdentifier"] = nb.PlatformIdentifier
	}
	if nb.VolumeSizeInGB > 0 {
		resp["VolumeSizeInGB"] = nb.VolumeSizeInGB
	}
	if len(nb.SecurityGroupIds) > 0 {
		resp["SecurityGroupIds"] = nb.SecurityGroupIds
	}
	if len(nb.AcceleratorTypes) > 0 {
		resp["AcceleratorTypes"] = nb.AcceleratorTypes
	}
	if len(nb.AdditionalCodeRepositories) > 0 {
		resp["AdditionalCodeRepositories"] = nb.AdditionalCodeRepositories
	}
	if nb.URL != "" {
		resp["Url"] = nb.URL
	}

	return json.Marshal(resp)
}

// handleUpdateNotebookInstanceFull supports all mutable fields (#1 update coverage).
func (h *Handler) handleUpdateNotebookInstanceFullFull(ctx context.Context, body []byte) error {
	var req struct {
		AdditionalCodeRepositories             []string `json:"AdditionalCodeRepositories,omitempty"`
		NotebookInstanceName                   string   `json:"NotebookInstanceName"`
		InstanceType                           string   `json:"InstanceType,omitempty"`
		RoleArn                                string   `json:"RoleArn,omitempty"`
		LifecycleConfigName                    string   `json:"NotebookInstanceLifecycleConfigName,omitempty"`
		DefaultCodeRepository                  string   `json:"DefaultCodeRepository,omitempty"`
		VolumeSizeInGB                         int32    `json:"VolumeSizeInGB,omitempty"`
		DisassociateLifecycleConfig            bool     `json:"DisassociateLifecycleConfig,omitempty"`
		DisassociateDefaultCodeRepository      bool     `json:"DisassociateDefaultCodeRepository,omitempty"`
		DisassociateAdditionalCodeRepositories bool     `json:"DisassociateAdditionalCodeRepositories,omitempty"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if req.NotebookInstanceName == "" {
		return fmt.Errorf("%w: NotebookInstanceName is required", errInvalidRequest)
	}

	if err := h.Backend.UpdateNotebookInstanceFull(req.NotebookInstanceName, NotebookUpdateOptions{
		InstanceType:                           req.InstanceType,
		RoleArn:                                req.RoleArn,
		LifecycleConfigName:                    req.LifecycleConfigName,
		DefaultCodeRepository:                  req.DefaultCodeRepository,
		AdditionalCodeRepositories:             req.AdditionalCodeRepositories,
		VolumeSizeInGB:                         req.VolumeSizeInGB,
		DisassociateLifecycleConfig:            req.DisassociateLifecycleConfig,
		DisassociateDefaultCodeRepository:      req.DisassociateDefaultCodeRepository,
		DisassociateAdditionalCodeRepositories: req.DisassociateAdditionalCodeRepositories,
	}); err != nil {
		return err
	}

	log := logger.Load(ctx)
	log.InfoContext(
		ctx,
		"sagemaker: updated notebook instance (full)",
		"name",
		req.NotebookInstanceName,
	)

	return nil
}

// ---------------------------------------------------------------------------
// Expanded CreateTrainingJob (full fields, gaps #4, #5, #6)
// ---------------------------------------------------------------------------

type createTrainingJobFullRequest struct {
	AlgorithmSpecification struct {
		MetricDefinitions []struct {
			Name  string `json:"Name"`
			Regex string `json:"Regex,omitempty"`
		} `json:"MetricDefinitions,omitempty"`
		ContainerEntrypoint              []string `json:"ContainerEntrypoint,omitempty"`
		ContainerArguments               []string `json:"ContainerArguments,omitempty"`
		TrainingImage                    string   `json:"TrainingImage,omitempty"`
		AlgorithmName                    string   `json:"AlgorithmName,omitempty"`
		TrainingInputMode                string   `json:"TrainingInputMode,omitempty"`
		EnableSageMakerMetricsTimeSeries bool     `json:"EnableSageMakerMetricsTimeSeries,omitempty"`
	} `json:"AlgorithmSpecification"`
	InputDataConfig                       []Channel         `json:"InputDataConfig,omitempty"`
	OutputDataConfig                      OutputDataConfig  `json:"OutputDataConfig,omitempty"`
	ResourceConfig                        ResourceConfig    `json:"ResourceConfig,omitempty"`
	StoppingCondition                     StoppingCondition `json:"StoppingCondition,omitempty"`
	VpcConfig                             *VpcConfig        `json:"VpcConfig,omitempty"`
	CheckpointConfig                      *CheckpointConfig `json:"CheckpointConfig,omitempty"`
	HyperParameters                       map[string]string `json:"HyperParameters,omitempty"`
	Environment                           map[string]string `json:"Environment,omitempty"`
	Tags                                  []tagObject       `json:"Tags"`
	TrainingJobName                       string            `json:"TrainingJobName"`
	RoleArn                               string            `json:"RoleArn"`
	EnableNetworkIsolation                bool              `json:"EnableNetworkIsolation,omitempty"`
	EnableManagedSpotTraining             bool              `json:"EnableManagedSpotTraining,omitempty"`
	EnableInterContainerTrafficEncryption bool              `json:"EnableInterContainerTrafficEncryption,omitempty"`
}

func (h *Handler) handleCreateTrainingJobFull(ctx context.Context, body []byte) ([]byte, error) {
	var req createTrainingJobFullRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if req.TrainingJobName == "" {
		return nil, fmt.Errorf("%w: TrainingJobName is required", errInvalidRequest)
	}

	metrics := make([]MetricDefinition, len(req.AlgorithmSpecification.MetricDefinitions))
	for i, md := range req.AlgorithmSpecification.MetricDefinitions {
		metrics[i] = MetricDefinition{Name: md.Name, Regex: md.Regex}
	}

	tj, err := h.Backend.CreateTrainingJobFull(TrainingJobOptions{
		TrainingJobName: req.TrainingJobName,
		RoleArn:         req.RoleArn,
		AlgorithmSpecification: AlgorithmSpecification{
			TrainingImage:                    req.AlgorithmSpecification.TrainingImage,
			AlgorithmName:                    req.AlgorithmSpecification.AlgorithmName,
			TrainingInputMode:                req.AlgorithmSpecification.TrainingInputMode,
			MetricDefinitions:                metrics,
			ContainerEntrypoint:              req.AlgorithmSpecification.ContainerEntrypoint,
			ContainerArguments:               req.AlgorithmSpecification.ContainerArguments,
			EnableSageMakerMetricsTimeSeries: req.AlgorithmSpecification.EnableSageMakerMetricsTimeSeries,
		},
		InputDataConfig:                       req.InputDataConfig,
		OutputDataConfig:                      req.OutputDataConfig,
		ResourceConfig:                        req.ResourceConfig,
		StoppingCondition:                     req.StoppingCondition,
		VpcConfig:                             req.VpcConfig,
		CheckpointConfig:                      req.CheckpointConfig,
		HyperParameters:                       req.HyperParameters,
		Environment:                           req.Environment,
		Tags:                                  fromTagObjects(req.Tags),
		EnableNetworkIsolation:                req.EnableNetworkIsolation,
		EnableManagedSpotTraining:             req.EnableManagedSpotTraining,
		EnableInterContainerTrafficEncryption: req.EnableInterContainerTrafficEncryption,
	})
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(
		ctx,
		"sagemaker: created training job (full)",
		"name",
		tj.TrainingJobName,
		"arn",
		tj.TrainingJobArn,
	)

	return json.Marshal(map[string]string{keyTrainingJobArn: tj.TrainingJobArn})
}

func (h *Handler) handleDescribeTrainingJobFull(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		TrainingJobName string `json:"TrainingJobName"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if req.TrainingJobName == "" {
		return nil, fmt.Errorf("%w: TrainingJobName is required", errInvalidRequest)
	}

	tj, err := h.Backend.DescribeTrainingJob(req.TrainingJobName)
	if err != nil {
		return nil, err
	}

	resp := map[string]any{
		"TrainingJobName":        tj.TrainingJobName,
		keyTrainingJobArn:        tj.TrainingJobArn,
		"TrainingJobStatus":      tj.TrainingJobStatus,
		"SecondaryStatus":        tj.SecondaryStatus,
		keyRoleArn:               tj.RoleArn,
		"AlgorithmSpecification": tj.AlgorithmSpecification,
		"ResourceConfig":         tj.ResourceConfig,
		"StoppingCondition":      tj.StoppingCondition,
		keyCreationTime:          epochSeconds(tj.CreationTime),
		keyLastModifiedTime:      epochSeconds(tj.LastModifiedTime),
	}
	if len(tj.InputDataConfig) > 0 {
		resp["InputDataConfig"] = tj.InputDataConfig
	}
	if tj.OutputDataConfig.S3OutputPath != "" {
		resp["OutputDataConfig"] = tj.OutputDataConfig
	}
	if tj.VpcConfig != nil {
		resp["VpcConfig"] = tj.VpcConfig
	}
	if tj.CheckpointConfig != nil {
		resp["CheckpointConfig"] = tj.CheckpointConfig
	}
	if len(tj.HyperParameters) > 0 {
		resp["HyperParameters"] = tj.HyperParameters
	}
	if len(tj.Environment) > 0 {
		resp["Environment"] = tj.Environment
	}
	if tj.ModelArtifacts != nil {
		resp["ModelArtifacts"] = tj.ModelArtifacts
	}
	if tj.TrainingStartTime != nil {
		resp["TrainingStartTime"] = epochSeconds(*tj.TrainingStartTime)
	}
	if tj.TrainingEndTime != nil {
		resp["TrainingEndTime"] = epochSeconds(*tj.TrainingEndTime)
	}
	if tj.BillableTimeInSeconds > 0 {
		resp["BillableTimeInSeconds"] = tj.BillableTimeInSeconds
	}
	if tj.TrainingTimeInSeconds > 0 {
		resp["TrainingTimeInSeconds"] = tj.TrainingTimeInSeconds
	}
	if len(tj.SecondaryStatusTransitions) > 0 {
		resp["SecondaryStatusTransitions"] = buildSecondaryStatusTransitions(
			tj.SecondaryStatusTransitions,
		)
	}
	if tj.FailureReason != "" {
		resp["FailureReason"] = tj.FailureReason
	}

	return json.Marshal(resp)
}

func buildSecondaryStatusTransitions(transitions []SecondaryStatusTransition) []map[string]any {
	result := make([]map[string]any, len(transitions))
	for i, t := range transitions {
		entry := map[string]any{
			"Status":        t.Status,
			"StartTime":     epochSeconds(t.StartTime),
			"StatusMessage": t.StatusMessage,
		}
		if t.EndTime != nil {
			entry["EndTime"] = epochSeconds(*t.EndTime)
		}
		result[i] = entry
	}
	return result
}

func (h *Handler) handleStopTrainingJobFSM(ctx context.Context, body []byte) error {
	var req struct {
		TrainingJobName string `json:"TrainingJobName"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if req.TrainingJobName == "" {
		return fmt.Errorf("%w: TrainingJobName is required", errInvalidRequest)
	}

	if err := h.Backend.StopTrainingJobFSM(req.TrainingJobName); err != nil {
		return err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "sagemaker: stopped training job (FSM)", "name", req.TrainingJobName)

	return nil
}

func (h *Handler) handleListTrainingJobsFiltered(body []byte) ([]byte, error) {
	var req struct {
		CreationTimeAfter  *time.Time `json:"CreationTimeAfter,omitempty"`
		CreationTimeBefore *time.Time `json:"CreationTimeBefore,omitempty"`
		NextToken          string     `json:"NextToken"`
		NameContains       string     `json:"NameContains,omitempty"`
		StatusEquals       string     `json:"StatusEquals,omitempty"`
		MaxResults         int32      `json:"MaxResults,omitempty"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	jobs, nextToken := h.Backend.ListTrainingJobsFiltered(req.NextToken, ListTrainingJobsFilter{
		StatusEquals:       req.StatusEquals,
		NameContains:       req.NameContains,
		CreationTimeAfter:  req.CreationTimeAfter,
		CreationTimeBefore: req.CreationTimeBefore,
		MaxResults:         req.MaxResults,
	})

	summaries := make([]trainingJobSummary, 0, len(jobs))
	for _, tj := range jobs {
		summaries = append(summaries, trainingJobSummary{
			TrainingJobName:   tj.TrainingJobName,
			TrainingJobArn:    tj.TrainingJobArn,
			TrainingJobStatus: tj.TrainingJobStatus,
			CreationTime:      epochSeconds(tj.CreationTime),
			LastModifiedTime:  epochSeconds(tj.LastModifiedTime),
		})
	}

	resp := map[string]any{"TrainingJobSummaries": summaries}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return json.Marshal(resp)
}
