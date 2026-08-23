package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// ---------------------------------------------------------------------------
// Pipeline ops handlers (#29) — wire real backend to stub-bypassed ops
// ---------------------------------------------------------------------------

// pipelineDefinitionS3Location mirrors types.PipelineDefinitionS3Location
// (api_op_CreatePipeline.go:59, api_op_UpdatePipeline.go:43, types/types.go:17313,
// sagemaker@v1.263.2). CreatePipeline/UpdatePipeline fetch the real object
// through h.Backend's wired S3Accessor (see s3pipeline.go); an unreadable
// object (no S3 backend wired, missing bucket/key) fails with ErrValidation
// rather than fabricating a definition.
type pipelineDefinitionS3Location struct {
	Bucket    string `json:"Bucket"`
	ObjectKey string `json:"ObjectKey"`
	VersionID string `json:"VersionId,omitempty"`
}

// retryPipelineExecutionInput mirrors RetryPipelineExecutionInput
// (api_op_RetryPipelineExecution.go:29-45, sagemaker@v1.263.2).
// ClientRequestToken (required by AWS) is a pure idempotency token this
// single-process in-memory backend has no use for — omitted, matching this
// service's existing convention (no sagemaker op models it).
type retryPipelineExecutionInput struct {
	ParallelismConfiguration *ParallelismConfiguration `json:"ParallelismConfiguration,omitempty"`
	PipelineExecutionArn     string                    `json:"PipelineExecutionArn"`
}

func (h *Handler) handleRetryPipelineExecution(ctx context.Context, body []byte) ([]byte, error) {
	var req retryPipelineExecutionInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if req.PipelineExecutionArn == "" {
		return nil, fmt.Errorf("%w: PipelineExecutionArn is required", errInvalidRequest)
	}

	exec, err := h.Backend.RetryPipelineExecution(ctx, req.PipelineExecutionArn, req.ParallelismConfiguration)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "sagemaker: retried pipeline execution", "arn", exec.PipelineExecutionArn)

	return json.Marshal(map[string]string{keyPipelineExecutionArn: exec.PipelineExecutionArn})
}

// stopPipelineExecutionInput mirrors StopPipelineExecutionInput
// (api_op_StopPipelineExecution.go:29-38, sagemaker@v1.263.2). ClientRequestToken
// omitted for the same reason as retryPipelineExecutionInput.
type stopPipelineExecutionInput struct {
	PipelineExecutionArn string `json:"PipelineExecutionArn"`
}

func (h *Handler) handleStopPipelineExecution(ctx context.Context, body []byte) ([]byte, error) {
	var req stopPipelineExecutionInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if req.PipelineExecutionArn == "" {
		return nil, fmt.Errorf("%w: PipelineExecutionArn is required", errInvalidRequest)
	}

	exec, err := h.Backend.StopPipelineExecution(ctx, req.PipelineExecutionArn)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "sagemaker: stopped pipeline execution", "arn", exec.PipelineExecutionArn)

	return json.Marshal(map[string]string{keyPipelineExecutionArn: exec.PipelineExecutionArn})
}

// sendPipelineExecutionStepSuccessInput mirrors SendPipelineExecutionStepSuccessInput
// (api_op_SendPipelineExecutionStepSuccess.go:29-43, sagemaker@v1.263.2).
// AWS resolves the target step from CallbackToken alone; there is no
// PipelineExecutionArn or StepName field on the real wire shape at all (the
// previous version of this handler read both, which no real client ever
// sends). ClientRequestToken (idempotency-only) is omitted like the other
// pipeline ops above.
type sendPipelineExecutionStepSuccessInput struct {
	CallbackToken    string              `json:"CallbackToken"`
	OutputParameters []PipelineParameter `json:"OutputParameters,omitempty"`
}

func (h *Handler) handleSendPipelineExecutionStepSuccess(
	ctx context.Context,
	body []byte,
) ([]byte, error) {
	var req sendPipelineExecutionStepSuccessInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if req.CallbackToken == "" {
		return nil, fmt.Errorf("%w: CallbackToken is required", errInvalidRequest)
	}

	if err := h.Backend.SendPipelineExecutionStepSuccess(ctx, req.CallbackToken, req.OutputParameters); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "sagemaker: sent pipeline step success", "token", req.CallbackToken)

	// Per pipelineCallbackStepName's doc comment, CallbackToken is this
	// backend's execution-ARN convention, so it doubles as the real
	// response's PipelineExecutionArn (SendPipelineExecutionStepSuccessOutput,
	// api_op_SendPipelineExecutionStepSuccess.go:48-50).
	return json.Marshal(map[string]string{keyPipelineExecutionArn: req.CallbackToken})
}

// sendPipelineExecutionStepFailureInput mirrors SendPipelineExecutionStepFailureInput
// (api_op_SendPipelineExecutionStepFailure.go:29-42, sagemaker@v1.263.2) — see
// sendPipelineExecutionStepSuccessInput's doc comment for why PipelineExecutionArn
// and StepName are gone.
type sendPipelineExecutionStepFailureInput struct {
	CallbackToken string `json:"CallbackToken"`
	FailureReason string `json:"FailureReason,omitempty"`
}

// handleSendPipelineExecutionStepFailure handles the AWS callback token API.
func (h *Handler) handleSendPipelineExecutionStepFailure(
	ctx context.Context,
	body []byte,
) ([]byte, error) {
	var req sendPipelineExecutionStepFailureInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if req.CallbackToken == "" {
		return nil, fmt.Errorf("%w: CallbackToken is required", errInvalidRequest)
	}

	if err := h.Backend.SendPipelineExecutionStepFailure(ctx, req.CallbackToken, req.FailureReason); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "sagemaker: sent pipeline step failure", "token", req.CallbackToken)

	return json.Marshal(map[string]string{keyPipelineExecutionArn: req.CallbackToken})
}

// pipelineStepCallbackMetadata mirrors types.CallbackStepMetadata
// (types/types.go:3641, sagemaker@v1.263.2). SqsQueueUrl is not modeled: this
// backend never notifies a real SQS queue, so there is no URL to report.
type pipelineStepCallbackMetadata struct {
	CallbackToken    string              `json:"CallbackToken,omitempty"`
	OutputParameters []PipelineParameter `json:"OutputParameters,omitempty"`
}

// pipelineStepMetadata mirrors types.PipelineExecutionStepMetadata
// (types/types.go:17421, sagemaker@v1.263.2). Only the Callback member is
// modeled — this backend has no per-step-type job metadata for
// TrainingJob/ProcessingJob/etc. to report, since it doesn't run real steps.
type pipelineStepMetadata struct {
	Callback *pipelineStepCallbackMetadata `json:"Callback,omitempty"`
}

type pipelineExecStepSummary struct {
	Metadata      *pipelineStepMetadata `json:"Metadata,omitempty"`
	StepName      string                `json:"StepName"`
	StepType      string                `json:"StepType,omitempty"`
	StepStatus    string                `json:"StepStatus"`
	FailureReason string                `json:"FailureReason,omitempty"`
	StartTime     float64               `json:"StartTime,omitempty"`
	EndTime       float64               `json:"EndTime,omitempty"`
}

// listPipelineExecutionStepsInput mirrors ListPipelineExecutionStepsInput
// (api_op_ListPipelineExecutionSteps.go:29-43, sagemaker@v1.263.2).
type listPipelineExecutionStepsInput struct {
	PipelineExecutionArn string `json:"PipelineExecutionArn"`
	NextToken            string `json:"NextToken"`
	SortOrder            string `json:"SortOrder"`
	MaxResults           int32  `json:"MaxResults"`
}

func (h *Handler) handleListPipelineExecutionSteps(ctx context.Context, body []byte) ([]byte, error) {
	var req listPipelineExecutionStepsInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if req.PipelineExecutionArn == "" {
		return nil, fmt.Errorf("%w: PipelineExecutionArn is required", errInvalidRequest)
	}

	// Verify execution exists before listing steps.
	if _, err := h.Backend.DescribePipelineExecution(ctx, req.PipelineExecutionArn); err != nil {
		return nil, err
	}

	steps, nextToken := h.Backend.ListPipelineExecutionSteps(ctx, ListPipelineExecutionStepsParams{
		ExecutionArn: req.PipelineExecutionArn,
		NextToken:    req.NextToken,
		SortOrder:    req.SortOrder,
		MaxResults:   req.MaxResults,
	})
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
		if s.StepType == stepTypeCallback {
			sum.Metadata = &pipelineStepMetadata{Callback: &pipelineStepCallbackMetadata{
				CallbackToken:    s.CallbackToken,
				OutputParameters: s.OutputParameters,
			}}
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
// Extended CreatePipeline/UpdatePipeline/StartPipelineExecution handlers (gaps #23, #25)
// ---------------------------------------------------------------------------

// createPipelineInput mirrors CreatePipelineInput (api_op_CreatePipeline.go:
// 29-64, sagemaker@v1.263.2). ClientRequestToken omitted, see
// retryPipelineExecutionInput's doc comment.
type createPipelineInput struct {
	ParallelismConfiguration     *ParallelismConfiguration     `json:"ParallelismConfiguration,omitempty"`
	PipelineDefinitionS3Location *pipelineDefinitionS3Location `json:"PipelineDefinitionS3Location,omitempty"`
	PipelineName                 string                        `json:"PipelineName"`
	PipelineDefinition           string                        `json:"PipelineDefinition,omitempty"`
	PipelineDisplayName          string                        `json:"PipelineDisplayName,omitempty"`
	PipelineDescription          string                        `json:"PipelineDescription,omitempty"`
	RoleArn                      string                        `json:"RoleArn,omitempty"`
	Tags                         []tagObject                   `json:"Tags,omitempty"`
}

func (h *Handler) handleCreatePipelineFull(ctx context.Context, body []byte) ([]byte, error) {
	var req createPipelineInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.PipelineName == "" {
		return nil, fmt.Errorf("%w: PipelineName is required", errInvalidRequest)
	}

	definition := req.PipelineDefinition

	if loc := req.PipelineDefinitionS3Location; loc != nil {
		fetched, err := h.Backend.readPipelineDefinitionFromS3(ctx, loc.Bucket, loc.ObjectKey, loc.VersionID)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrValidation, err)
		}

		definition = fetched
	}

	p, err := h.Backend.CreatePipelineFull(ctx, CreatePipelineOptions{
		PipelineName:             req.PipelineName,
		PipelineDefinition:       definition,
		PipelineDisplayName:      req.PipelineDisplayName,
		PipelineDescription:      req.PipelineDescription,
		RoleArn:                  req.RoleArn,
		ParallelismConfiguration: req.ParallelismConfiguration,
		Tags:                     fromTagObjects(req.Tags),
	})
	if err != nil {
		return nil, err
	}

	logger.Load(ctx).InfoContext(ctx, "sagemaker: created pipeline (full)", "name", p.PipelineName)

	return json.Marshal(map[string]string{keyPipelineArn: p.PipelineArn})
}

// updatePipelineInput mirrors UpdatePipelineInput (api_op_UpdatePipeline.go:
// 29-52, sagemaker@v1.263.2).
type updatePipelineInput struct {
	ParallelismConfiguration     *ParallelismConfiguration     `json:"ParallelismConfiguration,omitempty"`
	PipelineDefinitionS3Location *pipelineDefinitionS3Location `json:"PipelineDefinitionS3Location,omitempty"`
	PipelineName                 string                        `json:"PipelineName"`
	PipelineDefinition           string                        `json:"PipelineDefinition,omitempty"`
	PipelineDisplayName          string                        `json:"PipelineDisplayName,omitempty"`
	PipelineDescription          string                        `json:"PipelineDescription,omitempty"`
	RoleArn                      string                        `json:"RoleArn,omitempty"`
}

func (h *Handler) handleUpdatePipelineFull(ctx context.Context, body []byte) ([]byte, error) {
	var req updatePipelineInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.PipelineName == "" {
		return nil, fmt.Errorf("%w: PipelineName is required", errInvalidRequest)
	}

	definition := req.PipelineDefinition

	if loc := req.PipelineDefinitionS3Location; loc != nil {
		fetched, err := h.Backend.readPipelineDefinitionFromS3(ctx, loc.Bucket, loc.ObjectKey, loc.VersionID)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrValidation, err)
		}

		definition = fetched
	}

	p, err := h.Backend.UpdatePipelineFull(
		ctx,
		req.PipelineName,
		definition,
		req.PipelineDisplayName,
		req.PipelineDescription,
		req.RoleArn,
		req.ParallelismConfiguration,
	)
	if err != nil {
		return nil, err
	}

	logger.Load(ctx).InfoContext(ctx, "sagemaker: updated pipeline (full)", "name", p.PipelineName)

	return json.Marshal(map[string]string{keyPipelineArn: p.PipelineArn})
}

// startPipelineExecutionInput mirrors StartPipelineExecutionInput
// (api_op_StartPipelineExecution.go:29-63, sagemaker@v1.263.2).
// ClientRequestToken omitted, see retryPipelineExecutionInput's doc comment.
// Field order and types must stay identical to StartPipelineExecutionOptions
// (pipelines.go) — handleStartPipelineExecutionFull converts req to it
// directly rather than copying field by field.
type startPipelineExecutionInput struct {
	ParallelismConfiguration     *ParallelismConfiguration `json:"ParallelismConfiguration,omitempty"`
	SelectiveExecutionConfig     *SelectiveExecutionConfig `json:"SelectiveExecutionConfig,omitempty"`
	PipelineName                 string                    `json:"PipelineName"`
	PipelineExecutionDisplayName string                    `json:"PipelineExecutionDisplayName,omitempty"`
	PipelineExecutionDescription string                    `json:"PipelineExecutionDescription,omitempty"`
	MlflowExperimentName         string                    `json:"MlflowExperimentName,omitempty"`
	PipelineParameters           []PipelineParameter       `json:"PipelineParameters,omitempty"`
	PipelineVersionID            int64                     `json:"PipelineVersionId,omitempty"`
}

func (h *Handler) handleStartPipelineExecutionFull(
	ctx context.Context,
	body []byte,
) ([]byte, error) {
	var req startPipelineExecutionInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.PipelineName == "" {
		return nil, fmt.Errorf("%w: PipelineName is required", errInvalidRequest)
	}

	pe, err := h.Backend.StartPipelineExecutionFull(ctx, StartPipelineExecutionOptions(req))
	if err != nil {
		return nil, err
	}

	logger.Load(ctx).InfoContext(
		ctx,
		"sagemaker: started pipeline execution (full)",
		"arn",
		pe.PipelineExecutionArn,
	)

	return json.Marshal(map[string]string{keyPipelineExecutionArn: pe.PipelineExecutionArn})
}

// ---------------------------------------------------------------------------
// ListPipelineParametersForExecution — gap #25
// ---------------------------------------------------------------------------

// listPipelineParametersForExecutionInput mirrors
// ListPipelineParametersForExecutionInput (api_op_ListPipelineParametersForExecution.go:
// 29-42, sagemaker@v1.263.2).
type listPipelineParametersForExecutionInput struct {
	PipelineExecutionArn string `json:"PipelineExecutionArn"`
	NextToken            string `json:"NextToken,omitempty"`
	MaxResults           int32  `json:"MaxResults,omitempty"`
}

func (h *Handler) handleListPipelineParametersForExecution(
	ctx context.Context,
	body []byte,
) ([]byte, error) {
	var req listPipelineParametersForExecutionInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.PipelineExecutionArn == "" {
		return nil, fmt.Errorf("%w: PipelineExecutionArn is required", errInvalidRequest)
	}

	pe, err := h.Backend.DescribePipelineExecution(ctx, req.PipelineExecutionArn)
	if err != nil {
		return nil, err
	}

	params := pe.PipelineParameters
	if params == nil {
		params = []PipelineParameter{}
	}

	page, nextToken := paginateSlice(params, req.NextToken, req.MaxResults)

	resp := map[string]any{"PipelineParameters": page}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return json.Marshal(resp)
}

// ---------------------------------------------------------------------------
// Pipeline handlers
// ---------------------------------------------------------------------------

// describePipelineInput mirrors DescribePipelineInput (api_op_DescribePipeline.go:
// 29-38, sagemaker@v1.263.2).
type describePipelineInput struct {
	PipelineName      string `json:"PipelineName"`
	PipelineVersionID int64  `json:"PipelineVersionId,omitempty"`
}

func (h *Handler) handleDescribePipeline(ctx context.Context, body []byte) ([]byte, error) {
	var req describePipelineInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.PipelineName == "" {
		return nil, fmt.Errorf("%w: PipelineName is required", errInvalidRequest)
	}

	p, lastRunTime, err := h.Backend.DescribePipeline(ctx, req.PipelineName, req.PipelineVersionID)
	if err != nil {
		return nil, err
	}

	resp := map[string]any{
		"PipelineName":        p.PipelineName,
		keyPipelineArn:        p.PipelineArn,
		"PipelineStatus":      p.PipelineStatus,
		keyPipelineDefinition: p.PipelineDefinition,
		keyRoleArn:            p.RoleArn,
		keyCreationTime:       epochSeconds(p.CreationTime),
		keyLastModifiedTime:   epochSeconds(p.LastModifiedTime),
	}
	if p.PipelineDisplayName != "" {
		resp["PipelineDisplayName"] = p.PipelineDisplayName
	}
	if p.PipelineDescription != "" {
		resp["PipelineDescription"] = p.PipelineDescription
	}
	if p.ParallelismConfiguration != nil {
		resp["ParallelismConfiguration"] = p.ParallelismConfiguration
	}
	if !lastRunTime.IsZero() {
		resp["LastRunTime"] = epochSeconds(lastRunTime)
	}

	return json.Marshal(resp)
}

type pipelineSummary struct {
	PipelineName        string  `json:"PipelineName"`
	PipelineArn         string  `json:"PipelineArn"`
	PipelineStatus      string  `json:"PipelineStatus"`
	PipelineDescription string  `json:"PipelineDescription,omitempty"`
	PipelineDisplayName string  `json:"PipelineDisplayName,omitempty"`
	RoleArn             string  `json:"RoleArn,omitempty"`
	CreationTime        float64 `json:"CreationTime"`
	LastModifiedTime    float64 `json:"LastModifiedTime"`
	LastExecutionTime   float64 `json:"LastExecutionTime,omitempty"`
}

// listPipelinesInput mirrors ListPipelinesInput (api_op_ListPipelines.go:
// 29-58, sagemaker@v1.263.2). Timestamps are epoch-seconds floats on the
// wire, like every other List* op in this service (epochSeconds/
// timeFromEpochSecondsPtr, handler.go).
type listPipelinesInput struct {
	CreatedAfter       *float64 `json:"CreatedAfter"`
	CreatedBefore      *float64 `json:"CreatedBefore"`
	NextToken          string   `json:"NextToken"`
	PipelineNamePrefix string   `json:"PipelineNamePrefix"`
	SortBy             string   `json:"SortBy"`
	SortOrder          string   `json:"SortOrder"`
	MaxResults         int32    `json:"MaxResults"`
}

func (h *Handler) handleListPipelines(ctx context.Context, body []byte) ([]byte, error) {
	var req listPipelinesInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	ps, nextToken := h.Backend.ListPipelines(ctx, ListPipelinesParams{
		CreatedAfter:       timeFromEpochSecondsPtr(req.CreatedAfter),
		CreatedBefore:      timeFromEpochSecondsPtr(req.CreatedBefore),
		PipelineNamePrefix: req.PipelineNamePrefix,
		NextToken:          req.NextToken,
		SortBy:             req.SortBy,
		SortOrder:          req.SortOrder,
		MaxResults:         req.MaxResults,
	})
	summaries := make([]pipelineSummary, 0, len(ps))

	for _, p := range ps {
		sum := pipelineSummary{
			PipelineName:        p.PipelineName,
			PipelineArn:         p.PipelineArn,
			PipelineStatus:      p.PipelineStatus,
			PipelineDescription: p.PipelineDescription,
			PipelineDisplayName: p.PipelineDisplayName,
			RoleArn:             p.RoleArn,
			CreationTime:        epochSeconds(p.CreationTime),
			LastModifiedTime:    epochSeconds(p.LastModifiedTime),
		}

		if last := h.Backend.PipelineLastExecutionTime(ctx, p.PipelineArn); !last.IsZero() {
			sum.LastExecutionTime = epochSeconds(last)
		}

		summaries = append(summaries, sum)
	}

	resp := map[string]any{"PipelineSummaries": summaries}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return json.Marshal(resp)
}

// deletePipelineInput mirrors DeletePipelineInput (api_op_DeletePipeline.go:
// 29-38, sagemaker@v1.263.2). ClientRequestToken omitted, see
// retryPipelineExecutionInput's doc comment.
type deletePipelineInput struct {
	PipelineName string `json:"PipelineName"`
}

func (h *Handler) handleDeletePipeline(ctx context.Context, body []byte) ([]byte, error) {
	var req deletePipelineInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.PipelineName == "" {
		return nil, fmt.Errorf("%w: PipelineName is required", errInvalidRequest)
	}

	p, err := h.Backend.DeletePipeline(ctx, req.PipelineName)
	if err != nil {
		return nil, err
	}

	logger.Load(ctx).InfoContext(ctx, "sagemaker: deleted pipeline", "name", req.PipelineName)

	return json.Marshal(map[string]string{keyPipelineArn: p.PipelineArn})
}

// describePipelineExecutionInput mirrors DescribePipelineExecutionInput
// (api_op_DescribePipelineExecution.go:29-35, sagemaker@v1.263.2).
type describePipelineExecutionInput struct {
	PipelineExecutionArn string `json:"PipelineExecutionArn"`
}

func (h *Handler) handleDescribePipelineExecution(ctx context.Context, body []byte) ([]byte, error) {
	var req describePipelineExecutionInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.PipelineExecutionArn == "" {
		return nil, fmt.Errorf("%w: PipelineExecutionArn is required", errInvalidRequest)
	}

	pe, err := h.Backend.DescribePipelineExecution(ctx, req.PipelineExecutionArn)
	if err != nil {
		return nil, err
	}

	resp := map[string]any{
		keyPipelineArn:             pe.PipelineArn,
		keyPipelineExecutionArn:    pe.PipelineExecutionArn,
		keyPipelineExecutionStatus: pe.PipelineExecutionStatus,
		"StartTime":                epochSeconds(pe.StartTime),
	}
	if pe.PipelineExecutionDisplayName != "" {
		resp["PipelineExecutionDisplayName"] = pe.PipelineExecutionDisplayName
	}
	if pe.PipelineExecutionDescription != "" {
		resp["PipelineExecutionDescription"] = pe.PipelineExecutionDescription
	}
	if len(pe.PipelineParameters) > 0 {
		resp["PipelineParameters"] = pe.PipelineParameters
	}
	if pe.FailureReason != "" {
		resp["FailureReason"] = pe.FailureReason
	}
	if pe.ParallelismConfiguration != nil {
		resp["ParallelismConfiguration"] = pe.ParallelismConfiguration
	}
	if pe.PipelineVersionID != 0 {
		resp["PipelineVersionId"] = pe.PipelineVersionID
	}
	if pe.SelectiveExecutionConfig != nil {
		resp["SelectiveExecutionConfig"] = pe.SelectiveExecutionConfig
	}
	if pe.MlflowExperimentName != "" {
		resp["MLflowConfig"] = map[string]string{"MlflowExperimentName": pe.MlflowExperimentName}
	}

	return json.Marshal(resp)
}

type pipelineExecutionSummary struct {
	PipelineExecutionArn           string  `json:"PipelineExecutionArn"`
	PipelineExecutionDisplayName   string  `json:"PipelineExecutionDisplayName,omitempty"`
	PipelineExecutionDescription   string  `json:"PipelineExecutionDescription,omitempty"`
	PipelineExecutionFailureReason string  `json:"PipelineExecutionFailureReason,omitempty"`
	PipelineExecutionStatus        string  `json:"PipelineExecutionStatus"`
	StartTime                      float64 `json:"StartTime"`
}

// listPipelineExecutionsInput mirrors ListPipelineExecutionsInput
// (api_op_ListPipelineExecutions.go:29-62, sagemaker@v1.263.2).
type listPipelineExecutionsInput struct {
	CreatedAfter  *float64 `json:"CreatedAfter"`
	CreatedBefore *float64 `json:"CreatedBefore"`
	PipelineName  string   `json:"PipelineName"`
	NextToken     string   `json:"NextToken"`
	SortBy        string   `json:"SortBy"`
	SortOrder     string   `json:"SortOrder"`
	MaxResults    int32    `json:"MaxResults"`
}

func (h *Handler) handleListPipelineExecutions(ctx context.Context, body []byte) ([]byte, error) {
	var req listPipelineExecutionsInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.PipelineName == "" {
		return nil, fmt.Errorf("%w: PipelineName is required", errInvalidRequest)
	}

	pes, nextToken := h.Backend.ListPipelineExecutions(ctx, ListPipelineExecutionsParams{
		CreatedAfter:  timeFromEpochSecondsPtr(req.CreatedAfter),
		CreatedBefore: timeFromEpochSecondsPtr(req.CreatedBefore),
		PipelineName:  req.PipelineName,
		NextToken:     req.NextToken,
		SortBy:        req.SortBy,
		SortOrder:     req.SortOrder,
		MaxResults:    req.MaxResults,
	})
	summaries := make([]pipelineExecutionSummary, 0, len(pes))

	for _, pe := range pes {
		summaries = append(summaries, pipelineExecutionSummary{
			PipelineExecutionArn:           pe.PipelineExecutionArn,
			PipelineExecutionDisplayName:   pe.PipelineExecutionDisplayName,
			PipelineExecutionDescription:   pe.PipelineExecutionDescription,
			PipelineExecutionFailureReason: pe.FailureReason,
			PipelineExecutionStatus:        pe.PipelineExecutionStatus,
			StartTime:                      epochSeconds(pe.StartTime),
		})
	}

	resp := map[string]any{"PipelineExecutionSummaries": summaries}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return json.Marshal(resp)
}
