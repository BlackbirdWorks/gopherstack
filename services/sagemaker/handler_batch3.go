package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

const (
	keyEndpointNameField = "EndpointName"
	keyTagsField         = "Tags"
)

// batch3SupportedOperations returns the 50 real stateful operations implemented in batch 3.
func batch3SupportedOperations() []string {
	return []string{
		// DataQuality job definitions
		"CreateDataQualityJobDefinition",
		"DescribeDataQualityJobDefinition",
		"DeleteDataQualityJobDefinition",
		"ListDataQualityJobDefinitions",
		// ModelBias job definitions
		"CreateModelBiasJobDefinition",
		"DescribeModelBiasJobDefinition",
		"DeleteModelBiasJobDefinition",
		"ListModelBiasJobDefinitions",
		// ModelQuality job definitions
		"CreateModelQualityJobDefinition",
		"DescribeModelQualityJobDefinition",
		"DeleteModelQualityJobDefinition",
		"ListModelQualityJobDefinitions",
		// ModelExplainability job definitions
		"CreateModelExplainabilityJobDefinition",
		"DescribeModelExplainabilityJobDefinition",
		"DeleteModelExplainabilityJobDefinition",
		"ListModelExplainabilityJobDefinitions",
		// MonitoringAlert / MonitoringExecution
		"ListMonitoringAlerts",
		"ListMonitoringAlertHistory",
		"UpdateMonitoringAlert",
		"ListMonitoringExecutions",
		// HumanTaskUI
		"CreateHumanTaskUi",
		"DescribeHumanTaskUi",
		"DeleteHumanTaskUi",
		// Workforce
		"CreateWorkforce",
		"DescribeWorkforce",
		"UpdateWorkforce",
		"DeleteWorkforce",
		"ListWorkforces",
		// FlowDefinition
		"CreateFlowDefinition",
		"DescribeFlowDefinition",
		"DeleteFlowDefinition",
		// AppImageConfig
		"CreateAppImageConfig",
		"DescribeAppImageConfig",
		"DeleteAppImageConfig",
		"UpdateAppImageConfig",
		// InferenceExperiment
		"CreateInferenceExperiment",
		"DescribeInferenceExperiment",
		"StopInferenceExperiment",
		"DeleteInferenceExperiment",
		// MlflowTrackingServer
		"CreateMlflowTrackingServer",
		"DescribeMlflowTrackingServer",
		"DeleteMlflowTrackingServer",
		"StartMlflowTrackingServer",
		"StopMlflowTrackingServer",
		// ModelCard
		"CreateModelCard",
		"DescribeModelCard",
		"UpdateModelCard",
		"DeleteModelCard",
		// OptimizationJob
		"CreateOptimizationJob",
		"DescribeOptimizationJob",
		"DeleteOptimizationJob",
		"StopOptimizationJob",
		// StudioLifecycleConfig
		"CreateStudioLifecycleConfig",
		"DescribeStudioLifecycleConfig",
		"DeleteStudioLifecycleConfig",
		// PartnerApp
		"CreatePartnerApp",
		"DescribePartnerApp",
		"DeletePartnerApp",
		// TrainingPlan
		"CreateTrainingPlan",
		"DescribeTrainingPlan",
	}
}

// dispatchBatch3Ops dispatches the 50 real stateful operations (batch 3).
//
//nolint:cyclop,gocyclo,funlen // large switch is required for dispatching many operations
func (h *Handler) dispatchBatch3Ops(
	ctx context.Context,
	op string,
	body []byte,
) ([]byte, bool, error) {
	switch op {
	// DataQualityJobDefinition
	case "CreateDataQualityJobDefinition":
		r, err := h.handleCreateDataQualityJobDefinition(ctx, body)

		return r, true, err
	case "DescribeDataQualityJobDefinition":
		r, err := h.handleDescribeDataQualityJobDefinition(ctx, body)

		return r, true, err
	case "DeleteDataQualityJobDefinition":
		return nil, true, h.handleDeleteDataQualityJobDefinition(ctx, body)
	case "ListDataQualityJobDefinitions":
		r, err := h.handleListDataQualityJobDefinitions(ctx, body)

		return r, true, err

	// ModelBiasJobDefinition
	case "CreateModelBiasJobDefinition":
		r, err := h.handleCreateModelBiasJobDefinition(ctx, body)

		return r, true, err
	case "DescribeModelBiasJobDefinition":
		r, err := h.handleDescribeModelBiasJobDefinition(ctx, body)

		return r, true, err
	case "DeleteModelBiasJobDefinition":
		return nil, true, h.handleDeleteModelBiasJobDefinition(ctx, body)
	case "ListModelBiasJobDefinitions":
		r, err := h.handleListModelBiasJobDefinitions(ctx, body)

		return r, true, err

	// ModelQualityJobDefinition
	case "CreateModelQualityJobDefinition":
		r, err := h.handleCreateModelQualityJobDefinition(ctx, body)

		return r, true, err
	case "DescribeModelQualityJobDefinition":
		r, err := h.handleDescribeModelQualityJobDefinition(ctx, body)

		return r, true, err
	case "DeleteModelQualityJobDefinition":
		return nil, true, h.handleDeleteModelQualityJobDefinition(ctx, body)
	case "ListModelQualityJobDefinitions":
		r, err := h.handleListModelQualityJobDefinitions(ctx, body)

		return r, true, err

	// ModelExplainabilityJobDefinition
	case "CreateModelExplainabilityJobDefinition":
		r, err := h.handleCreateModelExplainabilityJobDefinition(ctx, body)

		return r, true, err
	case "DescribeModelExplainabilityJobDefinition":
		r, err := h.handleDescribeModelExplainabilityJobDefinition(ctx, body)

		return r, true, err
	case "DeleteModelExplainabilityJobDefinition":
		return nil, true, h.handleDeleteModelExplainabilityJobDefinition(ctx, body)
	case "ListModelExplainabilityJobDefinitions":
		r, err := h.handleListModelExplainabilityJobDefinitions(ctx, body)

		return r, true, err

	// MonitoringAlert / MonitoringExecution
	case "ListMonitoringAlerts":
		r, err := h.handleListMonitoringAlerts(ctx, body)

		return r, true, err
	case "ListMonitoringAlertHistory":
		r, err := h.handleListMonitoringAlertHistory(ctx, body)

		return r, true, err
	case "UpdateMonitoringAlert":
		r, err := h.handleUpdateMonitoringAlert(ctx, body)

		return r, true, err
	case "ListMonitoringExecutions":
		r, err := h.handleListMonitoringExecutions(ctx, body)

		return r, true, err

	// HumanTaskUI
	case "CreateHumanTaskUi":
		r, err := h.handleCreateHumanTaskUI(ctx, body)

		return r, true, err
	case "DescribeHumanTaskUi":
		r, err := h.handleDescribeHumanTaskUI(ctx, body)

		return r, true, err
	case "DeleteHumanTaskUi":
		return nil, true, h.handleDeleteHumanTaskUI(ctx, body)

	// Workforce
	case "CreateWorkforce":
		r, err := h.handleCreateWorkforce(ctx, body)

		return r, true, err
	case "DescribeWorkforce":
		r, err := h.handleDescribeWorkforce(ctx, body)

		return r, true, err
	case "UpdateWorkforce":
		r, err := h.handleUpdateWorkforce(ctx, body)

		return r, true, err
	case "DeleteWorkforce":
		r, err := h.handleDeleteWorkforce(ctx, body)

		return r, true, err
	case "ListWorkforces":
		r, err := h.handleListWorkforces(ctx, body)

		return r, true, err

	// FlowDefinition
	case "CreateFlowDefinition":
		r, err := h.handleCreateFlowDefinition(ctx, body)

		return r, true, err
	case "DescribeFlowDefinition":
		r, err := h.handleDescribeFlowDefinition(ctx, body)

		return r, true, err
	case "DeleteFlowDefinition":
		return nil, true, h.handleDeleteFlowDefinition(ctx, body)

	// AppImageConfig
	case "CreateAppImageConfig":
		r, err := h.handleCreateAppImageConfig(ctx, body)

		return r, true, err
	case "DescribeAppImageConfig":
		r, err := h.handleDescribeAppImageConfig(ctx, body)

		return r, true, err
	case "DeleteAppImageConfig":
		return nil, true, h.handleDeleteAppImageConfig(ctx, body)
	case "UpdateAppImageConfig":
		r, err := h.handleUpdateAppImageConfig(ctx, body)

		return r, true, err

	// InferenceExperiment
	case "CreateInferenceExperiment":
		r, err := h.handleCreateInferenceExperiment(ctx, body)

		return r, true, err
	case "DescribeInferenceExperiment":
		r, err := h.handleDescribeInferenceExperiment(ctx, body)

		return r, true, err
	case "StopInferenceExperiment":
		return nil, true, h.handleStopInferenceExperiment(ctx, body)
	case "DeleteInferenceExperiment":
		return nil, true, h.handleDeleteInferenceExperiment(ctx, body)

	// MlflowTrackingServer
	case "CreateMlflowTrackingServer":
		r, err := h.handleCreateMlflowTrackingServer(ctx, body)

		return r, true, err
	case "DescribeMlflowTrackingServer":
		r, err := h.handleDescribeMlflowTrackingServer(ctx, body)

		return r, true, err
	case "DeleteMlflowTrackingServer":
		return nil, true, h.handleDeleteMlflowTrackingServer(ctx, body)
	case "StartMlflowTrackingServer":
		return nil, true, h.handleStartMlflowTrackingServer(ctx, body)
	case "StopMlflowTrackingServer":
		return nil, true, h.handleStopMlflowTrackingServer(ctx, body)

	// ModelCard
	case "CreateModelCard":
		r, err := h.handleCreateModelCard(ctx, body)

		return r, true, err
	case "DescribeModelCard":
		r, err := h.handleDescribeModelCard(ctx, body)

		return r, true, err
	case "UpdateModelCard":
		r, err := h.handleUpdateModelCard(ctx, body)

		return r, true, err
	case "DeleteModelCard":
		return nil, true, h.handleDeleteModelCard(ctx, body)

	// OptimizationJob
	case "CreateOptimizationJob":
		r, err := h.handleCreateOptimizationJob(ctx, body)

		return r, true, err
	case "DescribeOptimizationJob":
		r, err := h.handleDescribeOptimizationJob(ctx, body)

		return r, true, err
	case "DeleteOptimizationJob":
		return nil, true, h.handleDeleteOptimizationJob(ctx, body)
	case "StopOptimizationJob":
		return nil, true, h.handleStopOptimizationJob(ctx, body)

	// StudioLifecycleConfig
	case "CreateStudioLifecycleConfig":
		r, err := h.handleCreateStudioLifecycleConfig(ctx, body)

		return r, true, err
	case "DescribeStudioLifecycleConfig":
		r, err := h.handleDescribeStudioLifecycleConfig(ctx, body)

		return r, true, err
	case "DeleteStudioLifecycleConfig":
		return nil, true, h.handleDeleteStudioLifecycleConfig(ctx, body)

	// PartnerApp
	case "CreatePartnerApp":
		r, err := h.handleCreatePartnerApp(ctx, body)

		return r, true, err
	case "DescribePartnerApp":
		r, err := h.handleDescribePartnerApp(ctx, body)

		return r, true, err
	case "DeletePartnerApp":
		return nil, true, h.handleDeletePartnerApp(ctx, body)

	// TrainingPlan
	case "CreateTrainingPlan":
		r, err := h.handleCreateTrainingPlan(ctx, body)

		return r, true, err
	case "DescribeTrainingPlan":
		r, err := h.handleDescribeTrainingPlan(ctx, body)

		return r, true, err
	}

	return nil, false, nil
}

// ---------------------------------------------------------------------------
// Shared JobDefinition request/response helpers
//
// The four Model Monitor job definition types (DataQuality, ModelBias,
// ModelQuality, ModelExplainability) all share the same wire shape modulo
// which field names carry their AppSpecification/JobInput/JobOutputConfig —
// e.g. "DataQualityJobInput" vs "ModelBiasJobInput". NOTE: despite the
// per-type request field names AWS uses elsewhere in these APIs, the actual
// name/identifier field on Create/Describe/Delete is always the bare
// "JobDefinitionName" (confirmed against aws-sdk-go-v2's sagemaker
// serializers), not e.g. "DataQualityJobDefinitionName".
// ---------------------------------------------------------------------------

// jobDefRequest is the parsed representation of a Create*JobDefinition
// request body: JobDefinitionName/RoleArn/Tags are extracted for validation
// and storage; everything else (the differently-named AppSpecification/
// JobInput/JobOutputConfig blocks plus the shared JobResources/NetworkConfig/
// StoppingCondition/BaselineConfig) is kept verbatim in Config.
type jobDefRequest struct {
	Config            map[string]json.RawMessage
	Tags              map[string]string
	JobDefinitionName string
	RoleArn           string
	EndpointName      string
}

// parseJobDefRequest decodes a Create*JobDefinition body. jobInputKey is the
// wire field name of the type's JobInput block (e.g. "DataQualityJobInput"),
// used to derive EndpointName for List filtering/summaries.
func parseJobDefRequest(body []byte, jobInputKey string) (jobDefRequest, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		return jobDefRequest{}, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	req := jobDefRequest{Config: make(map[string]json.RawMessage, len(raw))}

	for k, v := range raw {
		switch k {
		case keyJobDefinitionName:
			if err := json.Unmarshal(v, &req.JobDefinitionName); err != nil {
				return jobDefRequest{}, fmt.Errorf("%w: %w", errInvalidRequest, err)
			}
		case "RoleArn":
			if err := json.Unmarshal(v, &req.RoleArn); err != nil {
				return jobDefRequest{}, fmt.Errorf("%w: %w", errInvalidRequest, err)
			}
		case keyTagsField:
			if err := json.Unmarshal(v, &req.Tags); err != nil {
				return jobDefRequest{}, fmt.Errorf("%w: %w", errInvalidRequest, err)
			}
		default:
			req.Config[k] = v
		}
	}

	if req.JobDefinitionName == "" {
		return jobDefRequest{}, fmt.Errorf("%w: JobDefinitionName is required", errInvalidRequest)
	}

	if jobInput, ok := req.Config[jobInputKey]; ok {
		req.EndpointName = extractEndpointName(jobInput)
	}

	return req, nil
}

// extractEndpointName pulls EndpointInput.EndpointName out of a job input
// block (DataQualityJobInput / ModelBiasJobInput / ModelQualityJobInput /
// ModelExplainabilityJobInput all share this shape).
func extractEndpointName(jobInput json.RawMessage) string {
	var in struct {
		EndpointInput *struct {
			EndpointName string `json:"EndpointName"`
		} `json:"EndpointInput"`
	}

	if err := json.Unmarshal(jobInput, &in); err != nil || in.EndpointInput == nil {
		return ""
	}

	return in.EndpointInput.EndpointName
}

// parseJobDefinitionName decodes the {"JobDefinitionName": "..."} body shared
// by Describe*JobDefinition and Delete*JobDefinition.
func parseJobDefinitionName(body []byte) (string, error) {
	var req struct {
		JobDefinitionName string `json:"JobDefinitionName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return "", fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.JobDefinitionName == "" {
		return "", fmt.Errorf("%w: JobDefinitionName is required", errInvalidRequest)
	}

	return req.JobDefinitionName, nil
}

// jobDefResponseCommonFieldCount is the number of fields buildJobDefinitionResponse
// adds on top of the type-specific Config blocks (JobDefinitionName, JobDefinitionArn,
// RoleArn, CreationTime).
const jobDefResponseCommonFieldCount = 4

// buildJobDefinitionResponse renders a Describe*JobDefinition response: the
// type-specific Config blocks verbatim, plus the fields common to all four
// types. Real AWS Describe*JobDefinition outputs do not include Tags.
func buildJobDefinitionResponse(j *JobDefinition) map[string]any {
	resp := make(map[string]any, len(j.Config)+jobDefResponseCommonFieldCount)
	for k, v := range j.Config {
		resp[k] = v
	}

	resp[keyJobDefinitionName] = j.JobDefinitionName
	resp[keyJobDefinitionArn] = j.JobDefinitionArn
	resp["RoleArn"] = j.RoleArn
	resp[keyCreationTime] = epochSeconds(j.CreationTime)

	return resp
}

// jobDefListRequest is the parsed representation of a List*JobDefinitions
// request body, shared by all four job definition types.
type jobDefListRequest struct {
	NextToken string
	Filter    JobDefinitionFilter
}

func parseJobDefinitionListRequest(body []byte) (jobDefListRequest, error) {
	var req struct {
		CreationTimeAfter  *float64 `json:"CreationTimeAfter,omitempty"`
		CreationTimeBefore *float64 `json:"CreationTimeBefore,omitempty"`
		EndpointName       string   `json:"EndpointName,omitempty"`
		NameContains       string   `json:"NameContains,omitempty"`
		NextToken          string   `json:"NextToken,omitempty"`
		SortBy             string   `json:"SortBy,omitempty"`
		SortOrder          string   `json:"SortOrder,omitempty"`
		MaxResults         int32    `json:"MaxResults,omitempty"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return jobDefListRequest{}, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	return jobDefListRequest{
		NextToken: req.NextToken,
		Filter: JobDefinitionFilter{
			CreationTimeAfter:  epochPtr(req.CreationTimeAfter),
			CreationTimeBefore: epochPtr(req.CreationTimeBefore),
			EndpointName:       req.EndpointName,
			NameContains:       req.NameContains,
			SortBy:             req.SortBy,
			SortOrder:          req.SortOrder,
			MaxResults:         req.MaxResults,
		},
	}, nil
}

// buildJobDefinitionListResponse renders a List*JobDefinitions response.
func buildJobDefinitionListResponse(items []*JobDefinition, next string) map[string]any {
	summaries := make([]map[string]any, 0, len(items))
	for _, j := range items {
		summaries = append(summaries, map[string]any{
			"MonitoringJobDefinitionName": j.JobDefinitionName,
			"MonitoringJobDefinitionArn":  j.JobDefinitionArn,
			keyEndpointNameField:          j.EndpointName,
			keyCreationTime:               epochSeconds(j.CreationTime),
		})
	}

	resp := map[string]any{"JobDefinitionSummaries": summaries}
	if next != "" {
		resp[keyNextToken] = next
	}

	return resp
}

// epochPtr converts an optional epoch-seconds JSON number into a *time.Time,
// as required by filters like CreationTimeAfter/CreationTimeBefore.
func epochPtr(f *float64) *time.Time {
	if f == nil {
		return nil
	}

	t := time.Unix(int64(*f), 0)

	return &t
}

// ---------------------------------------------------------------------------
// DataQualityJobDefinition handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateDataQualityJobDefinition(ctx context.Context, body []byte) ([]byte, error) {
	req, err := parseJobDefRequest(body, "DataQualityJobInput")
	if err != nil {
		return nil, err
	}

	result, err := h.Backend.CreateDataQualityJobDefinition(
		ctx, req.JobDefinitionName, req.RoleArn, req.EndpointName, req.Config, req.Tags,
	)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyJobDefinitionArn: result.JobDefinitionArn})
}

func (h *Handler) handleDescribeDataQualityJobDefinition(ctx context.Context, body []byte) ([]byte, error) {
	name, err := parseJobDefinitionName(body)
	if err != nil {
		return nil, err
	}

	result, err := h.Backend.DescribeDataQualityJobDefinition(ctx, name)
	if err != nil {
		return nil, err
	}

	return json.Marshal(buildJobDefinitionResponse(result))
}

func (h *Handler) handleDeleteDataQualityJobDefinition(ctx context.Context, body []byte) error {
	name, err := parseJobDefinitionName(body)
	if err != nil {
		return err
	}

	return h.Backend.DeleteDataQualityJobDefinition(ctx, name)
}

func (h *Handler) handleListDataQualityJobDefinitions(ctx context.Context, body []byte) ([]byte, error) {
	req, err := parseJobDefinitionListRequest(body)
	if err != nil {
		return nil, err
	}

	items, next := h.Backend.ListDataQualityJobDefinitions(ctx, req.NextToken, req.Filter)

	return json.Marshal(buildJobDefinitionListResponse(items, next))
}

// ---------------------------------------------------------------------------
// ModelBiasJobDefinition handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateModelBiasJobDefinition(ctx context.Context, body []byte) ([]byte, error) {
	req, err := parseJobDefRequest(body, "ModelBiasJobInput")
	if err != nil {
		return nil, err
	}

	result, err := h.Backend.CreateModelBiasJobDefinition(
		ctx, req.JobDefinitionName, req.RoleArn, req.EndpointName, req.Config, req.Tags,
	)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyJobDefinitionArn: result.JobDefinitionArn})
}

func (h *Handler) handleDescribeModelBiasJobDefinition(ctx context.Context, body []byte) ([]byte, error) {
	name, err := parseJobDefinitionName(body)
	if err != nil {
		return nil, err
	}

	result, err := h.Backend.DescribeModelBiasJobDefinition(ctx, name)
	if err != nil {
		return nil, err
	}

	return json.Marshal(buildJobDefinitionResponse(result))
}

func (h *Handler) handleDeleteModelBiasJobDefinition(ctx context.Context, body []byte) error {
	name, err := parseJobDefinitionName(body)
	if err != nil {
		return err
	}

	return h.Backend.DeleteModelBiasJobDefinition(ctx, name)
}

func (h *Handler) handleListModelBiasJobDefinitions(ctx context.Context, body []byte) ([]byte, error) {
	req, err := parseJobDefinitionListRequest(body)
	if err != nil {
		return nil, err
	}

	items, next := h.Backend.ListModelBiasJobDefinitions(ctx, req.NextToken, req.Filter)

	return json.Marshal(buildJobDefinitionListResponse(items, next))
}

// ---------------------------------------------------------------------------
// ModelQualityJobDefinition handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateModelQualityJobDefinition(ctx context.Context, body []byte) ([]byte, error) {
	req, err := parseJobDefRequest(body, "ModelQualityJobInput")
	if err != nil {
		return nil, err
	}

	result, err := h.Backend.CreateModelQualityJobDefinition(
		ctx, req.JobDefinitionName, req.RoleArn, req.EndpointName, req.Config, req.Tags,
	)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyJobDefinitionArn: result.JobDefinitionArn})
}

func (h *Handler) handleDescribeModelQualityJobDefinition(ctx context.Context, body []byte) ([]byte, error) {
	name, err := parseJobDefinitionName(body)
	if err != nil {
		return nil, err
	}

	result, err := h.Backend.DescribeModelQualityJobDefinition(ctx, name)
	if err != nil {
		return nil, err
	}

	return json.Marshal(buildJobDefinitionResponse(result))
}

func (h *Handler) handleDeleteModelQualityJobDefinition(ctx context.Context, body []byte) error {
	name, err := parseJobDefinitionName(body)
	if err != nil {
		return err
	}

	return h.Backend.DeleteModelQualityJobDefinition(ctx, name)
}

func (h *Handler) handleListModelQualityJobDefinitions(ctx context.Context, body []byte) ([]byte, error) {
	req, err := parseJobDefinitionListRequest(body)
	if err != nil {
		return nil, err
	}

	items, next := h.Backend.ListModelQualityJobDefinitions(ctx, req.NextToken, req.Filter)

	return json.Marshal(buildJobDefinitionListResponse(items, next))
}

// ---------------------------------------------------------------------------
// ModelExplainabilityJobDefinition handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateModelExplainabilityJobDefinition(ctx context.Context, body []byte) ([]byte, error) {
	req, err := parseJobDefRequest(body, "ModelExplainabilityJobInput")
	if err != nil {
		return nil, err
	}

	result, err := h.Backend.CreateModelExplainabilityJobDefinition(
		ctx, req.JobDefinitionName, req.RoleArn, req.EndpointName, req.Config, req.Tags,
	)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyJobDefinitionArn: result.JobDefinitionArn})
}

func (h *Handler) handleDescribeModelExplainabilityJobDefinition(ctx context.Context, body []byte) ([]byte, error) {
	name, err := parseJobDefinitionName(body)
	if err != nil {
		return nil, err
	}

	result, err := h.Backend.DescribeModelExplainabilityJobDefinition(ctx, name)
	if err != nil {
		return nil, err
	}

	return json.Marshal(buildJobDefinitionResponse(result))
}

func (h *Handler) handleDeleteModelExplainabilityJobDefinition(ctx context.Context, body []byte) error {
	name, err := parseJobDefinitionName(body)
	if err != nil {
		return err
	}

	return h.Backend.DeleteModelExplainabilityJobDefinition(ctx, name)
}

func (h *Handler) handleListModelExplainabilityJobDefinitions(ctx context.Context, body []byte) ([]byte, error) {
	req, err := parseJobDefinitionListRequest(body)
	if err != nil {
		return nil, err
	}

	items, next := h.Backend.ListModelExplainabilityJobDefinitions(ctx, req.NextToken, req.Filter)

	return json.Marshal(buildJobDefinitionListResponse(items, next))
}

// ---------------------------------------------------------------------------
// MonitoringAlert / MonitoringExecution handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleUpdateMonitoringAlert(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		MonitoringScheduleName string `json:"MonitoringScheduleName"`
		MonitoringAlertName    string `json:"MonitoringAlertName"`
		DatapointsToAlert      int32  `json:"DatapointsToAlert"`
		EvaluationPeriod       int32  `json:"EvaluationPeriod"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.MonitoringScheduleName == "" {
		return nil, fmt.Errorf("%w: MonitoringScheduleName is required", errInvalidRequest)
	}

	if req.MonitoringAlertName == "" {
		return nil, fmt.Errorf("%w: MonitoringAlertName is required", errInvalidRequest)
	}

	alert, scheduleArn, err := h.Backend.UpdateMonitoringAlert(
		ctx, req.MonitoringScheduleName, req.MonitoringAlertName, req.DatapointsToAlert, req.EvaluationPeriod,
	)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		keyMonitoringScheduleArn: scheduleArn,
		keyMonitoringAlertName:   alert.MonitoringAlertName,
	})
}

func (h *Handler) handleListMonitoringAlerts(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		MonitoringScheduleName string `json:"MonitoringScheduleName"`
		NextToken              string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.MonitoringScheduleName == "" {
		return nil, fmt.Errorf("%w: MonitoringScheduleName is required", errInvalidRequest)
	}

	items, next, err := h.Backend.ListMonitoringAlerts(ctx, req.MonitoringScheduleName, req.NextToken)
	if err != nil {
		return nil, err
	}

	summaries := make([]map[string]any, 0, len(items))
	for _, a := range items {
		summaries = append(summaries, map[string]any{
			keyMonitoringAlertName: a.MonitoringAlertName,
			"AlertStatus":          a.AlertStatus,
			"DatapointsToAlert":    a.DatapointsToAlert,
			"EvaluationPeriod":     a.EvaluationPeriod,
			keyCreationTime:        epochSeconds(a.CreationTime),
			keyLastModifiedTime:    epochSeconds(a.LastModifiedTime),
			"Actions": map[string]any{
				"ModelDashboardIndicator": map[string]any{"Enabled": a.DashboardIndicatorEnabled},
			},
		})
	}

	resp := map[string]any{"MonitoringAlertSummaries": summaries}
	if next != "" {
		resp[keyNextToken] = next
	}

	return json.Marshal(resp)
}

func (h *Handler) handleListMonitoringAlertHistory(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		CreationTimeAfter      *float64 `json:"CreationTimeAfter,omitempty"`
		CreationTimeBefore     *float64 `json:"CreationTimeBefore,omitempty"`
		MonitoringScheduleName string   `json:"MonitoringScheduleName,omitempty"`
		MonitoringAlertName    string   `json:"MonitoringAlertName,omitempty"`
		StatusEquals           string   `json:"StatusEquals,omitempty"`
		SortOrder              string   `json:"SortOrder,omitempty"`
		NextToken              string   `json:"NextToken,omitempty"`
		MaxResults             int32    `json:"MaxResults,omitempty"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	f := MonitoringAlertHistoryFilter{
		CreationTimeAfter:      epochPtr(req.CreationTimeAfter),
		CreationTimeBefore:     epochPtr(req.CreationTimeBefore),
		MonitoringScheduleName: req.MonitoringScheduleName,
		MonitoringAlertName:    req.MonitoringAlertName,
		StatusEquals:           req.StatusEquals,
		SortOrder:              req.SortOrder,
		MaxResults:             req.MaxResults,
	}

	items, next := h.Backend.ListMonitoringAlertHistory(ctx, req.NextToken, f)

	summaries := make([]map[string]any, 0, len(items))
	for _, e := range items {
		summaries = append(summaries, map[string]any{
			keyMonitoringScheduleName: e.MonitoringScheduleName,
			keyMonitoringAlertName:    e.MonitoringAlertName,
			"AlertStatus":             e.AlertStatus,
			keyCreationTime:           epochSeconds(e.CreationTime),
		})
	}

	resp := map[string]any{"MonitoringAlertHistory": summaries}
	if next != "" {
		resp[keyNextToken] = next
	}

	return json.Marshal(resp)
}

// listMonitoringExecutionsRequest is the parsed representation of a
// ListMonitoringExecutions request body.
type listMonitoringExecutionsRequest struct {
	CreationTimeAfter           *float64 `json:"CreationTimeAfter,omitempty"`
	CreationTimeBefore          *float64 `json:"CreationTimeBefore,omitempty"`
	LastModifiedTimeAfter       *float64 `json:"LastModifiedTimeAfter,omitempty"`
	LastModifiedTimeBefore      *float64 `json:"LastModifiedTimeBefore,omitempty"`
	ScheduledTimeAfter          *float64 `json:"ScheduledTimeAfter,omitempty"`
	ScheduledTimeBefore         *float64 `json:"ScheduledTimeBefore,omitempty"`
	EndpointName                string   `json:"EndpointName,omitempty"`
	MonitoringJobDefinitionName string   `json:"MonitoringJobDefinitionName,omitempty"`
	MonitoringScheduleName      string   `json:"MonitoringScheduleName,omitempty"`
	MonitoringTypeEquals        string   `json:"MonitoringTypeEquals,omitempty"`
	StatusEquals                string   `json:"StatusEquals,omitempty"`
	SortBy                      string   `json:"SortBy,omitempty"`
	SortOrder                   string   `json:"SortOrder,omitempty"`
	NextToken                   string   `json:"NextToken,omitempty"`
	MaxResults                  int32    `json:"MaxResults,omitempty"`
}

func (r listMonitoringExecutionsRequest) toFilter() MonitoringExecutionFilter {
	return MonitoringExecutionFilter{
		CreationTimeAfter:           epochPtr(r.CreationTimeAfter),
		CreationTimeBefore:          epochPtr(r.CreationTimeBefore),
		LastModifiedTimeAfter:       epochPtr(r.LastModifiedTimeAfter),
		LastModifiedTimeBefore:      epochPtr(r.LastModifiedTimeBefore),
		ScheduledTimeAfter:          epochPtr(r.ScheduledTimeAfter),
		ScheduledTimeBefore:         epochPtr(r.ScheduledTimeBefore),
		EndpointName:                r.EndpointName,
		MonitoringJobDefinitionName: r.MonitoringJobDefinitionName,
		MonitoringScheduleName:      r.MonitoringScheduleName,
		MonitoringTypeEquals:        r.MonitoringTypeEquals,
		StatusEquals:                r.StatusEquals,
		SortBy:                      r.SortBy,
		SortOrder:                   r.SortOrder,
		MaxResults:                  r.MaxResults,
	}
}

func (h *Handler) handleListMonitoringExecutions(ctx context.Context, body []byte) ([]byte, error) {
	var req listMonitoringExecutionsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	items, next := h.Backend.ListMonitoringExecutions(ctx, req.NextToken, req.toFilter())

	return json.Marshal(buildMonitoringExecutionListResponse(items, next))
}

func buildMonitoringExecutionListResponse(items []*MonitoringExecution, next string) map[string]any {
	summaries := make([]map[string]any, 0, len(items))
	for _, e := range items {
		summaries = append(summaries, buildMonitoringExecutionSummary(e))
	}

	resp := map[string]any{"MonitoringExecutionSummaries": summaries}
	if next != "" {
		resp[keyNextToken] = next
	}

	return resp
}

func buildMonitoringExecutionSummary(e *MonitoringExecution) map[string]any {
	s := map[string]any{
		keyMonitoringScheduleName:   e.MonitoringScheduleName,
		"MonitoringExecutionStatus": e.MonitoringExecutionStatus,
		keyCreationTime:             epochSeconds(e.CreationTime),
		keyLastModifiedTime:         epochSeconds(e.LastModifiedTime),
		"ScheduledTime":             epochSeconds(e.ScheduledTime),
	}

	if e.EndpointName != "" {
		s[keyEndpointNameField] = e.EndpointName
	}

	if e.MonitoringJobDefinitionName != "" {
		s["MonitoringJobDefinitionName"] = e.MonitoringJobDefinitionName
	}

	if e.MonitoringType != "" {
		s["MonitoringType"] = e.MonitoringType
	}

	if e.ProcessingJobArn != "" {
		s["ProcessingJobArn"] = e.ProcessingJobArn
	}

	if e.FailureReason != "" {
		s["FailureReason"] = e.FailureReason
	}

	return s
}

// ---------------------------------------------------------------------------
// HumanTaskUI handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateHumanTaskUI(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Tags            map[string]string `json:"Tags"`
		HumanTaskUIName string            `json:"HumanTaskUiName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.HumanTaskUIName == "" {
		return nil, fmt.Errorf("%w: HumanTaskUiName is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateHumanTaskUI(ctx, req.HumanTaskUIName, req.Tags)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyHumanTaskUIArn: result.HumanTaskUIArn})
}

func (h *Handler) handleDescribeHumanTaskUI(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		HumanTaskUIName string `json:"HumanTaskUiName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.HumanTaskUIName == "" {
		return nil, fmt.Errorf("%w: HumanTaskUiName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeHumanTaskUI(ctx, req.HumanTaskUIName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleDeleteHumanTaskUI(ctx context.Context, body []byte) error {
	var req struct {
		HumanTaskUIName string `json:"HumanTaskUiName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.HumanTaskUIName == "" {
		return fmt.Errorf("%w: HumanTaskUiName is required", errInvalidRequest)
	}

	return h.Backend.DeleteHumanTaskUI(ctx, req.HumanTaskUIName)
}

// ---------------------------------------------------------------------------
// Workforce handlers
// ---------------------------------------------------------------------------

// oidcConfigRequest is the wire shape of OidcConfig on Create/UpdateWorkforce
// requests. Unlike the stored/returned OidcConfig, it includes ClientSecret.
type oidcConfigRequest struct {
	AuthenticationRequestExtraParams map[string]string `json:"AuthenticationRequestExtraParams"`
	ClientID                         string            `json:"ClientId"`
	ClientSecret                     string            `json:"ClientSecret"`
	Issuer                           string            `json:"Issuer"`
	AuthorizationEndpoint            string            `json:"AuthorizationEndpoint"`
	TokenEndpoint                    string            `json:"TokenEndpoint"`
	UserInfoEndpoint                 string            `json:"UserInfoEndpoint"`
	LogoutEndpoint                   string            `json:"LogoutEndpoint"`
	JwksURI                          string            `json:"JwksUri"`
	Scope                            string            `json:"Scope"`
}

func (r *oidcConfigRequest) toOidcConfig() *OidcConfig {
	if r == nil {
		return nil
	}

	return &OidcConfig{
		AuthenticationRequestExtraParams: r.AuthenticationRequestExtraParams,
		ClientID:                         r.ClientID,
		ClientSecret:                     r.ClientSecret,
		Issuer:                           r.Issuer,
		AuthorizationEndpoint:            r.AuthorizationEndpoint,
		TokenEndpoint:                    r.TokenEndpoint,
		UserInfoEndpoint:                 r.UserInfoEndpoint,
		LogoutEndpoint:                   r.LogoutEndpoint,
		JwksURI:                          r.JwksURI,
		Scope:                            r.Scope,
	}
}

// workforceResponseMap builds the AWS wire representation of a Workforce,
// converting timestamps to epoch seconds as required by the aws-json-1.1 protocol.
func workforceResponseMap(w *Workforce) map[string]any {
	resp := map[string]any{
		"WorkforceName":   w.WorkforceName,
		keyWorkforceArn:   w.WorkforceArn,
		keyStatus:         w.Status,
		"CreateDate":      epochSeconds(w.CreateDate),
		"LastUpdatedDate": epochSeconds(w.LastUpdatedDate),
	}

	if w.CognitoConfig != nil {
		resp["CognitoConfig"] = w.CognitoConfig
	}

	if w.OidcConfig != nil {
		resp["OidcConfig"] = w.OidcConfig
	}

	if w.SourceIPConfig != nil {
		resp["SourceIpConfig"] = w.SourceIPConfig
	}

	if w.WorkforceVpcConfig != nil {
		resp["WorkforceVpcConfig"] = w.WorkforceVpcConfig
	}

	if w.SubDomain != "" {
		resp["SubDomain"] = w.SubDomain
	}

	return resp
}

func (h *Handler) handleCreateWorkforce(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		CognitoConfig      *CognitoConfig      `json:"CognitoConfig"`
		OidcConfig         *oidcConfigRequest  `json:"OidcConfig"`
		SourceIPConfig     *SourceIPConfig     `json:"SourceIpConfig"`
		WorkforceVpcConfig *WorkforceVpcConfig `json:"WorkforceVpcConfig"`
		WorkforceName      string              `json:"WorkforceName"`
		Tags               []tagObject         `json:"Tags"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.WorkforceName == "" {
		return nil, fmt.Errorf("%w: WorkforceName is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateWorkforce(ctx, CreateWorkforceOptions{
		Name:               req.WorkforceName,
		CognitoConfig:      req.CognitoConfig,
		OidcConfig:         req.OidcConfig.toOidcConfig(),
		SourceIPConfig:     req.SourceIPConfig,
		WorkforceVpcConfig: req.WorkforceVpcConfig,
		Tags:               fromTagObjects(req.Tags),
	})
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyWorkforceArn: result.WorkforceArn})
}

func (h *Handler) handleDescribeWorkforce(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		WorkforceName string `json:"WorkforceName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.WorkforceName == "" {
		return nil, fmt.Errorf("%w: WorkforceName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeWorkforce(ctx, req.WorkforceName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{"Workforce": workforceResponseMap(result)})
}

func (h *Handler) handleUpdateWorkforce(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		OidcConfig         *oidcConfigRequest  `json:"OidcConfig"`
		SourceIPConfig     *SourceIPConfig     `json:"SourceIpConfig"`
		WorkforceVpcConfig *WorkforceVpcConfig `json:"WorkforceVpcConfig"`
		WorkforceName      string              `json:"WorkforceName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.WorkforceName == "" {
		return nil, fmt.Errorf("%w: WorkforceName is required", errInvalidRequest)
	}

	result, err := h.Backend.UpdateWorkforce(ctx, UpdateWorkforceOptions{
		Name:               req.WorkforceName,
		OidcConfig:         req.OidcConfig.toOidcConfig(),
		SourceIPConfig:     req.SourceIPConfig,
		WorkforceVpcConfig: req.WorkforceVpcConfig,
	})
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{"Workforce": workforceResponseMap(result)})
}

func (h *Handler) handleDeleteWorkforce(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		WorkforceName string `json:"WorkforceName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.WorkforceName == "" {
		return nil, fmt.Errorf("%w: WorkforceName is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteWorkforce(ctx, req.WorkforceName); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{})
}

func (h *Handler) handleListWorkforces(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	items, next := h.Backend.ListWorkforces(ctx, req.NextToken)

	summaries := make([]map[string]any, 0, len(items))
	for _, w := range items {
		summaries = append(summaries, workforceResponseMap(w))
	}

	resp := map[string]any{"Workforces": summaries}
	if next != "" {
		resp[keyNextToken] = next
	}

	return json.Marshal(resp)
}

// ---------------------------------------------------------------------------
// FlowDefinition handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateFlowDefinition(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Tags               map[string]string `json:"Tags"`
		FlowDefinitionName string            `json:"FlowDefinitionName"`
		RoleArn            string            `json:"RoleArn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.FlowDefinitionName == "" {
		return nil, fmt.Errorf("%w: FlowDefinitionName is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateFlowDefinition(ctx, req.FlowDefinitionName, req.RoleArn, req.Tags)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyFlowDefinitionArn: result.FlowDefinitionArn})
}

func (h *Handler) handleDescribeFlowDefinition(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		FlowDefinitionName string `json:"FlowDefinitionName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.FlowDefinitionName == "" {
		return nil, fmt.Errorf("%w: FlowDefinitionName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeFlowDefinition(ctx, req.FlowDefinitionName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleDeleteFlowDefinition(ctx context.Context, body []byte) error {
	var req struct {
		FlowDefinitionName string `json:"FlowDefinitionName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.FlowDefinitionName == "" {
		return fmt.Errorf("%w: FlowDefinitionName is required", errInvalidRequest)
	}

	return h.Backend.DeleteFlowDefinition(ctx, req.FlowDefinitionName)
}

// ---------------------------------------------------------------------------
// AppImageConfig handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateAppImageConfig(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Tags               map[string]string `json:"Tags"`
		AppImageConfigName string            `json:"AppImageConfigName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.AppImageConfigName == "" {
		return nil, fmt.Errorf("%w: AppImageConfigName is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateAppImageConfig(ctx, req.AppImageConfigName, req.Tags)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyAppImageConfigArn: result.AppImageConfigArn})
}

func (h *Handler) handleDescribeAppImageConfig(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		AppImageConfigName string `json:"AppImageConfigName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.AppImageConfigName == "" {
		return nil, fmt.Errorf("%w: AppImageConfigName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeAppImageConfig(ctx, req.AppImageConfigName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleDeleteAppImageConfig(ctx context.Context, body []byte) error {
	var req struct {
		AppImageConfigName string `json:"AppImageConfigName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.AppImageConfigName == "" {
		return fmt.Errorf("%w: AppImageConfigName is required", errInvalidRequest)
	}

	return h.Backend.DeleteAppImageConfig(ctx, req.AppImageConfigName)
}

func (h *Handler) handleUpdateAppImageConfig(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		AppImageConfigName string `json:"AppImageConfigName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.AppImageConfigName == "" {
		return nil, fmt.Errorf("%w: AppImageConfigName is required", errInvalidRequest)
	}

	result, err := h.Backend.UpdateAppImageConfig(ctx, req.AppImageConfigName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyAppImageConfigArn: result.AppImageConfigArn})
}

// ---------------------------------------------------------------------------
// InferenceExperiment handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateInferenceExperiment(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Tags    map[string]string `json:"Tags"`
		Name    string            `json:"Name"`
		Type    string            `json:"Type"`
		RoleArn string            `json:"RoleArn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateInferenceExperiment(ctx, req.Name, req.Type, req.RoleArn, req.Tags)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyInferenceExperimentArn: result.Arn})
}

func (h *Handler) handleDescribeInferenceExperiment(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Name string `json:"Name"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeInferenceExperiment(ctx, req.Name)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleStopInferenceExperiment(ctx context.Context, body []byte) error {
	var req struct {
		Name string `json:"Name"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Name == "" {
		return fmt.Errorf("%w: Name is required", errInvalidRequest)
	}

	return h.Backend.StopInferenceExperiment(ctx, req.Name)
}

func (h *Handler) handleDeleteInferenceExperiment(ctx context.Context, body []byte) error {
	var req struct {
		Name string `json:"Name"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Name == "" {
		return fmt.Errorf("%w: Name is required", errInvalidRequest)
	}

	return h.Backend.DeleteInferenceExperiment(ctx, req.Name)
}

// ---------------------------------------------------------------------------
// MlflowTrackingServer handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateMlflowTrackingServer(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Tags               map[string]string `json:"Tags"`
		TrackingServerName string            `json:"TrackingServerName"`
		RoleArn            string            `json:"RoleArn"`
		MlflowVersion      string            `json:"MlflowVersion"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.TrackingServerName == "" {
		return nil, fmt.Errorf("%w: TrackingServerName is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateMlflowTrackingServer(ctx,
		req.TrackingServerName, req.RoleArn, req.MlflowVersion, req.Tags,
	)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyTrackingServerArn: result.TrackingServerArn})
}

func (h *Handler) handleDescribeMlflowTrackingServer(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		TrackingServerName string `json:"TrackingServerName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.TrackingServerName == "" {
		return nil, fmt.Errorf("%w: TrackingServerName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeMlflowTrackingServer(ctx, req.TrackingServerName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleDeleteMlflowTrackingServer(ctx context.Context, body []byte) error {
	var req struct {
		TrackingServerName string `json:"TrackingServerName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.TrackingServerName == "" {
		return fmt.Errorf("%w: TrackingServerName is required", errInvalidRequest)
	}

	return h.Backend.DeleteMlflowTrackingServer(ctx, req.TrackingServerName)
}

func (h *Handler) handleStartMlflowTrackingServer(ctx context.Context, body []byte) error {
	var req struct {
		TrackingServerName string `json:"TrackingServerName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.TrackingServerName == "" {
		return fmt.Errorf("%w: TrackingServerName is required", errInvalidRequest)
	}

	return h.Backend.StartMlflowTrackingServer(ctx, req.TrackingServerName)
}

func (h *Handler) handleStopMlflowTrackingServer(ctx context.Context, body []byte) error {
	var req struct {
		TrackingServerName string `json:"TrackingServerName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.TrackingServerName == "" {
		return fmt.Errorf("%w: TrackingServerName is required", errInvalidRequest)
	}

	return h.Backend.StopMlflowTrackingServer(ctx, req.TrackingServerName)
}

// ---------------------------------------------------------------------------
// ModelCard handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateModelCard(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Tags          map[string]string `json:"Tags"`
		ModelCardName string            `json:"ModelCardName"`
		Content       string            `json:"Content"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelCardName == "" {
		return nil, fmt.Errorf("%w: ModelCardName is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateModelCard(ctx, req.ModelCardName, req.Content, req.Tags)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyModelCardArn: result.ModelCardArn})
}

func (h *Handler) handleDescribeModelCard(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ModelCardName string `json:"ModelCardName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelCardName == "" {
		return nil, fmt.Errorf("%w: ModelCardName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeModelCard(ctx, req.ModelCardName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleUpdateModelCard(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ModelCardName string `json:"ModelCardName"`
		Content       string `json:"Content"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelCardName == "" {
		return nil, fmt.Errorf("%w: ModelCardName is required", errInvalidRequest)
	}

	result, err := h.Backend.UpdateModelCard(ctx, req.ModelCardName, req.Content)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyModelCardArn: result.ModelCardArn})
}

func (h *Handler) handleDeleteModelCard(ctx context.Context, body []byte) error {
	var req struct {
		ModelCardName string `json:"ModelCardName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelCardName == "" {
		return fmt.Errorf("%w: ModelCardName is required", errInvalidRequest)
	}

	return h.Backend.DeleteModelCard(ctx, req.ModelCardName)
}

// ---------------------------------------------------------------------------
// OptimizationJob handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateOptimizationJob(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Tags                map[string]string `json:"Tags"`
		OptimizationJobName string            `json:"OptimizationJobName"`
		RoleArn             string            `json:"RoleArn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.OptimizationJobName == "" {
		return nil, fmt.Errorf("%w: OptimizationJobName is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateOptimizationJob(ctx, req.OptimizationJobName, req.RoleArn, req.Tags)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyOptimizationJobArn: result.OptimizationJobArn})
}

func (h *Handler) handleDescribeOptimizationJob(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		OptimizationJobName string `json:"OptimizationJobName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.OptimizationJobName == "" {
		return nil, fmt.Errorf("%w: OptimizationJobName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeOptimizationJob(ctx, req.OptimizationJobName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleDeleteOptimizationJob(ctx context.Context, body []byte) error {
	var req struct {
		OptimizationJobName string `json:"OptimizationJobName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.OptimizationJobName == "" {
		return fmt.Errorf("%w: OptimizationJobName is required", errInvalidRequest)
	}

	return h.Backend.DeleteOptimizationJob(ctx, req.OptimizationJobName)
}

func (h *Handler) handleStopOptimizationJob(ctx context.Context, body []byte) error {
	var req struct {
		OptimizationJobName string `json:"OptimizationJobName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.OptimizationJobName == "" {
		return fmt.Errorf("%w: OptimizationJobName is required", errInvalidRequest)
	}

	return h.Backend.StopOptimizationJob(ctx, req.OptimizationJobName)
}

// ---------------------------------------------------------------------------
// StudioLifecycleConfig handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateStudioLifecycleConfig(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Tags                         map[string]string `json:"Tags"`
		StudioLifecycleConfigName    string            `json:"StudioLifecycleConfigName"`
		StudioLifecycleConfigAppType string            `json:"StudioLifecycleConfigAppType"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.StudioLifecycleConfigName == "" {
		return nil, fmt.Errorf("%w: StudioLifecycleConfigName is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateStudioLifecycleConfig(ctx,
		req.StudioLifecycleConfigName, req.StudioLifecycleConfigAppType, req.Tags,
	)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyStudioLifecycleConfigArn: result.StudioLifecycleConfigArn})
}

func (h *Handler) handleDescribeStudioLifecycleConfig(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		StudioLifecycleConfigName string `json:"StudioLifecycleConfigName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.StudioLifecycleConfigName == "" {
		return nil, fmt.Errorf("%w: StudioLifecycleConfigName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeStudioLifecycleConfig(ctx, req.StudioLifecycleConfigName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleDeleteStudioLifecycleConfig(ctx context.Context, body []byte) error {
	var req struct {
		StudioLifecycleConfigName string `json:"StudioLifecycleConfigName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.StudioLifecycleConfigName == "" {
		return fmt.Errorf("%w: StudioLifecycleConfigName is required", errInvalidRequest)
	}

	return h.Backend.DeleteStudioLifecycleConfig(ctx, req.StudioLifecycleConfigName)
}

// ---------------------------------------------------------------------------
// PartnerApp handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreatePartnerApp(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Tags map[string]string `json:"Tags"`
		Name string            `json:"Name"`
		Type string            `json:"Type"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", errInvalidRequest)
	}

	result, err := h.Backend.CreatePartnerApp(ctx, req.Name, req.Type, req.Tags)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyGenericArn: result.Arn})
}

func (h *Handler) handleDescribePartnerApp(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Arn string `json:"Arn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Arn == "" {
		return nil, fmt.Errorf("%w: Arn is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribePartnerApp(ctx, req.Arn)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleDeletePartnerApp(ctx context.Context, body []byte) error {
	var req struct {
		Arn string `json:"Arn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Arn == "" {
		return fmt.Errorf("%w: Arn is required", errInvalidRequest)
	}

	return h.Backend.DeletePartnerApp(ctx, req.Arn)
}

// ---------------------------------------------------------------------------
// TrainingPlan handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateTrainingPlan(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Tags             map[string]string `json:"Tags"`
		TrainingPlanName string            `json:"TrainingPlanName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.TrainingPlanName == "" {
		return nil, fmt.Errorf("%w: TrainingPlanName is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateTrainingPlan(ctx, req.TrainingPlanName, req.Tags)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyTrainingPlanArn: result.TrainingPlanArn})
}

func (h *Handler) handleDescribeTrainingPlan(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		TrainingPlanName string `json:"TrainingPlanName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.TrainingPlanName == "" {
		return nil, fmt.Errorf("%w: TrainingPlanName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeTrainingPlan(ctx, req.TrainingPlanName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}
