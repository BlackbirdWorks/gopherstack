package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

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
			var tags []tagObject
			if err := json.Unmarshal(v, &tags); err != nil {
				return jobDefRequest{}, fmt.Errorf("%w: %w", errInvalidRequest, err)
			}

			req.Tags = fromTagObjects(tags)
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
