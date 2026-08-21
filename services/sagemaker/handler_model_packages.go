package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
)

// ---------------------------------------------------------------------------
// ModelPackage handlers
// ---------------------------------------------------------------------------

// createModelPackageInput is the wire request body for CreateModelPackage
// (api_op_CreateModelPackage.go:47-171, sagemaker@v1.263.2). ClientToken is
// deliberately unmodeled (see CreateModelPackageOptions' doc comment).
type createModelPackageInput struct {
	CertifyForMarketplace             *bool             `json:"CertifyForMarketplace"`
	CustomerMetadataProperties        map[string]string `json:"CustomerMetadataProperties"`
	Task                              string            `json:"Task"`
	Domain                            string            `json:"Domain"`
	SamplePayloadURL                  string            `json:"SamplePayloadUrl"`
	SourceURI                         string            `json:"SourceUri"`
	ModelPackageDescription           string            `json:"ModelPackageDescription"`
	ManagedStorageType                string            `json:"ManagedStorageType"`
	ModelPackageRegistrationType      string            `json:"ModelPackageRegistrationType"`
	SkipModelValidation               string            `json:"SkipModelValidation"`
	ModelApprovalStatus               string            `json:"ModelApprovalStatus"`
	ModelPackageGroupName             string            `json:"ModelPackageGroupName"`
	ModelPackageName                  string            `json:"ModelPackageName"`
	InferenceSpecification            json.RawMessage   `json:"InferenceSpecification"`
	ValidationSpecification           json.RawMessage   `json:"ValidationSpecification"`
	DriftCheckBaselines               json.RawMessage   `json:"DriftCheckBaselines"`
	ModelMetrics                      json.RawMessage   `json:"ModelMetrics"`
	ModelCard                         json.RawMessage   `json:"ModelCard"`
	ModelLifeCycle                    json.RawMessage   `json:"ModelLifeCycle"`
	MetadataProperties                json.RawMessage   `json:"MetadataProperties"`
	SecurityConfig                    json.RawMessage   `json:"SecurityConfig"`
	AdditionalInferenceSpecifications json.RawMessage   `json:"AdditionalInferenceSpecifications"`
	Tags                              []tagObject       `json:"Tags"`
	SourceAlgorithmSpecification      json.RawMessage   `json:"SourceAlgorithmSpecification"`
}

func (h *Handler) handleCreateModelPackage(ctx context.Context, body []byte) ([]byte, error) {
	var req createModelPackageInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelPackageName == "" {
		return nil, fmt.Errorf("%w: ModelPackageName is required", errInvalidRequest)
	}

	certify := false
	if req.CertifyForMarketplace != nil {
		certify = *req.CertifyForMarketplace
	}

	result, err := h.Backend.CreateModelPackage(ctx, CreateModelPackageOptions{
		Name:                              req.ModelPackageName,
		GroupName:                         req.ModelPackageGroupName,
		Description:                       req.ModelPackageDescription,
		Domain:                            req.Domain,
		SamplePayloadURL:                  req.SamplePayloadURL,
		SourceURI:                         req.SourceURI,
		Task:                              req.Task,
		ManagedStorageType:                req.ManagedStorageType,
		RegistrationType:                  req.ModelPackageRegistrationType,
		SkipModelValidation:               req.SkipModelValidation,
		ApprovalStatus:                    req.ModelApprovalStatus,
		Tags:                              fromTagObjects(req.Tags),
		CustomerMetadataProperties:        req.CustomerMetadataProperties,
		InferenceSpecification:            req.InferenceSpecification,
		SourceAlgorithmSpecification:      req.SourceAlgorithmSpecification,
		ValidationSpecification:           req.ValidationSpecification,
		DriftCheckBaselines:               req.DriftCheckBaselines,
		ModelMetrics:                      req.ModelMetrics,
		ModelCard:                         req.ModelCard,
		ModelLifeCycle:                    req.ModelLifeCycle,
		MetadataProperties:                req.MetadataProperties,
		SecurityConfig:                    req.SecurityConfig,
		AdditionalInferenceSpecifications: req.AdditionalInferenceSpecifications,
		CertifyForMarketplace:             certify,
	})
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyModelPackageArn: result.ModelPackageArn})
}

// describeModelPackageInput is the wire request body for
// DescribeModelPackage (api_op_DescribeModelPackage.go:16-42,
// sagemaker@v1.263.2).
type describeModelPackageInput struct {
	ModelPackageName string `json:"ModelPackageName"`
	IncludedData     string `json:"IncludedData"`
}

func (h *Handler) handleDescribeModelPackage(ctx context.Context, body []byte) ([]byte, error) {
	var req describeModelPackageInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelPackageName == "" {
		return nil, fmt.Errorf("%w: ModelPackageName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeModelPackage(ctx, req.ModelPackageName, req.IncludedData)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

// deleteModelPackageInput is the wire request body for DeleteModelPackage
// (api_op_DeleteModelPackage.go:16-27, sagemaker@v1.263.2).
type deleteModelPackageInput struct {
	ModelPackageName string `json:"ModelPackageName"`
}

func (h *Handler) handleDeleteModelPackage(ctx context.Context, body []byte) error {
	var req deleteModelPackageInput

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelPackageName == "" {
		return fmt.Errorf("%w: ModelPackageName is required", errInvalidRequest)
	}

	return h.Backend.DeleteModelPackage(ctx, req.ModelPackageName)
}

// listModelPackagesInput is the wire request body for ListModelPackages
// (api_op_ListModelPackages.go:29-77, sagemaker@v1.263.2).
type listModelPackagesInput struct {
	CreationTimeAfter     *float64 `json:"CreationTimeAfter"`
	CreationTimeBefore    *float64 `json:"CreationTimeBefore"`
	ModelApprovalStatus   string   `json:"ModelApprovalStatus"`
	ModelPackageGroupName string   `json:"ModelPackageGroupName"`
	ModelPackageType      string   `json:"ModelPackageType"`
	NameContains          string   `json:"NameContains"`
	NextToken             string   `json:"NextToken"`
	SortBy                string   `json:"SortBy"`
	SortOrder             string   `json:"SortOrder"`
	MaxResults            int32    `json:"MaxResults"`
}

func (h *Handler) handleListModelPackages(ctx context.Context, body []byte) ([]byte, error) {
	var req listModelPackagesInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	params := ListModelPackagesParams{
		ApprovalStatus:   req.ModelApprovalStatus,
		GroupName:        req.ModelPackageGroupName,
		ModelPackageType: req.ModelPackageType,
		NameContains:     req.NameContains,
		NextToken:        req.NextToken,
		SortBy:           req.SortBy,
		SortOrder:        req.SortOrder,
		MaxResults:       req.MaxResults,
		CreatedAfter:     timeFromEpochSecondsPtr(req.CreationTimeAfter),
		CreatedBefore:    timeFromEpochSecondsPtr(req.CreationTimeBefore),
	}

	items, next := h.Backend.ListModelPackages(ctx, params)

	summaries := make([]map[string]any, 0, len(items))
	for _, mp := range items {
		s := map[string]any{
			"ModelPackageName":             mp.ModelPackageName,
			"ModelPackageArn":              mp.ModelPackageArn,
			"ModelPackageStatus":           mp.ModelPackageStatus,
			keyCreationTime:                epochSeconds(mp.CreationTime),
			keyModelApprovalStatus:         mp.ModelApprovalStatus,
			"ModelPackageDescription":      mp.ModelPackageDescription,
			keyModelPackageGroupName:       mp.ModelPackageGroupName,
			"ModelPackageRegistrationType": mp.ModelPackageRegistrationType,
		}
		if len(mp.ModelLifeCycle) > 0 {
			s["ModelLifeCycle"] = mp.ModelLifeCycle
		}

		summaries = append(summaries, s)
	}

	return json.Marshal(map[string]any{
		"ModelPackageSummaryList": summaries,
		keyNextToken:              next,
	})
}

// ---------------------------------------------------------------------------
// ModelPackageGroup handlers
// ---------------------------------------------------------------------------

// createModelPackageGroupInput is the wire request body for
// CreateModelPackageGroup (api_op_CreateModelPackageGroup.go:16-40,
// sagemaker@v1.263.2).
type createModelPackageGroupInput struct {
	ManagedConfiguration         *ManagedConfiguration `json:"ManagedConfiguration"`
	ModelPackageGroupName        string                `json:"ModelPackageGroupName"`
	ModelPackageGroupDescription string                `json:"ModelPackageGroupDescription"`
	Tags                         []tagObject           `json:"Tags"`
}

func (h *Handler) handleCreateModelPackageGroup(ctx context.Context, body []byte) ([]byte, error) {
	var req createModelPackageGroupInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelPackageGroupName == "" {
		return nil, fmt.Errorf("%w: ModelPackageGroupName is required", errInvalidRequest)
	}

	opts := CreateModelPackageGroupOptions{
		Name:        req.ModelPackageGroupName,
		Description: req.ModelPackageGroupDescription,
		Tags:        fromTagObjects(req.Tags),
	}
	if req.ManagedConfiguration != nil {
		opts.ManagedStorageType = req.ManagedConfiguration.ManagedStorageType
	}

	result, err := h.Backend.CreateModelPackageGroup(ctx, opts)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyModelPackageGroupArn: result.ModelPackageGroupArn})
}

// describeModelPackageGroupInput is the wire request body for
// DescribeModelPackageGroup (api_op_DescribeModelPackageGroup.go:16-27,
// sagemaker@v1.263.2).
type describeModelPackageGroupInput struct {
	ModelPackageGroupName string `json:"ModelPackageGroupName"`
}

func (h *Handler) handleDescribeModelPackageGroup(ctx context.Context, body []byte) ([]byte, error) {
	var req describeModelPackageGroupInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelPackageGroupName == "" {
		return nil, fmt.Errorf("%w: ModelPackageGroupName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeModelPackageGroup(ctx, req.ModelPackageGroupName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

// deleteModelPackageGroupInput is the wire request body for
// DeleteModelPackageGroup (api_op_DeleteModelPackageGroup.go:16-24,
// sagemaker@v1.263.2).
type deleteModelPackageGroupInput struct {
	ModelPackageGroupName string `json:"ModelPackageGroupName"`
}

func (h *Handler) handleDeleteModelPackageGroup(ctx context.Context, body []byte) error {
	var req deleteModelPackageGroupInput

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelPackageGroupName == "" {
		return fmt.Errorf("%w: ModelPackageGroupName is required", errInvalidRequest)
	}

	return h.Backend.DeleteModelPackageGroup(ctx, req.ModelPackageGroupName)
}

// listModelPackageGroupsInput is the wire request body for
// ListModelPackageGroups (api_op_ListModelPackageGroups.go:29-72,
// sagemaker@v1.263.2).
type listModelPackageGroupsInput struct {
	CreationTimeAfter        *float64 `json:"CreationTimeAfter"`
	CreationTimeBefore       *float64 `json:"CreationTimeBefore"`
	CrossAccountFilterOption string   `json:"CrossAccountFilterOption"`
	NameContains             string   `json:"NameContains"`
	NextToken                string   `json:"NextToken"`
	SortBy                   string   `json:"SortBy"`
	SortOrder                string   `json:"SortOrder"`
	MaxResults               int32    `json:"MaxResults"`
}

func (h *Handler) handleListModelPackageGroups(ctx context.Context, body []byte) ([]byte, error) {
	var req listModelPackageGroupsInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	params := ListModelPackageGroupsParams{
		CrossAccountFilterOption: req.CrossAccountFilterOption,
		NameContains:             req.NameContains,
		NextToken:                req.NextToken,
		SortBy:                   req.SortBy,
		SortOrder:                req.SortOrder,
		MaxResults:               req.MaxResults,
		CreatedAfter:             timeFromEpochSecondsPtr(req.CreationTimeAfter),
		CreatedBefore:            timeFromEpochSecondsPtr(req.CreationTimeBefore),
	}

	items, next := h.Backend.ListModelPackageGroups(ctx, params)

	summaries := make([]map[string]any, 0, len(items))
	for _, g := range items {
		s := map[string]any{
			keyModelPackageGroupName:  g.ModelPackageGroupName,
			"ModelPackageGroupArn":    g.ModelPackageGroupArn,
			"ModelPackageGroupStatus": g.ModelPackageGroupStatus,
			keyCreationTime:           epochSeconds(g.CreationTime),
		}
		if g.ModelPackageGroupDescription != "" {
			s["ModelPackageGroupDescription"] = g.ModelPackageGroupDescription
		}

		if g.ManagedConfiguration != nil {
			s["ManagedConfiguration"] = g.ManagedConfiguration
		}

		summaries = append(summaries, s)
	}

	return json.Marshal(map[string]any{
		"ModelPackageGroupSummaryList": summaries,
		keyNextToken:                   next,
	})
}

// getModelPackageGroupPolicyInput is the wire request body for
// GetModelPackageGroupPolicy (api_op_GetModelPackageGroupPolicy.go:16-24,
// sagemaker@v1.263.2).
type getModelPackageGroupPolicyInput struct {
	ModelPackageGroupName string `json:"ModelPackageGroupName"`
}

func (h *Handler) handleGetModelPackageGroupPolicy(ctx context.Context, body []byte) ([]byte, error) {
	var req getModelPackageGroupPolicyInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelPackageGroupName == "" {
		return nil, fmt.Errorf("%w: ModelPackageGroupName is required", errInvalidRequest)
	}

	policy, err := h.Backend.GetModelPackageGroupPolicy(ctx, req.ModelPackageGroupName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{"ResourcePolicy": policy})
}

// putModelPackageGroupPolicyInput is the wire request body for
// PutModelPackageGroupPolicy (api_op_PutModelPackageGroupPolicy.go:16-31,
// sagemaker@v1.263.2).
type putModelPackageGroupPolicyInput struct {
	ModelPackageGroupName string `json:"ModelPackageGroupName"`
	ResourcePolicy        string `json:"ResourcePolicy"`
}

func (h *Handler) handlePutModelPackageGroupPolicy(ctx context.Context, body []byte) ([]byte, error) {
	var req putModelPackageGroupPolicyInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelPackageGroupName == "" {
		return nil, fmt.Errorf("%w: ModelPackageGroupName is required", errInvalidRequest)
	}

	if req.ResourcePolicy == "" {
		return nil, fmt.Errorf("%w: ResourcePolicy is required", errInvalidRequest)
	}

	g, err := h.Backend.PutModelPackageGroupPolicy(ctx, req.ModelPackageGroupName, req.ResourcePolicy)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyModelPackageGroupArn: g.ModelPackageGroupArn})
}

// deleteModelPackageGroupPolicyInput is the wire request body for
// DeleteModelPackageGroupPolicy
// (api_op_DeleteModelPackageGroupPolicy.go:16-24, sagemaker@v1.263.2).
type deleteModelPackageGroupPolicyInput struct {
	ModelPackageGroupName string `json:"ModelPackageGroupName"`
}

func (h *Handler) handleDeleteModelPackageGroupPolicy(ctx context.Context, body []byte) error {
	var req deleteModelPackageGroupPolicyInput

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelPackageGroupName == "" {
		return fmt.Errorf("%w: ModelPackageGroupName is required", errInvalidRequest)
	}

	return h.Backend.DeleteModelPackageGroupPolicy(ctx, req.ModelPackageGroupName)
}

// updateModelPackageInput is the wire request body for UpdateModelPackage
// (api_op_UpdateModelPackage.go:26-95, sagemaker@v1.263.2).
//
// The real op's sole identifier is ModelPackageArn -- UpdateModelPackageInput
// has no ModelPackageName field at all. The previous handler decoded
// "ModelPackageName" instead, a field that does not exist on any real
// UpdateModelPackageInput a genuine aws-sdk-go-v2 client could ever send, so
// every real client's UpdateModelPackage call against this backend failed
// with "ModelPackageName is required" -- the same class of wire-shape
// fabrication parity-10 found on SendPipelineExecutionStepSuccess/Failure.
type updateModelPackageInput struct {
	ModelPackageArn                        string            `json:"ModelPackageArn"`
	ModelApprovalStatus                    string            `json:"ModelApprovalStatus"`
	ApprovalDescription                    string            `json:"ApprovalDescription"`
	ModelPackageRegistrationType           string            `json:"ModelPackageRegistrationType"`
	SourceURI                              string            `json:"SourceUri"`
	CustomerMetadataProperties             map[string]string `json:"CustomerMetadataProperties"`
	CustomerMetadataPropertiesToRemove     []string          `json:"CustomerMetadataPropertiesToRemove"`
	InferenceSpecification                 json.RawMessage   `json:"InferenceSpecification"`
	ModelCard                              json.RawMessage   `json:"ModelCard"`
	ModelLifeCycle                         json.RawMessage   `json:"ModelLifeCycle"`
	AdditionalInferenceSpecificationsToAdd json.RawMessage   `json:"AdditionalInferenceSpecificationsToAdd"`
}

func (h *Handler) handleUpdateModelPackage(ctx context.Context, body []byte) ([]byte, error) {
	var req updateModelPackageInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ModelPackageArn == "" {
		return nil, fmt.Errorf("%w: ModelPackageArn is required", errInvalidRequest)
	}

	mp, err := h.Backend.UpdateModelPackage(ctx, req.ModelPackageArn, UpdateModelPackageOptions{
		ApprovalStatus:                         req.ModelApprovalStatus,
		ApprovalDescription:                    req.ApprovalDescription,
		RegistrationType:                       req.ModelPackageRegistrationType,
		SourceURI:                              req.SourceURI,
		CustomerMetadataProperties:             req.CustomerMetadataProperties,
		CustomerMetadataPropertiesToRemove:     req.CustomerMetadataPropertiesToRemove,
		InferenceSpecification:                 req.InferenceSpecification,
		ModelCard:                              req.ModelCard,
		ModelLifeCycle:                         req.ModelLifeCycle,
		AdditionalInferenceSpecificationsToAdd: req.AdditionalInferenceSpecificationsToAdd,
	})
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyModelPackageArn: mp.ModelPackageArn})
}

// batchDescribeModelPackageRequest is the request body for BatchDescribeModelPackage.
type batchDescribeModelPackageRequest struct {
	ModelPackageArnList []string `json:"ModelPackageArnList"`
}

// modelPackageSummary is a serializable model package for batch describe responses.
type modelPackageSummary struct {
	ModelPackageName      string  `json:"ModelPackageName"`
	ModelPackageArn       string  `json:"ModelPackageArn"`
	ModelPackageStatus    string  `json:"ModelPackageStatus"`
	ModelPackageGroupName string  `json:"ModelPackageGroupName,omitempty"`
	CreationTime          float64 `json:"CreationTime"`
}

// batchDescribeModelPackageError holds an error entry for BatchDescribeModelPackage.
type batchDescribeModelPackageError struct {
	ErrorCode    string `json:"ErrorCode"`
	ErrorMessage string `json:"ErrorMessage"`
}

func (h *Handler) handleBatchDescribeModelPackage(ctx context.Context, body []byte) ([]byte, error) {
	var req batchDescribeModelPackageRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	results := h.Backend.BatchDescribeModelPackage(ctx, req.ModelPackageArnList)

	modelPackageMap := make(map[string]modelPackageSummary)
	errorsMap := make(map[string]batchDescribeModelPackageError)

	for arnStr, result := range results {
		if result.ErrorCode != "" {
			errorsMap[arnStr] = batchDescribeModelPackageError{
				ErrorCode:    result.ErrorCode,
				ErrorMessage: result.ErrorMessage,
			}

			continue
		}

		mp := result.ModelPackage
		modelPackageMap[arnStr] = modelPackageSummary{
			ModelPackageName:      mp.ModelPackageName,
			ModelPackageArn:       mp.ModelPackageArn,
			ModelPackageStatus:    mp.ModelPackageStatus,
			ModelPackageGroupName: mp.ModelPackageGroupName,
			CreationTime:          epochSeconds(mp.CreationTime),
		}
	}

	return json.Marshal(map[string]any{
		"ModelPackageSummaries":             modelPackageMap,
		"BatchDescribeModelPackageErrorMap": errorsMap,
	})
}
