package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// ---------------------------------------------------------------------------
// NotebookInstanceLifecycleConfig handlers (#3)
// ---------------------------------------------------------------------------

type lifecycleHookRequest struct {
	Content string `json:"Content,omitempty"`
}

type createNotebookLifecycleConfigRequest struct {
	NotebookInstanceLifecycleConfigName string                 `json:"NotebookInstanceLifecycleConfigName"`
	OnCreate                            []lifecycleHookRequest `json:"OnCreate,omitempty"`
	OnStart                             []lifecycleHookRequest `json:"OnStart,omitempty"`
	Tags                                []tagObject            `json:"Tags,omitempty"`
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
		onCreate[i] = NotebookLifecycleHook(h)
	}
	onStart := make([]NotebookLifecycleHook, len(req.OnStart))
	for i, h := range req.OnStart {
		onStart[i] = NotebookLifecycleHook(h)
	}

	lc, err := h.Backend.CreateNotebookInstanceLifecycleConfig(
		ctx,
		req.NotebookInstanceLifecycleConfigName,
		onCreate,
		onStart,
		fromTagObjects(req.Tags),
	)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "sagemaker: created notebook lifecycle config", "name", lc.Name)

	return json.Marshal(map[string]string{keyNotebookLifecycleConfigArn: lc.ARN})
}

// describeNotebookInstanceLifecycleConfigInput is
// DescribeNotebookInstanceLifecycleConfig's request shape
// (api_op_DescribeNotebookInstanceLifecycleConfig.go:33-41).
type describeNotebookInstanceLifecycleConfigInput struct {
	NotebookInstanceLifecycleConfigName string `json:"NotebookInstanceLifecycleConfigName"`
}

func (h *Handler) handleDescribeNotebookInstanceLifecycleConfig(
	ctx context.Context,
	body []byte,
) ([]byte, error) {
	var req describeNotebookInstanceLifecycleConfigInput
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
		ctx,
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

// updateNotebookInstanceLifecycleConfigInput is
// UpdateNotebookInstanceLifecycleConfig's request shape
// (api_op_UpdateNotebookInstanceLifecycleConfig.go:37-54).
type updateNotebookInstanceLifecycleConfigInput struct {
	NotebookInstanceLifecycleConfigName string                 `json:"NotebookInstanceLifecycleConfigName"`
	OnCreate                            []lifecycleHookRequest `json:"OnCreate,omitempty"`
	OnStart                             []lifecycleHookRequest `json:"OnStart,omitempty"`
}

func (h *Handler) handleUpdateNotebookInstanceLifecycleConfig(
	ctx context.Context,
	body []byte,
) ([]byte, error) {
	var req updateNotebookInstanceLifecycleConfigInput
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
			onCreate[i] = NotebookLifecycleHook(h)
		}
	}
	var onStart []NotebookLifecycleHook
	if req.OnStart != nil {
		onStart = make([]NotebookLifecycleHook, len(req.OnStart))
		for i, h := range req.OnStart {
			onStart[i] = NotebookLifecycleHook(h)
		}
	}

	_, err := h.Backend.UpdateNotebookInstanceLifecycleConfig(
		ctx,
		req.NotebookInstanceLifecycleConfigName,
		onCreate,
		onStart,
	)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{})
}

// deleteNotebookInstanceLifecycleConfigInput is
// DeleteNotebookInstanceLifecycleConfig's request shape
// (api_op_DeleteNotebookInstanceLifecycleConfig.go:27-35).
type deleteNotebookInstanceLifecycleConfigInput struct {
	NotebookInstanceLifecycleConfigName string `json:"NotebookInstanceLifecycleConfigName"`
}

func (h *Handler) handleDeleteNotebookInstanceLifecycleConfig(
	ctx context.Context,
	body []byte,
) error {
	var req deleteNotebookInstanceLifecycleConfigInput
	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if req.NotebookInstanceLifecycleConfigName == "" {
		return fmt.Errorf("%w: NotebookInstanceLifecycleConfigName is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteNotebookInstanceLifecycleConfig(
		ctx,
		req.NotebookInstanceLifecycleConfigName,
	); err != nil {
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

// listNotebookInstanceLifecycleConfigsInput is
// ListNotebookInstanceLifecycleConfigs' request shape
// (api_op_ListNotebookInstanceLifecycleConfigs.go:32-69).
type listNotebookInstanceLifecycleConfigsInput struct {
	CreationTimeAfter      *float64 `json:"CreationTimeAfter"`
	CreationTimeBefore     *float64 `json:"CreationTimeBefore"`
	LastModifiedTimeAfter  *float64 `json:"LastModifiedTimeAfter"`
	LastModifiedTimeBefore *float64 `json:"LastModifiedTimeBefore"`
	NameContains           string   `json:"NameContains,omitempty"`
	NextToken              string   `json:"NextToken"`
	SortBy                 string   `json:"SortBy,omitempty"`
	SortOrder              string   `json:"SortOrder,omitempty"`
	MaxResults             int32    `json:"MaxResults"`
}

func (h *Handler) handleListNotebookInstanceLifecycleConfigs(ctx context.Context, body []byte) ([]byte, error) {
	var req listNotebookInstanceLifecycleConfigsInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	params := ListNotebookInstanceLifecycleConfigsParams{
		CreationTimeAfter:      timeFromEpochSecondsPtr(req.CreationTimeAfter),
		CreationTimeBefore:     timeFromEpochSecondsPtr(req.CreationTimeBefore),
		LastModifiedTimeAfter:  timeFromEpochSecondsPtr(req.LastModifiedTimeAfter),
		LastModifiedTimeBefore: timeFromEpochSecondsPtr(req.LastModifiedTimeBefore),
		NameContains:           req.NameContains,
		NextToken:              req.NextToken,
		SortBy:                 req.SortBy,
		SortOrder:              req.SortOrder,
		MaxResults:             req.MaxResults,
	}
	configs, nextToken := h.Backend.ListNotebookInstanceLifecycleConfigs(ctx, params)
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
// Expanded CreateNotebookInstance (uses FSM + full fields, gaps #1, #2)
// ---------------------------------------------------------------------------

// imdsConfigRequest mirrors
// types.InstanceMetadataServiceConfiguration (types/types.go:12707-12718).
type imdsConfigRequest struct {
	MinimumInstanceMetadataServiceVersion string `json:"MinimumInstanceMetadataServiceVersion"`
}

// createNotebookInstanceFullRequest is CreateNotebookInstance's request shape
// (api_op_CreateNotebookInstance.go:67-188). LifecycleConfigName's wire key is
// "LifecycleConfigName" — NOT "NotebookInstanceLifecycleConfigName", despite
// DescribeNotebookInstanceOutput/NotebookInstanceSummary using the longer name
// for the same concept (serializers.go:41852 vs. deserializers.go:117035);
// AWS's own request/response field names disagree here.
type createNotebookInstanceFullRequest struct {
	LifecycleConfigName                  string             `json:"LifecycleConfigName,omitempty"`
	DefaultCodeRepository                string             `json:"DefaultCodeRepository,omitempty"`
	RootAccess                           string             `json:"RootAccess,omitempty"`
	DirectInternetAccess                 string             `json:"DirectInternetAccess,omitempty"`
	NotebookInstanceName                 string             `json:"NotebookInstanceName"`
	InstanceType                         string             `json:"InstanceType"`
	PlatformIdentifier                   string             `json:"PlatformIdentifier,omitempty"`
	KmsKeyID                             string             `json:"KmsKeyId,omitempty"`
	RoleArn                              string             `json:"RoleArn"`
	SubnetID                             string             `json:"SubnetId,omitempty"`
	IPAddressType                        string             `json:"IpAddressType,omitempty"`
	InstanceMetadataServiceConfiguration *imdsConfigRequest `json:"InstanceMetadataServiceConfiguration,omitempty"`
	AdditionalCodeRepositories           []string           `json:"AdditionalCodeRepositories,omitempty"`
	AcceleratorTypes                     []string           `json:"AcceleratorTypes,omitempty"`
	Tags                                 []tagObject        `json:"Tags"`
	SecurityGroupIDs                     []string           `json:"SecurityGroupIds,omitempty"`
	VolumeSizeInGB                       int32              `json:"VolumeSizeInGB,omitempty"`
}

func (h *Handler) handleCreateNotebookInstanceFull(
	ctx context.Context,
	body []byte,
) ([]byte, error) {
	var req createNotebookInstanceFullRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	var minIMDSVersion string
	if req.InstanceMetadataServiceConfiguration != nil {
		minIMDSVersion = req.InstanceMetadataServiceConfiguration.MinimumInstanceMetadataServiceVersion
	}

	nb, err := h.Backend.CreateNotebookInstanceFSM(ctx, NotebookInstanceOptions{
		Name:                                  req.NotebookInstanceName,
		InstanceType:                          req.InstanceType,
		RoleArn:                               req.RoleArn,
		SubnetID:                              req.SubnetID,
		SecurityGroupIDs:                      req.SecurityGroupIDs,
		KmsKeyID:                              req.KmsKeyID,
		LifecycleConfigName:                   req.LifecycleConfigName,
		DirectInternetAccess:                  req.DirectInternetAccess,
		RootAccess:                            req.RootAccess,
		AcceleratorTypes:                      req.AcceleratorTypes,
		AdditionalCodeRepositories:            req.AdditionalCodeRepositories,
		DefaultCodeRepository:                 req.DefaultCodeRepository,
		VolumeSizeInGB:                        req.VolumeSizeInGB,
		PlatformIdentifier:                    req.PlatformIdentifier,
		IPAddressType:                         req.IPAddressType,
		MinimumInstanceMetadataServiceVersion: minIMDSVersion,
		Tags:                                  fromTagObjects(req.Tags),
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

	return json.Marshal(map[string]string{keyNotebookInstanceArn: nb.NotebookInstanceArn})
}

// describeNotebookInstanceInput is DescribeNotebookInstance's request shape
// (api_op_DescribeNotebookInstance.go:34-42).
type describeNotebookInstanceInput struct {
	NotebookInstanceName string `json:"NotebookInstanceName"`
}

// handleDescribeNotebookInstanceFull returns all notebook fields.
func (h *Handler) handleDescribeNotebookInstanceFull(
	ctx context.Context,
	body []byte,
) ([]byte, error) {
	var req describeNotebookInstanceInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if req.NotebookInstanceName == "" {
		return nil, fmt.Errorf("%w: NotebookInstanceName is required", errInvalidRequest)
	}

	nb, err := h.Backend.DescribeNotebookInstance(ctx, req.NotebookInstanceName)
	if err != nil {
		return nil, err
	}

	resp := map[string]any{
		"NotebookInstanceName":   nb.NotebookInstanceName,
		keyNotebookInstanceArn:   nb.NotebookInstanceArn,
		"NotebookInstanceStatus": nb.NotebookInstanceStatus,
		"InstanceType":           nb.InstanceType,
		keyRoleArn:               nb.RoleArn,
		keyCreationTime:          epochSeconds(nb.CreationTime),
		keyLastModifiedTime:      epochSeconds(nb.LastModifiedTime),
	}
	addNotebookOptionalFields(resp, nb)

	return json.Marshal(resp)
}

// addNotebookOptionalFields adds every describable optional field.
// FailureReason and NetworkInterfaceId (api_op_DescribeNotebookInstance.go)
// are disclosed absent, not fabricated: this backend's notebook FSM
// (lifecycle.go) never reaches a Failed status, so there is no real failure
// to report a reason for, and no VPC ENI subsystem exists to generate a
// network interface ID from.
func addNotebookOptionalFields(resp map[string]any, nb *NotebookInstance) {
	if nb.SubnetID != "" {
		resp["SubnetId"] = nb.SubnetID
	}
	if nb.KmsKeyID != "" {
		resp["KmsKeyId"] = nb.KmsKeyID
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
	if len(nb.SecurityGroupIDs) > 0 {
		resp["SecurityGroupIds"] = nb.SecurityGroupIDs
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
	if nb.IPAddressType != "" {
		resp["IpAddressType"] = nb.IPAddressType
	}
	if nb.MinimumInstanceMetadataServiceVersion != "" {
		resp["InstanceMetadataServiceConfiguration"] = imdsConfigRequest{
			MinimumInstanceMetadataServiceVersion: nb.MinimumInstanceMetadataServiceVersion,
		}
	}
}

// updateNotebookInstanceInput is UpdateNotebookInstance's request shape
// (api_op_UpdateNotebookInstance.go:38-143). AcceleratorTypes and
// DisassociateAcceleratorTypes are not modeled: AWS's own doc comment marks
// both "no longer supported. Elastic Inference (EI) is no longer available"
// (api_op_UpdateNotebookInstance.go:45-49,72-76) — a real client sending them
// is a documented no-op on real AWS too, not a gap this backend introduces.
type updateNotebookInstanceInput struct {
	NotebookInstanceName string `json:"NotebookInstanceName"`
	InstanceType         string `json:"InstanceType,omitempty"`
	RoleArn              string `json:"RoleArn,omitempty"`
	// Wire key is "LifecycleConfigName", not "NotebookInstanceLifecycleConfigName"
	// (serializers.go:51314) — see createNotebookInstanceFullRequest.
	LifecycleConfigName                    string             `json:"LifecycleConfigName,omitempty"`
	DefaultCodeRepository                  string             `json:"DefaultCodeRepository,omitempty"`
	PlatformIdentifier                     string             `json:"PlatformIdentifier,omitempty"`
	RootAccess                             string             `json:"RootAccess,omitempty"`
	IPAddressType                          string             `json:"IpAddressType,omitempty"`
	InstanceMetadataServiceConfiguration   *imdsConfigRequest `json:"InstanceMetadataServiceConfiguration,omitempty"`
	AdditionalCodeRepositories             []string           `json:"AdditionalCodeRepositories,omitempty"`
	VolumeSizeInGB                         int32              `json:"VolumeSizeInGB,omitempty"`
	DisassociateLifecycleConfig            bool               `json:"DisassociateLifecycleConfig,omitempty"`
	DisassociateDefaultCodeRepository      bool               `json:"DisassociateDefaultCodeRepository,omitempty"`
	DisassociateAdditionalCodeRepositories bool               `json:"DisassociateAdditionalCodeRepositories,omitempty"`
}

// handleUpdateNotebookInstanceFull supports all mutable fields (#1 update coverage).
func (h *Handler) handleUpdateNotebookInstanceFull(ctx context.Context, body []byte) error {
	var req updateNotebookInstanceInput
	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if req.NotebookInstanceName == "" {
		return fmt.Errorf("%w: NotebookInstanceName is required", errInvalidRequest)
	}

	var minIMDSVersion string
	if req.InstanceMetadataServiceConfiguration != nil {
		minIMDSVersion = req.InstanceMetadataServiceConfiguration.MinimumInstanceMetadataServiceVersion
	}

	if err := h.Backend.UpdateNotebookInstanceFull(ctx, req.NotebookInstanceName, NotebookUpdateOptions{
		InstanceType:                           req.InstanceType,
		RoleArn:                                req.RoleArn,
		LifecycleConfigName:                    req.LifecycleConfigName,
		DefaultCodeRepository:                  req.DefaultCodeRepository,
		PlatformIdentifier:                     req.PlatformIdentifier,
		RootAccess:                             req.RootAccess,
		IPAddressType:                          req.IPAddressType,
		MinimumInstanceMetadataServiceVersion:  minIMDSVersion,
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
// NotebookInstance handlers
// ---------------------------------------------------------------------------

// notebookSummary is NotebookInstanceSummary's wire shape
// (types/types.go:16173-16225, sagemaker@v1.263.2).
type notebookSummary struct {
	NotebookInstanceName                string   `json:"NotebookInstanceName"`
	NotebookInstanceArn                 string   `json:"NotebookInstanceArn"`
	NotebookInstanceStatus              string   `json:"NotebookInstanceStatus"`
	InstanceType                        string   `json:"InstanceType,omitempty"`
	DefaultCodeRepository               string   `json:"DefaultCodeRepository,omitempty"`
	NotebookInstanceLifecycleConfigName string   `json:"NotebookInstanceLifecycleConfigName,omitempty"`
	URL                                 string   `json:"Url,omitempty"`
	AdditionalCodeRepositories          []string `json:"AdditionalCodeRepositories,omitempty"`
	CreationTime                        float64  `json:"CreationTime"`
	LastModifiedTime                    float64  `json:"LastModifiedTime"`
}

// listNotebookInstancesInput is ListNotebookInstances' request shape
// (api_op_ListNotebookInstances.go:31-90).
type listNotebookInstancesInput struct {
	CreationTimeAfter                           *float64 `json:"CreationTimeAfter"`
	CreationTimeBefore                          *float64 `json:"CreationTimeBefore"`
	LastModifiedTimeAfter                       *float64 `json:"LastModifiedTimeAfter"`
	LastModifiedTimeBefore                      *float64 `json:"LastModifiedTimeBefore"`
	StatusEquals                                string   `json:"StatusEquals,omitempty"`
	NameContains                                string   `json:"NameContains,omitempty"`
	AdditionalCodeRepositoryEquals              string   `json:"AdditionalCodeRepositoryEquals,omitempty"`
	DefaultCodeRepositoryContains               string   `json:"DefaultCodeRepositoryContains,omitempty"`
	NotebookInstanceLifecycleConfigNameContains string   `json:"NotebookInstanceLifecycleConfigNameContains,omitempty"`
	NextToken                                   string   `json:"NextToken"`
	SortBy                                      string   `json:"SortBy,omitempty"`
	SortOrder                                   string   `json:"SortOrder,omitempty"`
	MaxResults                                  int32    `json:"MaxResults"`
}

func (h *Handler) handleListNotebookInstances(ctx context.Context, body []byte) ([]byte, error) {
	var req listNotebookInstancesInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	nbs, nextToken := h.Backend.ListNotebookInstances(ctx, ListNotebookInstancesParams{
		CreationTimeAfter:                           timeFromEpochSecondsPtr(req.CreationTimeAfter),
		CreationTimeBefore:                          timeFromEpochSecondsPtr(req.CreationTimeBefore),
		LastModifiedTimeAfter:                       timeFromEpochSecondsPtr(req.LastModifiedTimeAfter),
		LastModifiedTimeBefore:                      timeFromEpochSecondsPtr(req.LastModifiedTimeBefore),
		StatusEquals:                                req.StatusEquals,
		NameContains:                                req.NameContains,
		AdditionalCodeRepositoryEquals:              req.AdditionalCodeRepositoryEquals,
		DefaultCodeRepositoryContains:               req.DefaultCodeRepositoryContains,
		NotebookInstanceLifecycleConfigNameContains: req.NotebookInstanceLifecycleConfigNameContains,
		NextToken:                                   req.NextToken,
		SortBy:                                      req.SortBy,
		SortOrder:                                   req.SortOrder,
		MaxResults:                                  req.MaxResults,
	})
	summaries := make([]notebookSummary, 0, len(nbs))

	for _, nb := range nbs {
		summaries = append(summaries, notebookSummary{
			NotebookInstanceName:                nb.NotebookInstanceName,
			NotebookInstanceArn:                 nb.NotebookInstanceArn,
			NotebookInstanceStatus:              nb.NotebookInstanceStatus,
			InstanceType:                        nb.InstanceType,
			DefaultCodeRepository:               nb.DefaultCodeRepository,
			NotebookInstanceLifecycleConfigName: nb.LifecycleConfigName,
			URL:                                 nb.URL,
			AdditionalCodeRepositories:          nb.AdditionalCodeRepositories,
			CreationTime:                        epochSeconds(nb.CreationTime),
			LastModifiedTime:                    epochSeconds(nb.LastModifiedTime),
		})
	}

	resp := map[string]any{"NotebookInstances": summaries}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return json.Marshal(resp)
}

// deleteNotebookInstanceInput is DeleteNotebookInstance's request shape
// (api_op_DeleteNotebookInstance.go:33-41).
type deleteNotebookInstanceInput struct {
	NotebookInstanceName string `json:"NotebookInstanceName"`
}

func (h *Handler) handleDeleteNotebookInstance(ctx context.Context, body []byte) error {
	var req deleteNotebookInstanceInput

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.NotebookInstanceName == "" {
		return fmt.Errorf("%w: NotebookInstanceName is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteNotebookInstance(ctx, req.NotebookInstanceName); err != nil {
		return err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "sagemaker: deleted notebook instance", "name", req.NotebookInstanceName)

	return nil
}

// startNotebookInstanceInput is StartNotebookInstance's request shape
// (api_op_StartNotebookInstance.go:31-39).
type startNotebookInstanceInput struct {
	NotebookInstanceName string `json:"NotebookInstanceName"`
}

func (h *Handler) handleStartNotebookInstance(ctx context.Context, body []byte) error {
	var req startNotebookInstanceInput

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.NotebookInstanceName == "" {
		return fmt.Errorf("%w: NotebookInstanceName is required", errInvalidRequest)
	}

	if err := h.Backend.StartNotebookInstance(ctx, req.NotebookInstanceName); err != nil {
		return err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "sagemaker: started notebook instance", "name", req.NotebookInstanceName)

	return nil
}

// stopNotebookInstanceInput is StopNotebookInstance's request shape
// (api_op_StopNotebookInstance.go:35-43).
type stopNotebookInstanceInput struct {
	NotebookInstanceName string `json:"NotebookInstanceName"`
}

func (h *Handler) handleStopNotebookInstance(ctx context.Context, body []byte) error {
	var req stopNotebookInstanceInput

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.NotebookInstanceName == "" {
		return fmt.Errorf("%w: NotebookInstanceName is required", errInvalidRequest)
	}

	if err := h.Backend.StopNotebookInstance(ctx, req.NotebookInstanceName); err != nil {
		return err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "sagemaker: stopped notebook instance", "name", req.NotebookInstanceName)

	return nil
}

// createPresignedNotebookInstanceURLInput is
// CreatePresignedNotebookInstanceUrl's request shape
// (api_op_CreatePresignedNotebookInstanceUrl.go:49-60).
// SessionExpirationDurationInSeconds is modeled for wire visibility but
// disclosed no-op: this backend's presigned URL (below) is a static string
// with no TTL/session-expiry enforcement mechanism, the same structural gap
// already disclosed for CreatePresignedMlflowAppUrl/
// CreatePresignedMlflowTrackingServerUrl (parity-11) and hub.go's
// PresignedUrlAccessConfig.
type createPresignedNotebookInstanceURLInput struct {
	SessionExpirationDurationInSeconds *int32 `json:"SessionExpirationDurationInSeconds,omitempty"`
	NotebookInstanceName               string `json:"NotebookInstanceName"`
}

func (h *Handler) handleCreatePresignedNotebookInstanceURL(
	ctx context.Context,
	body []byte,
) ([]byte, error) {
	var req createPresignedNotebookInstanceURLInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.NotebookInstanceName == "" {
		return nil, fmt.Errorf("%w: NotebookInstanceName is required", errInvalidRequest)
	}

	url, err := h.Backend.CreatePresignedNotebookInstanceURL(ctx, req.NotebookInstanceName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{"AuthorizedUrl": url})
}
