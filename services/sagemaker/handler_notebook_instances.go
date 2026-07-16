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
	)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "sagemaker: created notebook lifecycle config", "name", lc.Name)

	return json.Marshal(map[string]string{keyNotebookLifecycleConfigArn: lc.ARN})
}

func (h *Handler) handleDescribeNotebookInstanceLifecycleConfig(
	ctx context.Context,
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

func (h *Handler) handleUpdateNotebookInstanceLifecycleConfig(
	ctx context.Context,
	body []byte,
) ([]byte, error) {
	var req struct {
		NotebookInstanceLifecycleConfigName string                 `json:"NotebookInstanceLifecycleConfigName"`
		OnCreate                            []lifecycleHookRequest `json:"OnCreate,omitempty"`
		OnStart                             []lifecycleHookRequest `json:"OnStart,omitempty"`
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

func (h *Handler) handleListNotebookInstanceLifecycleConfigs(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	configs, nextToken := h.Backend.ListNotebookInstanceLifecycleConfigs(ctx, req.NextToken)
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

type createNotebookInstanceFullRequest struct {
	LifecycleConfigName        string      `json:"NotebookInstanceLifecycleConfigName,omitempty"`
	DefaultCodeRepository      string      `json:"DefaultCodeRepository,omitempty"`
	RootAccess                 string      `json:"RootAccess,omitempty"`
	DirectInternetAccess       string      `json:"DirectInternetAccess,omitempty"`
	NotebookInstanceName       string      `json:"NotebookInstanceName"`
	InstanceType               string      `json:"InstanceType"`
	PlatformIdentifier         string      `json:"PlatformIdentifier,omitempty"`
	KmsKeyID                   string      `json:"KmsKeyId,omitempty"`
	RoleArn                    string      `json:"RoleArn"`
	SubnetID                   string      `json:"SubnetId,omitempty"`
	AdditionalCodeRepositories []string    `json:"AdditionalCodeRepositories,omitempty"`
	AcceleratorTypes           []string    `json:"AcceleratorTypes,omitempty"`
	Tags                       []tagObject `json:"Tags"`
	SecurityGroupIDs           []string    `json:"SecurityGroupIds,omitempty"`
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

	nb, err := h.Backend.CreateNotebookInstanceFSM(ctx, NotebookInstanceOptions{
		Name:                       req.NotebookInstanceName,
		InstanceType:               req.InstanceType,
		RoleArn:                    req.RoleArn,
		SubnetID:                   req.SubnetID,
		SecurityGroupIDs:           req.SecurityGroupIDs,
		KmsKeyID:                   req.KmsKeyID,
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

	return json.Marshal(map[string]string{keyNotebookInstanceArn: nb.NotebookInstanceArn})
}

// handleDescribeNotebookInstanceFull returns all notebook fields.
func (h *Handler) handleDescribeNotebookInstanceFull(
	ctx context.Context,
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
}

// handleUpdateNotebookInstanceFull supports all mutable fields (#1 update coverage).
func (h *Handler) handleUpdateNotebookInstanceFull(ctx context.Context, body []byte) error {
	var req struct {
		NotebookInstanceName                   string   `json:"NotebookInstanceName"`
		InstanceType                           string   `json:"InstanceType,omitempty"`
		RoleArn                                string   `json:"RoleArn,omitempty"`
		LifecycleConfigName                    string   `json:"NotebookInstanceLifecycleConfigName,omitempty"`
		DefaultCodeRepository                  string   `json:"DefaultCodeRepository,omitempty"`
		AdditionalCodeRepositories             []string `json:"AdditionalCodeRepositories,omitempty"`
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

	if err := h.Backend.UpdateNotebookInstanceFull(ctx, req.NotebookInstanceName, NotebookUpdateOptions{
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
