package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
)

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

func (h *Handler) handleCreatePresignedMlflowTrackingServerURL(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		TrackingServerName string `json:"TrackingServerName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.TrackingServerName == "" {
		return nil, fmt.Errorf("%w: TrackingServerName is required", errInvalidRequest)
	}

	url, err := h.Backend.CreatePresignedMlflowTrackingServerURL(ctx, req.TrackingServerName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyAuthorizedURL: url})
}

// ---------------------------------------------------------------------------
// MlflowApp handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateMlflowApp(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Tags                  map[string]string `json:"Tags"`
		Name                  string            `json:"Name"`
		ArtifactStoreURI      string            `json:"ArtifactStoreUri"`
		RoleArn               string            `json:"RoleArn"`
		AccountDefaultStatus  string            `json:"AccountDefaultStatus,omitempty"`
		ModelRegistrationMode string            `json:"ModelRegistrationMode,omitempty"`
		DefaultDomainIDList   []string          `json:"DefaultDomainIdList,omitempty"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateMlflowApp(ctx, CreateMlflowAppOptions{
		Name:                  req.Name,
		ArtifactStoreURI:      req.ArtifactStoreURI,
		RoleArn:               req.RoleArn,
		AccountDefaultStatus:  req.AccountDefaultStatus,
		ModelRegistrationMode: req.ModelRegistrationMode,
		DefaultDomainIDList:   req.DefaultDomainIDList,
		Tags:                  req.Tags,
	})
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyGenericArn: result.Arn})
}

// describeMlflowAppResponse is the response body for DescribeMlflowApp.
type describeMlflowAppResponse struct {
	Arn                   string   `json:"Arn"`
	Name                  string   `json:"Name"`
	Status                string   `json:"Status"`
	ArtifactStoreURI      string   `json:"ArtifactStoreUri,omitempty"`
	RoleArn               string   `json:"RoleArn,omitempty"`
	MlflowVersion         string   `json:"MlflowVersion,omitempty"`
	AccountDefaultStatus  string   `json:"AccountDefaultStatus,omitempty"`
	ModelRegistrationMode string   `json:"ModelRegistrationMode,omitempty"`
	DefaultDomainIDList   []string `json:"DefaultDomainIdList,omitempty"`
	CreationTime          float64  `json:"CreationTime"`
	LastModifiedTime      float64  `json:"LastModifiedTime"`
}

func (h *Handler) handleDescribeMlflowApp(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Arn string `json:"Arn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Arn == "" {
		return nil, fmt.Errorf("%w: Arn is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeMlflowApp(ctx, req.Arn)
	if err != nil {
		return nil, err
	}

	return json.Marshal(describeMlflowAppResponse{
		Arn:                   result.Arn,
		Name:                  result.Name,
		Status:                result.Status,
		ArtifactStoreURI:      result.ArtifactStoreURI,
		RoleArn:               result.RoleArn,
		MlflowVersion:         result.MlflowVersion,
		AccountDefaultStatus:  result.AccountDefaultStatus,
		ModelRegistrationMode: result.ModelRegistrationMode,
		DefaultDomainIDList:   result.DefaultDomainIDList,
		CreationTime:          epochSeconds(result.CreationTime),
		LastModifiedTime:      epochSeconds(result.LastModifiedTime),
	})
}

func (h *Handler) handleDeleteMlflowApp(ctx context.Context, body []byte) error {
	var req struct {
		Arn string `json:"Arn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Arn == "" {
		return fmt.Errorf("%w: Arn is required", errInvalidRequest)
	}

	return h.Backend.DeleteMlflowApp(ctx, req.Arn)
}

func (h *Handler) handleUpdateMlflowApp(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Arn                   string   `json:"Arn"`
		ArtifactStoreURI      string   `json:"ArtifactStoreUri,omitempty"`
		AccountDefaultStatus  string   `json:"AccountDefaultStatus,omitempty"`
		ModelRegistrationMode string   `json:"ModelRegistrationMode,omitempty"`
		DefaultDomainIDList   []string `json:"DefaultDomainIdList,omitempty"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Arn == "" {
		return nil, fmt.Errorf("%w: Arn is required", errInvalidRequest)
	}

	result, err := h.Backend.UpdateMlflowApp(ctx, UpdateMlflowAppOptions{
		Arn:                   req.Arn,
		ArtifactStoreURI:      req.ArtifactStoreURI,
		AccountDefaultStatus:  req.AccountDefaultStatus,
		ModelRegistrationMode: req.ModelRegistrationMode,
		DefaultDomainIDList:   req.DefaultDomainIDList,
	})
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyGenericArn: result.Arn})
}

func (h *Handler) handleListMlflowApps(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	apps, nextToken := h.Backend.ListMlflowApps(ctx, req.NextToken)

	items := make([]map[string]any, 0, len(apps))
	for _, a := range apps {
		items = append(items, map[string]any{
			keyGenericArn:       a.Arn,
			keyGenericName:      a.Name,
			keyStatus:           a.Status,
			keyCreationTime:     epochSeconds(a.CreationTime),
			keyLastModifiedTime: epochSeconds(a.LastModifiedTime),
		})
	}

	return listResp("Summaries", items, nextToken)
}

func (h *Handler) handleCreatePresignedMlflowAppURL(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Arn string `json:"Arn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Arn == "" {
		return nil, fmt.Errorf("%w: Arn is required", errInvalidRequest)
	}

	url, err := h.Backend.CreatePresignedMlflowAppURL(ctx, req.Arn)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyAuthorizedURL: url})
}

// ---------------------------------------------------------------------------
// MLflow tracking server handlers (list + update)
// ---------------------------------------------------------------------------

func (h *Handler) handleListMlflowTrackingServers(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	servers, nextToken := h.Backend.ListMlflowTrackingServers(ctx, req.NextToken)

	items := make([]map[string]any, 0, len(servers))
	for _, s := range servers {
		entry := map[string]any{
			"TrackingServerName":   s.TrackingServerName,
			"TrackingServerArn":    s.TrackingServerArn,
			"TrackingServerStatus": s.TrackingServerStatus,
			keyCreationTime:        epochSeconds(s.CreationTime),
			keyLastModifiedTime:    epochSeconds(s.LastModifiedTime),
		}
		if s.MlflowVersion != "" {
			entry["MlflowVersion"] = s.MlflowVersion
		}

		items = append(items, entry)
	}

	return listResp("TrackingServerSummaries", items, nextToken)
}

func (h *Handler) handleUpdateMlflowTrackingServer(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		TrackingServerName string `json:"TrackingServerName"`
		MlflowVersion      string `json:"MlflowVersion,omitempty"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.TrackingServerName == "" {
		return nil, fmt.Errorf("%w: TrackingServerName is required", errInvalidRequest)
	}

	s, err := h.Backend.UpdateMlflowTrackingServer(ctx, req.TrackingServerName, req.MlflowVersion)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyTrackingServerArn: s.TrackingServerArn})
}
