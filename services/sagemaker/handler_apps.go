package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// ---------------------------------------------------------------------------
// App handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateApp(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		DomainID        string      `json:"DomainId"`
		UserProfileName string      `json:"UserProfileName"`
		AppType         string      `json:"AppType"`
		AppName         string      `json:"AppName"`
		Tags            []tagObject `json:"Tags"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.DomainID == "" {
		return nil, fmt.Errorf("%w: DomainId is required", errInvalidRequest)
	}

	if req.AppType == "" {
		return nil, fmt.Errorf("%w: AppType is required", errInvalidRequest)
	}

	if req.AppName == "" {
		return nil, fmt.Errorf("%w: AppName is required", errInvalidRequest)
	}

	a, err := h.Backend.CreateApp(
		ctx,
		req.DomainID,
		req.UserProfileName,
		req.AppType,
		req.AppName,
		fromTagObjects(req.Tags),
	)
	if err != nil {
		return nil, err
	}

	logger.Load(ctx).InfoContext(ctx, "sagemaker: created app", "name", a.AppName, "arn", a.AppArn)

	return json.Marshal(map[string]string{keyAppArn: a.AppArn})
}

func (h *Handler) handleDescribeApp(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		DomainID        string `json:"DomainId"`
		UserProfileName string `json:"UserProfileName"`
		AppType         string `json:"AppType"`
		AppName         string `json:"AppName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.DomainID == "" {
		return nil, fmt.Errorf("%w: DomainId is required", errInvalidRequest)
	}

	if req.AppType == "" {
		return nil, fmt.Errorf("%w: AppType is required", errInvalidRequest)
	}

	if req.AppName == "" {
		return nil, fmt.Errorf("%w: AppName is required", errInvalidRequest)
	}

	a, err := h.Backend.DescribeApp(ctx, req.DomainID, req.UserProfileName, req.AppType, req.AppName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"DomainId":         a.DomainID,
		keyUserProfileName: a.UserProfileName,
		"AppType":          a.AppType,
		"AppName":          a.AppName,
		keyAppArn:          a.AppArn,
		keyStatus:          a.Status,
		keyCreationTime:    epochSeconds(a.CreationTime),
	})
}

type appSummary struct {
	DomainID        string  `json:"DomainId"`
	UserProfileName string  `json:"UserProfileName"`
	AppType         string  `json:"AppType"`
	AppName         string  `json:"AppName"`
	AppArn          string  `json:"AppArn"`
	Status          string  `json:"Status"`
	CreationTime    float64 `json:"CreationTime"`
}

func (h *Handler) handleListApps(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		DomainIDEquals string `json:"DomainIDEquals"`
		NextToken      string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	apps, nextToken := h.Backend.ListApps(ctx, req.DomainIDEquals, req.NextToken)
	summaries := make([]appSummary, 0, len(apps))

	for _, a := range apps {
		summaries = append(summaries, appSummary{
			DomainID:        a.DomainID,
			UserProfileName: a.UserProfileName,
			AppType:         a.AppType,
			AppName:         a.AppName,
			AppArn:          a.AppArn,
			Status:          a.Status,
			CreationTime:    epochSeconds(a.CreationTime),
		})
	}

	resp := map[string]any{"Apps": summaries}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return json.Marshal(resp)
}

func (h *Handler) handleDeleteApp(ctx context.Context, body []byte) error {
	var req struct {
		DomainID        string `json:"DomainId"`
		UserProfileName string `json:"UserProfileName"`
		AppType         string `json:"AppType"`
		AppName         string `json:"AppName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.DomainID == "" {
		return fmt.Errorf("%w: DomainId is required", errInvalidRequest)
	}

	if req.AppType == "" {
		return fmt.Errorf("%w: AppType is required", errInvalidRequest)
	}

	if req.AppName == "" {
		return fmt.Errorf("%w: AppName is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteApp(ctx, req.DomainID, req.UserProfileName, req.AppType, req.AppName); err != nil {
		return err
	}

	logger.Load(ctx).InfoContext(ctx, "sagemaker: deleted app", "name", req.AppName)

	return nil
}
