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

// createAppInput is the CreateApp request shape (named, not inline, so
// wire-field-audit tooling that only inspects named types can see it — see
// gopherstack-oc9v).
type createAppInput struct {
	DomainID        string          `json:"DomainId"`
	UserProfileName string          `json:"UserProfileName"`
	SpaceName       string          `json:"SpaceName"`
	AppType         string          `json:"AppType"`
	AppName         string          `json:"AppName"`
	ResourceSpec    json.RawMessage `json:"ResourceSpec"`
	Tags            []tagObject     `json:"Tags"`
	RecoveryMode    bool            `json:"RecoveryMode"`
}

func (h *Handler) handleCreateApp(ctx context.Context, body []byte) ([]byte, error) {
	var req createAppInput

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
	// Exactly one of UserProfileName/SpaceName identifies the app's owner in
	// the real API ("The name of the space. If this value is not set, then
	// UserProfileName must be set.") — previously only UserProfileName
	// existed on the wire struct at all, so a client creating an app for a
	// Space (a resource this backend has supported since spaces.go) had no
	// way to do so through CreateApp.
	if req.UserProfileName == "" && req.SpaceName == "" {
		return nil, fmt.Errorf(
			"%w: one of UserProfileName or SpaceName is required", errInvalidRequest,
		)
	}

	if req.UserProfileName != "" && req.SpaceName != "" {
		return nil, fmt.Errorf(
			"%w: UserProfileName and SpaceName cannot both be set", errInvalidRequest,
		)
	}

	a, err := h.Backend.CreateApp(
		ctx,
		req.DomainID,
		req.UserProfileName,
		req.AppType,
		req.AppName,
		fromTagObjects(req.Tags),
		CreateAppOptions{
			SpaceName:    req.SpaceName,
			ResourceSpec: req.ResourceSpec,
			RecoveryMode: req.RecoveryMode,
		},
	)
	if err != nil {
		return nil, err
	}

	logger.Load(ctx).InfoContext(ctx, "sagemaker: created app", "name", a.AppName, "arn", a.AppArn)

	return json.Marshal(map[string]string{keyAppArn: a.AppArn})
}

type describeAppInput struct {
	DomainID        string `json:"DomainId"`
	UserProfileName string `json:"UserProfileName"`
	SpaceName       string `json:"SpaceName"`
	AppType         string `json:"AppType"`
	AppName         string `json:"AppName"`
}

func (h *Handler) handleDescribeApp(ctx context.Context, body []byte) ([]byte, error) {
	var req describeAppInput

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

	a, err := h.Backend.DescribeApp(ctx, req.DomainID, req.UserProfileName, req.SpaceName, req.AppType, req.AppName)
	if err != nil {
		return nil, err
	}

	resp := map[string]any{
		"DomainId":      a.DomainID,
		"AppType":       a.AppType,
		"AppName":       a.AppName,
		keyAppArn:       a.AppArn,
		keyStatus:       a.Status,
		keyCreationTime: epochSeconds(a.CreationTime),
		"RecoveryMode":  a.RecoveryMode,
	}

	if a.UserProfileName != "" {
		resp[keyUserProfileName] = a.UserProfileName
	}

	if a.SpaceName != "" {
		resp["SpaceName"] = a.SpaceName
	}

	if len(a.ResourceSpec) > 0 {
		resp["ResourceSpec"] = a.ResourceSpec
	}

	return json.Marshal(resp)
}

type appSummary struct {
	DomainID        string  `json:"DomainId"`
	UserProfileName string  `json:"UserProfileName,omitempty"`
	SpaceName       string  `json:"SpaceName,omitempty"`
	AppType         string  `json:"AppType"`
	AppName         string  `json:"AppName"`
	AppArn          string  `json:"AppArn"`
	Status          string  `json:"Status"`
	CreationTime    float64 `json:"CreationTime"`
}

type listAppsInput struct {
	DomainIDEquals        string `json:"DomainIDEquals"`
	UserProfileNameEquals string `json:"UserProfileNameEquals"`
	SpaceNameEquals       string `json:"SpaceNameEquals"`
	SortBy                string `json:"SortBy"`
	SortOrder             string `json:"SortOrder"`
	NextToken             string `json:"NextToken"`
	MaxResults            int32  `json:"MaxResults"`
}

func (h *Handler) handleListApps(ctx context.Context, body []byte) ([]byte, error) {
	var req listAppsInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	apps, nextToken := h.Backend.ListApps(ctx, ListAppsParams{
		DomainIDEquals:        req.DomainIDEquals,
		UserProfileNameEquals: req.UserProfileNameEquals,
		SpaceNameEquals:       req.SpaceNameEquals,
		SortOrder:             req.SortOrder,
		NextToken:             req.NextToken,
		MaxResults:            req.MaxResults,
	})
	summaries := make([]appSummary, 0, len(apps))

	for _, a := range apps {
		summaries = append(summaries, appSummary{
			DomainID:        a.DomainID,
			UserProfileName: a.UserProfileName,
			SpaceName:       a.SpaceName,
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

type deleteAppInput struct {
	DomainID        string `json:"DomainId"`
	UserProfileName string `json:"UserProfileName"`
	SpaceName       string `json:"SpaceName"`
	AppType         string `json:"AppType"`
	AppName         string `json:"AppName"`
}

func (h *Handler) handleDeleteApp(ctx context.Context, body []byte) error {
	var req deleteAppInput

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

	if err := h.Backend.DeleteApp(
		ctx, req.DomainID, req.UserProfileName, req.SpaceName, req.AppType, req.AppName,
	); err != nil {
		return err
	}

	logger.Load(ctx).InfoContext(ctx, "sagemaker: deleted app", "name", req.AppName)

	return nil
}
