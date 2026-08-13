package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// ---------------------------------------------------------------------------
// UserProfile handlers
// ---------------------------------------------------------------------------

// createUserProfileInput is the CreateUserProfile request shape (named, not
// inline, so wire-field-audit tooling that only inspects named types can see
// it — see gopherstack-oc9v). UserSettings is carried as opaque
// json.RawMessage passthrough per this file's established convention.
type createUserProfileInput struct {
	DomainID                   string          `json:"DomainId"`
	UserProfileName            string          `json:"UserProfileName"`
	SingleSignOnUserIdentifier string          `json:"SingleSignOnUserIdentifier"`
	SingleSignOnUserValue      string          `json:"SingleSignOnUserValue"`
	UserSettings               json.RawMessage `json:"UserSettings"`
	Tags                       []tagObject     `json:"Tags"`
}

func (h *Handler) handleCreateUserProfile(ctx context.Context, body []byte) ([]byte, error) {
	var req createUserProfileInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.DomainID == "" {
		return nil, fmt.Errorf("%w: DomainId is required", errInvalidRequest)
	}

	if req.UserProfileName == "" {
		return nil, fmt.Errorf("%w: UserProfileName is required", errInvalidRequest)
	}

	up, err := h.Backend.CreateUserProfile(
		ctx,
		req.DomainID,
		req.UserProfileName,
		fromTagObjects(req.Tags),
		CreateUserProfileOptions{
			SingleSignOnUserIdentifier: req.SingleSignOnUserIdentifier,
			SingleSignOnUserValue:      req.SingleSignOnUserValue,
			UserSettings:               req.UserSettings,
		},
	)
	if err != nil {
		return nil, err
	}

	logger.Load(ctx).InfoContext(ctx, "sagemaker: created user profile", "name", up.UserProfileName)

	return json.Marshal(map[string]string{keyUserProfileArn: up.UserProfileArn})
}

type describeUserProfileInput struct {
	DomainID        string `json:"DomainId"`
	UserProfileName string `json:"UserProfileName"`
}

func (h *Handler) handleDescribeUserProfile(ctx context.Context, body []byte) ([]byte, error) {
	var req describeUserProfileInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.DomainID == "" {
		return nil, fmt.Errorf("%w: DomainId is required", errInvalidRequest)
	}

	if req.UserProfileName == "" {
		return nil, fmt.Errorf("%w: UserProfileName is required", errInvalidRequest)
	}

	up, err := h.Backend.DescribeUserProfile(ctx, req.DomainID, req.UserProfileName)
	if err != nil {
		return nil, err
	}

	resp := map[string]any{
		"DomainId":          up.DomainID,
		"UserProfileName":   up.UserProfileName,
		keyUserProfileArn:   up.UserProfileArn,
		keyStatus:           up.Status,
		keyCreationTime:     epochSeconds(up.CreationTime),
		keyLastModifiedTime: epochSeconds(up.LastModifiedTime),
	}

	if up.SingleSignOnUserIdentifier != "" {
		resp["SingleSignOnUserIdentifier"] = up.SingleSignOnUserIdentifier
	}

	if up.SingleSignOnUserValue != "" {
		resp["SingleSignOnUserValue"] = up.SingleSignOnUserValue
	}

	if len(up.UserSettings) > 0 {
		resp["UserSettings"] = up.UserSettings
	}

	return json.Marshal(resp)
}

type userProfileSummary struct {
	DomainID        string  `json:"DomainId"`
	UserProfileName string  `json:"UserProfileName"`
	UserProfileArn  string  `json:"UserProfileArn"`
	Status          string  `json:"Status"`
	CreationTime    float64 `json:"CreationTime"`
}

type listUserProfilesInput struct {
	DomainIDEquals          string `json:"DomainIDEquals"`
	UserProfileNameContains string `json:"UserProfileNameContains"`
	SortBy                  string `json:"SortBy"`
	SortOrder               string `json:"SortOrder"`
	NextToken               string `json:"NextToken"`
	MaxResults              int32  `json:"MaxResults"`
}

func (h *Handler) handleListUserProfiles(ctx context.Context, body []byte) ([]byte, error) {
	var req listUserProfilesInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	ups, nextToken := h.Backend.ListUserProfiles(ctx, ListUserProfilesParams(req))
	summaries := make([]userProfileSummary, 0, len(ups))

	for _, up := range ups {
		summaries = append(summaries, userProfileSummary{
			DomainID:        up.DomainID,
			UserProfileName: up.UserProfileName,
			UserProfileArn:  up.UserProfileArn,
			Status:          up.Status,
			CreationTime:    epochSeconds(up.CreationTime),
		})
	}

	resp := map[string]any{"UserProfiles": summaries}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return json.Marshal(resp)
}

type deleteUserProfileInput struct {
	DomainID        string `json:"DomainId"`
	UserProfileName string `json:"UserProfileName"`
}

func (h *Handler) handleDeleteUserProfile(ctx context.Context, body []byte) error {
	var req deleteUserProfileInput

	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.DomainID == "" {
		return fmt.Errorf("%w: DomainId is required", errInvalidRequest)
	}

	if req.UserProfileName == "" {
		return fmt.Errorf("%w: UserProfileName is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteUserProfile(ctx, req.DomainID, req.UserProfileName); err != nil {
		return err
	}

	logger.Load(ctx).
		InfoContext(ctx, "sagemaker: deleted user profile", "name", req.UserProfileName)

	return nil
}

type updateUserProfileInput struct {
	DomainID        string `json:"DomainId"`
	UserProfileName string `json:"UserProfileName"`
}

func (h *Handler) handleUpdateUserProfile(ctx context.Context, body []byte) ([]byte, error) {
	var req updateUserProfileInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.DomainID == "" {
		return nil, fmt.Errorf("%w: DomainId is required", errInvalidRequest)
	}

	if req.UserProfileName == "" {
		return nil, fmt.Errorf("%w: UserProfileName is required", errInvalidRequest)
	}

	up, err := h.Backend.UpdateUserProfile(ctx, req.DomainID, req.UserProfileName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyUserProfileArn: up.UserProfileArn})
}
