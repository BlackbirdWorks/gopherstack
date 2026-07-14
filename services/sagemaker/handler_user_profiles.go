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

func (h *Handler) handleCreateUserProfile(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		DomainID        string      `json:"DomainId"`
		UserProfileName string      `json:"UserProfileName"`
		Tags            []tagObject `json:"Tags"`
	}

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
	)
	if err != nil {
		return nil, err
	}

	logger.Load(ctx).InfoContext(ctx, "sagemaker: created user profile", "name", up.UserProfileName)

	return json.Marshal(map[string]string{keyUserProfileArn: up.UserProfileArn})
}

func (h *Handler) handleDescribeUserProfile(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		DomainID        string `json:"DomainId"`
		UserProfileName string `json:"UserProfileName"`
	}

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

	return json.Marshal(map[string]any{
		"DomainId":          up.DomainID,
		"UserProfileName":   up.UserProfileName,
		keyUserProfileArn:   up.UserProfileArn,
		keyStatus:           up.Status,
		keyCreationTime:     epochSeconds(up.CreationTime),
		keyLastModifiedTime: epochSeconds(up.LastModifiedTime),
	})
}

type userProfileSummary struct {
	DomainID        string  `json:"DomainId"`
	UserProfileName string  `json:"UserProfileName"`
	UserProfileArn  string  `json:"UserProfileArn"`
	Status          string  `json:"Status"`
	CreationTime    float64 `json:"CreationTime"`
}

func (h *Handler) handleListUserProfiles(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		DomainIDEquals string `json:"DomainIDEquals"`
		NextToken      string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	ups, nextToken := h.Backend.ListUserProfiles(ctx, req.DomainIDEquals, req.NextToken)
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

func (h *Handler) handleDeleteUserProfile(ctx context.Context, body []byte) error {
	var req struct {
		DomainID        string `json:"DomainId"`
		UserProfileName string `json:"UserProfileName"`
	}

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

func (h *Handler) handleUpdateUserProfile(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		DomainID        string `json:"DomainId"`
		UserProfileName string `json:"UserProfileName"`
	}

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
