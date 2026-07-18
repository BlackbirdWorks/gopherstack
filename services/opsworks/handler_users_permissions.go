package opsworks

import (
	"context"
	"encoding/json"
	"fmt"
)

// handleCreateUserProfile handles CreateUserProfile requests.
func (h *Handler) handleCreateUserProfile(_ context.Context, body []byte) (any, error) {
	var req struct {
		IamUserArn          string `json:"IamUserArn"`
		SSHUsername         string `json:"SshUsername"`
		SSHPublicKey        string `json:"SshPublicKey"`
		AllowSelfManagement bool   `json:"AllowSelfManagement"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	profile, err := h.Backend.CreateUserProfile(
		req.IamUserArn,
		req.SSHUsername,
		req.SSHPublicKey,
		req.AllowSelfManagement,
	)
	if err != nil {
		return nil, err
	}

	return map[string]any{fieldIamUserArn: profile.IamUserArn}, nil
}

// handleDeleteUserProfile handles DeleteUserProfile requests.
func (h *Handler) handleDeleteUserProfile(_ context.Context, body []byte) (any, error) {
	var req struct {
		IamUserArn string `json:"IamUserArn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.DeleteUserProfile(req.IamUserArn); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleDescribeUserProfiles handles DescribeUserProfiles requests.
func (h *Handler) handleDescribeUserProfiles(_ context.Context, body []byte) (any, error) {
	var req struct {
		IamUserArns []string `json:"IamUserArns"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
		}
	}

	profiles, err := h.Backend.DescribeUserProfiles(req.IamUserArns)
	if err != nil {
		return nil, err
	}

	return map[string]any{"UserProfiles": userProfilesToJSON(profiles)}, nil
}

// handleUpdateUserProfile handles UpdateUserProfile requests.
func (h *Handler) handleUpdateUserProfile(_ context.Context, body []byte) (any, error) {
	var req struct {
		IamUserArn   string `json:"IamUserArn"`
		SSHUsername  string `json:"SshUsername"`
		SSHPublicKey string `json:"SshPublicKey"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.UpdateUserProfile(req.IamUserArn, req.SSHUsername, req.SSHPublicKey); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleDescribeMyUserProfile handles DescribeMyUserProfile requests.
func (h *Handler) handleDescribeMyUserProfile(_ context.Context, _ []byte) (any, error) {
	profile, err := h.Backend.DescribeMyUserProfile()
	if err != nil {
		return nil, err
	}

	return map[string]any{"UserProfile": userProfileToJSON(profile)}, nil
}

// handleUpdateMyUserProfile handles UpdateMyUserProfile requests.
func (h *Handler) handleUpdateMyUserProfile(_ context.Context, body []byte) (any, error) {
	var req struct {
		SSHPublicKey string `json:"SshPublicKey"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.UpdateMyUserProfile(req.SSHPublicKey); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

func userProfileToJSON(u *UserProfile) map[string]any {
	return map[string]any{
		fieldIamUserArn:       u.IamUserArn,
		keyName:               u.Name,
		"SshUsername":         u.SSHUsername,
		"SshPublicKey":        u.SSHPublicKey,
		"AllowSelfManagement": u.AllowSelfManagement,
	}
}

func userProfilesToJSON(profiles []*UserProfile) []map[string]any {
	result := make([]map[string]any, 0, len(profiles))
	for _, u := range profiles {
		result = append(result, userProfileToJSON(u))
	}

	return result
}

// handleSetPermission handles SetPermission requests.
func (h *Handler) handleSetPermission(_ context.Context, body []byte) (any, error) {
	var req struct {
		StackID    string `json:"StackId"`
		IamUserArn string `json:"IamUserArn"`
		Level      string `json:"Level"`
		AllowSSH   bool   `json:"AllowSsh"`
		AllowSudo  bool   `json:"AllowSudo"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.SetPermission(req.StackID, req.IamUserArn, req.Level, req.AllowSSH, req.AllowSudo); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleDescribePermissions handles DescribePermissions requests.
func (h *Handler) handleDescribePermissions(_ context.Context, body []byte) (any, error) {
	var req struct {
		StackID    string `json:"StackId"`
		IamUserArn string `json:"IamUserArn"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
		}
	}

	perms, err := h.Backend.DescribePermissions(req.StackID, req.IamUserArn)
	if err != nil {
		return nil, err
	}

	return map[string]any{"Permissions": permissionsToJSON(perms)}, nil
}

func permissionsToJSON(perms []*Permission) []map[string]any {
	result := make([]map[string]any, 0, len(perms))
	for _, p := range perms {
		result = append(result, map[string]any{
			keyStackID:      p.StackID,
			fieldIamUserArn: p.IamUserArn,
			"Level":         p.Level,
			"AllowSsh":      p.AllowSSH,
			"AllowSudo":     p.AllowSudo,
		})
	}

	return result
}

// handleGrantAccess handles GrantAccess requests.
func (h *Handler) handleGrantAccess(_ context.Context, body []byte) (any, error) {
	var req struct {
		InstanceID        string `json:"InstanceId"`
		ValidForInMinutes int32  `json:"ValidForInMinutes"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	creds, err := h.Backend.GrantAccess(req.InstanceID, req.ValidForInMinutes)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"TemporaryCredential": map[string]any{
			"InstanceId":        creds.InstanceID,
			"Username":          creds.Username,
			"Password":          creds.Password,
			"ValidForInMinutes": creds.ValidForInMinutes,
		},
	}, nil
}
