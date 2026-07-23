package rolesanywhere

import (
	"context"
	"encoding/json"
	"net/http"
	"time"
)

// ---- Profile handlers ----

func (h *Handler) handleCreateProfile(ctx context.Context, body []byte) (any, int, error) {
	var req struct {
		DurationSeconds           *int32     `json:"durationSeconds"`
		Enabled                   *bool      `json:"enabled"`
		AcceptRoleSessionName     *bool      `json:"acceptRoleSessionName"`
		Name                      string     `json:"name"`
		SessionPolicy             string     `json:"sessionPolicy"`
		RoleArns                  []string   `json:"roleArns"`
		Tags                      []TagEntry `json:"tags"`
		ManagedPolicyArns         []string   `json:"managedPolicyArns"`
		RequireInstanceProperties bool       `json:"requireInstanceProperties"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, 0, ErrValidation
	}

	if len(req.Tags) > maxResourceTags {
		return nil, 0, ErrValidation
	}

	p, err := h.Backend.CreateProfile(
		ctx, req.Name, req.RoleArns, req.Tags,
		req.DurationSeconds, req.ManagedPolicyArns,
		req.SessionPolicy, req.RequireInstanceProperties,
		req.Enabled, req.AcceptRoleSessionName,
	)
	if err != nil {
		return nil, 0, err
	}

	return map[string]any{keyProfile: h.profileJSON(ctx, p)}, http.StatusCreated, nil
}

func (h *Handler) handleGetProfile(ctx context.Context, path string) (any, int, error) {
	id := extractID(path, pathProfile)

	p, err := h.Backend.GetProfile(ctx, id)
	if err != nil {
		return nil, 0, err
	}

	return map[string]any{keyProfile: h.profileJSON(ctx, p)}, http.StatusOK, nil
}

func (h *Handler) handleListProfiles(ctx context.Context, query string) (any, int, error) {
	pageToken, maxResults, ppErr := parsePageParams(query)
	if ppErr != nil {
		return nil, 0, ppErr
	}

	all, next, err := h.Backend.ListProfiles(ctx, pageToken, maxResults)
	if err != nil {
		return nil, 0, err
	}

	list := make([]any, 0, len(all))

	for _, p := range all {
		list = append(list, h.profileJSON(ctx, p))
	}

	resp := map[string]any{keyProfiles: list}

	if next != "" {
		resp["nextToken"] = next
	}

	return resp, http.StatusOK, nil
}

func (h *Handler) handleDeleteProfile(ctx context.Context, path string) (any, int, error) {
	id := extractID(path, pathProfile)

	p, err := h.Backend.DeleteProfile(ctx, id)
	if err != nil {
		return nil, 0, err
	}

	return map[string]any{keyProfile: h.profileJSON(ctx, p)}, http.StatusOK, nil
}

func (h *Handler) handleUpdateProfile(ctx context.Context, path string, body []byte) (any, int, error) {
	id := extractID(path, pathProfile)

	var req struct {
		DurationSeconds           *int32   `json:"durationSeconds"`
		RequireInstanceProperties *bool    `json:"requireInstanceProperties"`
		AcceptRoleSessionName     *bool    `json:"acceptRoleSessionName"`
		Name                      string   `json:"name"`
		SessionPolicy             string   `json:"sessionPolicy"`
		RoleArns                  []string `json:"roleArns"`
		ManagedPolicyArns         []string `json:"managedPolicyArns"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, 0, ErrValidation
	}

	p, err := h.Backend.UpdateProfile(
		ctx, id, req.Name, req.RoleArns,
		req.DurationSeconds, req.ManagedPolicyArns,
		req.SessionPolicy, req.RequireInstanceProperties,
		req.AcceptRoleSessionName,
	)
	if err != nil {
		return nil, 0, err
	}

	return map[string]any{keyProfile: h.profileJSON(ctx, p)}, http.StatusOK, nil
}

func (h *Handler) handleEnableProfile(ctx context.Context, path string) (any, int, error) {
	id := extractID(path, pathProfile)

	p, err := h.Backend.EnableProfile(ctx, id)
	if err != nil {
		return nil, 0, err
	}

	return map[string]any{keyProfile: h.profileJSON(ctx, p)}, http.StatusOK, nil
}

func (h *Handler) handleDisableProfile(ctx context.Context, path string) (any, int, error) {
	id := extractID(path, pathProfile)

	p, err := h.Backend.DisableProfile(ctx, id)
	if err != nil {
		return nil, 0, err
	}

	return map[string]any{keyProfile: h.profileJSON(ctx, p)}, http.StatusOK, nil
}

// ---- JSON serialization ----

// profileJSON renders p together with its current attribute mappings. AWS's
// ProfileDetail carries attributeMappings on every read (Get, List, Create,
// Update, Enable, Disable, Delete), not only on the dedicated
// Put/DeleteAttributeMapping responses, so every profile handler routes
// through this instead of the bare profileToJSON.
func (h *Handler) profileJSON(ctx context.Context, p *Profile) map[string]any {
	mappings := h.Backend.GetAttributeMappings(ctx, p.ProfileID)

	return profileWithMappingsToJSON(p, mappings)
}

// profileToJSON renders p's base fields. Deliberately no "tags" key: real
// AWS's ProfileDetail carries no tags field at all (confirmed field-by-field
// against aws-sdk-go-v2/service/rolesanywhere/types.ProfileDetail) -- tags
// are visible only via ListTagsForResource. See trustAnchorToJSON's doc
// comment for why a prior version's inclusion of p.Tags here was a bug, not
// a feature.
func profileToJSON(p *Profile) map[string]any {
	m := map[string]any{
		"profileId":  p.ProfileID,
		"profileArn": p.ProfileArn,
		"name":       p.Name, //nolint:goconst // existing issue.
		"roleArns":   p.RoleArns,
		"enabled":    p.Enabled,                        //nolint:goconst // existing issue.
		"createdAt":  p.CreatedAt.Format(time.RFC3339), //nolint:goconst // existing issue.
		"updatedAt":  p.UpdatedAt.Format(time.RFC3339), //nolint:goconst // existing issue.
	}

	if p.CreatedBy != "" {
		m["createdBy"] = p.CreatedBy
	}

	if p.DurationSeconds != nil {
		m["durationSeconds"] = *p.DurationSeconds
	}

	if len(p.ManagedPolicyArns) > 0 {
		m["managedPolicyArns"] = p.ManagedPolicyArns
	}

	if p.SessionPolicy != "" {
		m["sessionPolicy"] = p.SessionPolicy
	}

	if p.RequireInstanceProperties {
		m["requireInstanceProperties"] = true
	}

	if p.AcceptRoleSessionName {
		m["acceptRoleSessionName"] = true
	}

	return m
}

func profileWithMappingsToJSON(p *Profile, mappings []AttributeMapping) map[string]any {
	m := profileToJSON(p)

	if len(mappings) > 0 {
		m["attributeMappings"] = mappings
	}

	return m
}
