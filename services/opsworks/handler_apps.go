package opsworks

import (
	"context"
	"encoding/json"
	"fmt"
)

// handleCreateApp handles CreateApp requests.
func (h *Handler) handleCreateApp(_ context.Context, body []byte) (any, error) {
	var req struct {
		StackID string `json:"StackId"`
		Name    string `json:"Name"`
		Type    string `json:"Type"`
		AppOptions
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	app, err := h.Backend.CreateApp(req.StackID, req.Name, req.Type, req.AppOptions)
	if err != nil {
		return nil, err
	}

	return map[string]any{keyAppID: app.AppID}, nil
}

// handleDescribeApps handles DescribeApps requests.
func (h *Handler) handleDescribeApps(_ context.Context, body []byte) (any, error) {
	var req struct {
		StackID string   `json:"StackId"`
		AppIDs  []string `json:"AppIds"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
		}
	}

	apps, err := h.Backend.DescribeApps(req.StackID, req.AppIDs)
	if err != nil {
		return nil, err
	}

	return map[string]any{"Apps": appsToJSON(apps)}, nil
}

// handleUpdateApp handles UpdateApp requests.
func (h *Handler) handleUpdateApp(_ context.Context, body []byte) (any, error) {
	var req struct {
		AppID string `json:"AppId"`
		Name  string `json:"Name"`
		Type  string `json:"Type"`
		AppOptions
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.UpdateApp(req.AppID, req.Name, req.Type, req.AppOptions); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleDeleteApp handles DeleteApp requests.
func (h *Handler) handleDeleteApp(_ context.Context, body []byte) (any, error) {
	var req struct {
		AppID string `json:"AppId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.DeleteApp(req.AppID); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

const filteredValue = "*****FILTERED*****"

// appsToJSON deliberately omits Arn: the real types.App has no Arn member
// (see App's doc comment in interfaces.go) -- a previous pass invented one
// and serialized it on the wire.
func appsToJSON(apps []*App) []map[string]any {
	result := make([]map[string]any, 0, len(apps))
	for _, a := range apps {
		m := map[string]any{
			keyAppID:     a.AppID,
			keyStackID:   a.StackID,
			keyName:      a.Name,
			keyType:      a.Type,
			keyCreatedAt: formatOpsWorksTime(a.CreatedAt),
		}
		addAppOptions(m, a.Options)
		result = append(result, m)
	}

	return result
}

// addAppOptions renders set options; Source Password/SshKey and Secure env values are masked per types.go.
func addAppOptions(m map[string]any, o AppOptions) {
	if o.Description != "" {
		m["Description"] = o.Description
	}

	if o.Shortname != "" {
		m["Shortname"] = o.Shortname
	}

	if len(o.Attributes) > 0 {
		m["Attributes"] = o.Attributes
	}

	if len(o.Domains) > 0 {
		m["Domains"] = o.Domains
	}

	if o.EnableSsl != nil {
		m["EnableSsl"] = *o.EnableSsl
	}

	if o.SslConfiguration != nil {
		m["SslConfiguration"] = o.SslConfiguration
	}

	if len(o.DataSources) > 0 {
		m["DataSources"] = o.DataSources
	}

	if o.AppSource != nil {
		src := *o.AppSource
		if src.Password != "" {
			src.Password = filteredValue
		}

		if src.SSHKey != "" {
			src.SSHKey = filteredValue
		}

		m["AppSource"] = src
	}

	if len(o.Environment) > 0 {
		env := make([]AppEnvVar, len(o.Environment))
		for i, e := range o.Environment {
			if e.Secure != nil && *e.Secure {
				e.Value = filteredValue
			}

			env[i] = e
		}

		m["Environment"] = env
	}
}
