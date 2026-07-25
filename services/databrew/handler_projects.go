package databrew

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
)

func parseProjectOp(method, name, subOp string) string {
	if subOp == "sendProjectSessionAction" && method == http.MethodPut {
		return opSendProjectSessionAction
	}
	if subOp == "startProjectSession" && method == http.MethodPut {
		return opStartProjectSession
	}
	switch method {
	case http.MethodPost:
		if name == "" {
			return opCreateProject
		}
	case http.MethodGet:
		if name == "" {
			return opListProjects
		}

		return opDescribeProject
	case http.MethodPut:
		if name != "" {
			return opUpdateProject
		}
	case http.MethodDelete:
		if name != "" {
			return opDeleteProject
		}
	}

	return opUnknown
}

func (h *Handler) dispatchProject(
	ctx context.Context,
	action string,
	body []byte,
) ([]byte, bool, error) {
	switch action {
	case opCreateProject:
		r, e := h.handleCreateProject(ctx, body)

		return r, true, e
	case opDescribeProject:
		r, e := h.handleDescribeProject(ctx, body)

		return r, true, e
	case opListProjects:
		r, e := h.handleListProjects(ctx, body)

		return r, true, e
	case opUpdateProject:
		r, e := h.handleUpdateProject(ctx, body)

		return r, true, e
	case opDeleteProject:
		r, e := h.handleDeleteProject(ctx, body)

		return r, true, e
	case opStartProjectSession:
		r, e := h.handleStartProjectSession(ctx, body)

		return r, true, e
	case opSendProjectSessionAction:
		r, e := h.handleSendProjectSessionAction(ctx, body)

		return r, true, e
	}

	return nil, false, nil
}

func (h *Handler) handleCreateProject(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Tags        map[string]string `json:"Tags"`
		Name        string            `json:"Name"`
		DatasetName string            `json:"DatasetName"`
		RecipeName  string            `json:"RecipeName"`
		RoleArn     string            `json:"RoleArn"`
		Sample      Sample            `json:"Sample"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	p, err := h.Backend.CreateProject(
		ctx,
		req.Name,
		req.DatasetName,
		req.RecipeName,
		req.RoleArn,
		req.Sample,
		req.Tags,
	)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyName: p.Name})
}

func (h *Handler) handleDescribeProject(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Name string `json:"Name"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	p, err := h.Backend.DescribeProject(ctx, req.Name)
	if err != nil {
		return nil, err
	}

	return json.Marshal(p)
}

func (h *Handler) handleListProjects(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		MaxResults string `json:"MaxResults"`
		NextToken  string `json:"NextToken"`
	}
	_ = json.Unmarshal(body, &req)
	maxResults, _ := strconv.Atoi(req.MaxResults)

	projects, next := h.Backend.ListProjects(ctx, maxResults, req.NextToken)

	return json.Marshal(map[string]any{"Projects": projects, nextTokenKey: next})
}

func (h *Handler) handleUpdateProject(ctx context.Context, body []byte) ([]byte, error) {
	// Note: UpdateProjectInput has no DatasetName member in the real SDK
	// (only Name/RoleArn/Sample) -- see UpdateProject's doc comment.
	var req struct {
		Name    string `json:"Name"`
		RoleArn string `json:"RoleArn"`
		Sample  Sample `json:"Sample"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if err := h.Backend.UpdateProject(ctx, req.Name, req.RoleArn, req.Sample); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyName: req.Name})
}

func (h *Handler) handleDeleteProject(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Name string `json:"Name"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if err := h.Backend.DeleteProject(ctx, req.Name); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyName: req.Name})
}

func (h *Handler) handleStartProjectSession(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		Name          string `json:"Name"`
		AssumeControl bool   `json:"AssumeControl"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	return json.Marshal(map[string]string{keyName: req.Name})
}

func (h *Handler) handleSendProjectSessionAction(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		Action map[string]any `json:"Action"`
		Name   string         `json:"Name"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	return json.Marshal(map[string]string{keyName: req.Name})
}
