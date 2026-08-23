package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
)

// ---------------------------------------------------------------------------
// CreatePresignedDomainUrl / RenderUiTemplate / StartSession
// ---------------------------------------------------------------------------

// templateVarSubmatchCount is the expected length of a regexp match slice
// containing the full match plus one capture group.
const templateVarSubmatchCount = 2

// presignedSessionOpsSupported returns the operations dispatched by
// dispatchPresignedSessionOps.
func presignedSessionOpsSupported() []string {
	return []string{
		"CreatePresignedDomainUrl",
		"RenderUiTemplate",
		"StartSession",
	}
}

func (h *Handler) dispatchPresignedSessionOps(
	ctx context.Context,
	op string,
	body []byte,
) ([]byte, bool, error) {
	switch op {
	case "CreatePresignedDomainUrl":
		r, err := h.handleCreatePresignedDomainURL(ctx, body)

		return r, true, err
	case "RenderUiTemplate":
		r, err := h.handleRenderUITemplate(ctx, body)

		return r, true, err
	case "StartSession":
		r, err := h.handleStartSession(ctx, body)

		return r, true, err
	}

	return nil, false, nil
}

// createPresignedDomainURLRequest is the request body for CreatePresignedDomainUrl.
type createPresignedDomainURLRequest struct {
	DomainID                           string `json:"DomainId"`
	UserProfileName                    string `json:"UserProfileName"`
	LandingURI                         string `json:"LandingUri,omitempty"`
	ExpiresInSeconds                   int32  `json:"ExpiresInSeconds,omitempty"`
	SessionExpirationDurationInSeconds int32  `json:"SessionExpirationDurationInSeconds,omitempty"`
}

func (h *Handler) handleCreatePresignedDomainURL(ctx context.Context, body []byte) ([]byte, error) {
	var req createPresignedDomainURLRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.DomainID == "" {
		return nil, fmt.Errorf("%w: DomainId is required", errInvalidRequest)
	}

	if req.UserProfileName == "" {
		return nil, fmt.Errorf("%w: UserProfileName is required", errInvalidRequest)
	}

	url, err := h.Backend.CreatePresignedDomainURL(ctx, req.DomainID, req.UserProfileName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyAuthorizedURL: url})
}

// templateVarPattern matches simple Liquid-style "{{ task.input.<field> }}"
// substitution variables (allowing arbitrary surrounding whitespace).
var templateVarPattern = regexp.MustCompile(`\{\{\s*task\.input\.([A-Za-z0-9_]+)\s*\}\}`)

// renderableTask mirrors types.RenderableTask (types/types.go:19548): a
// RenderUiTemplate request's only task field, a JSON-object Input string.
type renderableTask struct {
	Input string `json:"Input"`
}

// uiTemplate mirrors types.UiTemplate: the worker UI template to render.
type uiTemplate struct {
	Content string `json:"Content"`
}

// renderUITemplateRequest is the request body for RenderUiTemplate.
type renderUITemplateRequest struct {
	UITemplate     *uiTemplate    `json:"UiTemplate,omitempty"`
	Task           renderableTask `json:"Task"`
	HumanTaskUIArn string         `json:"HumanTaskUiArn,omitempty"`
	RoleArn        string         `json:"RoleArn"`
}

func (h *Handler) handleRenderUITemplate(ctx context.Context, body []byte) ([]byte, error) {
	var req renderUITemplateRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.RoleArn == "" {
		return nil, fmt.Errorf("%w: RoleArn is required", errInvalidRequest)
	}

	if req.Task.Input == "" {
		return nil, fmt.Errorf("%w: Task.Input is required", errInvalidRequest)
	}

	content := ""

	switch {
	case req.UITemplate != nil:
		content = req.UITemplate.Content
	case req.HumanTaskUIArn != "":
		if !h.Backend.HumanTaskUIExistsByARN(ctx, req.HumanTaskUIArn) {
			return nil, fmt.Errorf("%w: human task UI %q not found", ErrHumanTaskUINotFound, req.HumanTaskUIArn)
		}
		// This backend does not persist a HumanTaskUi's template body, so
		// there is nothing to render; correctly report an empty result
		// rather than fabricate content.
	default:
		return nil, fmt.Errorf("%w: either UiTemplate or HumanTaskUiArn is required", errInvalidRequest)
	}

	rendered := renderUITemplateContent(content, req.Task.Input)

	return json.Marshal(map[string]any{
		"RenderedContent": rendered,
		"Errors":          []any{},
	})
}

// renderUITemplateContent substitutes "{{ task.input.<field> }}" variables in
// content with values parsed from the task's Input JSON object.
func renderUITemplateContent(content, taskInput string) string {
	if content == "" || taskInput == "" {
		return content
	}

	var fields map[string]any
	if err := json.Unmarshal([]byte(taskInput), &fields); err != nil {
		return content
	}

	return templateVarPattern.ReplaceAllStringFunc(content, func(match string) string {
		sub := templateVarPattern.FindStringSubmatch(match)
		if len(sub) != templateVarSubmatchCount {
			return match
		}

		v, ok := fields[sub[1]]
		if !ok {
			return match
		}

		return fmt.Sprintf("%v", v)
	})
}

// startSessionRequest is the request body for StartSession.
type startSessionRequest struct {
	ResourceIdentifier string `json:"ResourceIdentifier"`
}

func (h *Handler) handleStartSession(ctx context.Context, body []byte) ([]byte, error) {
	var req startSessionRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceIdentifier == "" {
		return nil, fmt.Errorf("%w: ResourceIdentifier is required", errInvalidRequest)
	}

	result, err := h.Backend.StartSession(ctx, req.ResourceIdentifier)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{
		"SessionId":  result.SessionID,
		"StreamUrl":  result.StreamURL,
		"TokenValue": result.TokenValue,
	})
}
