package codecommit

import (
	"encoding/json"
	"fmt"
)

func (h *Handler) handleGetRepositoryTriggers(body []byte) (any, error) {
	var req struct {
		RepositoryName string `json:"repositoryName"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.RepositoryName == "" {
		return nil, fmt.Errorf("%w: repositoryName is required", errInvalidRequest)
	}

	triggers, err := h.Backend.GetRepositoryTriggers(req.RepositoryName)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"triggers":        triggers,
		"configurationId": "",
	}, nil
}

func (h *Handler) handlePutRepositoryTriggers(body []byte) (any, error) {
	var req struct {
		RepositoryName string              `json:"repositoryName"`
		Triggers       []RepositoryTrigger `json:"triggers"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.RepositoryName == "" {
		return nil, fmt.Errorf("%w: repositoryName is required", errInvalidRequest)
	}

	if err := h.Backend.PutRepositoryTriggers(req.RepositoryName, req.Triggers); err != nil {
		return nil, err
	}

	return map[string]any{
		"configurationId": "",
	}, nil
}

func (h *Handler) handleTestRepositoryTriggers(body []byte) (any, error) {
	var req struct {
		RepositoryName string              `json:"repositoryName"`
		Triggers       []RepositoryTrigger `json:"triggers"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if req.RepositoryName == "" {
		return nil, fmt.Errorf("%w: repositoryName is required", errInvalidRequest)
	}

	names, err := h.Backend.TestRepositoryTriggers(req.RepositoryName)
	if err != nil {
		return nil, err
	}

	// TestRepositoryTriggersOutput.SuccessfulExecutions (codecommit@v1.36.4
	// api_op_TestRepositoryTriggers.go) is []string, not an array of
	// {"triggerName": ...} objects.
	return map[string]any{
		"successfulExecutions": names,
		"failedExecutions":     []any{},
	}, nil
}
