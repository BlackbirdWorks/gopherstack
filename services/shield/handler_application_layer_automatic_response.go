package shield

import (
	"encoding/json"
	"fmt"
)

// alarAction is the nested action object in ALAR requests.
type alarAction struct {
	Block *struct{} `json:"Block"`
	Count *struct{} `json:"Count"`
}

// alarRequest is the shared request body for ALAR enable/update operations.
type alarRequest struct {
	Action      alarAction `json:"Action"`
	ResourceArn string     `json:"ResourceArn"`
}

// alarActionString extracts "BLOCK" or "COUNT" from an alarAction, or returns an error.
func alarActionString(a alarAction) (string, error) {
	switch {
	case a.Block != nil:
		return "BLOCK", nil
	case a.Count != nil:
		return "COUNT", nil
	default:
		return "", fmt.Errorf("%w: Action must specify Block or Count", errInvalidRequest)
	}
}

func (h *Handler) handleEnableApplicationLayerAutomaticResponse(body []byte) error {
	var req alarRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceArn == "" {
		return fmt.Errorf("%w: ResourceArn is required", errInvalidRequest)
	}

	action, err := alarActionString(req.Action)
	if err != nil {
		return err
	}

	return h.Backend.EnableApplicationLayerAutomaticResponse(req.ResourceArn, action)
}

// disableALARRequest is the request body for DisableApplicationLayerAutomaticResponse.
type disableALARRequest struct {
	ResourceArn string `json:"ResourceArn"`
}

func (h *Handler) handleDisableApplicationLayerAutomaticResponse(body []byte) error {
	var req disableALARRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceArn == "" {
		return fmt.Errorf("%w: ResourceArn is required", errInvalidRequest)
	}

	return h.Backend.DisableApplicationLayerAutomaticResponse(req.ResourceArn)
}

func (h *Handler) handleUpdateApplicationLayerAutomaticResponse(body []byte) error {
	var req alarRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceArn == "" {
		return fmt.Errorf("%w: ResourceArn is required", errInvalidRequest)
	}

	action, err := alarActionString(req.Action)
	if err != nil {
		return err
	}

	return h.Backend.UpdateApplicationLayerAutomaticResponse(req.ResourceArn, action)
}
