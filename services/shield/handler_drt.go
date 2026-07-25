package shield

import (
	"encoding/json"
	"fmt"
)

// associateDRTLogBucketRequest is the request body for AssociateDRTLogBucket.
type associateDRTLogBucketRequest struct {
	LogBucket string `json:"LogBucket"`
}

func (h *Handler) handleAssociateDRTLogBucket(body []byte) error {
	var req associateDRTLogBucketRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.LogBucket == "" {
		return fmt.Errorf("%w: LogBucket is required", errInvalidRequest)
	}

	if err := h.Backend.AssociateDRTLogBucket(req.LogBucket); err != nil {
		return err
	}

	return nil
}

// disassociateDRTLogBucketRequest is the request body for DisassociateDRTLogBucket.
type disassociateDRTLogBucketRequest struct {
	LogBucket string `json:"LogBucket"`
}

func (h *Handler) handleDisassociateDRTLogBucket(body []byte) error {
	var req disassociateDRTLogBucketRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.LogBucket == "" {
		return fmt.Errorf("%w: LogBucket is required", errInvalidRequest)
	}

	return h.Backend.DisassociateDRTLogBucket(req.LogBucket)
}

// associateDRTRoleRequest is the request body for AssociateDRTRole.
type associateDRTRoleRequest struct {
	RoleArn string `json:"RoleArn"`
}

func (h *Handler) handleAssociateDRTRole(body []byte) error {
	var req associateDRTRoleRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.RoleArn == "" {
		return fmt.Errorf("%w: RoleArn is required", errInvalidRequest)
	}

	if err := h.Backend.AssociateDRTRole(req.RoleArn); err != nil {
		return err
	}

	return nil
}

func (h *Handler) handleDisassociateDRTRole() error {
	return h.Backend.DisassociateDRTRole()
}

func (h *Handler) handleDescribeDRTAccess() ([]byte, error) {
	access := h.Backend.DescribeDRTAccess()

	resp := map[string]any{"LogBucketList": access.LogBucketList}

	// DescribeDRTAccessOutput.RoleArn is *string in the real SDK (types.go) -- omit the key
	// entirely when unset rather than emitting an empty string, matching how the real API leaves
	// the field absent until AssociateDRTRole has been called.
	if access.RoleArn != "" {
		resp["RoleArn"] = access.RoleArn
	}

	return json.Marshal(resp)
}
