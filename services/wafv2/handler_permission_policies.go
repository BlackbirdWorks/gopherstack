package wafv2

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// deletePermissionPolicyRequest is the request body for DeletePermissionPolicy.
type deletePermissionPolicyRequest struct {
	ResourceArn string `json:"ResourceArn"`
}

func (h *Handler) handleDeletePermissionPolicy(ctx context.Context, body []byte) ([]byte, error) {
	var req deletePermissionPolicyRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", errInvalidRequest)
	}

	if err := h.Backend.DeletePermissionPolicy(ctx, req.ResourceArn); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "wafv2: deleted permission policy", "resourceArn", req.ResourceArn)

	return nil, nil
}

// putPermissionPolicyRequest is the request body for PutPermissionPolicy.
type putPermissionPolicyRequest struct {
	ResourceArn string `json:"ResourceArn"`
	Policy      string `json:"Policy"`
}

func (h *Handler) handlePutPermissionPolicy(ctx context.Context, body []byte) ([]byte, error) {
	var req putPermissionPolicyRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", errInvalidRequest)
	}

	if err := h.Backend.PutPermissionPolicy(ctx, req.ResourceArn, req.Policy); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "wafv2: put permission policy", "resourceArn", req.ResourceArn)

	return nil, nil
}

// getPermissionPolicyRequest is the request body for GetPermissionPolicy.
type getPermissionPolicyRequest struct {
	ResourceArn string `json:"ResourceArn"`
}

func (h *Handler) handleGetPermissionPolicy(ctx context.Context, body []byte) ([]byte, error) {
	var req getPermissionPolicyRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", errInvalidRequest)
	}

	policy, err := h.Backend.GetPermissionPolicy(ctx, req.ResourceArn)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{"Policy": policy})
}

// permissionPolicyDispatchOps returns the permission-policy-family operation dispatch
// entries. Each entry is a bound method value -- handleDeletePermissionPolicy et al.
// already match the dispatchFn signature, so no wrapper closure is needed.
func (h *Handler) permissionPolicyDispatchOps() map[string]dispatchFn {
	return map[string]dispatchFn{
		"DeletePermissionPolicy": h.handleDeletePermissionPolicy,
		"PutPermissionPolicy":    h.handlePutPermissionPolicy,
		"GetPermissionPolicy":    h.handleGetPermissionPolicy,
	}
}
