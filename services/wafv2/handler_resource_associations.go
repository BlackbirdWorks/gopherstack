package wafv2

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
)

// regionalResourceServices are the AWS service identifiers accepted for REGIONAL WebACL associations.
var regionalResourceServices = []string{ //nolint:gochecknoglobals // package-level lookup table
	"elasticloadbalancing",
	"execute-api",
	"appsync",
	"cognito-idp",
	"apprunner",
}

// associateWebACLRequest is the request body for AssociateWebACL.
type associateWebACLRequest struct {
	WebACLArn   string `json:"WebACLArn"`
	ResourceArn string `json:"ResourceArn"`
}

func (h *Handler) handleAssociateWebACL(ctx context.Context, body []byte) ([]byte, error) {
	var req associateWebACLRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.WebACLArn == "" {
		return nil, fmt.Errorf("%w: WebACLArn is required", errInvalidRequest)
	}

	if req.ResourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", errInvalidRequest)
	}

	if err := h.validateAssociationScope(req.WebACLArn, req.ResourceArn); err != nil {
		return nil, err
	}

	if err := h.Backend.AssociateWebACL(ctx, req.WebACLArn, req.ResourceArn); err != nil {
		return nil, err
	}

	return nil, nil
}

// validateAssociationScope checks that the resource ARN's service is compatible with the WebACL scope.
func (h *Handler) validateAssociationScope(webACLArn, resourceArn string) error {
	// Determine WebACL scope from ARN (global → CLOUDFRONT, regional → REGIONAL).
	if strings.Contains(webACLArn, "/global/") {
		return fmt.Errorf(
			"%w: CLOUDFRONT WebACL associations must be managed through the CloudFront API",
			ErrInvalidOperation,
		)
	}

	// For REGIONAL WebACLs, validate service.
	service := extractARNService(resourceArn)
	if slices.Contains(regionalResourceServices, service) {
		return nil
	}

	// If service is unrecognised, still allow (for compatibility with unknown resource types).
	return nil
}

const arnServiceIndex = 2 // position of service segment in arn:partition:SERVICE:...
// extractARNService extracts the service segment from an ARN (arn:partition:SERVICE:...).
func extractARNService(arnStr string) string {
	const minARNParts = 3

	parts := strings.Split(arnStr, ":")
	if len(parts) >= minARNParts {
		return parts[arnServiceIndex]
	}

	return ""
}

// disassociateWebACLRequest is the request body for DisassociateWebACL.
type disassociateWebACLRequest struct {
	ResourceArn string `json:"ResourceArn"`
}

func (h *Handler) handleDisassociateWebACL(ctx context.Context, body []byte) ([]byte, error) {
	var req disassociateWebACLRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", errInvalidRequest)
	}

	if err := h.Backend.DisassociateWebACL(ctx, req.ResourceArn); err != nil {
		return nil, err
	}

	return nil, nil
}

// getWebACLForResourceRequest is the request body for GetWebACLForResource.
type getWebACLForResourceRequest struct {
	ResourceArn string `json:"ResourceArn"`
}

func (h *Handler) handleGetWebACLForResource(ctx context.Context, body []byte) ([]byte, error) {
	var req getWebACLForResourceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", errInvalidRequest)
	}

	w, err := h.Backend.GetWebACLForResource(ctx, req.ResourceArn)
	if err != nil {
		return nil, err
	}

	return h.marshalWebACL(w)
}

// listResourcesForWebACLRequest is the request body for ListResourcesForWebACL.
type listResourcesForWebACLRequest struct {
	WebACLArn    string `json:"WebACLArn"`
	ResourceType string `json:"ResourceType"`
}

func (h *Handler) handleListResourcesForWebACL(ctx context.Context, body []byte) ([]byte, error) {
	var req listResourcesForWebACLRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.WebACLArn == "" {
		return nil, fmt.Errorf("%w: WebACLArn is required", errInvalidRequest)
	}

	resources, err := h.Backend.ListResourcesForWebACL(ctx, req.WebACLArn)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{"ResourceArns": resources})
}

// resourceAssociationDispatchOps returns the WebACL-resource-association operation
// dispatch entries. Each entry is a bound method value -- handleAssociateWebACL et al.
// already match the dispatchFn signature, so no wrapper closure is needed.
func (h *Handler) resourceAssociationDispatchOps() map[string]dispatchFn {
	return map[string]dispatchFn{
		"AssociateWebACL":        h.handleAssociateWebACL,
		"DisassociateWebACL":     h.handleDisassociateWebACL,
		"GetWebACLForResource":   h.handleGetWebACLForResource,
		"ListResourcesForWebACL": h.handleListResourcesForWebACL,
	}
}
