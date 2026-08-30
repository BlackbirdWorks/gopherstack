package wafv2

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

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

	// resourceTypeForARN implements the same 8-format classification as
	// AssociateWebACLInput.ResourceArn's own doc comment (wafv2@v1.77.3
	// api_op_AssociateWebACL.go); an ARN whose service segment matches none
	// of them "corresponds to a resource with which a web ACL can't be
	// associated" per WAFInvalidParameterException's doc comment
	// (types/errors.go), which is the error real AWS returns for it.
	if resourceTypeForARN(resourceArn) == "" {
		return fmt.Errorf(
			"%w: ResourceArn %q does not correspond to a resource with which a web ACL can be associated",
			errInvalidRequest, resourceArn,
		)
	}

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

	return h.marshalWebACL(ctx, w)
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

	resourceType := req.ResourceType
	if resourceType == "" {
		resourceType = resourceTypeApplicationLoadBalancer
	}

	resources, err := h.Backend.ListResourcesForWebACL(ctx, req.WebACLArn)
	if err != nil {
		return nil, err
	}

	filtered := make([]string, 0, len(resources))

	for _, r := range resources {
		if resourceTypeForARN(r) == resourceType {
			filtered = append(filtered, r)
		}
	}

	return json.Marshal(map[string]any{"ResourceArns": filtered})
}

const (
	resourceTypeApplicationLoadBalancer = "APPLICATION_LOAD_BALANCER"
	resourceTypeAPIGateway              = "API_GATEWAY"
	resourceTypeAppsync                 = "APPSYNC"
	resourceTypeCognitoUserPool         = "COGNITO_USER_POOL"
	resourceTypeAppRunnerService        = "APP_RUNNER_SERVICE"
	resourceTypeVerifiedAccessInstance  = "VERIFIED_ACCESS_INSTANCE"
	resourceTypeAmplify                 = "AMPLIFY"
	resourceTypeAgentcoreGateway        = "AGENTCORE_GATEWAY"
)

// resourceTypeForARN classifies a resource ARN into its WAF ResourceType,
// per AssociateWebACLInput.ResourceArn's doc comment (wafv2@v1.77.3
// api_op_AssociateWebACL.go), which enumerates the exact ARN format for
// each of the 8 supported resource types. verified-access-instance is the
// only type sharing a service segment (ec2) with other resource kinds, so
// it's matched on its resource-id prefix rather than service alone.
func resourceTypeForARN(arnStr string) string {
	switch extractARNService(arnStr) {
	case "elasticloadbalancing":
		return resourceTypeApplicationLoadBalancer
	case "apigateway":
		return resourceTypeAPIGateway
	case "appsync":
		return resourceTypeAppsync
	case "cognito-idp":
		return resourceTypeCognitoUserPool
	case "apprunner":
		return resourceTypeAppRunnerService
	case "amplify":
		return resourceTypeAmplify
	case "bedrock-agentcore":
		return resourceTypeAgentcoreGateway
	case "ec2":
		if strings.Contains(arnStr, ":verified-access-instance/") {
			return resourceTypeVerifiedAccessInstance
		}

		return ""
	default:
		return ""
	}
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
