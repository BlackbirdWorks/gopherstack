package wafv2

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

// getRateBasedStatementManagedKeysRequest is the request body for GetRateBasedStatementManagedKeys.
type getRateBasedStatementManagedKeysRequest struct {
	Scope             string `json:"Scope"`
	WebACLName        string `json:"WebACLName"`
	WebACLId          string `json:"WebACLId"`
	RuleGroupRuleName string `json:"RuleGroupRuleName"`
	RuleName          string `json:"RuleName"`
}

// handleGetRateBasedStatementManagedKeys returns empty rate-based managed keys.
func (h *Handler) handleGetRateBasedStatementManagedKeys(ctx context.Context, body []byte) ([]byte, error) {
	var req getRateBasedStatementManagedKeysRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Scope == "" {
		return nil, fmt.Errorf("%w: Scope is required", errInvalidRequest)
	}

	if req.WebACLName == "" {
		return nil, fmt.Errorf("%w: WebACLName is required", errInvalidRequest)
	}

	if req.WebACLId == "" {
		return nil, fmt.Errorf("%w: WebACLId is required", errInvalidRequest)
	}

	if req.RuleName == "" {
		return nil, fmt.Errorf("%w: RuleName is required", errInvalidRequest)
	}

	if _, err := h.Backend.GetWebACL(ctx, req.WebACLId); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"ManagedKeysIPV4": map[string]any{keyIPAddressVersion: "IPV4", keyAddresses: []any{}},
		"ManagedKeysIPV6": map[string]any{keyIPAddressVersion: "IPV6", keyAddresses: []any{}},
	})
}

// getSampledRequestsRequest is the request body for GetSampledRequests.
type getSampledRequestsRequest struct {
	TimeWindow     map[string]any `json:"TimeWindow"`
	WebACLArn      string         `json:"WebAclArn"`
	RuleMetricName string         `json:"RuleMetricName"`
	Scope          string         `json:"Scope"`
	MaxItems       int64          `json:"MaxItems"`
}

// handleGetSampledRequests returns an empty sampled requests response.
func (h *Handler) handleGetSampledRequests(ctx context.Context, body []byte) ([]byte, error) {
	var req getSampledRequestsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Scope == "" {
		return nil, fmt.Errorf("%w: Scope is required", errInvalidRequest)
	}

	if req.WebACLArn == "" {
		return nil, fmt.Errorf("%w: WebAclArn is required", errInvalidRequest)
	}

	if req.RuleMetricName == "" {
		return nil, fmt.Errorf("%w: RuleMetricName is required", errInvalidRequest)
	}

	if req.MaxItems < 1 || req.MaxItems > maxSampledRequestsItems {
		return nil, fmt.Errorf("%w: MaxItems must be between 1 and %d", errInvalidRequest, maxSampledRequestsItems)
	}

	if req.TimeWindow == nil {
		return nil, fmt.Errorf("%w: TimeWindow is required", errInvalidRequest)
	}

	// Extract WebACL ID from the ARN (last path segment).
	arnParts := strings.Split(req.WebACLArn, "/")
	webACLID := arnParts[len(arnParts)-1]

	if _, err := h.Backend.GetWebACL(ctx, webACLID); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"SampledRequests": []any{},
		"PopulationSize":  int64(0),
		"TimeWindow":      req.TimeWindow,
	})
}

// getTopPathStatisticsByTrafficRequest is the request body for GetTopPathStatisticsByTraffic.
type getTopPathStatisticsByTrafficRequest struct {
	TimeWindow map[string]any `json:"TimeWindow"`
	Scope      string         `json:"Scope"`
	WebACLName string         `json:"WebACLName"`
	WebACLId   string         `json:"WebACLId"`
}

// handleGetTopPathStatisticsByTraffic returns empty top path statistics.
func (h *Handler) handleGetTopPathStatisticsByTraffic(ctx context.Context, body []byte) ([]byte, error) {
	var req getTopPathStatisticsByTrafficRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Scope == "" {
		return nil, fmt.Errorf("%w: Scope is required", errInvalidRequest)
	}

	if req.WebACLName == "" {
		return nil, fmt.Errorf("%w: WebACLName is required", errInvalidRequest)
	}

	if req.WebACLId == "" {
		return nil, fmt.Errorf("%w: WebACLId is required", errInvalidRequest)
	}

	if req.TimeWindow == nil {
		return nil, fmt.Errorf("%w: TimeWindow is required", errInvalidRequest)
	}

	if _, err := h.Backend.GetWebACL(ctx, req.WebACLId); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{"UrlStatistics": []any{}})
}

// rateBasedRuleDispatchOps returns the rate-based-rule and traffic-monitoring operation
// dispatch entries. Each entry is a bound method value --
// handleGetRateBasedStatementManagedKeys et al. already match the dispatchFn signature,
// so no wrapper closure is needed.
func (h *Handler) rateBasedRuleDispatchOps() map[string]dispatchFn {
	return map[string]dispatchFn{
		"GetRateBasedStatementManagedKeys": h.handleGetRateBasedStatementManagedKeys,
		"GetSampledRequests":               h.handleGetSampledRequests,
		"GetTopPathStatisticsByTraffic":    h.handleGetTopPathStatisticsByTraffic,
	}
}
