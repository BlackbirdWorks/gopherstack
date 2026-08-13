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

// getTopPathStatisticsByTrafficRequest is the request body for
// GetTopPathStatisticsByTraffic. Verified against
// awsAwsjson11_serializeOpDocumentGetTopPathStatisticsByTrafficInput
// (wafv2@v1.77.3 serializers.go:7682): the real request identifies the web
// ACL by WebAclArn, not WebACLName/WebACLId -- those two keys don't exist on
// this op's wire shape at all.
type getTopPathStatisticsByTrafficRequest struct {
	TimeWindow                    map[string]any `json:"TimeWindow"`
	Scope                         string         `json:"Scope"`
	WebACLArn                     string         `json:"WebAclArn"`
	URIPathPrefix                 string         `json:"UriPathPrefix"`
	BotCategory                   string         `json:"BotCategory"`
	BotName                       string         `json:"BotName"`
	BotOrganization               string         `json:"BotOrganization"`
	NextMarker                    string         `json:"NextMarker"`
	Limit                         int32          `json:"Limit"`
	NumberOfTopTrafficBotsPerPath int32          `json:"NumberOfTopTrafficBotsPerPath"`
}

// handleGetTopPathStatisticsByTraffic returns top-path traffic statistics.
// This backend has no per-request path/bot traffic model to aggregate (no
// GetSampledRequests-style log to derive real PathStatistics from either),
// so it returns the real required PathStatistics/TotalRequestCount keys with
// an honest empty/zero result once the referenced WebACL is confirmed to
// exist, matching GetSampledRequests' established void-result pattern for
// this same structural gap.
func (h *Handler) handleGetTopPathStatisticsByTraffic(ctx context.Context, body []byte) ([]byte, error) {
	var req getTopPathStatisticsByTrafficRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Scope == "" {
		return nil, fmt.Errorf("%w: Scope is required", errInvalidRequest)
	}

	if req.WebACLArn == "" {
		return nil, fmt.Errorf("%w: WebAclArn is required", errInvalidRequest)
	}

	if req.TimeWindow == nil {
		return nil, fmt.Errorf("%w: TimeWindow is required", errInvalidRequest)
	}

	if req.Limit < 1 || req.Limit > maxTopPathStatisticsLimit {
		return nil, fmt.Errorf("%w: Limit must be between 1 and %d", errInvalidRequest, maxTopPathStatisticsLimit)
	}

	if req.NumberOfTopTrafficBotsPerPath < 1 || req.NumberOfTopTrafficBotsPerPath > maxTopTrafficBotsPerPath {
		return nil, fmt.Errorf(
			"%w: NumberOfTopTrafficBotsPerPath must be between 1 and %d",
			errInvalidRequest, maxTopTrafficBotsPerPath,
		)
	}

	// Extract WebACL ID from the ARN (last path segment).
	arnParts := strings.Split(req.WebACLArn, "/")
	webACLID := arnParts[len(arnParts)-1]

	if _, err := h.Backend.GetWebACL(ctx, webACLID); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"PathStatistics":    []any{},
		"TotalRequestCount": int64(0),
	})
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
