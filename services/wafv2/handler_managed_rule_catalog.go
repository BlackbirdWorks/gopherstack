package wafv2

import (
	"context"
	"encoding/json"
	"fmt"
)

// handleDescribeAllManagedProducts returns the catalog of managed products.
func (h *Handler) handleDescribeAllManagedProducts(_ []byte) ([]byte, error) {
	products := make([]map[string]any, 0, len(getManagedRuleGroups()))

	for _, mrg := range getManagedRuleGroups() {
		products = append(products, map[string]any{
			keyVendorName:        mrg.VendorName,
			"ManagedRuleSetName": mrg.Name,
			"ProductDescription": mrg.Description,
		})
	}

	return json.Marshal(map[string]any{"ManagedProducts": products})
}

// describeManagedProductsByVendorRequest is the request body for DescribeManagedProductsByVendor.
type describeManagedProductsByVendorRequest struct {
	Scope      string `json:"Scope"`
	VendorName string `json:"VendorName"`
}

// handleDescribeManagedProductsByVendor returns catalog entries filtered by vendor.
func (h *Handler) handleDescribeManagedProductsByVendor(body []byte) ([]byte, error) {
	var req describeManagedProductsByVendorRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	products := make([]map[string]any, 0)

	for _, mrg := range getManagedRuleGroups() {
		if req.VendorName != "" && mrg.VendorName != req.VendorName {
			continue
		}

		products = append(products, map[string]any{
			keyVendorName:        mrg.VendorName,
			"ManagedRuleSetName": mrg.Name,
			"ProductDescription": mrg.Description,
		})
	}

	return json.Marshal(map[string]any{"ManagedProducts": products})
}

// describeManagedRuleGroupRequest is the request body for DescribeManagedRuleGroup.
type describeManagedRuleGroupRequest struct {
	Scope       string `json:"Scope"`
	VendorName  string `json:"VendorName"`
	Name        string `json:"Name"`
	VersionName string `json:"VersionName"`
}

// handleDescribeManagedRuleGroup returns catalog data for the requested managed rule group.
func (h *Handler) handleDescribeManagedRuleGroup(body []byte) ([]byte, error) {
	var req describeManagedRuleGroupRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	// Look up catalog entry.
	for _, mrg := range getManagedRuleGroups() {
		if mrg.VendorName == req.VendorName && mrg.Name == req.Name {
			return json.Marshal(map[string]any{
				keyCapacity:       mrg.Capacity,
				keyRules:          buildRuleList(mrg.Rules),
				"SnsTopicArn":     "",
				"AvailableLabels": buildLabelList(mrg.Rules),
				"ConsumedLabels":  []any{},
				"Description":     mrg.Description,
			})
		}
	}

	return nil, fmt.Errorf(
		"%w: managed rule group %q/%q not found",
		ErrManagedRuleGroupNotFound, req.VendorName, req.Name,
	)
}

// buildRuleList converts catalog rule entries to the AWS DescribeManagedRuleGroup Rules format.
func buildRuleList(rules []managedRuleInfo) []any {
	if len(rules) == 0 {
		return []any{}
	}

	out := make([]any, len(rules))
	for i, r := range rules {
		out[i] = map[string]any{
			keyName:  r.Name,
			"Action": map[string]any{capitalizeAction(r.DefaultAction): map[string]any{}},
		}
	}

	return out
}

// capitalizeAction maps lowercase action names to the title-case form AWS uses in responses.
func capitalizeAction(action string) string {
	switch action {
	case actionBlock:
		return "Block"
	case actionCount:
		return "Count"
	case "allow":
		return "Allow"
	default:
		return action
	}
}

// buildLabelList collects all unique labels from a rule set into AWS DescribeManagedRuleGroup
// AvailableLabels format: [{Name: "label"}].
func buildLabelList(rules []managedRuleInfo) []any {
	seen := make(map[string]bool)
	var out []any

	for _, r := range rules {
		for _, lbl := range r.Labels {
			if !seen[lbl] {
				seen[lbl] = true
				out = append(out, map[string]any{"Name": lbl})
			}
		}
	}

	if out == nil {
		return []any{}
	}

	return out
}

// generateMobileSdkReleaseUrlRequest is the request body for GenerateMobileSdkReleaseUrl.
type generateMobileSdkReleaseURLRequest struct {
	Platform       string `json:"Platform"`
	ReleaseVersion string `json:"ReleaseVersion"`
}

// handleGenerateMobileSdkReleaseURL returns a presigned-style URL for the requested mobile SDK release.
func (h *Handler) handleGenerateMobileSdkReleaseURL(body []byte) ([]byte, error) {
	var req generateMobileSdkReleaseURLRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Platform == "" {
		return nil, fmt.Errorf("%w: Platform is required", errInvalidRequest)
	}

	if req.ReleaseVersion == "" {
		return nil, fmt.Errorf("%w: ReleaseVersion is required", errInvalidRequest)
	}

	if getMobileSdkRelease(req.Platform, req.ReleaseVersion) == nil {
		return nil, fmt.Errorf(
			"%w: mobile SDK release %q/%q not found",
			ErrMobileSdkReleaseNotFound,
			req.Platform,
			req.ReleaseVersion,
		)
	}

	url := "https://d1mh8l8x6wqrj9.cloudfront.net/waf-mobile-sdk/" +
		req.Platform + "/" + req.ReleaseVersion + "/aws-waf-mobile-sdk.zip?X-Amz-Signature=simulated"

	return json.Marshal(map[string]any{
		"Url": url,
	})
}

// getMobileSdkReleaseRequest is the request body for GetMobileSdkRelease.
type getMobileSdkReleaseRequest struct {
	Platform       string `json:"Platform"`
	ReleaseVersion string `json:"ReleaseVersion"`
}

// handleGetMobileSdkRelease returns the mobile SDK release from the catalog.
func (h *Handler) handleGetMobileSdkRelease(body []byte) ([]byte, error) {
	var req getMobileSdkReleaseRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Platform == "" {
		return nil, fmt.Errorf("%w: Platform is required", errInvalidRequest)
	}

	if req.ReleaseVersion == "" {
		return nil, fmt.Errorf("%w: ReleaseVersion is required", errInvalidRequest)
	}

	release := getMobileSdkRelease(req.Platform, req.ReleaseVersion)
	if release == nil {
		return nil, fmt.Errorf(
			"%w: mobile SDK release %q/%q not found",
			ErrMobileSdkReleaseNotFound,
			req.Platform,
			req.ReleaseVersion,
		)
	}

	return json.Marshal(map[string]any{
		"MobileSdkRelease": map[string]any{
			"ReleaseVersion": release.ReleaseVersion,
			"Timestamp":      release.Timestamp,
			"ReleaseNotes":   release.ReleaseNotes,
			"Tags":           []any{},
		},
	})
}

// listAvailableManagedRuleGroupVersionsRequest is the request body for ListAvailableManagedRuleGroupVersions.
type listAvailableManagedRuleGroupVersionsRequest struct {
	Scope      string `json:"Scope"`
	VendorName string `json:"VendorName"`
	Name       string `json:"Name"`
	NextMarker string `json:"NextMarker"`
	Limit      int    `json:"Limit"`
}

// handleListAvailableManagedRuleGroupVersions returns versions for managed rule groups that support versioning.
func (h *Handler) handleListAvailableManagedRuleGroupVersions(body []byte) ([]byte, error) {
	var req listAvailableManagedRuleGroupVersionsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	// Look for versioning support in catalog.
	for _, mrg := range getManagedRuleGroups() {
		if mrg.VendorName == req.VendorName && mrg.Name == req.Name && mrg.VersioningSupported {
			return json.Marshal(map[string]any{
				"Versions": []map[string]any{
					{"Name": "Version_1.0", "LastUpdateTimestamp": nil},
				},
				"CurrentDefaultVersion": "Version_1.0",
			})
		}
	}

	return json.Marshal(map[string]any{"Versions": []any{}, "CurrentDefaultVersion": ""})
}

// listAvailableManagedRuleGroupsRequest is the request body for ListAvailableManagedRuleGroups.
type listAvailableManagedRuleGroupsRequest struct {
	Scope      string `json:"Scope"`
	NextMarker string `json:"NextMarker"`
	Limit      int    `json:"Limit"`
}

// handleListAvailableManagedRuleGroups returns the catalog of managed rule groups.
func (h *Handler) handleListAvailableManagedRuleGroups(body []byte) ([]byte, error) {
	var req listAvailableManagedRuleGroupsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	groups := make([]map[string]any, 0, len(getManagedRuleGroups()))

	for _, mrg := range getManagedRuleGroups() {
		groups = append(groups, map[string]any{
			keyVendorName:         mrg.VendorName,
			keyName:               mrg.Name,
			keyDescription:        mrg.Description,
			"VersioningSupported": mrg.VersioningSupported,
		})
	}

	return json.Marshal(map[string]any{"ManagedRuleGroups": groups})
}

// listMobileSdkReleasesRequest is the request body for ListMobileSdkReleases.
type listMobileSdkReleasesRequest struct {
	Platform   string `json:"Platform"`
	Scope      string `json:"Scope"`
	NextMarker string `json:"NextMarker"`
	Limit      int    `json:"Limit"`
}

// handleListMobileSdkReleases lists mobile SDK releases from the catalog.
func (h *Handler) handleListMobileSdkReleases(body []byte) ([]byte, error) {
	var req listMobileSdkReleasesRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	releases := getMobileSdkReleases(req.Platform)

	summaries := make([]map[string]any, 0, len(releases))

	for _, r := range releases {
		summaries = append(summaries, map[string]any{
			"ReleaseVersion": r.ReleaseVersion,
			"Timestamp":      r.Timestamp,
		})
	}

	return json.Marshal(map[string]any{"ReleaseSummaries": summaries})
}

// managedRuleCatalogDispatchOps returns the managed-rule-group and mobile-SDK catalog
// operation dispatch entries. wrapNoCtx adapts the catalog handlers, which don't need a
// context, to the ctx-taking dispatchFn signature the dispatch table requires.
func (h *Handler) managedRuleCatalogDispatchOps() map[string]dispatchFn {
	wrapNoCtx := func(f func([]byte) ([]byte, error)) dispatchFn {
		return func(_ context.Context, b []byte) ([]byte, error) { return f(b) }
	}

	return map[string]dispatchFn{
		"DescribeAllManagedProducts":            wrapNoCtx(h.handleDescribeAllManagedProducts),
		"DescribeManagedProductsByVendor":       wrapNoCtx(h.handleDescribeManagedProductsByVendor),
		"DescribeManagedRuleGroup":              wrapNoCtx(h.handleDescribeManagedRuleGroup),
		"GenerateMobileSdkReleaseUrl":           wrapNoCtx(h.handleGenerateMobileSdkReleaseURL),
		"GetMobileSdkRelease":                   wrapNoCtx(h.handleGetMobileSdkRelease),
		"ListMobileSdkReleases":                 wrapNoCtx(h.handleListMobileSdkReleases),
		"ListAvailableManagedRuleGroupVersions": wrapNoCtx(h.handleListAvailableManagedRuleGroupVersions),
		"ListAvailableManagedRuleGroups":        wrapNoCtx(h.handleListAvailableManagedRuleGroups),
	}
}
