package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
)

// ---------------------------------------------------------------------------
// DescribeFeatureMetadata / UpdateFeatureMetadata
//
// The backend logic (GetFeatureMetadata/UpdateFeatureMetadata) already lives
// in backend_feature_store.go; this file wires it up to the wire protocol.
// ---------------------------------------------------------------------------

// featureMetadataOpsSupported returns the operations dispatched by
// dispatchFeatureMetadataOps.
func featureMetadataOpsSupported() []string {
	return []string{
		"DescribeFeatureMetadata",
		"UpdateFeatureMetadata",
	}
}

func (h *Handler) dispatchFeatureMetadataOps(
	ctx context.Context,
	op string,
	body []byte,
) ([]byte, bool, error) {
	switch op {
	case "DescribeFeatureMetadata":
		r, err := h.handleDescribeFeatureMetadata(ctx, body)

		return r, true, err
	case "UpdateFeatureMetadata":
		r, err := h.handleUpdateFeatureMetadata(ctx, body)

		return r, true, err
	}

	return nil, false, nil
}

func (h *Handler) handleDescribeFeatureMetadata(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		FeatureGroupName string `json:"FeatureGroupName"`
		FeatureName      string `json:"FeatureName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.FeatureGroupName == "" {
		return nil, fmt.Errorf("%w: FeatureGroupName is required", errInvalidRequest)
	}

	if req.FeatureName == "" {
		return nil, fmt.Errorf("%w: FeatureName is required", errInvalidRequest)
	}

	fg, err := h.Backend.DescribeFeatureGroup(ctx, req.FeatureGroupName)
	if err != nil {
		return nil, err
	}

	if !featureExistsInGroup(fg, req.FeatureName) {
		return nil, fmt.Errorf(
			"%w: feature %q not found in feature group %q",
			ErrFeatureGroupNotFound, req.FeatureName, req.FeatureGroupName,
		)
	}

	meta, err := h.Backend.GetFeatureMetadata(ctx, req.FeatureGroupName, req.FeatureName)
	if err != nil {
		return nil, err
	}

	resp := map[string]any{
		keyFeatureGroupArn:  fg.FeatureGroupArn,
		keyFeatureGroupName: fg.FeatureGroupName,
		"FeatureName":       meta.FeatureName,
		"FeatureType":       meta.FeatureType,
		keyCreationTime:     epochSeconds(fg.CreationTime),
		keyLastModifiedTime: epochSeconds(fg.CreationTime),
	}
	if meta.Description != "" {
		resp["Description"] = meta.Description
	}
	if len(meta.Parameters) > 0 {
		resp["Parameters"] = featureParametersToWire(meta.Parameters)
	}

	return json.Marshal(resp)
}

func (h *Handler) handleUpdateFeatureMetadata(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		FeatureGroupName   string `json:"FeatureGroupName"`
		FeatureName        string `json:"FeatureName"`
		Description        string `json:"Description,omitempty"`
		ParameterAdditions []struct {
			Key   string `json:"Key"`
			Value string `json:"Value"`
		} `json:"ParameterAdditions,omitempty"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.FeatureGroupName == "" {
		return nil, fmt.Errorf("%w: FeatureGroupName is required", errInvalidRequest)
	}

	if req.FeatureName == "" {
		return nil, fmt.Errorf("%w: FeatureName is required", errInvalidRequest)
	}

	fg, err := h.Backend.DescribeFeatureGroup(ctx, req.FeatureGroupName)
	if err != nil {
		return nil, err
	}

	if !featureExistsInGroup(fg, req.FeatureName) {
		return nil, fmt.Errorf(
			"%w: feature %q not found in feature group %q",
			ErrFeatureGroupNotFound, req.FeatureName, req.FeatureGroupName,
		)
	}

	var params map[string]string
	if len(req.ParameterAdditions) > 0 {
		params = make(map[string]string, len(req.ParameterAdditions))
		for _, p := range req.ParameterAdditions {
			params[p.Key] = p.Value
		}
	}

	if updateErr := h.Backend.UpdateFeatureMetadata(
		ctx, req.FeatureGroupName, req.FeatureName, req.Description, params,
	); updateErr != nil {
		return nil, updateErr
	}

	return json.Marshal(map[string]any{})
}

// featureExistsInGroup reports whether featureName is one of fg's defined features.
func featureExistsInGroup(fg *FeatureGroup, featureName string) bool {
	for _, def := range fg.FeatureDefinitions {
		if def.FeatureName == featureName {
			return true
		}
	}

	return false
}

// featureParametersToWire converts a metadata parameter map to the
// Key/Value-object array shape used on the wire, sorted by key for
// deterministic output.
func featureParametersToWire(params map[string]string) []map[string]string {
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	out := make([]map[string]string, 0, len(keys))
	for _, k := range keys {
		out = append(out, map[string]string{"Key": k, "Value": params[k]})
	}

	return out
}
