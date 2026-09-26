package opensearch

import "fmt"

// Real AOSS lifecycle-policy JSON-RPC handlers (opensearchserverless@v1.34.4
// api_op_{Create,Update,Delete,List,BatchGet,BatchGetEffective}LifecyclePolicy.go,
// serializers.go/deserializers.go for every field key cited inline below).

func (h *Handler) jrCreateLifecyclePolicy(input map[string]any) (map[string]any, error) {
	name, _ := input["name"].(string)
	typ, _ := input[jsonKeyPolicyTypeJR].(string)
	desc, _ := input["description"].(string)
	policy, _ := input["policy"].(string)

	lp, err := h.Backend.CreateServerlessLifecyclePolicy(typ, name, desc, policy)
	if err != nil {
		return nil, err
	}

	return map[string]any{"lifecyclePolicyDetail": policyDetailJR(lp)}, nil
}

func (h *Handler) jrUpdateLifecyclePolicy(input map[string]any) (map[string]any, error) {
	name, _ := input["name"].(string)
	typ, _ := input[jsonKeyPolicyTypeJR].(string)
	desc, _ := input["description"].(string)
	policy, _ := input["policy"].(string)
	ver, _ := input["policyVersion"].(string)

	lp, err := h.Backend.UpdateServerlessLifecyclePolicy(typ, name, desc, policy, ver)
	if err != nil {
		return nil, err
	}

	return map[string]any{"lifecyclePolicyDetail": policyDetailJR(lp)}, nil
}

func (h *Handler) jrDeleteLifecyclePolicy(input map[string]any) (map[string]any, error) {
	name, _ := input["name"].(string)
	typ, _ := input[jsonKeyPolicyTypeJR].(string)

	if err := h.Backend.DeleteServerlessLifecyclePolicy(typ, name); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

func (h *Handler) jrListLifecyclePolicies(input map[string]any) (map[string]any, error) {
	typ, _ := input[jsonKeyPolicyTypeJR].(string)
	resources := strSliceJR(input, "resources")

	lps := h.Backend.ListServerlessLifecyclePolicies(typ, resources)
	summaries := make([]map[string]any, 0, len(lps))

	for _, lp := range lps {
		summaries = append(summaries, map[string]any{
			jsonKeyAppName:            lp.Name,
			jsonKeyPolicyTypeJR:       lp.Type,
			"policyVersion":           lp.PolicyVersion,
			jsonKeyCreatedDateJR:      lp.CreatedDate,
			jsonKeyLastModifiedDateJR: lp.LastModifiedDate,
			"description":             lp.Description,
		})
	}

	return map[string]any{"lifecyclePolicySummaries": summaries}, nil
}

// lifecyclePolicyIdentifiersJR decodes BatchGetLifecyclePolicyInput.Identifiers
// ([]types.LifecyclePolicyIdentifier -- serializeDocumentLifecyclePolicyIdentifier:
// {"name","type"}).
func lifecyclePolicyIdentifiersJR(raw any) []serverlessLifecyclePolicyIdentifier {
	list, ok := raw.([]any)
	if !ok {
		return nil
	}

	out := make([]serverlessLifecyclePolicyIdentifier, 0, len(list))

	for _, item := range list {
		entry, isMap := item.(map[string]any)
		if !isMap {
			continue
		}

		name, _ := entry["name"].(string)
		typ, _ := entry[jsonKeyPolicyTypeJR].(string)
		out = append(out, serverlessLifecyclePolicyIdentifier{Name: name, Type: typ})
	}

	return out
}

// lifecyclePolicyResourceIdentifiersJR decodes
// BatchGetEffectiveLifecyclePolicyInput.ResourceIdentifiers
// ([]types.LifecyclePolicyResourceIdentifier -- {"resource","type"}).
func lifecyclePolicyResourceIdentifiersJR(raw any) []serverlessLifecyclePolicyIdentifier {
	list, ok := raw.([]any)
	if !ok {
		return nil
	}

	out := make([]serverlessLifecyclePolicyIdentifier, 0, len(list))

	for _, item := range list {
		entry, isMap := item.(map[string]any)
		if !isMap {
			continue
		}

		resource, _ := entry["resource"].(string)
		typ, _ := entry[jsonKeyPolicyTypeJR].(string)
		out = append(out, serverlessLifecyclePolicyIdentifier{Name: resource, Type: typ})
	}

	return out
}

func (h *Handler) jrBatchGetLifecyclePolicy(input map[string]any) (map[string]any, error) {
	identifiers := lifecyclePolicyIdentifiersJR(input["identifiers"])
	if len(identifiers) == 0 {
		return nil, fmt.Errorf("%w: identifiers are required", ErrInvalidParameter)
	}

	found, errs := h.Backend.BatchGetServerlessLifecyclePolicies(identifiers)

	details := make([]map[string]any, 0, len(found))
	for _, lp := range found {
		details = append(details, policyDetailJR(lp))
	}

	errDetails := make([]map[string]any, 0, len(errs))
	for _, e := range errs {
		errDetails = append(errDetails, map[string]any{
			"name":                e.Name,
			jsonKeyPolicyTypeJR:   e.Type,
			jsonKeyErrorCodeJR:    e.ErrorCode,
			jsonKeyErrorMessageJR: e.ErrorMessage,
		})
	}

	return map[string]any{
		"lifecyclePolicyDetails":      details,
		"lifecyclePolicyErrorDetails": errDetails,
	}, nil
}

func (h *Handler) jrBatchGetEffectiveLifecyclePolicy(input map[string]any) (map[string]any, error) {
	identifiers := lifecyclePolicyResourceIdentifiersJR(input["resourceIdentifiers"])
	if len(identifiers) == 0 {
		return nil, fmt.Errorf("%w: identifiers are required", ErrInvalidParameter)
	}

	found, errs := h.Backend.BatchGetServerlessEffectiveLifecyclePolicies(identifiers)

	details := make([]map[string]any, 0, len(found))
	for _, r := range found {
		details = append(details, map[string]any{
			"policyName":           r.PolicyName,
			"resource":             r.Resource,
			"resourceType":         r.ResourceType,
			jsonKeyPolicyTypeJR:    r.Type,
			"retentionPeriod":      r.RetentionPeriod,
			"noMinRetentionPeriod": r.NoMinRetentionPeriod,
		})
	}

	errDetails := make([]map[string]any, 0, len(errs))
	for _, e := range errs {
		errDetails = append(errDetails, map[string]any{
			"resource":            e.Resource,
			jsonKeyPolicyTypeJR:   e.Type,
			jsonKeyErrorCodeJR:    e.ErrorCode,
			jsonKeyErrorMessageJR: e.ErrorMessage,
		})
	}

	return map[string]any{
		"effectiveLifecyclePolicyDetails":      details,
		"effectiveLifecyclePolicyErrorDetails": errDetails,
	}, nil
}
