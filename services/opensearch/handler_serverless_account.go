package opensearch

import "fmt"

// Real AOSS account-settings/stats/VPC-endpoint-read JSON-RPC handlers
// (opensearchserverless@v1.34.4 api_op_GetAccountSettings.go,
// api_op_UpdateAccountSettings.go, api_op_GetPoliciesStats.go,
// api_op_BatchGetVpcEndpoint.go; field keys verified against
// serializers.go/deserializers.go, cited inline below).

func accountSettingsDetailJR(cl ServerlessCapacityLimits) map[string]any {
	return map[string]any{
		jsonKeyCapacityLimitsJR: map[string]any{
			"maxIndexingCapacityInOCU": cl.MaxIndexingCapacityInOCU,
			"maxSearchCapacityInOCU":   cl.MaxSearchCapacityInOCU,
		},
	}
}

func (h *Handler) jrGetAccountSettings(_ map[string]any) (map[string]any, error) {
	cl := h.Backend.GetServerlessAccountSettings()

	return map[string]any{"accountSettingsDetail": accountSettingsDetailJR(cl)}, nil
}

func (h *Handler) jrUpdateAccountSettings(input map[string]any) (map[string]any, error) {
	raw, _ := input[jsonKeyCapacityLimitsJR].(map[string]any)

	var requested ServerlessCapacityLimits

	if raw != nil {
		if v, ok := raw["maxIndexingCapacityInOCU"].(float64); ok {
			i := int32(v)
			requested.MaxIndexingCapacityInOCU = &i
		}

		if v, ok := raw["maxSearchCapacityInOCU"].(float64); ok {
			i := int32(v)
			requested.MaxSearchCapacityInOCU = &i
		}
	}

	cl, err := h.Backend.UpdateServerlessAccountSettings(requested)
	if err != nil {
		return nil, err
	}

	return map[string]any{"accountSettingsDetail": accountSettingsDetailJR(cl)}, nil
}

// jrGetPoliciesStats builds GetPoliciesStatsOutput's response, whose top-level
// and nested keys are PascalCase -- unlike every other AOSS response body,
// verified against deserializers.go's
// awsAwsjson10_deserializeOpDocumentGetPoliciesStatsOutput (cases
// "AccessPolicyStats"/"LifecyclePolicyStats"/"SecurityConfigStats"/
// "SecurityPolicyStats"/"TotalPolicyCount") and each Stats struct's own
// deserializer (e.g. "DataPolicyCount", "RetentionPolicyCount").
func (h *Handler) jrGetPoliciesStats(_ map[string]any) (map[string]any, error) {
	stats := h.Backend.GetServerlessPoliciesStats()

	return map[string]any{
		"AccessPolicyStats":    map[string]any{"DataPolicyCount": stats.DataPolicyCount},
		"LifecyclePolicyStats": map[string]any{"RetentionPolicyCount": stats.RetentionPolicyCount},
		"SecurityConfigStats":  map[string]any{"SamlConfigCount": stats.SamlConfigCount},
		"SecurityPolicyStats": map[string]any{
			"EncryptionPolicyCount": stats.EncryptionPolicyCount,
			"NetworkPolicyCount":    stats.NetworkPolicyCount,
		},
		"TotalPolicyCount": stats.Total(),
	}, nil
}

// classicVpcEndpointDetailJR mirrors types.VpcEndpointDetail
// (types.go:1038-1070), resolved from the classic-domain VpcEndpoint store
// (vpc_endpoints.go). name/createdDate/failureCode/failureMessage aren't
// tracked on the classic VpcEndpoint model and are simply omitted -- all
// four are optional on the real type.
func classicVpcEndpointDetailJR(ep *VpcEndpoint) map[string]any {
	m := map[string]any{
		"id":               ep.VpcEndpointID,
		jsonKeyStatusLower: ep.Status,
	}

	if sg, ok := ep.VpcOptions["SecurityGroupIds"]; ok {
		m["securityGroupIds"] = sg
	}

	if sn, ok := ep.VpcOptions["SubnetIds"]; ok {
		m["subnetIds"] = sn
	}

	if vpcID, ok := ep.VpcOptions["VPCId"]; ok {
		m["vpcId"] = vpcID
	}

	return m
}

// nativeVpcEndpointDetailJR mirrors types.VpcEndpointDetail resolved from
// the AOSS-native store (serverless_vpc_endpoints.go), which -- unlike the
// classic store -- tracks name and createdDate.
func nativeVpcEndpointDetailJR(ep *ServerlessVpcEndpoint) map[string]any {
	m := map[string]any{
		"id":                 ep.ID,
		jsonKeyAppName:       ep.Name,
		jsonKeyStatusLower:   ep.Status,
		jsonKeyCreatedDateJR: ep.CreatedDate,
	}

	if len(ep.SubnetIDs) > 0 {
		m["subnetIds"] = ep.SubnetIDs
	}

	if len(ep.SecurityGroupIDs) > 0 {
		m["securityGroupIds"] = ep.SecurityGroupIDs
	}

	if ep.VpcID != "" {
		m["vpcId"] = ep.VpcID
	}

	if ep.FailureCode != "" {
		m["failureCode"] = ep.FailureCode
	}

	if ep.FailureMessage != "" {
		m["failureMessage"] = ep.FailureMessage
	}

	return m
}

// mergedVpcEndpointDetailJR dispatches to whichever store resolved the ID
// (see ServerlessVpcEndpointResult's doc comment).
func mergedVpcEndpointDetailJR(r ServerlessVpcEndpointResult) map[string]any {
	if r.Native != nil {
		return nativeVpcEndpointDetailJR(r.Native)
	}

	return classicVpcEndpointDetailJR(r.Classic)
}

func (h *Handler) jrBatchGetVpcEndpoint(input map[string]any) (map[string]any, error) {
	ids := strSliceJR(input, "ids")
	if len(ids) == 0 {
		return nil, fmt.Errorf("%w: Ids is required", ErrInvalidParameter)
	}

	found, errs := h.Backend.BatchGetServerlessVpcEndpoints(ids)

	details := make([]map[string]any, 0, len(found))
	for _, r := range found {
		details = append(details, mergedVpcEndpointDetailJR(r))
	}

	errDetails := make([]map[string]any, 0, len(errs))
	for _, e := range errs {
		errDetails = append(errDetails, map[string]any{
			"id": e.ID, jsonKeyErrorCodeJR: e.ErrorCode, jsonKeyErrorMessageJR: e.ErrorMessage,
		})
	}

	return map[string]any{
		"vpcEndpointDetails":      details,
		"vpcEndpointErrorDetails": errDetails,
	}, nil
}
