package opensearch

// Real AOSS-native VPC endpoint JSON-RPC handlers (opensearchserverless@v1.34.4
// api_op_{Create,Update,Delete,List}VpcEndpoint.go; field keys verified
// against serializers.go/deserializers.go, cited inline below).

// createVpcEndpointDetailJR mirrors types.CreateVpcEndpointDetail
// (types.go:452-464): id, name, status.
func createVpcEndpointDetailJR(ep *ServerlessVpcEndpoint) map[string]any {
	return map[string]any{"id": ep.ID, jsonKeyAppName: ep.Name, jsonKeyStatusLower: ep.Status}
}

// deleteVpcEndpointDetailJR mirrors types.DeleteVpcEndpointDetail
// (types.go:498-510): id, name, status.
func deleteVpcEndpointDetailJR(ep *ServerlessVpcEndpoint) map[string]any {
	return map[string]any{"id": ep.ID, jsonKeyAppName: ep.Name, jsonKeyStatusLower: ep.Status}
}

// updateVpcEndpointDetailJR mirrors types.UpdateVpcEndpointDetail
// (types.go:1002-1024): id, lastModifiedDate, name, securityGroupIds,
// status, subnetIds.
func updateVpcEndpointDetailJR(ep *ServerlessVpcEndpoint) map[string]any {
	m := map[string]any{
		"id":                      ep.ID,
		jsonKeyAppName:            ep.Name,
		jsonKeyStatusLower:        ep.Status,
		jsonKeyLastModifiedDateJR: ep.LastModifiedDate,
	}

	if len(ep.SubnetIDs) > 0 {
		m["subnetIds"] = ep.SubnetIDs
	}

	if len(ep.SecurityGroupIDs) > 0 {
		m["securityGroupIds"] = ep.SecurityGroupIDs
	}

	return m
}

// vpcEndpointSummaryJR mirrors types.VpcEndpointSummary (types.go:1097-1109):
// id, name, status.
func vpcEndpointSummaryJR(ep *ServerlessVpcEndpoint) map[string]any {
	return map[string]any{"id": ep.ID, jsonKeyAppName: ep.Name, jsonKeyStatusLower: ep.Status}
}

func (h *Handler) jrCreateVpcEndpoint(input map[string]any) (map[string]any, error) {
	name, _ := input[jsonKeyAppName].(string)
	vpcID, _ := input["vpcId"].(string)
	subnetIDs := strSliceJR(input, "subnetIds")
	securityGroupIDs := strSliceJR(input, "securityGroupIds")

	ep, err := h.Backend.CreateServerlessVpcEndpoint(name, vpcID, subnetIDs, securityGroupIDs)
	if err != nil {
		return nil, err
	}

	return map[string]any{"createVpcEndpointDetail": createVpcEndpointDetailJR(ep)}, nil
}

func (h *Handler) jrDeleteVpcEndpoint(input map[string]any) (map[string]any, error) {
	id, _ := input["id"].(string)

	ep, err := h.Backend.DeleteServerlessVpcEndpoint(id)
	if err != nil {
		return nil, err
	}

	return map[string]any{"deleteVpcEndpointDetail": deleteVpcEndpointDetailJR(ep)}, nil
}

func (h *Handler) jrUpdateVpcEndpoint(input map[string]any) (map[string]any, error) {
	id, _ := input["id"].(string)
	addSubnetIDs := strSliceJR(input, "addSubnetIds")
	removeSubnetIDs := strSliceJR(input, "removeSubnetIds")
	addSecurityGroupIDs := strSliceJR(input, "addSecurityGroupIds")
	removeSecurityGroupIDs := strSliceJR(input, "removeSecurityGroupIds")

	ep, err := h.Backend.UpdateServerlessVpcEndpoint(
		id, addSubnetIDs, removeSubnetIDs, addSecurityGroupIDs, removeSecurityGroupIDs,
	)
	if err != nil {
		return nil, err
	}

	// UpdateVpcEndpointOutput's wrapper key is "UpdateVpcEndpointDetail"
	// (PascalCase) -- unlike every other AOSS response wrapper key on this
	// surface, verified against deserializers.go's
	// awsAwsjson10_deserializeOpDocumentUpdateVpcEndpointOutput. Not a typo:
	// same kind of one-off casing quirk GetPoliciesStats already has
	// (handler_serverless_account.go's doc comment).
	return map[string]any{"UpdateVpcEndpointDetail": updateVpcEndpointDetailJR(ep)}, nil
}

func (h *Handler) jrListVpcEndpoints(input map[string]any) (map[string]any, error) {
	var statusFilter string
	if f, ok := input["vpcEndpointFilters"].(map[string]any); ok {
		statusFilter, _ = f[jsonKeyStatusLower].(string)
	}

	eps := h.Backend.ListServerlessVpcEndpoints(statusFilter)
	summaries := make([]map[string]any, 0, len(eps))

	for _, ep := range eps {
		summaries = append(summaries, vpcEndpointSummaryJR(ep))
	}

	return map[string]any{"vpcEndpointSummaries": summaries}, nil
}
