package app

import (
	"context"
	"fmt"

	apigwbackend "github.com/blackbirdworks/gopherstack/services/apigateway"
	agentcorebackend "github.com/blackbirdworks/gopherstack/services/bedrockagentcorecontrol"
)

var _ agentcorebackend.RestAPIHost = agentCoreRefs{}

// RestAPIExport exports a REST API stage as OpenAPI 3.
func (r agentCoreRefs) RestAPIExport(region, restAPIID, stage string) (map[string]any, error) {
	if r.apigw == nil {
		return nil, agentcorebackend.ErrRestAPIHostUnavailable
	}

	out, err := r.apigw.BackendFor(region).GetExport(restAPIID, stage, "oas30", apigwbackend.ExportOptions{})
	if err != nil {
		return nil, fmt.Errorf("export %s/%s: %w", restAPIID, stage, err)
	}

	return out, nil
}

// InvokeRestAPI serves the request through the stage's data-plane path.
func (r agentCoreRefs) InvokeRestAPI(
	ctx context.Context, _ string, req agentcorebackend.RestAPIRequest,
) (agentcorebackend.RestAPIResponse, error) {
	if r.apigw == nil {
		return agentcorebackend.RestAPIResponse{}, agentcorebackend.ErrRestAPIHostUnavailable
	}

	resp := r.apigw.InvokeStage(ctx, apigwbackend.StageRequest{
		APIID: req.APIID, Stage: req.Stage, Method: req.Method, Path: req.Path,
		Headers: req.Headers, Query: req.Query, Body: req.Body,
	})

	return agentcorebackend.RestAPIResponse{Status: resp.Status, Body: resp.Body}, nil
}
