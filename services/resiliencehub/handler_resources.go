package resiliencehub

import (
	"context"
	"net/http"
)

func (h *Handler) handleCreateAppVersionResource(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req struct {
		AppArn string `json:"appArn"`
		createAppVersionResourceRequest
	}
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	a, r, err := h.Backend.CreateAppVersionResource(req.AppArn, &req.createAppVersionResourceRequest)
	if err != nil {
		return nil, err
	}

	wire := toPhysicalResourceWire(r)

	return marshalResponse(physicalResourceEnvelope{AppArn: a.ARN, AppVersion: draftVersion, PhysicalResource: &wire})
}

func (h *Handler) handleDescribeAppVersionResource(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req describeAppVersionResourceRequest

	var envelope struct {
		describeAppVersionResourceRequest

		AppArn string `json:"appArn"`
	}
	if err := decodeJSONBody(body, &envelope); err != nil {
		return nil, err
	}

	req = envelope.describeAppVersionResourceRequest

	loc := findResourceLocator{
		logicalID: fromLogicalResourceIDWire(req.LogicalResourceID), resourceName: req.ResourceName,
		physicalID: req.PhysicalResourceID, awsAccountID: req.AwsAccountID, awsRegion: req.AwsRegion,
	}

	a, r, err := h.Backend.DescribeAppVersionResource(envelope.AppArn, req.AppVersion, loc)
	if err != nil {
		return nil, err
	}

	wire := toPhysicalResourceWire(r)

	return marshalResponse(physicalResourceEnvelope{AppArn: a.ARN, AppVersion: req.AppVersion, PhysicalResource: &wire})
}

func (h *Handler) handleUpdateAppVersionResource(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req struct {
		AppArn string `json:"appArn"`
		updateAppVersionResourceRequest
	}
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	loc := findResourceLocator{
		logicalID: fromLogicalResourceIDWire(req.LogicalResourceID), resourceName: req.ResourceName,
		physicalID: req.PhysicalResourceID, awsAccountID: req.AwsAccountID, awsRegion: req.AwsRegion,
	}

	a, r, err := h.Backend.UpdateAppVersionResource(req.AppArn, &req.updateAppVersionResourceRequest, loc)
	if err != nil {
		return nil, err
	}

	wire := toPhysicalResourceWire(r)

	return marshalResponse(physicalResourceEnvelope{AppArn: a.ARN, AppVersion: draftVersion, PhysicalResource: &wire})
}

func (h *Handler) handleDeleteAppVersionResource(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req struct {
		deleteAppVersionResourceRequest

		AppArn string `json:"appArn"`
	}
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	loc := findResourceLocator{
		logicalID: fromLogicalResourceIDWire(req.LogicalResourceID), resourceName: req.ResourceName,
		physicalID: req.PhysicalResourceID, awsAccountID: req.AwsAccountID, awsRegion: req.AwsRegion,
	}

	a, r, err := h.Backend.DeleteAppVersionResource(req.AppArn, loc)
	if err != nil {
		return nil, err
	}

	wire := toPhysicalResourceWire(r)

	return marshalResponse(physicalResourceEnvelope{AppArn: a.ARN, AppVersion: draftVersion, PhysicalResource: &wire})
}

func (h *Handler) handleListAppVersionResources(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req struct {
		AppArn       string `json:"appArn"`
		AppVersion   string `json:"appVersion"`
		ResolutionID string `json:"resolutionId,omitempty"`
		NextToken    string `json:"nextToken,omitempty"`
		MaxResults   int32  `json:"maxResults,omitempty"`
	}
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	_, v, p, err := h.Backend.ListAppVersionResources(req.AppArn, req.AppVersion, req.NextToken, int(req.MaxResults))
	if err != nil {
		return nil, err
	}

	resp := listAppVersionResourcesResponse{
		PhysicalResources: make([]physicalResourceWire, 0, len(p.Data)),
		NextToken:         p.Next,
	}
	if v.Resolution != nil {
		resp.ResolutionID = v.Resolution.ResolutionID
	}

	for _, r := range p.Data {
		resp.PhysicalResources = append(resp.PhysicalResources, toPhysicalResourceWire(r))
	}

	return marshalResponse(resp)
}

func (h *Handler) handleListUnsupportedAppVersionResources(
	_ context.Context,
	_ *http.Request,
	body []byte,
) ([]byte, error) {
	var req struct {
		AppArn       string `json:"appArn"`
		AppVersion   string `json:"appVersion"`
		ResolutionID string `json:"resolutionId,omitempty"`
		NextToken    string `json:"nextToken,omitempty"`
		MaxResults   int32  `json:"maxResults,omitempty"`
	}
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	_, v, p, err := h.Backend.ListUnsupportedAppVersionResources(
		req.AppArn,
		req.AppVersion,
		req.NextToken,
		int(req.MaxResults),
	)
	if err != nil {
		return nil, err
	}

	resp := listUnsupportedAppVersionResourcesResponse{
		UnsupportedResources: make([]unsupportedResourceWire, 0, len(p.Data)), NextToken: p.Next,
	}
	if v.Resolution != nil {
		resp.ResolutionID = v.Resolution.ResolutionID
	}

	for _, r := range p.Data {
		resp.UnsupportedResources = append(resp.UnsupportedResources, unsupportedResourceWire{
			LogicalResourceID:  toLogicalResourceIDWire(r.LogicalResourceID),
			PhysicalResourceID: toPhysicalResourceIDWire(r.PhysicalResourceID),
			ResourceType:       r.ResourceType,
		})
	}

	return marshalResponse(resp)
}
