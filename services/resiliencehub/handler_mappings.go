package resiliencehub

import (
	"context"
	"net/http"
)

func (h *Handler) handleAddDraftAppVersionResourceMappings(
	_ context.Context,
	_ *http.Request,
	body []byte,
) ([]byte, error) {
	var req struct {
		AppArn string `json:"appArn"`
		addDraftAppVersionResourceMappingsRequest
	}
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	a, mappings, err := h.Backend.AddDraftAppVersionResourceMappings(req.AppArn, req.ResourceMappings)
	if err != nil {
		return nil, err
	}

	return marshalResponse(
		resourceMappingsEnvelope{
			AppArn:           a.ARN,
			AppVersion:       draftVersion,
			ResourceMappings: toResourceMappingsWire(mappings),
		},
	)
}

func (h *Handler) handleRemoveDraftAppVersionResourceMappings(
	_ context.Context,
	_ *http.Request,
	body []byte,
) ([]byte, error) {
	var req struct {
		AppArn string `json:"appArn"`
		removeDraftAppVersionResourceMappingsRequest
	}
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	a, err := h.Backend.RemoveDraftAppVersionResourceMappings(
		req.AppArn,
		&req.removeDraftAppVersionResourceMappingsRequest,
	)
	if err != nil {
		return nil, err
	}

	return marshalResponse(appArnVersionResponse{AppArn: a.ARN, AppVersion: draftVersion})
}

func (h *Handler) handleListAppVersionResourceMappings(
	_ context.Context,
	_ *http.Request,
	body []byte,
) ([]byte, error) {
	var req struct {
		AppArn     string `json:"appArn"`
		AppVersion string `json:"appVersion"`
		NextToken  string `json:"nextToken,omitempty"`
		MaxResults int32  `json:"maxResults,omitempty"`
	}
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	p, err := h.Backend.ListAppVersionResourceMappings(req.AppArn, req.AppVersion, req.NextToken, int(req.MaxResults))
	if err != nil {
		return nil, err
	}

	return marshalResponse(
		listAppVersionResourceMappingsResponse{ResourceMappings: toResourceMappingsWire(p.Data), NextToken: p.Next},
	)
}

func (h *Handler) handleResolveAppVersionResources(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req struct {
		AppArn     string `json:"appArn"`
		AppVersion string `json:"appVersion"`
	}
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	a, resolutionID, status, err := h.Backend.ResolveAppVersionResources(req.AppArn, req.AppVersion)
	if err != nil {
		return nil, err
	}

	return marshalResponse(resolveAppVersionResourcesResponse{
		AppArn: a.ARN, AppVersion: req.AppVersion, ResolutionID: resolutionID, Status: status,
	})
}

func (h *Handler) handleDescribeAppVersionResourcesResolutionStatus(
	_ context.Context, _ *http.Request, body []byte,
) ([]byte, error) {
	var req struct {
		AppArn       string `json:"appArn"`
		AppVersion   string `json:"appVersion"`
		ResolutionID string `json:"resolutionId,omitempty"`
	}
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	resolutionID, status, errMsg, err := h.Backend.DescribeAppVersionResourcesResolutionStatus(
		req.AppArn,
		req.AppVersion,
		req.ResolutionID,
	)
	if err != nil {
		return nil, err
	}

	return marshalResponse(describeAppVersionResourcesResolutionStatusResponse{
		AppArn:       req.AppArn,
		AppVersion:   req.AppVersion,
		ResolutionID: resolutionID,
		Status:       status,
		ErrorMessage: errMsg,
	})
}

func (h *Handler) handleImportResourcesToDraftAppVersion(
	_ context.Context,
	_ *http.Request,
	body []byte,
) ([]byte, error) {
	var req struct {
		AppArn string `json:"appArn"`
		importResourcesToDraftAppVersionRequest
	}
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	a, status, err := h.Backend.ImportResourcesToDraftAppVersion(
		req.AppArn,
		&req.importResourcesToDraftAppVersionRequest,
	)
	if err != nil {
		return nil, err
	}

	return marshalResponse(importResourcesToDraftAppVersionResponse{
		AppArn: a.ARN, AppVersion: draftVersion, Status: status,
		SourceArns: req.SourceArns, EksSources: req.EksSources, TerraformSources: req.TerraformSources,
	})
}

func (h *Handler) handleDescribeDraftAppVersionResourcesImportStatus(
	_ context.Context, _ *http.Request, body []byte,
) ([]byte, error) {
	var req struct {
		AppArn string `json:"appArn"`
	}
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	a, imp, err := h.Backend.DescribeDraftAppVersionResourcesImportStatus(req.AppArn)
	if err != nil {
		return nil, err
	}

	resp := describeDraftAppVersionResourcesImportStatusResponse{
		AppArn: a.ARN, AppVersion: draftVersion, Status: imp.Status, ErrorMessage: imp.ErrorMessage,
		StatusChangeTime: epochPtrT(imp.StatusChangeTime),
	}

	for _, e := range imp.ErrorDetails {
		resp.ErrorDetails = append(resp.ErrorDetails, struct {
			ErrorMessage string `json:"errorMessage,omitempty"`
		}{ErrorMessage: e})
	}

	return marshalResponse(resp)
}

func (h *Handler) handleDeleteAppInputSource(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req struct {
		deleteAppInputSourceRequest

		AppArn string `json:"appArn"`
	}
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	sourceArn := req.SourceArn
	if sourceArn == "" && req.TerraformSource != nil {
		sourceArn = req.TerraformSource.S3StateFileURL
	}

	if sourceArn == "" && req.EksSourceClusterNamespace != nil {
		sourceArn = req.EksSourceClusterNamespace.EksClusterArn
	}

	a, src, err := h.Backend.DeleteAppInputSource(req.AppArn, sourceArn)
	if err != nil {
		return nil, err
	}

	wire := appInputSourceWire{
		ImportType: src.ImportType, SourceArn: src.SourceArn, SourceName: src.SourceName,
		ResourceCount: src.ResourceCount,
	}

	return marshalResponse(appInputSourceEnvelope{AppArn: a.ARN, AppInputSource: &wire})
}

func (h *Handler) handleListAppInputSources(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req struct {
		AppArn     string `json:"appArn"`
		AppVersion string `json:"appVersion"`
		NextToken  string `json:"nextToken,omitempty"`
		MaxResults int32  `json:"maxResults,omitempty"`
	}
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	p, err := h.Backend.ListAppInputSources(req.AppArn, req.AppVersion, req.NextToken, int(req.MaxResults))
	if err != nil {
		return nil, err
	}

	resp := listAppInputSourcesResponse{AppInputSources: make([]appInputSourceWire, 0, len(p.Data)), NextToken: p.Next}
	for _, s := range p.Data {
		resp.AppInputSources = append(resp.AppInputSources, appInputSourceWire{
			ImportType: s.ImportType, SourceArn: s.SourceArn, SourceName: s.SourceName, ResourceCount: s.ResourceCount,
		})
	}

	return marshalResponse(resp)
}
