package resiliencehub

import (
	"context"
	"net/http"
	"strconv"
	"time"
)

func (h *Handler) handleCreateApp(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req createAppRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	a, err := h.Backend.CreateApp(&req)
	if err != nil {
		return nil, err
	}

	wire := toAppWire(a)

	return marshalResponse(appEnvelope{App: &wire})
}

func (h *Handler) handleDescribeApp(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req appArnRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	a, err := h.Backend.GetApp(req.AppArn)
	if err != nil {
		return nil, err
	}

	wire := toAppWire(a)

	return marshalResponse(appEnvelope{App: &wire})
}

func (h *Handler) handleUpdateApp(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req struct {
		AppArn string `json:"appArn"`
		updateAppRequest
	}
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	a, err := h.Backend.UpdateApp(req.AppArn, &req.updateAppRequest)
	if err != nil {
		return nil, err
	}

	wire := toAppWire(a)

	return marshalResponse(appEnvelope{App: &wire})
}

func (h *Handler) handleDeleteApp(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req struct {
		deleteAppRequest

		AppArn string `json:"appArn"`
	}
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	forceDelete := req.ForceDelete != nil && *req.ForceDelete

	if err := h.Backend.DeleteApp(req.AppArn, forceDelete); err != nil {
		return nil, err
	}

	return marshalResponse(appArnRequest{AppArn: req.AppArn})
}

func (h *Handler) handleListApps(_ context.Context, r *http.Request, _ []byte) ([]byte, error) {
	q := r.URL.Query()
	f := listAppsFilter{appArn: q.Get("appArn"), awsApplicationArn: q.Get("awsApplicationArn"), name: q.Get("name")}

	p := h.Backend.ListApps(f, q.Get("nextToken"), queryMaxResults(q))

	resp := listAppsResponse{AppSummaries: make([]appSummaryWire, 0, len(p.Data)), NextToken: p.Next}
	for _, a := range p.Data {
		resp.AppSummaries = append(resp.AppSummaries, toAppSummaryWire(a))
	}

	return marshalResponse(resp)
}

func (h *Handler) handleDescribeAppVersion(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req struct {
		AppArn     string `json:"appArn"`
		AppVersion string `json:"appVersion"`
	}
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	a, v, err := h.Backend.DescribeAppVersion(req.AppArn, req.AppVersion)
	if err != nil {
		return nil, err
	}

	return marshalResponse(
		describeAppVersionResponse{AppArn: a.ARN, AppVersion: v.Number, AdditionalInfo: v.AdditionalInfo},
	)
}

func (h *Handler) handleUpdateAppVersion(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req struct {
		updateAppVersionRequest

		AppArn string `json:"appArn"`
	}
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	a, v, err := h.Backend.UpdateAppVersion(req.AppArn, req.AdditionalInfo)
	if err != nil {
		return nil, err
	}

	return marshalResponse(
		describeAppVersionResponse{AppArn: a.ARN, AppVersion: v.Number, AdditionalInfo: v.AdditionalInfo},
	)
}

func (h *Handler) handleListAppVersions(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req struct {
		StartTime  *float64 `json:"startTime,omitempty"`
		EndTime    *float64 `json:"endTime,omitempty"`
		AppArn     string   `json:"appArn"`
		NextToken  string   `json:"nextToken,omitempty"`
		MaxResults int32    `json:"maxResults,omitempty"`
	}
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	p, err := h.Backend.ListAppVersions(
		req.AppArn,
		floatPtrToTime(req.StartTime),
		floatPtrToTime(req.EndTime),
		req.NextToken,
		int(req.MaxResults),
	)
	if err != nil {
		return nil, err
	}

	return marshalResponse(listAppVersionsResponse{AppVersions: p.Data, NextToken: p.Next})
}

func floatPtrToTime(f *float64) time.Time {
	if f == nil {
		return time.Time{}
	}

	return time.Unix(0, int64(*f*float64(time.Second))).UTC()
}

func (h *Handler) handlePublishAppVersion(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req struct {
		publishAppVersionRequest

		AppArn string `json:"appArn"`
	}
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	a, number, err := h.Backend.PublishAppVersion(req.AppArn, req.VersionName)
	if err != nil {
		return nil, err
	}

	resp := publishAppVersionResponse{AppArn: a.ARN, AppVersion: number, VersionName: req.VersionName}

	if n, convErr := strconv.ParseInt(number, 10, 64); convErr == nil {
		resp.Identifier = &n
	}

	return marshalResponse(resp)
}

func (h *Handler) handleDescribeAppVersionTemplate(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req struct {
		AppArn     string `json:"appArn"`
		AppVersion string `json:"appVersion"`
	}
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	templateBody, err := h.Backend.DescribeAppVersionTemplate(req.AppArn, req.AppVersion)
	if err != nil {
		return nil, err
	}

	return marshalResponse(
		describeAppVersionTemplateResponse{
			AppArn:          req.AppArn,
			AppVersion:      req.AppVersion,
			AppTemplateBody: templateBody,
		},
	)
}

func (h *Handler) handlePutDraftAppVersionTemplate(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req struct {
		putDraftAppVersionTemplateRequest

		AppArn string `json:"appArn"`
	}
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	a, err := h.Backend.PutDraftAppVersionTemplate(req.AppArn, req.AppTemplateBody)
	if err != nil {
		return nil, err
	}

	return marshalResponse(appArnVersionResponse{AppArn: a.ARN, AppVersion: draftVersion})
}
