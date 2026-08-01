package resiliencehub

import (
	"context"
	"net/http"
)

func (h *Handler) handleCreateAppVersionAppComponent(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req struct {
		createAppVersionAppComponentRequest

		AppArn string `json:"appArn"`
	}
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	a, c, err := h.Backend.CreateAppVersionAppComponent(req.AppArn, &req.createAppVersionAppComponentRequest)
	if err != nil {
		return nil, err
	}

	wire := toAppComponentWire(c)

	return marshalResponse(appComponentEnvelope{AppArn: a.ARN, AppVersion: draftVersion, AppComponent: &wire})
}

func (h *Handler) handleDescribeAppVersionAppComponent(
	_ context.Context,
	_ *http.Request,
	body []byte,
) ([]byte, error) {
	var req struct {
		AppArn     string `json:"appArn"`
		AppVersion string `json:"appVersion"`
		ID         string `json:"id"`
	}
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	a, c, err := h.Backend.DescribeAppVersionAppComponent(req.AppArn, req.AppVersion, req.ID)
	if err != nil {
		return nil, err
	}

	wire := toAppComponentWire(c)

	return marshalResponse(appComponentEnvelope{AppArn: a.ARN, AppVersion: req.AppVersion, AppComponent: &wire})
}

func (h *Handler) handleUpdateAppVersionAppComponent(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req struct {
		updateAppVersionAppComponentRequest

		AppArn string `json:"appArn"`
		ID     string `json:"id"`
	}
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	a, c, err := h.Backend.UpdateAppVersionAppComponent(req.AppArn, &req.updateAppVersionAppComponentRequest, req.ID)
	if err != nil {
		return nil, err
	}

	wire := toAppComponentWire(c)

	return marshalResponse(appComponentEnvelope{AppArn: a.ARN, AppVersion: draftVersion, AppComponent: &wire})
}

func (h *Handler) handleDeleteAppVersionAppComponent(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req struct {
		deleteAppVersionAppComponentRequest

		AppArn string `json:"appArn"`
	}
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	a, c, err := h.Backend.DeleteAppVersionAppComponent(req.AppArn, req.ID)
	if err != nil {
		return nil, err
	}

	wire := toAppComponentWire(c)

	return marshalResponse(appComponentEnvelope{AppArn: a.ARN, AppVersion: draftVersion, AppComponent: &wire})
}

func (h *Handler) handleListAppVersionAppComponents(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req struct {
		AppArn     string `json:"appArn"`
		AppVersion string `json:"appVersion"`
		NextToken  string `json:"nextToken,omitempty"`
		MaxResults int32  `json:"maxResults,omitempty"`
	}
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	a, p, err := h.Backend.ListAppVersionAppComponents(req.AppArn, req.AppVersion, req.NextToken, int(req.MaxResults))
	if err != nil {
		return nil, err
	}

	return marshalResponse(listAppVersionAppComponentsResponse{
		AppArn: a.ARN, AppVersion: req.AppVersion, AppComponents: toAppComponentsWire(p.Data), NextToken: p.Next,
	})
}
