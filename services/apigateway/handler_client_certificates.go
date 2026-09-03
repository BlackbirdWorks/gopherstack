package apigateway

import (
	"encoding/json"
	"net/http"
)

const opUpdateClientCertificate = "UpdateClientCertificate"

type getClientCertificatesInput struct {
	Position string `json:"position"`
	Limit    int    `json:"limit"`
}

// parseAPIGWClientCertificatesPath handles /clientcertificates/... paths.
func parseAPIGWClientCertificatesPath(method string, segs []string, n int) (string, map[string]string, bool) {
	switch {
	// GET /clientcertificates → GetClientCertificates
	case n == 1 && method == http.MethodGet:
		return opGetClientCertificates, nil, true
	// POST /clientcertificates → GenerateClientCertificate
	case n == 1 && method == http.MethodPost:
		return opGenerateClientCertificate, nil, true
	// GET /clientcertificates/{id} → GetClientCertificate
	case n == pathDepth2 && method == http.MethodGet:
		return opGetClientCertificate, map[string]string{keyClientCertificateID: segs[1]}, true
	// DELETE /clientcertificates/{id} → DeleteClientCertificate
	case n == pathDepth2 && method == http.MethodDelete:
		return opDeleteClientCertificate, map[string]string{keyClientCertificateID: segs[1]}, true
	// PATCH /clientcertificates/{id} → UpdateClientCertificate
	case n == pathDepth2 && method == http.MethodPatch:
		return opUpdateClientCertificate, map[string]string{keyClientCertificateID: segs[1]}, true
	}

	return apiGWUnknownOp, nil, false
}

// clientCertificateActions returns the action map for client certificate CRUD operations.
func (h *Handler) clientCertificateActions() map[string]actionFn {
	return map[string]actionFn{
		opGenerateClientCertificate: h.generateClientCertificateAction,
		opGetClientCertificate:      h.getClientCertificateAction,
		opGetClientCertificates:     h.getClientCertificatesAction,
		opDeleteClientCertificate:   h.deleteClientCertificateAction,
		opUpdateClientCertificate:   h.updateClientCertificateAction,
	}
}

func (h *Handler) generateClientCertificateAction(b []byte) (int, any, error) {
	var input GenerateClientCertificateInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	out, err := h.Backend.GenerateClientCertificate(input)
	if err != nil {
		return 0, nil, err
	}

	return http.StatusCreated, out, nil
}

func (h *Handler) getClientCertificateAction(b []byte) (int, any, error) {
	var params struct {
		ClientCertificateID string `json:"clientCertificateId"`
	}
	if err := json.Unmarshal(b, &params); err != nil {
		return 0, nil, err
	}
	out, err := h.Backend.GetClientCertificate(params.ClientCertificateID)
	if err != nil {
		return 0, nil, err
	}

	return http.StatusOK, out, nil
}

func (h *Handler) getClientCertificatesAction(b []byte) (int, any, error) {
	var input getClientCertificatesInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	out, err := h.Backend.GetClientCertificates()
	if err != nil {
		return 0, nil, err
	}
	if input.Limit == 0 && input.Position == "" {
		return http.StatusOK, map[string]any{keyItem: out}, nil
	}
	page, position := paginatePageByKey(out, input.Limit, input.Position,
		func(c ClientCertificate) string { return c.ClientCertificateID })
	if position != "" {
		return http.StatusOK, map[string]any{keyItem: page, keyPosition: position}, nil
	}

	return http.StatusOK, map[string]any{keyItem: page}, nil
}

func (h *Handler) deleteClientCertificateAction(b []byte) (int, any, error) {
	var params struct {
		ClientCertificateID string `json:"clientCertificateId"`
	}
	if err := json.Unmarshal(b, &params); err != nil {
		return 0, nil, err
	}
	if err := h.Backend.DeleteClientCertificate(params.ClientCertificateID); err != nil {
		return 0, nil, err
	}

	return http.StatusNoContent, nil, nil
}

func (h *Handler) updateClientCertificateAction(b []byte) (int, any, error) {
	var input UpdateClientCertificateInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}

	cert, err := h.Backend.UpdateClientCertificate(input)
	if err != nil {
		return 0, nil, err
	}

	return http.StatusOK, cert, nil
}
