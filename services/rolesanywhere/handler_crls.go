package rolesanywhere

import (
	"context"
	"encoding/json"
	"net/http"
	"time"
)

// ---- CRL handlers ----

func (h *Handler) handleImportCrl(ctx context.Context, body []byte) (any, int, error) {
	var req struct {
		Enabled        *bool      `json:"enabled"`
		Name           string     `json:"name"`
		TrustAnchorArn string     `json:"trustAnchorArn"`
		CrlData        []byte     `json:"crlData"`
		Tags           []TagEntry `json:"tags"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, 0, ErrValidation
	}

	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}

	crl, err := h.Backend.ImportCrl(ctx, req.Name, req.CrlData, req.TrustAnchorArn, enabled, req.Tags)
	if err != nil {
		return nil, 0, err
	}

	return map[string]any{keyCrl: crlToJSON(crl)}, http.StatusCreated, nil
}

func (h *Handler) handleGetCrl(ctx context.Context, path string) (any, int, error) {
	id := extractID(path, pathCrl)

	crl, err := h.Backend.GetCrl(ctx, id)
	if err != nil {
		return nil, 0, err
	}

	return map[string]any{keyCrl: crlToJSON(crl)}, http.StatusOK, nil
}

func (h *Handler) handleListCrls(ctx context.Context, query string) (any, int, error) {
	pageToken, maxResults, ppErr := parsePageParams(query)
	if ppErr != nil {
		return nil, 0, ppErr
	}

	all, next, err := h.Backend.ListCrls(ctx, pageToken, maxResults)
	if err != nil {
		return nil, 0, err
	}

	list := make([]any, 0, len(all))

	for _, c := range all {
		list = append(list, crlToJSON(c))
	}

	resp := map[string]any{keyCrls: list}

	if next != "" {
		resp["nextToken"] = next
	}

	return resp, http.StatusOK, nil
}

func (h *Handler) handleUpdateCrl(ctx context.Context, path string, body []byte) (any, int, error) {
	id := extractID(path, pathCrl)

	var req struct {
		Name    string `json:"name"`
		CrlData []byte `json:"crlData"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, 0, ErrValidation
	}

	crl, err := h.Backend.UpdateCrl(ctx, id, req.Name, req.CrlData)
	if err != nil {
		return nil, 0, err
	}

	return map[string]any{keyCrl: crlToJSON(crl)}, http.StatusOK, nil
}

func (h *Handler) handleDeleteCrl(ctx context.Context, path string) (any, int, error) {
	id := extractID(path, pathCrl)

	crl, err := h.Backend.DeleteCrl(ctx, id)
	if err != nil {
		return nil, 0, err
	}

	return map[string]any{keyCrl: crlToJSON(crl)}, http.StatusOK, nil
}

func (h *Handler) handleEnableCrl(ctx context.Context, path string) (any, int, error) {
	id := extractID(path, pathCrl)

	crl, err := h.Backend.EnableCrl(ctx, id)
	if err != nil {
		return nil, 0, err
	}

	return map[string]any{keyCrl: crlToJSON(crl)}, http.StatusOK, nil
}

func (h *Handler) handleDisableCrl(ctx context.Context, path string) (any, int, error) {
	id := extractID(path, pathCrl)

	crl, err := h.Backend.DisableCrl(ctx, id)
	if err != nil {
		return nil, 0, err
	}

	return map[string]any{keyCrl: crlToJSON(crl)}, http.StatusOK, nil
}

func crlToJSON(c *Crl) map[string]any {
	return map[string]any{
		"crlId":          c.CrlID,
		"crlArn":         c.CrlArn,
		"name":           c.Name, //nolint:goconst // existing issue.
		"crlData":        c.CrlData,
		"trustAnchorArn": c.TrustAnchorArn,
		"enabled":        c.Enabled,                        //nolint:goconst // existing issue.
		"createdAt":      c.CreatedAt.Format(time.RFC3339), //nolint:goconst // existing issue.
		"updatedAt":      c.UpdatedAt.Format(time.RFC3339), //nolint:goconst // existing issue.
	}
}
