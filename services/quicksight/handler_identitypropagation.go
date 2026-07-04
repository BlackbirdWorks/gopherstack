package quicksight

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// JSON response/body keys used only by identity propagation config operations.
const (
	keyService           = "Service"
	keyServices          = "Services"
	keyAuthorizedTargets = "AuthorizedTargets"
)

func isIdentityPropOp(op string) bool {
	switch op {
	case opUpdateIdentityPropagationConfig, opDeleteIdentityPropagationConfig, opListIdentityPropagationConfigs:
		return true
	}

	return false
}

func (h *Handler) dispatchIdentityProp(c *echo.Context, op string) error {
	switch op {
	case opUpdateIdentityPropagationConfig:
		return h.handleUpdateIdentityPropagationConfig(c)
	case opDeleteIdentityPropagationConfig:
		return h.handleDeleteIdentityPropagationConfig(c)
	case opListIdentityPropagationConfigs:
		return h.handleListIdentityPropagationConfigs(c)
	}

	return writeError(
		c,
		http.StatusNotImplemented,
		"UnsupportedOperationException",
		"operation not implemented: "+op,
	)
}

func (h *Handler) handleUpdateIdentityPropagationConfig(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	service := seg(segs, segResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	if err = h.Backend.UpdateIdentityPropagationConfig(
		accountID, service, stringsFromBody(body, keyAuthorizedTargets),
	); err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleDeleteIdentityPropagationConfig(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	service := seg(segs, segResID)

	if err := h.Backend.DeleteIdentityPropagationConfig(accountID, service); err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleListIdentityPropagationConfigs(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	configs, err := h.Backend.ListIdentityPropagationConfigs(accountID)
	if err != nil {
		return httpErr(c, err)
	}

	items := make([]map[string]any, 0, len(configs))
	for _, cfg := range configs {
		items = append(items, map[string]any{
			keyService:           cfg.Service,
			keyAuthorizedTargets: cfg.AuthorizedTargets,
		})
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyServices:  items,
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}
