package quicksight

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// JSON response/body keys used only by embed URL operations.
const (
	keyEmbedURL                = "EmbedUrl"
	keyAnonymousUserArn        = "AnonymousUserArn"
	keyAuthorizedResourceArns  = "AuthorizedResourceArns"
	keyExperienceConfiguration = "ExperienceConfiguration"
	keyUserArn                 = "UserArn"
	keyIdentityTypeParam       = "creds-type"
	keyEntryPointParam         = "entry-point"
)

func isEmbedURLOp(op string) bool {
	switch op {
	case opGenerateEmbedForAnonUser, opGenerateEmbedForRegUser, opGenerateEmbedForRegUserIdentity,
		opGetDashboardEmbedUrl, opGetSessionEmbedUrl:
		return true
	}

	return false
}

func (h *Handler) dispatchEmbedURL(c *echo.Context, op string) error {
	switch op {
	case opGenerateEmbedForAnonUser:
		return h.handleGenerateEmbedForAnonymousUser(c)
	case opGenerateEmbedForRegUser:
		return h.handleGenerateEmbedForRegisteredUser(c)
	case opGenerateEmbedForRegUserIdentity:
		return h.handleGenerateEmbedForRegisteredUserWithIdentity(c)
	case opGetDashboardEmbedUrl:
		return h.handleGetDashboardEmbedURL(c)
	case opGetSessionEmbedUrl:
		return h.handleGetSessionEmbedURL(c)
	}

	return writeError(
		c,
		http.StatusNotImplemented,
		"UnsupportedOperationException",
		"operation not implemented: "+op,
	)
}

func (h *Handler) handleGenerateEmbedForAnonymousUser(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	namespace := strField(body, keyNamespace)
	if namespace == "" {
		namespace = defaultNamespace
	}

	embedURL, anonymousUserArn, err := h.Backend.GenerateEmbedURLForAnonymousUser(
		accountID, namespace,
		stringsFromBody(body, keyAuthorizedResourceArns),
		mapField(body, keyExperienceConfiguration),
	)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyEmbedURL:         embedURL,
		keyAnonymousUserArn: anonymousUserArn,
		keyRequestID:        reqIDPlaceholder,
		keyStatus:           http.StatusOK,
	})
}

func (h *Handler) handleGenerateEmbedForRegisteredUser(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	embedURL, err := h.Backend.GenerateEmbedURLForRegisteredUser(
		accountID, strField(body, keyUserArn), mapField(body, keyExperienceConfiguration),
	)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyEmbedURL:  embedURL,
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleGenerateEmbedForRegisteredUserWithIdentity(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	embedURL, err := h.Backend.GenerateEmbedURLForRegisteredUserWithIdentity(
		accountID, mapField(body, keyExperienceConfiguration),
	)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyEmbedURL:  embedURL,
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleGetDashboardEmbedURL(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	dashboardID := seg(segs, segResID)
	identityType := queryParam(c, keyIdentityTypeParam)

	embedURL, err := h.Backend.GetDashboardEmbedURL(accountID, dashboardID, identityType)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyEmbedURL:  embedURL,
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleGetSessionEmbedURL(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	entryPoint := queryParam(c, keyEntryPointParam)

	embedURL, err := h.Backend.GetSessionEmbedURL(accountID, entryPoint)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyEmbedURL:  embedURL,
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}
