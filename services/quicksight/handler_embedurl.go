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

// classifyEmbedURLPaths routes /accounts/{id}/embed-url/... paths.
func classifyEmbedURLPaths(method string, segs []string, n int) (string, string) {
	if n < nSegsAccountResID {
		return opUnknown, ""
	}

	accountID := seg(segs, segAccountID)
	subType := seg(segs, segResID)

	if method != http.MethodPost {
		return opUnknown, ""
	}

	switch subType {
	case "anonymous-user":
		return opGenerateEmbedForAnonUser, accountID
	case "registered-user":
		return opGenerateEmbedForRegUser, accountID
	case "registered-user-with-identity":
		return opGenerateEmbedForRegUserIdentity, accountID
	}

	return opUnknown, ""
}

// ---- Identity Context ----

// identityFromUserIdentifier extracts the (kind, value) pair from a
// UserIdentifier request field, a smithy union serialized as exactly one of
// {"Email":..}, {"UserArn":..}, {"UserName":..}. Returns ("", "") if none of
// those keys are present.
func identityFromUserIdentifier(m map[string]any) (string, string) {
	for _, kind := range []string{identityKindEmail, identityKindUserArn, identityKindUserName} {
		if v, ok := m[kind].(string); ok && v != "" {
			return kind, v
		}
	}

	return "", ""
}

// handleGetIdentityContext mints an identity-context token for a QuickSight
// user. Real GetIdentityContextOutput returns the token under Context (an
// STS-style identity token to pass as AssumeRole's ContextAssertion), not a
// fabricated field.
func (h *Handler) handleGetIdentityContext(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	userIdentifier, _ := body["UserIdentifier"].(map[string]any)
	kind, value := identityFromUserIdentifier(userIdentifier)

	token, err := h.Backend.GenerateIdentityContext(
		accountID, strField(body, "Namespace"), kind, value, strField(body, "ContextRegion"),
	)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"Context":    token,
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}
