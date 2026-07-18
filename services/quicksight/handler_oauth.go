package quicksight

import (
	"errors"
	"maps"
	"net/http"

	"github.com/labstack/echo/v5"
)

// JSON response/body keys used only by OAuth client application operations.
const (
	keyOAuthClientApplicationID = "OAuthClientApplicationId"
	keyOAuthClientApplication   = "OAuthClientApplication"
	keyOAuthClientApplications  = "OAuthClientApplications"
	keyCreationStatusField      = "CreationStatus"
	keyUpdateStatusField        = "UpdateStatus"
)

func isOAuthOp(op string) bool {
	switch op {
	case opCreateOAuthClientApp, opDescribeOAuthClientApp, opUpdateOAuthClientApp,
		opDeleteOAuthClientApp, opListOAuthClientApps:
		return true
	}

	return false
}

func (h *Handler) dispatchOAuth(c *echo.Context, op string) error {
	switch op {
	case opCreateOAuthClientApp:
		return h.handleCreateOAuthClientApp(c)
	case opDescribeOAuthClientApp:
		return h.handleDescribeOAuthClientApp(c)
	case opUpdateOAuthClientApp:
		return h.handleUpdateOAuthClientApp(c)
	case opDeleteOAuthClientApp:
		return h.handleDeleteOAuthClientApp(c)
	case opListOAuthClientApps:
		return h.handleListOAuthClientApps(c)
	}

	return writeError(
		c,
		http.StatusNotImplemented,
		"UnsupportedOperationException",
		"operation not implemented: "+op,
	)
}

// isOAuthAppModeledField reports whether k is a request-body field that AWS
// accepts on Create/UpdateOAuthClientApplication but never echoes back on
// Describe (the real OAuthClientApplication response shape has no
// ClientId/ClientSecret members), or is otherwise modeled as its own field
// rather than passed through in the Extra bag.
func isOAuthAppModeledField(k string) bool {
	switch k {
	case keyOAuthClientApplicationID, keyName, "ClientId", "ClientSecret":
		return true
	}

	return false
}

// oauthAppExtraFields returns every request-body field except
// OAuthClientApplicationId, Name, ClientId, and ClientSecret, stored as a
// passthrough bag alongside the modeled fields.
func oauthAppExtraFields(body map[string]any) map[string]any {
	extra := make(map[string]any, len(body))
	for k, v := range body {
		if isOAuthAppModeledField(k) {
			continue
		}
		extra[k] = v
	}

	return extra
}

func oauthAppToMap(app *OAuthClientApplication) map[string]any {
	m := map[string]any{
		keyOAuthClientApplicationID: app.ClientID,
		keyArn:                      app.Arn,
		keyName:                     app.Name,
		keyCreatedTime:              app.CreatedTime.Unix(),
		keyLastUpdatedTime:          app.LastUpdatedTime.Unix(),
	}
	maps.Copy(m, app.Extra)

	return m
}

func (h *Handler) handleCreateOAuthClientApp(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	clientID := strField(body, keyOAuthClientApplicationID)
	name := strField(body, keyName)

	app, err := h.Backend.CreateOAuthClientApplication(accountID, clientID, name, oauthAppExtraFields(body))
	if err != nil {
		if errors.Is(err, ErrOAuthClientAppAlreadyExists) {
			return writeError(c, http.StatusConflict, errResourceExistsCode, err.Error())
		}

		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyArn:                      app.Arn,
		keyOAuthClientApplicationID: app.ClientID,
		keyCreationStatusField:      app.Status,
		keyRequestID:                reqIDPlaceholder,
		keyStatus:                   http.StatusOK,
	})
}

func (h *Handler) handleDescribeOAuthClientApp(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	clientID := seg(segs, segResID)

	app, err := h.Backend.DescribeOAuthClientApplication(accountID, clientID)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyOAuthClientApplication: oauthAppToMap(app),
		keyRequestID:              reqIDPlaceholder,
		keyStatus:                 http.StatusOK,
	})
}

func (h *Handler) handleUpdateOAuthClientApp(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	clientID := seg(segs, segResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	name := strField(body, keyName)

	app, err := h.Backend.UpdateOAuthClientApplication(accountID, clientID, name, oauthAppExtraFields(body))
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyArn:                      app.Arn,
		keyOAuthClientApplicationID: app.ClientID,
		keyUpdateStatusField:        app.Status,
		keyRequestID:                reqIDPlaceholder,
		keyStatus:                   http.StatusOK,
	})
}

func (h *Handler) handleDeleteOAuthClientApp(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	clientID := seg(segs, segResID)

	app, err := h.Backend.DeleteOAuthClientApplication(accountID, clientID)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyArn:                      app.Arn,
		keyOAuthClientApplicationID: app.ClientID,
		keyRequestID:                reqIDPlaceholder,
		keyStatus:                   http.StatusOK,
	})
}

func (h *Handler) handleListOAuthClientApps(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	apps, next, err := h.Backend.ListOAuthClientApplications(accountID, maxResultsParam(c), nextTokenParam(c))
	if err != nil {
		return httpErr(c, err)
	}

	items := make([]map[string]any, 0, len(apps))
	for _, app := range apps {
		items = append(items, oauthAppToMap(app))
	}

	resp := map[string]any{
		keyOAuthClientApplications: items,
		keyRequestID:               reqIDPlaceholder,
		keyStatus:                  http.StatusOK,
	}
	if next != "" {
		resp[keyNextToken] = next
	}

	return writeJSON(c, http.StatusOK, resp)
}

// classifyOAuthAppPaths routes /accounts/{id}/oauth-client-applications/... paths.
func classifyOAuthAppPaths(method string, segs []string, n int) (string, string) {
	accountID := seg(segs, segAccountID)
	switch n {
	case nSegsAccountRes:
		switch method {
		case http.MethodPost:
			return opCreateOAuthClientApp, accountID
		case http.MethodGet:
			return opListOAuthClientApps, accountID
		}
	case nSegsAccountResID:
		id := seg(segs, segResID)
		switch method {
		case http.MethodGet:
			return opDescribeOAuthClientApp, id
		case http.MethodPut:
			return opUpdateOAuthClientApp, id
		case http.MethodDelete:
			return opDeleteOAuthClientApp, id
		}
	}

	return opUnknown, ""
}
