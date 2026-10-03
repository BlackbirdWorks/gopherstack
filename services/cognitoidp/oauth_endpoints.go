package cognitoidp

import (
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const jwtDotCount = 2

func bearerToken(r *http.Request) string {
	scheme, token, ok := strings.Cut(r.Header.Get("Authorization"), " ")
	if !ok || !strings.EqualFold(scheme, authTypeBearer) {
		return ""
	}

	return strings.TrimSpace(token)
}

func wwwAuthenticate(c *echo.Context, status int, code, desc string) error {
	hdr := c.Response().Header()
	hdr.Set("WWW-Authenticate", `error="`+code+`", error_description="`+desc+`"`)
	hdr.Set("Access-Control-Allow-Origin", "*")

	return c.NoContent(status)
}

func (h *Handler) handleOAuthUserInfo(c *echo.Context) error {
	token := bearerToken(c.Request())
	if token == "" {
		return wwwAuthenticate(c, http.StatusBadRequest, errInvalidRequest, "Bad OAuth2 request at UserInfo Endpoint")
	}

	claims, err := h.Backend.oauthUserInfo(token)
	if err != nil {
		return wwwAuthenticate(c, http.StatusUnauthorized, "invalid_token",
			"Access token is expired, disabled, or deleted, or the user has globally signed out.")
	}

	return writeOAuthJSON(c, http.StatusOK, claims)
}

func (h *Handler) handleOAuthRevoke(c *echo.Context) error {
	form, oerr := readOAuthForm(c)
	if oerr != nil {
		return oerr.write(c)
	}

	client, oerr := h.authenticateOAuthClient(c.Request(), form)
	if oerr != nil {
		if oerr.code == errInvalidClient {
			oerr.status = http.StatusUnauthorized
		}

		return oerr.write(c)
	}

	token := form.Get("token")
	if token == "" || !client.EnableTokenRevocation {
		return newOAuthError(errInvalidRequest, "").write(c)
	}

	if strings.Count(token, ".") >= jwtDotCount {
		return newOAuthError("unsupported_token_type", "").write(c)
	}

	if err := h.Backend.RevokeToken(token, client.ClientID); err != nil {
		return newOAuthError(errInvalidRequest, "").write(c)
	}

	c.Response().Header().Set("Access-Control-Allow-Origin", "*")

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleOIDCDiscovery(c *echo.Context) error {
	poolID := poolPathPrefix(c.Request().URL.Path, discoverySuffix)
	if poolID == "" {
		poolID = h.hostPool(c)
	}

	doc, err := h.Backend.oidcDiscovery(poolID)
	if err != nil {
		return c.JSON(http.StatusNotFound, service.JSONErrorResponse{
			Type: ErrUserPoolNotFound.Error(), Message: err.Error(),
		})
	}

	c.Response().Header().Set("Access-Control-Allow-Origin", "*")

	return c.JSON(http.StatusOK, doc)
}
