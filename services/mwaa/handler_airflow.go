package mwaa

import (
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

const (
	pathAirflowCLI = "/aws_mwaa/cli"
	pathAirflowSSO = "/aws_mwaa/aws-console-sso"
)

// airflowBackend is implemented by backends that can run a real Airflow.
type airflowBackend interface {
	AirflowEnabled() bool
	ExecCLI(ctx context.Context, token, command string) (string, string, error)
	WebLoginTarget(token string) (string, error)
}

// Shutdown stops every Airflow container the backend started.
func (h *Handler) Shutdown(_ context.Context) {
	if c, ok := h.Backend.(interface{ Close() }); ok {
		c.Close()
	}
}

func (h *Handler) airflow() (airflowBackend, bool) {
	ab, ok := h.Backend.(airflowBackend)
	if !ok || !ab.AirflowEnabled() {
		return nil, false
	}

	return ab, true
}

// serveAirflowPath serves the webserver-side endpoints (/aws_mwaa/*); false means the path is not one.
func (h *Handler) serveAirflowPath(c *echo.Context, path string) (bool, error) {
	switch path {
	case pathAirflowCLI:
		return true, h.handleAirflowCLI(c)
	case pathAirflowSSO:
		return true, h.handleAirflowSSO(c)
	}

	return false, nil
}

func bearerToken(r *http.Request) string {
	auth := strings.TrimSpace(r.Header.Get("Authorization"))

	return strings.TrimSpace(strings.TrimPrefix(auth, "Bearer "))
}

func (h *Handler) handleAirflowCLI(c *echo.Context) error {
	ab, ok := h.airflow()
	if !ok || c.Request().Method != http.MethodPost {
		return writeErrorResponse(c, http.StatusNotFound, "ResourceNotFoundException", "resource not found")
	}

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return writeErrorResponse(c, http.StatusBadRequest, "ValidationException", "failed to read request body")
	}

	stdout, stderr, err := ab.ExecCLI(c.Request().Context(), bearerToken(c.Request()), string(body))
	if err != nil {
		return writeAirflowError(c, err)
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, map[string]string{
		"stdout": base64.StdEncoding.EncodeToString([]byte(stdout)),
		"stderr": base64.StdEncoding.EncodeToString([]byte(stderr)),
	})

	return nil
}

func writeAirflowError(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, errAirflowToken):
		return writeErrorResponse(c, http.StatusForbidden, "AccessDeniedException", err.Error())
	case errors.Is(err, awserr.ErrNotFound):
		return writeErrorResponse(c, http.StatusNotFound, "ResourceNotFoundException", err.Error())
	case errors.Is(err, awserr.ErrInvalidParameter):
		return writeErrorResponse(c, http.StatusBadRequest, "ValidationException", err.Error())
	}

	return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerException", err.Error())
}

const ssoPage = `<!doctype html><title>MWAA login</title><script>
var t=location.hash.slice(1);
if(t){location.replace(location.pathname+"?login=true&token="+encodeURIComponent(t));}
else{document.write("missing web login token");}
</script>`

// handleAirflowSSO redirects a valid web login token to the environment's real Airflow webserver.
func (h *Handler) handleAirflowSSO(c *echo.Context) error {
	ab, ok := h.airflow()
	if !ok {
		return writeErrorResponse(c, http.StatusNotFound, "ResourceNotFoundException", "resource not found")
	}

	token := c.Request().URL.Query().Get("token")
	if token == "" {
		return c.HTML(http.StatusOK, ssoPage)
	}

	target, err := ab.WebLoginTarget(token)
	if err != nil {
		return writeAirflowError(c, err)
	}

	return c.Redirect(http.StatusFound, target)
}

// webHostname returns the host clients should use for /aws_mwaa/* calls when Airflow runs in a container.
func (h *Handler) webHostname(c *echo.Context, hostname string) string {
	if _, ok := h.airflow(); ok && c.Request().Host != "" {
		return c.Request().Host
	}

	return hostname
}
