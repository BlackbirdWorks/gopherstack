package dashboard

import (
	"encoding/json"
	"net/http"
	"slices"
	"strings"

	"github.com/labstack/echo/v5"
)

const (
	ociMySQLPrefix    = ociProxyPrefix + "/mysql"
	ociMySQLAPIPrefix = "/20190415"
	ociMySQLSystems   = "dbSystems"
	ociMySQLBackups   = "backups"
	ociMySQLConfigs   = "configurations"
	ociMySQLItemSegs  = 2
	ociMySQLActSegs   = 4
)

// registerOCIMySQLRoutes serves the MySQL HeatWave page by dispatching in-process to the emulator's handler without
// request signing. Only DB system, backup and configuration reads, start/stop/restart, delete and manual backup
// creation are reachable; every write forwards a body rebuilt from a fixed set of fields, and the emulator never
// returns admin passwords.
func (h *DashboardHandler) registerOCIMySQLRoutes() {
	var (
		inner *echo.Echo
		comps func() []string
	)

	if ops := h.config.OCIMySQLOps; ops != nil {
		inner = echo.New()
		inner.Any("/*", ops.Trusted())
		comps = ops.Backend.Compartments
	}

	guard := func(next echo.HandlerFunc) echo.HandlerFunc { return ociGuard(inner != nil, "OCI MySQL", next) }

	h.SubRouter.Add(http.MethodGet, ociMySQLPrefix+"/compartments", guard(func(c *echo.Context) error {
		return c.JSON(http.StatusOK, map[string]any{ociCompartmentsKey: comps()})
	}))

	serve := guard(func(c *echo.Context) error {
		r := c.Request()
		raw := strings.TrimPrefix(r.URL.EscapedPath(), ociMySQLPrefix+"/api")

		parts, ok := ociSplitPath(raw)
		if !ok || !ociMySQLAllowed(r.Method, parts) {
			return ociInvalid(c, "unsupported MySQL proxy request")
		}

		body, ok := ociMySQLBody(c, r.Method, parts)
		if !ok {
			return ociInvalid(c, "invalid MySQL proxy request body")
		}

		return ociDispatch(c, inner, ociMySQLAPIPrefix+raw, body)
	})

	for _, m := range []string{http.MethodGet, http.MethodPost, http.MethodDelete} {
		h.SubRouter.Add(m, ociMySQLPrefix+"/api/*", serve)
	}
}

func ociMySQLAllowed(method string, parts []string) bool {
	switch method {
	case http.MethodGet:
		return ociMySQLReadable(parts)
	case http.MethodDelete:
		return len(parts) == ociMySQLItemSegs && parts[0] == ociMySQLSystems
	case http.MethodPost:
		if len(parts) == 1 {
			return parts[0] == ociMySQLBackups
		}

		return len(parts) == ociMySQLActSegs && parts[0] == ociMySQLSystems && parts[2] == ociActionsSeg &&
			slices.Contains([]string{"start", "stop", "restart"}, parts[3])
	}

	return false
}

// ociMySQLBody builds the forwarded body of a POST; nil keeps the (empty) original.
func ociMySQLBody(c *echo.Context, method string, parts []string) ([]byte, bool) {
	if method != http.MethodPost {
		return nil, true
	}

	if len(parts) == 1 {
		var in struct {
			DBSystemID  string `json:"dbSystemId"`
			DisplayName string `json:"displayName"`
		}

		if !ociDecodeBody(c, &in) || in.DBSystemID == "" {
			return nil, false
		}

		out := map[string]any{"dbSystemId": in.DBSystemID}
		if in.DisplayName != "" {
			out["displayName"] = in.DisplayName
		}

		b, err := json.Marshal(out)

		return b, err == nil
	}

	if parts[3] == "start" {
		return []byte("{}"), true
	}

	return []byte(`{"shutdownType":"IMMEDIATE"}`), true
}

func ociMySQLReadable(parts []string) bool {
	switch len(parts) {
	case 1:
		return slices.Contains([]string{ociMySQLSystems, ociMySQLBackups, ociMySQLConfigs}, parts[0])
	case ociMySQLItemSegs:
		return slices.Contains([]string{ociMySQLSystems, ociMySQLConfigs}, parts[0])
	}

	return false
}
