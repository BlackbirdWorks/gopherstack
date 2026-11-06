package dashboard

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/netip"
	"regexp"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	sqlbackend "github.com/blackbirdworks/gopherstack/services/azuresql"
)

const (
	azureSQLPrefix      = "/dashboard/api/v1/azure/sql"
	azureSQLQueryPrefix = "/dashboard/api/v1/azure/sqlquery"
	sqlPxNamespace      = "microsoft.sql"
	sqlPxServers        = "servers"
	sqlPxDatabases      = "databases"
	sqlPxFirewall       = "firewallrules"
	sqlPxRGList         = 7
	sqlPxChildItem      = 2
	sqlPxMaxServerID    = 512
	sqlPxRowsField      = "rows"
	sqlPxPropsField     = "properties"
)

var sqlPxRuleRE = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,128}$`)

// registerAzureSQLRoutes exposes logical servers, databases and firewall rules by dispatching in-process to the ARM
// handler (firewall rules are the only writes, with a body rebuilt from fixed fields; the administrator password
// is scrubbed from every response), and serves the read-only query runner from the SQL backend.
func (h *DashboardHandler) registerAzureSQLRoutes() {
	h.registerARMProxy(armProxy{
		name: "Azure SQL", prefix: azureSQLPrefix,
		queryKeys: []string{armQueryVersion},
		methods:   []string{http.MethodGet, http.MethodPut, http.MethodDelete},
		allowed:   sqlProxyAllowed,
		body:      sqlProxyBody,
	})

	q := &azureSQLQueryAPI{}
	if h.config.AzureSQLOps != nil {
		q.b = h.config.AzureSQLOps.Backend
	}

	h.SubRouter.Add(http.MethodPost, azureSQLQueryPrefix, q.guard(q.run))
}

func sqlProxyAllowed(method string, p []string) bool {
	if len(p) == sqlPxRGList {
		return method == http.MethodGet && p[0] == armPxSubsSeg && p[flexPxRGIdx] == armPxRGsSeg &&
			p[flexPxRGProvIdx] == armPxProvSeg && p[flexPxRGNSIdx] == sqlPxNamespace &&
			p[flexPxRGTypeIdx] == sqlPxServers
	}

	typ, name, rest, ok := armScope(p, sqlPxNamespace)
	if !ok || typ != sqlPxServers {
		return false
	}

	if name == "" || len(rest) == 0 {
		return method == http.MethodGet
	}

	return sqlProxyChild(method, rest)
}

func sqlProxyChild(method string, rest []string) bool {
	readable := rest[0] == sqlPxDatabases || rest[0] == sqlPxFirewall

	switch {
	case len(rest) == 1:
		return method == http.MethodGet && readable
	case len(rest) != sqlPxChildItem:
		return false
	case method == http.MethodGet:
		return readable
	case method == http.MethodPut || method == http.MethodDelete:
		return rest[0] == sqlPxFirewall && sqlPxRuleRE.MatchString(rest[1])
	}

	return false
}

type sqlRuleIn struct {
	Properties map[string]string `json:"properties"`
}

func sqlProxyBody(method string, _ []string, in []byte) ([]byte, bool) {
	if method != http.MethodPut {
		return in, true
	}

	var req sqlRuleIn
	if json.Unmarshal(in, &req) != nil {
		return nil, false
	}

	start, err1 := netip.ParseAddr(strings.TrimSpace(req.Properties["startIpAddress"]))
	end, err2 := netip.ParseAddr(strings.TrimSpace(req.Properties["endIpAddress"]))

	if err1 != nil || err2 != nil || !start.Is4() || !end.Is4() {
		return nil, false
	}

	b, err := json.Marshal(map[string]any{sqlPxPropsField: map[string]any{
		"startIpAddress": start.String(), "endIpAddress": end.String(),
	}})

	return b, err == nil
}

type sqlQueryIn struct {
	Server   string `json:"server"`
	Database string `json:"database"`
	Query    string `json:"query"`
}

type azureSQLQueryAPI struct {
	b *sqlbackend.InMemoryBackend
}

func (a *azureSQLQueryAPI) guard(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c *echo.Context) error {
		if a.b == nil {
			return cloudUnavailable(c, "Azure SQL")
		}

		return next(c)
	}
}

// run executes one validated SELECT; the backend caps rows, cell size and time.
func (a *azureSQLQueryAPI) run(c *echo.Context) error {
	var in sqlQueryIn
	if !cloudJSONDecode(c, &in) {
		return nil
	}

	if in.Server == "" || len(in.Server) > sqlPxMaxServerID {
		return cloudJSONFail(c, http.StatusBadRequest, cloudStatusBadArg, "server is required")
	}

	started := time.Now()

	res, err := a.b.Query(c.Request().Context(), strings.ToLower(in.Server), in.Database, in.Query)
	if err != nil {
		switch {
		case errors.Is(err, sqlbackend.ErrNotFound):
			return cloudJSONFail(c, http.StatusNotFound, cloudStatusNotFnd, err.Error())
		case errors.Is(err, sqlbackend.ErrNoEngine):
			return cloudJSONFail(c, http.StatusConflict, "FAILED_PRECONDITION", err.Error())
		default:
			return cloudJSONFail(c, http.StatusBadRequest, cloudStatusBadArg, err.Error())
		}
	}

	return c.JSON(http.StatusOK, map[string]any{
		"columns": res.Columns, sqlPxRowsField: res.Rows, "truncated": res.Truncated,
		"maxRows": sqlbackend.MaxQueryRows, "elapsedMs": time.Since(started).Milliseconds(),
	})
}
