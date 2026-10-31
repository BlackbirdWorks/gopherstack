package dashboard

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

const (
	ociNoSQLPrefix    = ociProxyPrefix + "/nosql"
	ociNoSQLAPIPrefix = "/20190828"
	ociNoSQLTables    = "tables"
	ociNoSQLQuery     = "query"
	ociNoSQLItemSegs  = 2
	ociNoSQLSubSegs   = 3
	ociNoSQLMaxUnits  = 1 << 20
	ociKeyCompartment = "compartmentId"
	ociKeyName        = "name"
)

// registerOCINoSQLRoutes serves the NoSQL page by dispatching in-process to the emulator's handler without request
// signing. Reachable: table reads/create/drop, index and usage reads, row put/get/delete and SELECT queries. Every
// write forwards a body rebuilt from a fixed set of fields.
func (h *DashboardHandler) registerOCINoSQLRoutes() {
	var (
		inner *echo.Echo
		comps func() []string
	)

	if ops := h.config.OCINoSQLOps; ops != nil {
		inner = echo.New()
		inner.Any("/*", ops.Trusted())
		comps = ops.Backend.Compartments
	}

	guard := func(next echo.HandlerFunc) echo.HandlerFunc { return ociGuard(inner != nil, "OCI NoSQL", next) }

	h.SubRouter.Add(http.MethodGet, ociNoSQLPrefix+"/compartments", guard(func(c *echo.Context) error {
		return c.JSON(http.StatusOK, map[string]any{ociCompartmentsKey: comps()})
	}))

	serve := guard(func(c *echo.Context) error {
		r := c.Request()
		raw := strings.TrimPrefix(r.URL.EscapedPath(), ociNoSQLPrefix+"/api")

		parts, ok := ociSplitPath(raw)
		if !ok || !ociNoSQLAllowed(r.Method, parts) {
			return ociInvalid(c, "unsupported NoSQL proxy request")
		}

		body, ok := ociNoSQLBody(c, r.Method, parts)
		if !ok {
			return ociInvalid(c, "invalid NoSQL proxy request body")
		}

		return ociDispatch(c, inner, ociNoSQLAPIPrefix+raw, body)
	})

	for _, m := range []string{http.MethodGet, http.MethodPost, http.MethodPut, http.MethodDelete} {
		h.SubRouter.Add(m, ociNoSQLPrefix+"/api/*", serve)
	}
}

func ociNoSQLAllowed(method string, parts []string) bool {
	if parts[0] == ociNoSQLQuery {
		return method == http.MethodPost && len(parts) == 1
	}

	if parts[0] != ociNoSQLTables {
		return false
	}

	switch len(parts) {
	case 1:
		return method == http.MethodGet || method == http.MethodPost
	case ociNoSQLItemSegs:
		return method == http.MethodGet || method == http.MethodDelete
	case ociNoSQLSubSegs:
		switch parts[2] {
		case "indexes", "usage":
			return method == http.MethodGet
		case "rows":
			return method == http.MethodGet || method == http.MethodPut || method == http.MethodDelete
		}
	}

	return false
}

// ociNoSQLBody builds the forwarded body of a write; nil keeps the (empty) original.
func ociNoSQLBody(c *echo.Context, method string, parts []string) ([]byte, bool) {
	switch {
	case parts[0] == ociNoSQLQuery:
		return ociNoSQLQueryBody(c)
	case method == http.MethodPost:
		return ociNoSQLCreateBody(c)
	case method == http.MethodPut:
		return ociNoSQLRowBody(c)
	}

	return nil, true
}

func ociNoSQLCreateBody(c *echo.Context) ([]byte, bool) {
	var in struct {
		CompartmentID string `json:"compartmentId"`
		Name          string `json:"name"`
		DDL           string `json:"ddlStatement"`
		Limits        struct {
			Mode  string `json:"capacityMode"`
			Read  int    `json:"maxReadUnits"`
			Write int    `json:"maxWriteUnits"`
			GB    int    `json:"maxStorageInGBs"`
		} `json:"tableLimits"`
	}

	if !ociDecodeBody(c, &in) || in.CompartmentID == "" || in.Name == "" || in.DDL == "" {
		return nil, false
	}

	l := in.Limits
	if (l.Mode != "PROVISIONED" && l.Mode != "ON_DEMAND") ||
		l.Read < 0 || l.Write < 0 || l.GB < 0 || l.Read > ociNoSQLMaxUnits || l.Write > ociNoSQLMaxUnits ||
		l.GB > ociNoSQLMaxUnits {
		return nil, false
	}

	limits := map[string]any{"capacityMode": l.Mode, "maxStorageInGBs": l.GB}
	if l.Mode == "PROVISIONED" {
		limits["maxReadUnits"], limits["maxWriteUnits"] = l.Read, l.Write
	}

	b, err := json.Marshal(map[string]any{
		ociKeyCompartment: in.CompartmentID, ociKeyName: in.Name, "ddlStatement": in.DDL, "tableLimits": limits,
	})

	return b, err == nil
}

func ociNoSQLRowBody(c *echo.Context) ([]byte, bool) {
	var in struct {
		CompartmentID string          `json:"compartmentId"`
		Option        string          `json:"option"`
		Value         json.RawMessage `json:"value"`
	}

	if !ociDecodeBody(c, &in) || in.CompartmentID == "" || !bytes.HasPrefix(bytes.TrimSpace(in.Value), []byte("{")) {
		return nil, false
	}

	if in.Option != "" && in.Option != "IF_ABSENT" && in.Option != "IF_PRESENT" {
		return nil, false
	}

	out := map[string]any{ociKeyCompartment: in.CompartmentID, "value": in.Value}
	if in.Option != "" {
		out["option"] = in.Option
	}

	b, err := json.Marshal(out)

	return b, err == nil
}

func ociNoSQLQueryBody(c *echo.Context) ([]byte, bool) {
	var in struct {
		CompartmentID string `json:"compartmentId"`
		Statement     string `json:"statement"`
		Consistency   string `json:"consistency"`
	}

	if !ociDecodeBody(c, &in) || in.CompartmentID == "" ||
		!strings.HasPrefix(strings.ToLower(strings.TrimSpace(in.Statement)), "select") {
		return nil, false
	}

	out := map[string]any{ociKeyCompartment: in.CompartmentID, "statement": in.Statement}
	if in.Consistency == "EVENTUAL" || in.Consistency == "ABSOLUTE" {
		out["consistency"] = in.Consistency
	}

	b, err := json.Marshal(out)

	return b, err == nil
}
