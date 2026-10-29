package dashboard

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v5"
)

const (
	ociMonitoringPrefix    = ociProxyPrefix + "/monitoring"
	ociMonitoringAPIPrefix = "/20180401"
	ociMonitoringAlarms    = "alarms"
	ociMonitoringMetrics   = "metrics"
	ociMonitoringStatus    = "status"
	ociMonitoringItemSegs  = 2
	ociMonitoringActSegs   = 3
)

// registerOCIMonitoringRoutes serves the Monitoring page by dispatching in-process to the emulator's handler without
// request signing. Only alarm reads, metric listing and metric queries are reachable; the two POSTs forward a body
// rebuilt from a fixed set of fields.
func (h *DashboardHandler) registerOCIMonitoringRoutes() {
	var (
		inner *echo.Echo
		comps func() []string
	)

	if ops := h.config.OCIMonitoringOps; ops != nil {
		inner = echo.New()
		inner.Any("/*", ops.Trusted())
		comps = ops.Backend.Compartments
	}

	guard := func(next echo.HandlerFunc) echo.HandlerFunc { return ociGuard(inner != nil, "OCI Monitoring", next) }

	h.SubRouter.Add(http.MethodGet, ociMonitoringPrefix+"/compartments", guard(func(c *echo.Context) error {
		return c.JSON(http.StatusOK, map[string]any{ociCompartmentsKey: comps()})
	}))

	serve := guard(func(c *echo.Context) error {
		r := c.Request()
		raw := strings.TrimPrefix(r.URL.EscapedPath(), ociMonitoringPrefix+"/api")

		parts, ok := ociSplitPath(raw)
		if !ok {
			return ociInvalid(c, "unsupported Monitoring proxy request")
		}

		body, ok := ociMonitoringBody(c, r.Method, parts)
		if !ok {
			return ociInvalid(c, "unsupported Monitoring proxy request")
		}

		return ociDispatch(c, inner, ociMonitoringAPIPrefix+raw, body)
	})

	h.SubRouter.Add(http.MethodGet, ociMonitoringPrefix+"/api/*", serve)
	h.SubRouter.Add(http.MethodPost, ociMonitoringPrefix+"/api/*", serve)
}

func ociMonitoringReadable(parts []string) bool {
	if parts[0] != ociMonitoringAlarms {
		return false
	}

	switch len(parts) {
	case 1:
		return true
	case ociMonitoringItemSegs:
		return true
	case ociMonitoringActSegs:
		return parts[2] == "history"
	}

	return false
}

func ociMonitoringListBody(c *echo.Context) ([]byte, bool) {
	var in struct {
		Namespace     *string `json:"namespace"`
		Name          *string `json:"name"`
		ResourceGroup *string `json:"resourceGroup"`
	}

	if !ociDecodeBody(c, &in) {
		return nil, false
	}

	b, err := json.Marshal(map[string]any{
		ociNamespaceKey: in.Namespace, "name": in.Name, "resourceGroup": in.ResourceGroup,
	})

	return b, err == nil
}

func ociMonitoringSummarizeBody(c *echo.Context) ([]byte, bool) {
	var in struct {
		StartTime     *time.Time `json:"startTime,omitempty"`
		EndTime       *time.Time `json:"endTime,omitempty"`
		ResourceGroup *string    `json:"resourceGroup,omitempty"`
		Namespace     string     `json:"namespace"`
		Query         string     `json:"query"`
		Resolution    string     `json:"resolution,omitempty"`
	}

	if !ociDecodeBody(c, &in) {
		return nil, false
	}

	b, err := json.Marshal(in)

	return b, err == nil
}

// ociMonitoringBody validates a request and builds the body to forward (nil keeps the original).
func ociMonitoringBody(c *echo.Context, method string, parts []string) ([]byte, bool) {
	switch {
	case method == http.MethodGet:
		return nil, ociMonitoringReadable(parts)
	case method != http.MethodPost || len(parts) != ociMonitoringActSegs || parts[0] != ociMonitoringMetrics ||
		parts[1] != ociActionsSeg:
		return nil, false
	case parts[2] == "listMetrics":
		return ociMonitoringListBody(c)
	case parts[2] == "summarizeMetricsData":
		return ociMonitoringSummarizeBody(c)
	}

	return nil, false
}
