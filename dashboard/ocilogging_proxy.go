package dashboard

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
)

const (
	ociLoggingPrefix  = ociProxyPrefix + "/logging"
	ociLoggingMgmt    = "/20200531"
	ociLoggingIngest  = "/20200831"
	ociLoggingSearch  = "/20190909/search"
	ociLoggingGroups  = "logGroups"
	ociLoggingLogs    = "logs"
	ociLoggingSearchK = "search"
	ociLoggingGroupN  = 2
	ociLoggingLogsN   = 3
	ociLoggingLogN    = 4
	ociLoggingPushN   = 4
	ociLoggingMaxData = 4096
)

// registerOCILoggingRoutes serves the Logging page by dispatching in-process to the emulator's handler without
// request signing. Only log group and log reads, enabling or disabling a log, searching, and pushing a single test
// entry are reachable; writes forward a body rebuilt from a fixed set of fields.
func (h *DashboardHandler) registerOCILoggingRoutes() {
	var (
		inner *echo.Echo
		comps func() []string
	)

	if ops := h.config.OCILoggingOps; ops != nil {
		inner = echo.New()
		inner.Any("/*", ops.Trusted())
		comps = ops.Backend.Compartments
	}

	guard := func(next echo.HandlerFunc) echo.HandlerFunc { return ociGuard(inner != nil, "OCI Logging", next) }

	h.SubRouter.Add(http.MethodGet, ociLoggingPrefix+"/compartments", guard(func(c *echo.Context) error {
		return c.JSON(http.StatusOK, map[string]any{ociCompartmentsKey: comps()})
	}))

	serve := guard(func(c *echo.Context) error {
		r := c.Request()
		raw := strings.TrimPrefix(r.URL.EscapedPath(), ociLoggingPrefix+"/api")

		parts, ok := ociSplitPath(raw)
		if !ok {
			return ociInvalid(c, "unsupported Logging proxy request")
		}

		apiPath, body, ok := ociLoggingRewrite(c, r.Method, parts, raw)
		if !ok {
			return ociInvalid(c, "unsupported Logging proxy request")
		}

		return ociDispatch(c, inner, apiPath, body)
	})

	for _, m := range []string{http.MethodGet, http.MethodPut, http.MethodPost} {
		h.SubRouter.Add(m, ociLoggingPrefix+"/api/*", serve)
	}
}

// ociLoggingRewrite maps an allowed request to the emulator path and the body to forward (nil keeps the original).
func ociLoggingRewrite(c *echo.Context, method string, parts []string, raw string) (string, []byte, bool) {
	switch method {
	case http.MethodGet:
		return ociLoggingMgmt + raw, nil, ociLoggingReadable(parts)
	case http.MethodPut:
		return ociLoggingToggle(c, parts, raw)
	case http.MethodPost:
		return ociLoggingPost(c, parts)
	}

	return "", nil, false
}

func ociLoggingToggle(c *echo.Context, parts []string, raw string) (string, []byte, bool) {
	if len(parts) != ociLoggingLogN || parts[0] != ociLoggingGroups || parts[2] != ociLoggingLogs {
		return "", nil, false
	}

	var in struct {
		Enabled *bool `json:"isEnabled"`
	}

	if !ociDecodeBody(c, &in) || in.Enabled == nil {
		return "", nil, false
	}

	b, err := json.Marshal(map[string]any{"isEnabled": *in.Enabled})

	return ociLoggingMgmt + raw, b, err == nil
}

func ociLoggingPost(c *echo.Context, parts []string) (string, []byte, bool) {
	switch {
	case len(parts) == 1 && parts[0] == ociLoggingSearchK:
		return ociLoggingSearchRequest(c)
	case len(parts) == ociLoggingPushN && parts[0] == ociLoggingLogs && parts[2] == ociActionsSeg &&
		parts[3] == "push":
		return ociLoggingPushRequest(c, parts[1])
	}

	return "", nil, false
}

func ociLoggingReadable(parts []string) bool {
	if parts[0] != ociLoggingGroups {
		return false
	}

	switch len(parts) {
	case 1, ociLoggingGroupN:
		return true
	case ociLoggingLogsN, ociLoggingLogN:
		return parts[2] == ociLoggingLogs
	}

	return false
}

func ociLoggingSearchRequest(c *echo.Context) (string, []byte, bool) {
	var in struct {
		TimeStart   *time.Time `json:"timeStart"`
		TimeEnd     *time.Time `json:"timeEnd"`
		SearchQuery string     `json:"searchQuery"`
	}

	if !ociDecodeBody(c, &in) {
		return "", nil, false
	}

	b, err := json.Marshal(map[string]any{
		"timeStart": in.TimeStart, "timeEnd": in.TimeEnd, "searchQuery": in.SearchQuery, "isReturnFieldInfo": true,
	})

	return ociLoggingSearch, b, err == nil
}

func ociLoggingPushRequest(c *echo.Context, logID string) (string, []byte, bool) {
	var in struct {
		Data string `json:"data"`
	}

	if !ociDecodeBody(c, &in) || in.Data == "" || len(in.Data) > ociLoggingMaxData {
		return "", nil, false
	}

	now := time.Now().UTC()
	b, err := json.Marshal(map[string]any{
		"specversion": "1.0",
		"logEntryBatches": []any{map[string]any{
			"source": "dashboard", "type": "dashboard.test", "defaultlogentrytime": now,
			"entries": []any{map[string]any{"data": in.Data, "id": uuid.NewString(), "time": now}},
		}},
	})

	return ociLoggingIngest + "/logs/" + logID + "/actions/push", b, err == nil
}
