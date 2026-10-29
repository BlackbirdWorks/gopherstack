package dashboard

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"

	"github.com/labstack/echo/v5"
)

const (
	ociOKEPrefix    = ociProxyPrefix + "/containerengine"
	ociOKEAPIPrefix = "/20180222"
	ociOKEMaxBody   = 1 << 12
	ociOKEItemSegs  = 2
	ociOKEChildSegs = 3
	ociOKEClusters  = "clusters"
	ociOKEPools     = "nodePools"
	ociOKEWorkReqs  = "workRequests"
	ociOKEWRLogs    = "logs"
	ociOKEWRErrors  = "errors"
)

// registerOCIContainerEngineRoutes serves the Container Engine page by dispatching in-process to the emulator's
// handler without request signing. Only cluster, node pool and work request reads, cluster deletion and node pool
// resizing are reachable; kubeconfig generation is not, and a resize forwards nothing but the new size.
func (h *DashboardHandler) registerOCIContainerEngineRoutes() {
	var (
		inner *echo.Echo
		comps func() []string
	)

	if ops := h.config.OCIContainerEngineOps; ops != nil {
		inner = echo.New()
		inner.Any("/*", ops.Trusted())
		comps = ops.Backend.Compartments
	}

	guard := func(next echo.HandlerFunc) echo.HandlerFunc {
		return ociGuard(inner != nil, "OCI Container Engine", next)
	}

	h.SubRouter.Add(http.MethodGet, ociOKEPrefix+"/compartments", guard(func(c *echo.Context) error {
		return c.JSON(http.StatusOK, map[string]any{ociCompartmentsKey: comps()})
	}))

	serve := guard(func(c *echo.Context) error {
		r := c.Request()
		raw := strings.TrimPrefix(r.URL.EscapedPath(), ociOKEPrefix+"/api")

		if !ociOKEAllowed(r.Method, raw) {
			return cloudJSONFail(
				c,
				http.StatusBadRequest,
				"InvalidParameter",
				"unsupported Container Engine proxy request",
			)
		}

		fwd := r.Clone(r.Context())
		fwd.URL.Path, fwd.URL.RawPath = unescapedPath(ociOKEAPIPrefix+raw), ociOKEAPIPrefix+raw
		fwd.RequestURI = ""
		fwd.Body = http.MaxBytesReader(c.Response(), r.Body, ociOKEMaxBody)

		if r.Method == http.MethodPut {
			body, ok := ociOKEResizeBody(fwd.Body)
			if !ok {
				return cloudJSONFail(c, http.StatusBadRequest, "InvalidParameter",
					"a node pool update must set nodeConfigDetails.size or quantityPerSubnet")
			}

			fwd.Body, fwd.ContentLength = io.NopCloser(bytes.NewReader(body)), int64(len(body))
		}

		rec := httptest.NewRecorder()
		inner.ServeHTTP(rec, fwd)

		return ociRelay(c, rec, "opc-next-page", "etag", "opc-request-id", "opc-work-request-id")
	})

	for _, m := range []string{http.MethodGet, http.MethodPut, http.MethodDelete} {
		h.SubRouter.Add(m, ociOKEPrefix+"/api/*", serve)
	}
}

// ociOKEAllowed validates a proxied path (below /api) against the allowed operations.
func ociOKEAllowed(method, raw string) bool {
	parts := strings.Split(strings.TrimPrefix(raw, "/"), "/")

	for _, p := range parts {
		dec, err := url.PathUnescape(p)
		if err != nil || dec == "" || dec == "." || dec == ".." || strings.Contains(dec, "/") {
			return false
		}
	}

	switch method {
	case http.MethodGet:
		return ociOKEAllowedRead(parts)
	case http.MethodDelete:
		return len(parts) == ociOKEItemSegs && parts[0] == ociOKEClusters
	case http.MethodPut:
		return len(parts) == ociOKEItemSegs && parts[0] == ociOKEPools
	}

	return false
}

func ociOKEAllowedRead(parts []string) bool {
	switch len(parts) {
	case 1, ociOKEItemSegs:
		return slices.Contains([]string{ociOKEClusters, ociOKEPools, ociOKEWorkReqs}, parts[0])
	case ociOKEChildSegs:
		return parts[0] == ociOKEWorkReqs && slices.Contains([]string{ociOKEWRLogs, ociOKEWRErrors}, parts[2])
	}

	return false
}

// ociOKEResizeBody rebuilds a node pool update from the requested size alone.
func ociOKEResizeBody(in io.Reader) ([]byte, bool) {
	var req struct {
		NodeConfig *struct {
			Size *int `json:"size"`
		} `json:"nodeConfigDetails"`
		Quantity *int `json:"quantityPerSubnet"`
	}

	if err := json.NewDecoder(in).Decode(&req); err != nil {
		return nil, false
	}

	var out any

	switch {
	case req.NodeConfig != nil && req.NodeConfig.Size != nil:
		out = map[string]any{"nodeConfigDetails": map[string]any{"size": *req.NodeConfig.Size}}
	case req.Quantity != nil:
		out = map[string]any{"quantityPerSubnet": *req.Quantity}
	default:
		return nil, false
	}

	body, err := json.Marshal(out)

	return body, err == nil
}
