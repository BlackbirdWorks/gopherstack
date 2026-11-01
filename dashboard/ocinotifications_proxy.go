package dashboard

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"

	"github.com/labstack/echo/v5"
)

const (
	ociNotifPrefix     = ociProxyPrefix + "/notifications"
	ociNotifAPIPrefix  = "/20181201"
	ociNotifDeliveries = "/_gopherstack/oci/deliveries"
	ociNotifMaxBody    = 1 << 12
	ociNotifItemSegs   = 2
	ociNotifSubSegs    = 3
	ociNotifTopics     = "topics"
	ociNotifSubs       = "subscriptions"
	ociNotifDelivered  = "deliveries"
)

// registerOCINotificationsRoutes serves the Notifications page by dispatching in-process to the emulator's handler
// without request signing. Only topic and subscription reads, recorded deliveries, a subscription confirmation and
// resending a confirmation are reachable; publishing and deleting are not.
func (h *DashboardHandler) registerOCINotificationsRoutes() {
	var (
		inner *echo.Echo
		comps func() []string
	)

	if ops := h.config.OCINotificationsOps; ops != nil {
		inner = echo.New()
		inner.Any("/*", ops.Trusted())
		comps = ops.Backend.Compartments
	}

	guard := func(next echo.HandlerFunc) echo.HandlerFunc { return ociGuard(inner != nil, "OCI Notifications", next) }

	h.SubRouter.Add(http.MethodGet, ociNotifPrefix+"/compartments", guard(func(c *echo.Context) error {
		return c.JSON(http.StatusOK, map[string]any{ociCompartmentsKey: comps()})
	}))

	serve := guard(func(c *echo.Context) error {
		r := c.Request()
		raw := strings.TrimPrefix(r.URL.EscapedPath(), ociNotifPrefix+"/api")

		if !ociNotifAllowed(r.Method, raw) {
			return cloudJSONFail(
				c,
				http.StatusBadRequest,
				"InvalidParameter",
				"unsupported Notifications proxy request",
			)
		}

		target := ociNotifAPIPrefix + raw
		if raw == "/"+ociNotifDelivered {
			target = ociNotifDeliveries
		}

		fwd := r.Clone(r.Context())
		fwd.URL.Path, fwd.URL.RawPath = unescapedPath(target), target
		fwd.RequestURI = ""
		fwd.Body = http.MaxBytesReader(c.Response(), r.Body, ociNotifMaxBody)

		rec := httptest.NewRecorder()
		inner.ServeHTTP(rec, fwd)

		return ociRelay(c, rec, "opc-next-page", "etag", "opc-request-id")
	})

	h.SubRouter.Add(http.MethodGet, ociNotifPrefix+"/api/*", serve)
	h.SubRouter.Add(http.MethodPost, ociNotifPrefix+"/api/*", serve)
}

func ociNotifAllowed(method, raw string) bool {
	parts := strings.Split(strings.TrimPrefix(raw, "/"), "/")

	for _, p := range parts {
		dec, err := url.PathUnescape(p)
		if err != nil || dec == "" || dec == "." || dec == ".." || strings.Contains(dec, "/") {
			return false
		}
	}

	switch method {
	case http.MethodGet:
		return ociNotifReadAllowed(parts)
	case http.MethodPost:
		return len(parts) == ociNotifSubSegs && parts[0] == ociNotifSubs && parts[2] == "resendConfirmation"
	}

	return false
}

func ociNotifReadAllowed(parts []string) bool {
	switch len(parts) {
	case 1:
		return parts[0] == ociNotifTopics || parts[0] == ociNotifSubs || parts[0] == ociNotifDelivered
	case ociNotifItemSegs:
		return parts[0] == ociNotifTopics || parts[0] == ociNotifSubs
	case ociNotifSubSegs:
		return parts[0] == ociNotifSubs && parts[2] == "confirmation"
	}

	return false
}
