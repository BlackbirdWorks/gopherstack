package dashboard

import (
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

const (
	azureServiceBusProxyPrefix  = "/dashboard/api/v1/azure/servicebus"
	azureServiceBusMaxBodyBytes = 1 << 20
	azureServiceBusMaxSegments  = 6
	azureServiceBusResources    = "$Resources"
	azureServiceBusSubs         = "subscriptions"
	azureServiceBusDeadLetter   = "$DeadLetterQueue"
	azureServiceBusMessages     = "messages"
)

// registerAzureServiceBusRoutes exposes the Service Bus emulator's REST surface
// to the SPA by dispatching in-process to its handler.
func (h *DashboardHandler) registerAzureServiceBusRoutes() {
	var handler echo.HandlerFunc

	if h.config.AzureServiceBusOps != nil {
		handler = h.config.AzureServiceBusOps.Handler()
	}

	serve := newAzureProxyHandler(
		azureServiceBusProxyPrefix, azureServiceBusMaxBodyBytes, handler, azureServiceBusProxyPath,
	)

	for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodPost, http.MethodDelete} {
		h.SubRouter.Add(method, azureServiceBusProxyPrefix+"/*", serve)
	}
}

// azureServiceBusProxyPath admits only the management and data-plane shapes:
// /$Resources/{Queues|Topics}, /<entity>, /<topic>/subscriptions[/<sub>], and
// either entity's [/$DeadLetterQueue]/messages[/head|/<id>/<token>].
func azureServiceBusProxyPath(raw string) (string, bool) {
	if !strings.HasPrefix(raw, "/") || len(raw) < 2 {
		return "", false
	}

	segs := strings.Split(raw[1:], "/")
	if len(segs) > azureServiceBusMaxSegments {
		return "", false
	}

	for _, seg := range segs {
		if seg == "" || seg == ".." || seg == "." {
			return "", false
		}
	}

	if segs[0] == azureServiceBusResources {
		return raw, len(segs) == 2 && (segs[1] == "Queues" || segs[1] == "Topics")
	}

	if !azureServiceBusEntityShape(segs[1:]) {
		return "", false
	}

	return raw, true
}

// azureServiceBusEntityShape vets the segments after the entity name.
func azureServiceBusEntityShape(rest []string) bool {
	if len(rest) >= 2 && rest[0] == azureServiceBusSubs {
		rest = rest[2:]
	} else if len(rest) == 1 && rest[0] == azureServiceBusSubs {
		return true
	}

	if len(rest) > 0 && rest[0] == azureServiceBusDeadLetter {
		rest = rest[1:]
	}

	return len(rest) == 0 || rest[0] == azureServiceBusMessages
}
