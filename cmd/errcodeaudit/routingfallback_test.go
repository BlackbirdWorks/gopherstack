package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func routingFallbackFor(t *testing.T, cands []candidate, code string) bool {
	t.Helper()

	for _, c := range cands {
		if c.Code == code {
			return c.RoutingFallback
		}
	}

	require.Failf(t, "no candidate found", "code %q, candidates: %+v", code, cands)

	return false
}

// TestRoutingFallback_QuicksightDispatchShape pins quicksight's own
// dispatch() shape: a bare `switch { case isXOp(op): ...; default: ... }`
// whose every case shares "op" and whose default clause is reached only
// when classifyRequest matched no known operation at all -- confirmed live
// (services/quicksight/handler_dispatch.go:37-46). There is no operation
// here for any per-op deserializer to hold this code accountable to.
func TestRoutingFallback_QuicksightDispatchShape(t *testing.T) {
	t.Parallel()

	src := `package quicksight

import (
	"fmt"
	"net/http"

	"github.com/labstack/echo/v5"
)

func writeError(c *echo.Context, status int, errCode, msg string) error {
	type errBody struct {
		Code    string
		Message string
	}

	return c.JSON(status, errBody{Code: errCode, Message: msg})
}

func (h *Handler) dispatch(c *echo.Context) error {
	op, _ := classifyRequest(c.Request().Method, c.Request().URL.Path)
	switch {
	case isNamespaceOp(op):
		return h.dispatchNamespace(c, op)
	case op != opUnknown:
		return h.dispatchNew(c, op)
	default:
		return writeError(
			c,
			http.StatusNotImplemented,
			"UnsupportedOperationException",
			fmt.Sprintf("operation %q not implemented", op),
		)
	}
}
`

	cands := extractFixture(t, src)

	assert.True(
		t,
		routingFallbackFor(t, cands, "UnsupportedOperationException"),
		"dispatch()'s own default case fires only when op matched nothing; it must "+
			"be marked RoutingFallback",
	)
}

// TestRoutingFallback_Route53SwitchDefaultShape pins route53's own
// `switch method { ...; default: ... }` shape -- confirmed live
// (services/route53/handler_hosted_zones.go:56-64).
func TestRoutingFallback_Route53SwitchDefaultShape(t *testing.T) {
	t.Parallel()

	src := `package route53

import "net/http"

func xmlError(c *echo.Context, status int, code, message string) error {
	type xmlErrBody struct {
		Code    string
		Message string
	}

	return c.XML(status, xmlErrBody{Code: code, Message: message})
}

func (h *Handler) routeHostedZoneRoot(c *echo.Context, method string) error {
	switch method {
	case http.MethodPost:
		return h.createHostedZone(c)
	case http.MethodGet:
		return h.listHostedZones(c)
	default:
		return xmlError(c, http.StatusNotFound, "NoSuchOperation",
			"unsupported method on /hostedzone")
	}
}
`

	cands := extractFixture(t, src)

	assert.True(
		t,
		routingFallbackFor(t, cands, "NoSuchOperation"),
		"routeHostedZoneRoot's default case fires only when method matched no "+
			"known verb; it must be marked RoutingFallback",
	)
}

// TestRoutingFallback_Route53GuardChainShape pins route53's second shape:
// a chain of `if method == http.MethodX { return ... }` guards with no
// switch at all, falling through to an unconditional fallback return --
// confirmed live (services/route53/handler_query_logging.go's sibling
// idiom, e.g. handler_traffic_policies.go:76-84's single-guard form).
func TestRoutingFallback_Route53GuardChainShape(t *testing.T) {
	t.Parallel()

	src := `package route53

import "net/http"

func xmlError(c *echo.Context, status int, code, message string) error {
	type xmlErrBody struct {
		Code    string
		Message string
	}

	return c.XML(status, xmlErrBody{Code: code, Message: message})
}

func (h *Handler) routeTrafficPolicyRoot(c *echo.Context, method string) error {
	if method == http.MethodPost {
		return h.createTrafficPolicy(c)
	}

	return xmlError(
		c,
		http.StatusNotFound,
		"NoSuchOperation",
		"unsupported method on /trafficpolicy",
	)
}
`

	cands := extractFixture(t, src)

	assert.True(
		t,
		routingFallbackFor(t, cands, "NoSuchOperation"),
		"the trailing return fires only when method matched no guard; it must be "+
			"marked RoutingFallback",
	)
}

// TestRoutingFallback_DoesNotSuppressOperationLevelError is the regression
// guard: a code returned from INSIDE a matched case/guard -- a real
// operation's own handler, not the branch taken when nothing matched --
// must never be marked RoutingFallback merely because it sits near a
// method/op switch. Mirrors codepipeline's own dispatch shape, where a
// per-operation code sits inside a matched case, not the fallback.
func TestRoutingFallback_DoesNotSuppressOperationLevelError(t *testing.T) {
	t.Parallel()

	src := `package codepipeline

import "net/http"

func writeError(c *echo.Context, status int, code, message string) error {
	type errBody struct {
		Code    string
		Message string
	}

	return c.JSON(status, errBody{Code: code, Message: message})
}

func (h *Handler) dispatch(c *echo.Context, action string) error {
	switch action {
	case "CreatePipeline":
		return h.createPipeline(c)
	case "GetPipeline":
		if !h.exists(c) {
			return writeError(c, http.StatusBadRequest, "PipelineNotFoundException", "not found")
		}

		return h.getPipeline(c)
	default:
		return writeError(c, http.StatusBadRequest, "ValidationException", "unknown action")
	}
}
`

	cands := extractFixture(t, src)

	assert.False(
		t,
		routingFallbackFor(t, cands, "PipelineNotFoundException"),
		"PipelineNotFoundException is returned from inside a MATCHED case's own "+
			"guard, not dispatch's own default -- it must never be suppressed",
	)
}
