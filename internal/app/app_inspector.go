package app

import (
	"context"
	"net/http"
	"os"
	"strconv"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/apptrace"
)

const (
	appInspectorPath = "/_gopherstack/apptrace"

	envAppTrace         = "GOPHERSTACK_APPTRACE"
	envAppTraceCapacity = "GOPHERSTACK_APPTRACE_CAPACITY"
	envAppTraceBodies   = "GOPHERSTACK_APPTRACE_BODIES"
	envAppTraceMaxBody  = "GOPHERSTACK_APPTRACE_MAX_BODY"

	appInspectorDefaultLimit = 200
	appInspectorMaxLimit     = 1000
)

// appInspectorConfig reads the tracer settings: metadata on, bodies off.
func appInspectorConfig(getenv func(string) string) apptrace.Config {
	cfg := apptrace.Config{Enabled: getenv(envAppTrace) != "off" && getenv(envAppTrace) != "false"}

	cfg.Detail, _ = strconv.ParseBool(getenv(envAppTraceBodies))
	cfg.Capacity, _ = strconv.Atoi(getenv(envAppTraceCapacity))
	cfg.MaxBody, _ = strconv.Atoi(getenv(envAppTraceMaxBody))

	return cfg
}

// registerAppInspector installs the request-trace middleware and its
// /_gopherstack/apptrace API on e, and returns the tracer.
func registerAppInspector(e *echo.Echo) *apptrace.Tracer {
	tr := apptrace.New(appInspectorConfig(os.Getenv))

	e.Use(apptrace.Middleware(tr))

	g := e.Group(appInspectorPath)
	g.GET("/requests", appInspectorList(tr))
	g.GET("/requests/:id", appInspectorGet(tr))
	g.DELETE("/requests", appInspectorClear(tr))
	g.GET("/config", appInspectorGetConfig(tr))
	g.PUT("/config", appInspectorPutConfig(tr))

	return tr
}

type appInspectorConfigBody struct {
	Capacity int  `json:"capacity"`
	MaxBody  int  `json:"maxBody"`
	Enabled  bool `json:"enabled"`
	Detail   bool `json:"detail"`
}

func appInspectorConfigView(tr *apptrace.Tracer) appInspectorConfigBody {
	cfg := tr.Config()

	return appInspectorConfigBody{
		Capacity: cfg.Capacity, MaxBody: cfg.MaxBody, Enabled: cfg.Enabled, Detail: cfg.Detail,
	}
}

func appInspectorList(tr *apptrace.Tracer) echo.HandlerFunc {
	return func(c *echo.Context) error {
		q := func(k string) string { return c.QueryParam(k) }
		atoi := func(k string) int {
			n, _ := strconv.Atoi(q(k))

			return n
		}
		errorsOnly, _ := strconv.ParseBool(q("errorsOnly"))
		after, _ := strconv.ParseUint(q("afterSeq"), 10, 64)

		limit := atoi("limit")
		if limit <= 0 {
			limit = appInspectorDefaultLimit
		}

		res := tr.List(apptrace.Filter{
			Service: q("service"), Operation: q("operation"), Region: q("region"),
			Account: q("account"), Principal: q("principal"), ErrorCode: q("errorCode"),
			TraceID: q("traceId"), ParentID: q("parentId"), Kind: q("kind"), Query: q("q"),
			AfterSeq: after, Limit: min(limit, appInspectorMaxLimit), Offset: atoi("offset"),
			MinStatus: atoi("minStatus"), MaxStatus: atoi("maxStatus"),
			ErrorsOnly: errorsOnly,
		})

		return c.JSON(http.StatusOK, res)
	}
}

type appInspectorDetail struct {
	Trace []apptrace.Span `json:"trace"`
	Span  apptrace.Span   `json:"span"`
}

type appInspectorError struct {
	Error string `json:"error"`
}

func appInspectorGet(tr *apptrace.Tracer) echo.HandlerFunc {
	return func(c *echo.Context) error {
		sp, ok := tr.Get(c.Param("id"))
		if !ok {
			return c.JSON(http.StatusNotFound, appInspectorError{Error: "request not found"})
		}

		return c.JSON(http.StatusOK, appInspectorDetail{Span: sp, Trace: tr.Trace(sp.TraceID)})
	}
}

func appInspectorClear(tr *apptrace.Tracer) echo.HandlerFunc {
	return func(c *echo.Context) error {
		tr.Clear()

		return c.NoContent(http.StatusNoContent)
	}
}

func appInspectorGetConfig(tr *apptrace.Tracer) echo.HandlerFunc {
	return func(c *echo.Context) error {
		return c.JSON(http.StatusOK, appInspectorConfigView(tr))
	}
}

func appInspectorPutConfig(tr *apptrace.Tracer) echo.HandlerFunc {
	return func(c *echo.Context) error {
		var body struct {
			Enabled *bool `json:"enabled"`
			Detail  *bool `json:"detail"`
		}

		if err := c.Bind(&body); err != nil {
			return c.JSON(http.StatusBadRequest, appInspectorError{Error: "invalid JSON body"})
		}

		if body.Enabled != nil {
			tr.SetEnabled(*body.Enabled)
		}

		if body.Detail != nil {
			tr.SetDetail(*body.Detail)
		}

		return c.JSON(http.StatusOK, appInspectorConfigView(tr))
	}
}

// traceSQSDelivery opens a child span for a cross-service delivery into an SQS
// queue; call the returned func with the delivery error.
func traceSQSDelivery(ctx context.Context, queueARN string) func(error) {
	region := arnRegion(queueARN)

	_, span := apptrace.StartChild(ctx, apptrace.ChildInfo{
		Service: "sqs", Operation: "SendMessage", Resource: queueARN, Region: region,
	})

	return func(err error) {
		if err != nil {
			span.Finish("DeliveryFailed")

			return
		}

		span.Finish("")
	}
}
