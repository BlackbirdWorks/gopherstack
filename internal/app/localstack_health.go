package app

import (
	"net"
	"net/http"
	"os"
	"strings"
	"syscall"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	healthActionRestart = "restart"
	healthActionKill    = "kill"
	swaggerUIVersion    = "5.17.14"
	healthKeyAction     = "action"
	specGet             = "get"
	specPost            = "post"
)

type localstackHealthAction struct {
	Action string `json:"action"`
}

// buildLocalstackHealthPost serves POST /_localstack/health. restart resets all
// in-memory service state. kill shuts the process down gracefully; a remotely
// triggerable kill is dangerous, so it needs --localstack-health-kill and a
// loopback peer.
func buildLocalstackHealthPost(
	services []service.Registerable, allowKill bool, terminate func(),
) echo.HandlerFunc {
	return func(c *echo.Context) error {
		var req localstackHealthAction
		if err := c.Bind(&req); err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{keyMessageField: "invalid JSON body"})
		}

		switch strings.ToLower(req.Action) {
		case healthActionRestart:
			for _, svc := range services {
				if r, ok := svc.(service.Resettable); ok {
					r.Reset()
				}
			}

			return c.JSON(http.StatusOK, map[string]string{podKeyStatus: "restarted"})
		case healthActionKill:
			if !allowKill {
				return c.JSON(http.StatusForbidden, map[string]string{
					keyMessageField: "kill is disabled; start with --localstack-health-kill",
				})
			}

			if !isLoopbackPeer(c.Request().RemoteAddr) {
				return c.JSON(http.StatusForbidden, map[string]string{
					keyMessageField: "kill is only accepted from a loopback address",
				})
			}

			err := c.JSON(http.StatusOK, map[string]string{podKeyStatus: "killing"})
			go terminate()

			return err
		default:
			return c.JSON(http.StatusBadRequest, map[string]string{
				keyMessageField: "unknown action; expected restart or kill",
			})
		}
	}
}

func isLoopbackPeer(remoteAddr string) bool {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		host = remoteAddr
	}

	ip := net.ParseIP(strings.Trim(host, "[]"))

	return ip != nil && ip.IsLoopback()
}

// terminateSelf sends SIGTERM to this process so the normal graceful-shutdown path runs.
func terminateSelf() {
	if p, err := os.FindProcess(os.Getpid()); err == nil {
		_ = p.Signal(syscall.SIGTERM)
	}
}

func localstackOpenAPISpec() map[string]any {
	op := func(summary string) map[string]any {
		return map[string]any{
			"summary":   summary,
			"responses": map[string]any{"200": map[string]any{"description": "OK"}},
		}
	}

	post := op("Run a health action")
	post["requestBody"] = map[string]any{
		"required": true,
		"content": map[string]any{contentTypeJSON: map[string]any{"schema": map[string]any{
			"type":     "object",
			"required": []string{healthKeyAction},
			"properties": map[string]any{
				healthKeyAction: map[string]any{"type": "string", "enum": []string{"restart", "kill"}},
			},
		}}},
	}

	return map[string]any{
		"openapi": "3.0.3",
		"info":    map[string]any{"title": "Gopherstack LocalStack-compatible internal API", "version": "1.0.0"},
		"paths": map[string]any{
			"/_localstack/health":          map[string]any{specGet: op("Service health"), specPost: post},
			"/_localstack/info":            map[string]any{specGet: op("Version and edition")},
			"/_localstack/config":          map[string]any{specGet: op("Read config"), specPost: op("Update config")},
			"/_localstack/diagnose":        map[string]any{specGet: op("Diagnostics")},
			"/_localstack/plugins":         map[string]any{specGet: op("Loaded providers")},
			"/_localstack/init":            map[string]any{specGet: op("Init script status, all stages")},
			"/_localstack/init/{stage}":    map[string]any{specGet: op("Init script status for one stage")},
			"/_localstack/state/reset":     map[string]any{specPost: op("Reset in-memory state")},
			"/_localstack/swagger/openapi": map[string]any{specGet: op("This document")},
		},
	}
}

const swaggerPage = `<!doctype html>
<html><head><meta charset="utf-8"><title>Gopherstack internal API</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/swagger-ui-dist@` + swaggerUIVersion + `/swagger-ui.css"></head>
<body><div id="ui"></div>
<script src="https://cdn.jsdelivr.net/npm/swagger-ui-dist@` + swaggerUIVersion + `/swagger-ui-bundle.js"></script>
<script>SwaggerUIBundle({url: "/_localstack/swagger/openapi", dom_id: "#ui"});</script>
</body></html>`

func registerLocalstackSwagger(e *echo.Echo) {
	e.GET("/_localstack/swagger", func(c *echo.Context) error {
		return c.HTML(http.StatusOK, swaggerPage)
	})
	e.GET("/_localstack/swagger/openapi", func(c *echo.Context) error {
		return c.JSON(http.StatusOK, localstackOpenAPISpec())
	})
}
