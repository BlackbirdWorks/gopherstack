package app

import (
	"context"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/extensions"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	extensionsListPath     = "/_gopherstack/extensions"
	extensionsRegisterPath = "/_gopherstack/extensions/register"
	extensionsRestartPath  = "/_gopherstack/extensions/:name/restart"
	extensionsOutputPath   = "/_gopherstack/extensions/:name/output"
	localstackExtensions   = "/_localstack/extensions"
)

// extensionHost owns the extension manager and the lazily created container runtime.
type extensionHost struct {
	mgr *extensions.Manager
	rt  func() (container.Runtime, error)
}

// initExtensions loads the extensions config, builds the manager and mounts the control API.
func (c *CLI) initExtensions(log *slog.Logger, e *echo.Echo) error {
	cfg, err := extensions.LoadConfig(c.Extensions)
	if err != nil {
		return err
	}

	rt := sync.OnceValues(func() (container.Runtime, error) {
		return container.NewRuntime(container.Config{Logger: log})
	})

	mgr := extensions.New(cfg, extensions.Options{
		Logger:     log,
		NewRuntime: rt,
		ControlURL: "http://127.0.0.1:" + strings.TrimPrefix(c.Port, ":"),
	})
	c.extHost = &extensionHost{mgr: mgr, rt: rt}

	e.GET(extensionsListPath, func(ec *echo.Context) error {
		mgr.ServeList(ec.Response(), ec.Request())

		return nil
	})
	e.POST(extensionsRegisterPath, func(ec *echo.Context) error {
		mgr.ServeRegister(ec.Response(), ec.Request())

		return nil
	})

	e.POST(extensionsRestartPath, func(ec *echo.Context) error {
		mgr.ServeRestart(ec.Response(), ec.Request(), ec.Param("name"))

		return nil
	})
	e.GET(extensionsOutputPath, func(ec *echo.Context) error {
		mgr.ServeOutput(ec.Response(), ec.Request(), ec.Param("name"))

		return nil
	})

	registerLocalstackExtensionsAlias(e, c.extHost)

	return nil
}

// registerLocalstackExtensionsAlias serves the extension list at the LocalStack-style path.
func registerLocalstackExtensionsAlias(e *echo.Echo, h *extensionHost) {
	if h == nil {
		return
	}

	e.GET(localstackExtensions, func(ec *echo.Context) error {
		h.mgr.ServeList(ec.Response(), ec.Request())

		return nil
	})
}

func (c *CLI) extensionInterceptor() service.Interceptor {
	if c.extHost == nil || !c.extHost.mgr.Enabled() {
		return nil
	}

	return c.extHost.intercept
}

func (c *CLI) startExtensions(ctx context.Context) {
	if c.extHost == nil || !c.extHost.mgr.Enabled() {
		return
	}

	c.extHost.mgr.Start(ctx)
	c.extHost.markReadyWhenListening(ctx, c.Port)
}

func (c *CLI) stopExtensions(ctx context.Context) {
	if c.extHost == nil || !c.extHost.mgr.Enabled() {
		return
	}

	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
	defer cancel()

	c.extHost.mgr.Stop(sctx)

	if rt, err := c.extHost.rt(); err == nil && rt != nil {
		_ = rt.Close()
	}
}

func (h *extensionHost) markReadyWhenListening(ctx context.Context, port string) {
	go func() {
		if waitListening(ctx, port) {
			h.mgr.MarkReady(ctx)
		}
	}()
}

func waitListening(ctx context.Context, port string) bool {
	addr := net.JoinHostPort("127.0.0.1", strings.TrimPrefix(port, ":"))
	deadline := time.Now().Add(initListenTimeout)

	for time.Now().Before(deadline) {
		var d net.Dialer

		if conn, err := d.DialContext(ctx, "tcp", addr); err == nil {
			_ = conn.Close()

			return true
		}

		select {
		case <-ctx.Done():
			return false
		case <-time.After(initListenPoll):
		}
	}

	return false
}

func (h *extensionHost) intercept(c *echo.Context, entry *service.Entry) echo.HandlerFunc {
	shadowed := entry != nil && entry.Priority > service.PriorityCatchAll

	if p := h.mgr.Route(c.Request(), shadowed); p != nil {
		return func(ec *echo.Context) error {
			p.ServeHTTP(ec.Response(), ec.Request())

			return nil
		}
	}

	if entry == nil {
		return nil
	}

	hs := h.mgr.Hooks(entry.Registerable.Name())
	if hs == nil {
		return nil
	}

	return func(ec *echo.Context) error {
		if resp := hs.Pre(ec.Request()); resp != nil {
			resp.Write(ec.Response())

			return nil
		}

		if !hs.HasPost() {
			return entry.WrappedHandler(ec)
		}

		orig := ec.Response()
		rec := extensions.NewRecorder(orig)

		ec.SetResponse(rec)
		err := entry.WrappedHandler(ec)
		ec.SetResponse(orig)

		if err == nil {
			if resp := hs.Post(ec.Request(), rec); resp != nil {
				resp.Write(orig)

				return nil
			}
		}

		rec.Commit()

		return err
	}
}
