package app

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/inithooks"
)

const (
	initSchemeHTTP    = "http"
	initListenPoll    = 50 * time.Millisecond
	initListenTimeout = time.Minute
	initRunMaxBody    = 4 << 10
)

// initStageRoots returns the stage-directory roots: defaults plus --init-dir.
func initStageRoots(cli *CLI) []string {
	return append(inithooks.DefaultRoots(), cli.InitDirs...)
}

// initScriptEnv is the environment handed to stage scripts.
func initScriptEnv(cli *CLI) []string {
	scheme := initSchemeHTTP
	if tlsConfigFromCLI(cli).enabled {
		scheme = "https"
	}

	return []string{
		"AWS_ENDPOINT_URL=" + scheme + "://localhost:" + strings.TrimPrefix(cli.Port, ":"),
		"AWS_DEFAULT_REGION=" + cli.Region,
		"AWS_REGION=" + cli.Region,
		"AWS_ACCESS_KEY_ID=test",
		"AWS_SECRET_ACCESS_KEY=test",
	}
}

// startInitStages creates the stage manager and runs BOOT.
func startInitStages(ctx context.Context, cli *CLI, log *slog.Logger) {
	m := inithooks.NewManager(inithooks.ManagerConfig{
		Roots:           initStageRoots(cli),
		LegacyReadyDirs: inithooks.DefaultLegacyReadyDirs(),
		Env:             initScriptEnv(cli),
		Timeout:         cli.InitScriptTimeout,
		Log:             log,
	})
	cli.localstackInit.setManager(m)
	m.Run(ctx, inithooks.StageBoot)
}

func runInitStage(ctx context.Context, cli *CLI, stage inithooks.Stage) {
	if m := cli.localstackInit.mgr.Load(); m != nil {
		m.Run(ctx, stage)
	}
}

// launchReadyStage runs READY once the main listener accepts connections.
func launchReadyStage(ctx context.Context, cli *CLI) {
	m := cli.localstackInit.mgr.Load()
	if m == nil {
		return
	}

	addr := net.JoinHostPort("127.0.0.1", strings.TrimPrefix(cli.Port, ":"))

	go func() {
		var d net.Dialer

		deadline := time.Now().Add(initListenTimeout)

		for time.Now().Before(deadline) {
			if conn, err := d.DialContext(ctx, "tcp", addr); err == nil {
				_ = conn.Close()

				m.Run(ctx, inithooks.StageReady)

				return
			}

			select {
			case <-ctx.Done():
				return
			case <-time.After(initListenPoll):
			}
		}
	}()
}

// runShutdownStage runs SHUTDOWN scripts, bounded by shutdownTimeout.
func runShutdownStage(ctx context.Context, cli *CLI) {
	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
	defer cancel()

	runInitStage(sctx, cli, inithooks.StageShutdown)
}

// runStartupHooks runs START, the legacy --init-script hooks, S3 bucket
// creation, then schedules READY for when the listener accepts.
func runStartupHooks(ctx context.Context, cli *CLI, log *slog.Logger) {
	runInitStage(ctx, cli, inithooks.StageStart)
	runInitHooks(ctx, cli, log)
	createS3InitBuckets(ctx, cli, log)
	launchReadyStage(ctx, cli)
}

type initRunRequest struct {
	Stage  string `json:"stage"`
	Script string `json:"script"`
}

type initRunResult struct {
	Stage     string `json:"stage"`
	Name      string `json:"name"`
	Output    string `json:"output"`
	Error     string `json:"error,omitempty"`
	ExitCode  int    `json:"exit_code"`
	Truncated bool   `json:"truncated,omitempty"`
}

// buildInitRunHandler runs registered init scripts on demand (loopback peers
// only); script is matched against names found in the configured stage dirs.
func buildInitRunHandler(st *localstackInitState) echo.HandlerFunc {
	return func(c *echo.Context) error {
		if !isLoopbackPeer(c.Request().RemoteAddr) {
			return c.JSON(http.StatusForbidden, map[string]string{
				keyMessageField: "running init hooks is only accepted from a loopback address",
			})
		}

		var req initRunRequest
		if err := json.NewDecoder(http.MaxBytesReader(c.Response(), c.Request().Body, initRunMaxBody)).
			Decode(&req); err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{keyMessageField: "invalid JSON body"})
		}

		var m *inithooks.Manager
		if st != nil {
			m = st.mgr.Load()
		}

		if m == nil {
			return c.JSON(http.StatusConflict, map[string]string{keyMessageField: "init stages are not active"})
		}

		runs, err := m.RunNamed(c.Request().Context(), inithooks.Stage(req.Stage), req.Script)

		switch {
		case errors.Is(err, inithooks.ErrUnknownStage):
			return c.JSON(http.StatusBadRequest, map[string]string{keyMessageField: err.Error()})
		case errors.Is(err, inithooks.ErrUnknownScript):
			return c.JSON(http.StatusNotFound, map[string]string{keyMessageField: err.Error()})
		case err != nil:
			return c.JSON(http.StatusInternalServerError, map[string]string{keyMessageField: err.Error()})
		}

		out := make([]initRunResult, 0, len(runs))
		for _, r := range runs {
			out = append(out, initRunResult{
				Stage: string(r.Stage), Name: r.Name, Output: r.Output,
				Error: r.Error, ExitCode: r.ExitCode, Truncated: r.Truncated,
			})
		}

		return c.JSON(http.StatusOK, map[string]any{"results": out})
	}
}
