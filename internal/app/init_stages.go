package app

import (
	"context"
	"log/slog"
	"net"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/inithooks"
)

const (
	initSchemeHTTP    = "http"
	initListenPoll    = 50 * time.Millisecond
	initListenTimeout = time.Minute
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
		Roots: initStageRoots(cli), Env: initScriptEnv(cli), Timeout: cli.InitScriptTimeout, Log: log,
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
