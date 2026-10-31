package app

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/labstack/echo/v5"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/blackbirdworks/gopherstack/internal/mcpserver"
	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
	"github.com/blackbirdworks/gopherstack/pkgs/version"
)

const mcpPath = "/_gopherstack/mcp"

// MCPConfig configures the built-in Model Context Protocol server.
type MCPConfig struct {
	Token   string `name:"token"   env:"GOPHERSTACK_MCP_TOKEN" help:"Bearer token required by the MCP endpoint. Without it only loopback clients are accepted."` //nolint:lll // config struct tags are intentionally verbose
	ReadOnly bool  `name:"read-only" env:"GOPHERSTACK_MCP_READ_ONLY" help:"Expose only read-only MCP tools (no snapshots, resets, chaos or init hook runs)."` //nolint:lll // config struct tags are intentionally verbose
	Enabled bool   `name:"enabled" env:"GOPHERSTACK_MCP"       help:"Serve MCP (streamable HTTP) at /_gopherstack/mcp."`                                         //nolint:lll // config struct tags are intentionally verbose
}

// registerMCPEndpoint mounts the MCP endpoint on e; its tools call e in-process.
func registerMCPEndpoint(ctx context.Context, log *slog.Logger, e *echo.Echo, cfg MCPConfig) {
	if !cfg.Enabled {
		return
	}

	opts := mcpserver.InProcessOptions(e, version.Get())
	opts.ReadOnly = cfg.ReadOnly
	h := mcpserver.NewHTTPHandler(opts, cfg.Token)
	e.Any(mcpPath, echo.WrapHandler(h))
	e.Any(mcpPath+"/*", echo.WrapHandler(h))

	if log != nil {
		log.InfoContext(ctx, "MCP server enabled", "path", mcpPath, "token_required", cfg.Token != "")
	}
}

// McpCmd is `gopherstack mcp`: an MCP stdio server proxying a running instance.
type McpCmd struct {
	PodTarget `embed:""`
	ReadOnly  bool `name:"read-only" env:"GOPHERSTACK_MCP_READ_ONLY" help:"Expose only read-only MCP tools."`
}

// Run serves MCP on stdin/stdout until the client disconnects.
func (c *McpCmd) Run(d *lifecycleDeps) error {
	ctx, cancel := d.NewContext()
	defer cancel()

	return c.serve(ctx, d.HTTP, &mcp.StdioTransport{})
}

func (c *McpCmd) serve(ctx context.Context, client mcpserver.Doer, t mcp.Transport) error {
	if client == nil {
		client = httpclient.New(lifecycleHTTPTimeout)
	}

	srv := mcpserver.New(mcpserver.Options{
		Doer:    client,
		BaseURL: strings.TrimRight(c.Endpoint, "/"),
		Version: version.Get(),
		ReadOnly: c.ReadOnly,
	})

	if err := mcpserver.ServeStdio(ctx, srv, t); err != nil {
		return fmt.Errorf("mcp stdio: %w", err)
	}

	return nil
}
