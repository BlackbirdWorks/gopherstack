package mcpserver

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	serverName = "gopherstack"
	infoURI    = "gopherstack://info"
)

// Options configures a Gopherstack MCP server.
type Options struct {
	Doer    Doer
	BaseURL string
	Version string
	// ReadOnly omits every tool that can change state.
	ReadOnly bool
}

// New builds an MCP server whose tools call the admin API reachable through opts.Doer.
func New(opts Options) *mcp.Server {
	s := mcp.NewServer(
		&mcp.Implementation{Name: serverName, Title: "Gopherstack", Version: opts.Version},
		&mcp.ServerOptions{
			Instructions: "Inspect and control a local Gopherstack cloud emulator: health, state snapshots, " +
				"resets, chaos faults, request traces, IAM denials and init hooks. Secrets are never exposed.",
		},
	)
	b := &backend{doer: opts.Doer, base: opts.BaseURL}

	addTools(s, b, opts.ReadOnly)
	s.AddResource(&mcp.Resource{
		URI:         infoURI,
		Name:        "info",
		Title:       "Server info",
		Description: "Version, uptime and enabled services of the running Gopherstack (no secrets).",
		MIMEType:    "application/json",
	}, b.infoResource)

	return s
}

func (b *backend) infoResource(ctx context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
	var h HealthOutput
	if err := b.call(ctx, http.MethodGet, "/_gopherstack/health", nil, nil, &h); err != nil {
		return nil, err
	}

	raw, err := json.Marshal(h)
	if err != nil {
		return nil, err
	}

	return &mcp.ReadResourceResult{Contents: []*mcp.ResourceContents{
		{URI: req.Params.URI, MIMEType: "application/json", Text: string(raw)},
	}}, nil
}

// ServeStdio serves s over t until the client disconnects or ctx ends.
func ServeStdio(ctx context.Context, s *mcp.Server, t mcp.Transport) error {
	return s.Run(ctx, t)
}
