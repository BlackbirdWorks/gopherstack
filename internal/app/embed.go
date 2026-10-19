package app

import (
	"context"

	"github.com/alecthomas/kong"
)

// RunServer parses args (flags and env as for the CLI, without the program
// name) and runs the server until ctx is cancelled. Embedders such as the
// desktop app use it instead of Run, which owns os.Args and signal handling.
func RunServer(ctx context.Context, args []string) error {
	var root rootCLI

	applyLocalstackEnv()

	parser, err := kong.New(&root,
		kong.Name("gopherstack"),
		kong.Description("In-memory AWS DynamoDB + S3 compatible server."),
	)
	if err != nil {
		return err
	}

	if _, err = parser.Parse(append([]string{"serve"}, args...)); err != nil {
		return err
	}

	return run(ctx, root.Serve)
}
