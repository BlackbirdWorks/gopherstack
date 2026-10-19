package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"text/tabwriter"

	"github.com/blackbirdworks/gopherstack/internal/replicator"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const replicateTablePad = 2

var errReplicateFailed = errors.New("some resources failed to replicate")

// ReplicateCmd is `gopherstack replicate`: copy resources from a real AWS account or
// AWS-compatible endpoint into a running Gopherstack.
type ReplicateCmd struct {
	SourceProfile  string   `name:"source-profile"   help:"AWS shared-config profile for the source account."`
	SourceEndpoint string   `name:"source-endpoint"  help:"Source endpoint URL (empty means real AWS)."`
	Region         string   `name:"region"           help:"Source region."                                                 env:"AWS_REGION"           default:"us-east-1"`             //nolint:lll // config struct tags are intentionally verbose
	Target         string   `name:"target"           help:"Base URL of the running Gopherstack."                           env:"GOPHERSTACK_ENDPOINT" default:"http://localhost:8000"` //nolint:lll // config struct tags are intentionally verbose
	TargetRegion   string   `name:"target-region"    help:"Target region (default: the source region)."`
	Identifier     string   `name:"identifier"       help:"Only replicate the resource with this name."`
	ResourceType   []string `name:"resource-type"    help:"Resource type to replicate; repeatable (default: all)."`
	MaxObjectBytes int64    `name:"max-object-bytes" help:"Size cap for copied S3 objects and Lambda code."                                           default:"67108864"` //nolint:lll // config struct tags are intentionally verbose
	DryRun         bool     `name:"dry-run"          help:"Show the plan without writing to the target."`
	IncludeData    bool     `name:"include-data"     help:"Also copy S3 objects and DynamoDB items."`
	WithDecryption bool     `name:"with-decryption"  help:"Decrypt SecureString parameters so their values can be copied."`
	JSON           bool     `name:"json"             help:"Print the report as JSON."`
}

// Run executes `replicate`.
func (c *ReplicateCmd) Run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	ctx = logger.Save(ctx, buildLogger("warn"))

	return c.run(ctx, os.Stdout, replicator.Run)
}

func (c *ReplicateCmd) config() replicator.Config {
	tr := c.TargetRegion
	if tr == "" {
		tr = c.Region
	}

	return replicator.Config{
		Source: replicator.Endpoint{Profile: c.SourceProfile, URL: c.SourceEndpoint, Region: c.Region},
		Target: replicator.Endpoint{URL: c.Target, Region: tr},
		Types:  c.ResourceType, Identifier: c.Identifier, DryRun: c.DryRun,
		IncludeData: c.IncludeData, WithDecryption: c.WithDecryption, MaxObjectBytes: c.MaxObjectBytes,
	}
}

func (c *ReplicateCmd) run(ctx context.Context, out io.Writer, fn replicateFunc) error {
	rep, err := fn(ctx, c.config())
	if err != nil {
		return fmt.Errorf("replicate: %w", err)
	}

	if c.JSON {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")

		if err = enc.Encode(rep); err != nil {
			return fmt.Errorf("encode report: %w", err)
		}
	} else {
		printReplicateReport(out, rep)
	}

	if rep.Failed() {
		return errReplicateFailed
	}

	return nil
}

func printReplicateReport(out io.Writer, rep *replicator.Report) {
	if rep.DryRun {
		fmt.Fprintln(out, "DRY RUN: nothing was written to the target")
	}

	w := tabwriter.NewWriter(out, 0, 0, replicateTablePad, ' ', 0)
	fmt.Fprintln(w, "TYPE\tID\tACTION\tDETAIL")

	for _, r := range rep.Results {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", r.Type, r.ID, r.Action, r.Reason)
	}

	_ = w.Flush()

	fmt.Fprintf(out, "\ncreated=%d updated=%d skipped=%d failed=%d\n",
		rep.Counts[replicator.ActionCreated], rep.Counts[replicator.ActionUpdated],
		rep.Counts[replicator.ActionSkipped], rep.Counts[replicator.ActionFailed])
}
