// Command azure-servicebus-cosmos-worker has two modes:
//
//	worker  consume a Service Bus queue and upsert each order into Cosmos DB
//	feed    read the Cosmos DB change feed and print the changed documents
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"
)

const (
	database  = "shop"
	container = "orders"
)

func env(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}

	return def
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: azure-servicebus-cosmos-worker worker|feed [flags]")
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	if err := dispatch(ctx, os.Args[1], os.Args[2:]); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func dispatch(ctx context.Context, mode string, args []string) error {
	fs := flag.NewFlagSet(mode, flag.ExitOnError)
	count := fs.Int("n", 3, "number of messages (worker) or documents (feed) to expect")

	if err := fs.Parse(args); err != nil {
		return err
	}

	switch mode {
	case "worker":
		return runWorker(ctx, *count)
	case "feed":
		return runFeed(ctx, *count)
	default:
		return fmt.Errorf("unknown mode %q", mode)
	}
}
