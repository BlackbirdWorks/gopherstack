// Command driver deploys the push receiver to Cloud Run, wires a Pub/Sub push
// subscription to it and publishes messages.
//
//	driver deploy        create service, topic and push subscription
//	driver publish N     publish N messages
//	driver cleanup       delete everything this example created
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"
)

const (
	project  = "demo-project"
	location = "us-central1"
	svcID    = "push-receiver"
	topicID  = "orders"
	subID    = "orders-push"
	image    = "gopherstack-example/push-receiver:latest"
)

func env(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}

	return def
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	if err := dispatch(ctx, os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func dispatch(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New("usage: driver deploy|publish N|cleanup")
	}

	switch args[0] {
	case "deploy":
		return deploy(ctx)
	case "publish":
		n := 3
		if len(args) > 1 {
			n, _ = strconv.Atoi(args[1])
		}

		return publish(ctx, n)
	case "cleanup":
		return cleanup(ctx)
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}
