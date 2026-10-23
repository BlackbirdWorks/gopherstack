package main

import (
	"context"
	"fmt"
	"strings"
)

func deploy(ctx context.Context) error {
	svc, err := deployService(ctx)
	if err != nil {
		return fmt.Errorf("deploy service: %w", err)
	}

	host := strings.TrimPrefix(svc.GetUri(), "http://")
	fmt.Printf("deployed %s (revision %s)\n", svc.GetName(), svc.GetLatestReadyRevision())
	fmt.Printf("SERVICE_HOST=%s\n", host)

	endpoint, err := wirePush(ctx, svc.GetUri())
	if err != nil {
		return fmt.Errorf("wire pubsub: %w", err)
	}

	fmt.Printf("push subscription %s -> %s\n", subID, endpoint)

	return nil
}

func cleanup(ctx context.Context) error {
	ps, err := newPubSub(ctx)
	if err != nil {
		return err
	}
	defer ps.Close()

	if err = resetTopic(ctx, ps); err != nil {
		return err
	}

	return deleteService(ctx)
}
