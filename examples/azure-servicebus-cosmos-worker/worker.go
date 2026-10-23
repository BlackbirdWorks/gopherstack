package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
)

type order struct {
	ID       string  `json:"id"`
	Customer string  `json:"customer"`
	Amount   float64 `json:"amount"`
}

func parseOrder(body []byte) (order, error) {
	var o order
	if err := json.Unmarshal(body, &o); err != nil {
		return o, err
	}

	if o.ID == "" || o.Customer == "" {
		return o, errors.New("order needs id and customer")
	}

	return o, nil
}

func store(ctx context.Context, c *azcosmos.ContainerClient, o order) error {
	doc, err := json.Marshal(o)
	if err != nil {
		return err
	}

	_, err = c.UpsertItem(ctx, azcosmos.NewPartitionKeyString(o.Customer), doc, nil)

	return err
}

// handle stores one message and settles it; malformed messages are abandoned.
func handle(ctx context.Context, bus *busClient, c *azcosmos.ContainerClient, m *lockedMessage) error {
	o, err := parseOrder(m.Body)
	if err != nil {
		fmt.Printf("abandon %s: %v\n", m.ID, err)

		return bus.abandon(ctx, m)
	}

	if err = store(ctx, c, o); err != nil {
		return fmt.Errorf("store %s: %w", o.ID, err)
	}

	fmt.Printf("stored %s (customer=%s amount=%.2f)\n", o.ID, o.Customer, o.Amount)

	return bus.complete(ctx, m)
}

// runWorker consumes until want messages were handled or the queue stays empty.
func runWorker(ctx context.Context, want int) error {
	c, err := openOrders(ctx)
	if err != nil {
		return err
	}

	bus := newBusClient()
	handled := 0
	idleUntil := time.Now().Add(15 * time.Second)

	for handled < want && time.Now().Before(idleUntil) {
		m, perr := bus.peekLock(ctx)
		if perr != nil {
			return perr
		}

		if m == nil {
			time.Sleep(200 * time.Millisecond)

			continue
		}

		if err = handle(ctx, bus, c, m); err != nil {
			return err
		}

		handled++
	}

	fmt.Printf("worker handled %d message(s)\n", handled)

	if handled < want {
		return fmt.Errorf("expected %d messages, got %d", want, handled)
	}

	return nil
}
