package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// lockedMessage is a peek-locked Service Bus message.
type lockedMessage struct {
	ID       string
	Body     []byte
	Location string
}

type brokerProps struct {
	MessageID string `json:"MessageId"`
}

type busClient struct {
	base  string
	queue string
	http  *http.Client
}

func newBusClient() *busClient {
	return &busClient{
		base:  strings.TrimRight(env("SERVICEBUS_URL", "http://localhost:10003"), "/"),
		queue: env("SERVICEBUS_QUEUE", "orders"),
		http:  &http.Client{},
	}
}

func (b *busClient) do(ctx context.Context, method, path string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, b.base+path, nil)
	if err != nil {
		return nil, err
	}

	return b.http.Do(req)
}

// peekLock returns the next message, or nil when the queue is empty.
func (b *busClient) peekLock(ctx context.Context) (*lockedMessage, error) {
	resp, err := b.do(ctx, http.MethodPost, "/"+b.queue+"/messages/head")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent {
		return nil, nil
	}

	if resp.StatusCode != http.StatusCreated {
		return nil, fmt.Errorf("peek-lock: HTTP %d", resp.StatusCode)
	}

	var props brokerProps
	if err = json.Unmarshal([]byte(resp.Header.Get("BrokerProperties")), &props); err != nil {
		return nil, fmt.Errorf("broker properties: %w", err)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	return &lockedMessage{ID: props.MessageID, Body: body, Location: resp.Header.Get("Location")}, nil
}

func (b *busClient) settle(ctx context.Context, method string, m *lockedMessage) error {
	resp, err := b.do(ctx, method, m.Location)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("%s %s: HTTP %d", method, m.Location, resp.StatusCode)
	}

	return nil
}

// complete removes the message; abandon releases the lock for redelivery.
func (b *busClient) complete(ctx context.Context, m *lockedMessage) error {
	return b.settle(ctx, http.MethodDelete, m)
}

func (b *busClient) abandon(ctx context.Context, m *lockedMessage) error {
	return b.settle(ctx, http.MethodPut, m)
}
