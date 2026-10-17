package mq

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/go-stomp/stomp/v3"
)

// probeBroker logs in to the broker with its configured user, proving it is up and the credentials took.
func probeBroker(ctx context.Context, engine, addr, username, password string) error {
	if engine == EngineTypeRabbitMQ {
		return probeAMQP(ctx, addr, username, password)
	}

	return probeSTOMP(ctx, addr, username, password)
}

func probeAMQP(ctx context.Context, addr, username, password string) error {
	conn, err := amqp.DialConfig("amqp://"+addr+"/", amqp.Config{
		SASL: []amqp.Authentication{&amqp.PlainAuth{Username: username, Password: password}},
		Dial: func(network, a string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, a)
		},
	})
	if err != nil {
		return fmt.Errorf("amqp dial: %w", err)
	}

	return conn.Close()
}

func probeSTOMP(ctx context.Context, addr, username, password string) error {
	nc, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("stomp dial: %w", err)
	}

	conn, err := stomp.ConnectWithContext(ctx, nc, stomp.ConnOpt.Login(username, password))
	if err != nil {
		_ = nc.Close()

		return fmt.Errorf("stomp connect: %w", err)
	}

	return conn.Disconnect()
}

// probeTLS completes a TLS handshake; a published port with nothing behind it fails here.
func probeTLS(ctx context.Context, addr string) error {
	cfg := &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS12} //nolint:gosec // readiness only
	d := &tls.Dialer{Config: cfg}

	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("tls dial: %w", err)
	}

	return conn.Close() //nolint:wrapcheck // close error is irrelevant
}
