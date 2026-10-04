package lambda

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/go-stomp/stomp/v3"
	"github.com/go-stomp/stomp/v3/frame"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	mqDialTimeout     = 5 * time.Second
	mqEngineRabbitMQ  = "RABBITMQ"
	mqMaxPrefetch     = 65535
	mqPriorityDefault = 4
	stompTrue         = "true"
)

var errMQConnectionLost = errors.New("broker connection lost")

// mqConsumerConfig describes the queue consumer an ESM worker needs.
type mqConsumerConfig struct {
	Engine   string
	Addr     string
	Queue    string
	VHost    string
	Username string
	Password string
}

// mqConsumer reads messages from one queue and settles them after the function ran.
type mqConsumer interface {
	// Poll blocks for a message, then returns up to maxMessages that are ready; a done ctx ends it without error.
	Poll(ctx context.Context, maxMessages int) ([]mqMessage, error)
	Ack(ctx context.Context, msgs []mqMessage) error
	// Requeue returns msgs to the broker for redelivery.
	Requeue(ctx context.Context, msgs []mqMessage) error
	Close()
}

type mqConsumerFactory func(cfg mqConsumerConfig, prefetch int) (mqConsumer, error)

func newMQConsumer(cfg mqConsumerConfig, prefetch int) (mqConsumer, error) {
	if cfg.Engine == mqEngineRabbitMQ {
		return newAMQPConsumer(cfg, prefetch)
	}

	return newSTOMPConsumer(cfg, prefetch)
}

type amqpConsumer struct {
	conn       *amqp.Connection
	ch         *amqp.Channel
	deliveries <-chan amqp.Delivery
	queue      string
}

func newAMQPConsumer(cfg mqConsumerConfig, prefetch int) (mqConsumer, error) {
	conn, err := amqp.DialConfig("amqp://"+cfg.Addr+"/", amqp.Config{
		Vhost: cfg.VHost,
		SASL:  []amqp.Authentication{&amqp.PlainAuth{Username: cfg.Username, Password: cfg.Password}},
		Dial:  amqp.DefaultDial(mqDialTimeout),
	})
	if err != nil {
		return nil, fmt.Errorf("amqp dial: %w", err)
	}

	ch, err := conn.Channel()
	if err == nil {
		err = ch.Qos(min(prefetch, mqMaxPrefetch), 0, false)
	}

	var deliveries <-chan amqp.Delivery

	if err == nil {
		deliveries, err = ch.Consume(cfg.Queue, "", false, false, false, false, nil)
	}

	if err != nil {
		_ = conn.Close()

		return nil, fmt.Errorf("amqp consume: %w", err)
	}

	return &amqpConsumer{conn: conn, ch: ch, deliveries: deliveries, queue: cfg.Queue}, nil
}

func (c *amqpConsumer) Poll(ctx context.Context, maxMessages int) ([]mqMessage, error) {
	var out []mqMessage

	select {
	case <-ctx.Done():
		return nil, nil
	case d, ok := <-c.deliveries:
		if !ok {
			return nil, errMQConnectionLost
		}

		out = append(out, c.toMessage(d))
	}

	for len(out) < maxMessages {
		select {
		case d, ok := <-c.deliveries:
			if !ok {
				return out, nil
			}

			out = append(out, c.toMessage(d))
		default:
			return out, nil
		}
	}

	return out, nil
}

func (c *amqpConsumer) toMessage(d amqp.Delivery) mqMessage {
	return mqMessage{
		Queue:           c.queue,
		Body:            d.Body,
		Redelivered:     d.Redelivered,
		Headers:         d.Headers,
		ContentType:     d.ContentType,
		ContentEncoding: d.ContentEncoding,
		DeliveryMode:    int(d.DeliveryMode),
		Priority:        int(d.Priority),
		CorrelationID:   d.CorrelationId,
		ReplyTo:         d.ReplyTo,
		Expiration:      d.Expiration,
		MessageID:       d.MessageId,
		Timestamp:       d.Timestamp,
		Type:            d.Type,
		UserID:          d.UserId,
		AppID:           d.AppId,
		raw:             d,
	}
}

func (c *amqpConsumer) settle(msgs []mqMessage, fn func(amqp.Delivery) error) error {
	for _, m := range msgs {
		if d, ok := m.raw.(amqp.Delivery); ok {
			if err := fn(d); err != nil {
				return fmt.Errorf("settle delivery: %w", err)
			}
		}
	}

	return nil
}

func (c *amqpConsumer) Ack(_ context.Context, msgs []mqMessage) error {
	return c.settle(msgs, func(d amqp.Delivery) error { return d.Ack(false) })
}

func (c *amqpConsumer) Requeue(_ context.Context, msgs []mqMessage) error {
	return c.settle(msgs, func(d amqp.Delivery) error { return d.Nack(false, true) })
}

func (c *amqpConsumer) Close() {
	_ = c.ch.Close()
	_ = c.conn.Close()
}

type stompConsumer struct {
	conn *stomp.Conn
	sub  *stomp.Subscription
	dest string
}

func newSTOMPConsumer(cfg mqConsumerConfig, prefetch int) (mqConsumer, error) {
	ctx, cancel := context.WithTimeout(context.Background(), mqDialTimeout)
	defer cancel()

	nc, err := (&net.Dialer{}).DialContext(ctx, "tcp", cfg.Addr)
	if err != nil {
		return nil, fmt.Errorf("stomp dial: %w", err)
	}

	conn, err := stomp.ConnectWithContext(ctx, nc, stomp.ConnOpt.Login(cfg.Username, cfg.Password))
	if err != nil {
		_ = nc.Close()

		return nil, fmt.Errorf("stomp connect: %w", err)
	}

	sub, err := conn.Subscribe(mqQueuePrefix+cfg.Queue, stomp.AckClientIndividual,
		stomp.SubscribeOpt.Header("activemq.prefetchSize", strconv.Itoa(prefetch)))
	if err != nil {
		_ = conn.Disconnect()

		return nil, fmt.Errorf("stomp subscribe: %w", err)
	}

	return &stompConsumer{conn: conn, sub: sub, dest: cfg.Queue}, nil
}

func (c *stompConsumer) Poll(ctx context.Context, maxMessages int) ([]mqMessage, error) {
	var out []mqMessage

	select {
	case <-ctx.Done():
		return nil, nil
	case m, ok := <-c.sub.C:
		first, err := c.next(m, ok)
		if err != nil {
			return nil, err
		}

		out = append(out, first)
	}

	for len(out) < maxMessages {
		next, more := c.tryNext()
		if !more {
			break
		}

		out = append(out, next)
	}

	return out, nil
}

// tryNext takes an already-delivered message without blocking; false when none is ready or the link broke.
func (c *stompConsumer) tryNext() (mqMessage, bool) {
	select {
	case m, ok := <-c.sub.C:
		msg, err := c.next(m, ok)

		return msg, err == nil
	default:
		return mqMessage{}, false
	}
}

func (c *stompConsumer) next(m *stomp.Message, ok bool) (mqMessage, error) {
	if !ok {
		return mqMessage{}, errMQConnectionLost
	}

	if m.Err != nil {
		return mqMessage{}, fmt.Errorf("stomp message: %w", m.Err)
	}

	return stompToMessage(m, c.dest), nil
}

func stompReserved(k string) bool {
	switch k {
	case frame.MessageId, frame.Destination, frame.Subscription, frame.ContentLength, frame.ContentType, frame.Ack,
		"timestamp", "expires", "priority", "correlation-id", "reply-to", "type", "persistent", "redelivered":
		return true
	}

	return false
}

func stompToMessage(m *stomp.Message, queue string) mqMessage {
	h := m.Header
	_, hasLen := h.Contains(frame.ContentLength)
	now := time.Now()

	deliveryMode := 1
	if h.Get("persistent") == stompTrue {
		deliveryMode = 2
	}

	props := make(map[string]string)

	for i := range h.Len() {
		k, v := h.GetAt(i)
		if !stompReserved(k) {
			props[k] = v
		}
	}

	ts := now
	if ms, err := strconv.ParseInt(h.Get("timestamp"), 10, 64); err == nil {
		ts = time.UnixMilli(ms)
	}

	return mqMessage{
		Queue:         queue,
		Body:          m.Body,
		MessageID:     h.Get(frame.MessageId),
		MessageType:   stompMessageType(hasLen),
		DeliveryMode:  deliveryMode,
		ReplyTo:       h.Get("reply-to"),
		Type:          h.Get("type"),
		Expiration:    h.Get("expires"),
		Priority:      atoiOr(h.Get("priority"), mqPriorityDefault),
		CorrelationID: h.Get("correlation-id"),
		Redelivered:   h.Get("redelivered") == stompTrue,
		Timestamp:     ts,
		DeliveredAt:   now,
		Properties:    props,
		raw:           m,
	}
}

func (c *stompConsumer) Ack(_ context.Context, msgs []mqMessage) error {
	for _, m := range msgs {
		if raw, ok := m.raw.(*stomp.Message); ok {
			if err := c.conn.Ack(raw); err != nil {
				return fmt.Errorf("stomp ack: %w", err)
			}
		}
	}

	return nil
}

// Requeue drops the connection: the broker redelivers everything unacknowledged, flagged redelivered.
func (c *stompConsumer) Requeue(_ context.Context, _ []mqMessage) error {
	c.Close()

	return nil
}

func (c *stompConsumer) Close() { _ = c.conn.Disconnect() }
