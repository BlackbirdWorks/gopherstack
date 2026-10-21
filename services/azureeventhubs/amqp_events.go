package azureeventhubs

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/amqp1"
)

const (
	annSequence     = "x-opt-sequence-number"
	annOffset       = "x-opt-offset"
	annEnqueued     = "x-opt-enqueued-time"
	annPartitionKey = "x-opt-partition-key"
)

func selectorOf(src *amqp1.Source) (Selector, error) {
	for k, v := range src.Filter {
		if amqpKey(k) != SelectorFilterName {
			continue
		}

		d, ok := v.(amqp1.Described)
		if !ok {
			return Selector{}, ErrInvalidSelector
		}

		expr, _ := d.Value.(string)

		return ParseSelector(expr)
	}

	return Selector{}, nil
}

func receiverError(err error, addr string) *amqp1.Error {
	switch {
	case errors.Is(err, ErrReceiverStolen):
		return &amqp1.Error{Condition: condStolen, Description: err.Error()}
	case errors.Is(err, ErrTooManyReceivers):
		return &amqp1.Error{Condition: amqp1.CondResourceLimit, Description: err.Error()}
	}

	return notFound(addr)
}

// eventWriter accepts events sent to a hub or one of its partitions.
type eventWriter struct {
	b   *InMemoryBackend
	ref HubRef
	pid int
}

func (*eventWriter) Close() {}

func rejected(cond amqp1.Symbol, desc string) amqp1.DeliveryState {
	return amqp1.DeliveryState{Kind: amqp1.StateRejected, Error: &amqp1.Error{Condition: cond, Description: desc}}
}

func (w *eventWriter) Deliver(_ context.Context, in *amqp1.Incoming) amqp1.DeliveryState {
	evs, err := decodeEvents(in)
	if err != nil {
		return rejected(amqp1.CondDecode, err.Error())
	}

	if _, appendErr := w.b.Append(w.ref, w.pid, evs); appendErr != nil {
		switch {
		case errors.Is(appendErr, ErrEventTooLarge):
			return rejected(amqp1.CondMessageTooLarge, appendErr.Error())
		case errors.Is(appendErr, ErrHubNotFound), errors.Is(appendErr, ErrPartitionNotFound):
			return rejected(amqp1.CondNotFound, appendErr.Error())
		}

		return rejected(amqp1.CondInternal, appendErr.Error())
	}

	return amqp1.DeliveryState{Kind: amqp1.StateAccepted}
}

func decodeEvents(in *amqp1.Incoming) ([]NewEvent, error) {
	outer, err := amqp1.DecodeMessage(in.Payload)
	if err != nil {
		return nil, err
	}

	if in.Format != amqp1.BatchMessageFormat {
		ev, evErr := newEvent(outer, "")

		return []NewEvent{ev}, evErr
	}

	outerKey := annString(outer.Annotations, annPartitionKey)
	evs := make([]NewEvent, 0, len(outer.Data))

	for _, d := range outer.Data {
		inner, decErr := amqp1.DecodeMessage(d)
		if decErr != nil {
			return nil, decErr
		}

		ev, evErr := newEvent(inner, outerKey)
		if evErr != nil {
			return nil, evErr
		}

		evs = append(evs, ev)
	}

	return evs, nil
}

func annString(m amqp1.Map, name string) string {
	for k, v := range m {
		if amqpKey(k) == name {
			s, _ := v.(string)

			return s
		}
	}

	return ""
}

func newEvent(m *amqp1.Message, fallbackKey string) (NewEvent, error) {
	key := annString(m.Annotations, annPartitionKey)
	if key == "" {
		key = fallbackKey
	}

	for _, name := range []string{annSequence, annOffset, annEnqueued, annPartitionKey} {
		delete(m.Annotations, amqp1.Symbol(name))
		delete(m.Annotations, name)
	}

	if key != "" {
		if m.Annotations == nil {
			m.Annotations = amqp1.Map{}
		}

		m.Annotations[amqp1.Symbol(annPartitionKey)] = key
	}

	raw, err := m.Encode()
	if err != nil {
		return NewEvent{}, err
	}

	return NewEvent{Key: key, Raw: raw}, nil
}

// eventReader feeds one receiver link from a partition.
type eventReader struct {
	c      *amqpConn
	b      *InMemoryBackend
	reg    *ReceiverReg
	ref    HubRef
	cursor int64
	pid    int
}

func (r *eventReader) Next(ctx context.Context) (*amqp1.Outgoing, error) {
	ev, err := r.b.Next(ctx, r.ref, r.pid, r.cursor, r.reg.Kicked())

	switch {
	case err == nil:
	case errors.Is(err, ErrReceiverStolen):
		return nil, &amqp1.Error{Condition: condStolen, Description: r.reg.Reason()}
	case ctx.Err() != nil:
		return nil, ctx.Err()
	default:
		return nil, notFound(r.ref.Hub)
	}

	r.cursor = ev.Seq + 1

	payload, err := deliverable(ev)
	if err != nil {
		return nil, &amqp1.Error{Condition: amqp1.CondInternal, Description: err.Error()}
	}

	return &amqp1.Outgoing{Tag: r.c.nextTag(), Payload: payload, Settled: true}, nil
}

func deliverable(ev *event) ([]byte, error) {
	m, err := amqp1.DecodeMessage(ev.Raw)
	if err != nil {
		return nil, fmt.Errorf("azureeventhubs: stored event: %w", err)
	}

	if m.Annotations == nil {
		m.Annotations = amqp1.Map{}
	}

	m.Annotations[amqp1.Symbol(annSequence)] = ev.Seq
	m.Annotations[amqp1.Symbol(annOffset)] = strconv.FormatInt(ev.Offset, 10)
	m.Annotations[amqp1.Symbol(annEnqueued)] = ev.Enqueued.Truncate(time.Millisecond)

	return m.Encode()
}

func (*eventReader) Settle(*amqp1.Outgoing, amqp1.DeliveryState) {}
func (*eventReader) Release(*amqp1.Outgoing)                     {}

func (r *eventReader) Close() { r.b.UnregisterReceiver(r.reg) }
