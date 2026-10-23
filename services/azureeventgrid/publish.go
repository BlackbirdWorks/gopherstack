package azureeventgrid

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"
)

const (
	// MaxRequestBytes is the largest publish request body accepted (Event Grid's 1 MB limit).
	MaxRequestBytes = 1 << 20
	// MaxEventsPerRequest bounds the events of one publish request.
	MaxEventsPerRequest = 5000
)

// Event is one validated event: its raw JSON, decoded form and ID.
type Event struct {
	Fields map[string]any
	ID     string
	Raw    json.RawMessage
}

func invalidEvent(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidEvent, fmt.Sprintf(format, args...))
}

// ParseEvents decodes a publish body (a JSON array or a single object) and
// validates every event against the schema.
func ParseEvents(body []byte, schema string) ([]Event, error) {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 {
		return nil, invalidEvent("the request body is empty")
	}

	var raws []json.RawMessage

	if trimmed[0] == '[' {
		if err := json.Unmarshal(trimmed, &raws); err != nil {
			return nil, invalidEvent("the request body is not valid JSON")
		}
	} else {
		raws = []json.RawMessage{trimmed}
	}

	if len(raws) == 0 {
		return nil, invalidEvent("the request body contains no events")
	}

	if len(raws) > MaxEventsPerRequest {
		return nil, ErrTooManyEvents
	}

	out := make([]Event, 0, len(raws))

	for i, raw := range raws {
		ev, err := parseEvent(raw, schema)
		if err != nil {
			return nil, fmt.Errorf("event %d: %w", i, err)
		}

		out = append(out, ev)
	}

	return out, nil
}

func parseEvent(raw json.RawMessage, schema string) (Event, error) {
	var fields map[string]any
	if err := json.Unmarshal(raw, &fields); err != nil || fields == nil {
		return Event{}, invalidEvent("the event is not a JSON object")
	}

	required := []string{"id", "subject", "eventType", "eventTime", "dataVersion"}

	if schema == SchemaCloudEvent {
		required = []string{"specversion", "id", "source", attrType}
	} else if _, isCloud := fields["specversion"]; isCloud {
		return Event{}, invalidEvent("CloudEvents are not accepted by a topic with the %s input schema", schema)
	}

	for _, k := range required {
		if s, ok := fields[k].(string); !ok || s == "" {
			return Event{}, invalidEvent("required property '%s' not found in JSON", k)
		}
	}

	if schema == SchemaCloudEvent && fields["specversion"] != "1.0" {
		return Event{}, invalidEvent("unsupported CloudEvents specversion")
	}

	id, _ := fields["id"].(string)

	return Event{Fields: fields, ID: id, Raw: raw}, nil
}

func (e *Event) stamp(topicRID, schema string) error {
	if schema == SchemaEventGrid {
		e.Fields["topic"], e.Fields["metadataVersion"] = topicRID, "1"
	}

	raw, err := json.Marshal(e.Fields)
	if err != nil {
		return invalidEvent("the event cannot be encoded")
	}

	e.Raw = raw

	return nil
}

// Publish fans events out to the matching event subscriptions of a topic.
func (b *InMemoryBackend) Publish(topicName string, events []Event) error {
	b.mu.Lock("Publish")
	defer b.mu.Unlock()

	t, err := b.topicLocked(topicName)
	if err != nil {
		return err
	}

	now := b.now().UTC()

	for i := range events {
		if err = events[i].stamp(t.ResourceID, t.InputSchema); err != nil {
			return err
		}

		for _, s := range t.Subs {
			s.enqueue(events[i], now)
		}
	}

	select {
	case b.kick <- struct{}{}:
	default:
	}

	return nil
}

func (s *subscription) expired(now time.Time) bool {
	if s.Spec.ExpirationTimeUTC == "" {
		return false
	}

	exp, err := time.Parse(time.RFC3339Nano, s.Spec.ExpirationTimeUTC)

	return err == nil && now.After(exp)
}

func (s *subscription) enqueue(ev Event, now time.Time) {
	if s.State != StateSucceeded || s.expired(now) || len(s.Pending) >= maxPendingPerSub ||
		!s.Spec.Filter.Matches(ev.Fields) {
		return
	}

	s.Pending = append(s.Pending, &delivery{Due: now, Published: now, ID: ev.ID, Event: ev.Raw})
}
