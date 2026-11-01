package ocievents

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
)

// Delivery statuses.
const (
	deliveryPending   = "PENDING"
	deliveryDelivered = "DELIVERED"
	deliveryFailed    = "FAILED"
)

const (
	cloudEventsVersion = "0.1"
	eventTypeVersion   = "2.0"
	deliveryTimeout    = 30 * time.Second
	eventTimeLayout    = "2006-01-02T15:04:05.000Z"
)

var errNoTarget = errors.New("the target service is not available")

// Event is what a service reports; the backend wraps it in the OCI event envelope.
type Event struct {
	Time               time.Time
	AdditionalDetails  map[string]any
	EventType          string
	Source             string
	CompartmentID      string
	ResourceName       string
	ResourceID         string
	AvailabilityDomain string
}

type eventData struct {
	AdditionalDetails  map[string]any `json:"additionalDetails"`
	CompartmentID      string         `json:"compartmentId"`
	ResourceName       string         `json:"resourceName"`
	ResourceID         string         `json:"resourceId"`
	AvailabilityDomain string         `json:"availabilityDomain"`
}

type envelope struct {
	Extensions         map[string]string `json:"extensions"`
	Data               eventData         `json:"data"`
	EventType          string            `json:"eventType"`
	CloudEventsVersion string            `json:"cloudEventsVersion"`
	EventTypeVersion   string            `json:"eventTypeVersion"`
	Source             string            `json:"source"`
	EventTime          string            `json:"eventTime"`
	ContentType        string            `json:"contentType"`
	EventID            string            `json:"eventID"`
}

// Delivery records one attempt to hand an event to a rule action; failed attempts are not retried.
type Delivery struct {
	Time       time.Time `json:"time"`
	EventID    string    `json:"eventId"`
	EventType  string    `json:"eventType"`
	RuleID     string    `json:"ruleId"`
	ActionID   string    `json:"actionId"`
	ActionType string    `json:"actionType"`
	TargetID   string    `json:"targetId"`
	Status     string    `json:"status"`
	Error      string    `json:"error,omitempty"`
}

type job struct {
	rec    *Delivery
	ruleID string
	action Action
	body   []byte
}

func buildEnvelope(ev Event, id string) ([]byte, map[string]any, error) {
	details := ev.AdditionalDetails
	if details == nil {
		details = map[string]any{}
	}

	ad := ev.AvailabilityDomain
	if ad == "" {
		ad = "all"
	}

	raw, err := json.Marshal(envelope{
		EventType: ev.EventType, CloudEventsVersion: cloudEventsVersion, EventTypeVersion: eventTypeVersion,
		Source: ev.Source, EventTime: ev.Time.UTC().Format(eventTimeLayout), ContentType: "application/json",
		EventID: id, Extensions: map[string]string{"compartmentId": ev.CompartmentID},
		Data: eventData{
			CompartmentID: ev.CompartmentID, ResourceName: ev.ResourceName, ResourceID: ev.ResourceID,
			AvailabilityDomain: ad, AdditionalDetails: details,
		},
	})
	if err != nil {
		return nil, nil, err //nolint:wrapcheck // plain marshal failure
	}

	var generic map[string]any
	if err = json.Unmarshal(raw, &generic); err != nil {
		return nil, nil, err //nolint:wrapcheck // plain unmarshal failure
	}

	return raw, generic, nil
}

func targetOf(a *Action) string {
	switch a.ActionType {
	case actionONS:
		return a.TopicID
	case actionOSS:
		return a.StreamID
	default:
		return a.FunctionID
	}
}

// Emit matches an event against every enabled rule that covers its compartment and delivers it to the
// enabled actions of each matching rule.
func (b *InMemoryBackend) Emit(ev Event) {
	if ev.Time.IsZero() {
		ev.Time = b.clockNow()
	}

	id := uuid.NewString()

	raw, generic, err := buildEnvelope(ev, id)
	if err != nil {
		return
	}

	jobs, spawn := b.plan(ev, id, raw, generic)

	for _, j := range jobs {
		spawn(func() { b.finish(j.rec, b.deliver(j)) })
	}
}

func (b *InMemoryBackend) clockNow() time.Time {
	b.mu.RLock("clockNow")
	defer b.mu.RUnlock()

	return b.nowUTC()
}

func (b *InMemoryBackend) plan(ev Event, id string, raw []byte, generic map[string]any) ([]job, func(func())) {
	b.mu.Lock("Emit")
	defer b.mu.Unlock()

	var jobs []job

	for _, r := range b.sortedRules() {
		if r.State != stateActive || !r.IsEnabled || !b.inScope(ev.CompartmentID, r.CompartmentID) {
			continue
		}

		cond, perr := parseCondition(r.Condition)
		if perr != nil || !matchCondition(cond, generic) {
			continue
		}

		for _, a := range r.Actions {
			if !a.IsEnabled {
				continue
			}

			rec := &Delivery{
				Time: b.nowUTC(), EventID: id, EventType: ev.EventType, RuleID: r.ID, ActionID: a.ID,
				ActionType: a.ActionType, TargetID: targetOf(a), Status: deliveryPending,
			}
			b.deliveries = append(b.deliveries, rec)
			jobs = append(jobs, job{rec: rec, action: *a, body: raw, ruleID: r.ID})
		}
	}

	if over := len(b.deliveries) - maxDeliveries; over > 0 {
		b.deliveries = b.deliveries[over:]
	}

	return jobs, b.spawn
}

func (b *InMemoryBackend) sortedRules() []*Rule {
	all := make([]*Rule, 0, len(b.rules))
	for _, r := range b.rules {
		all = append(all, r)
	}

	return paginate(all, ListOpts{Order: "ASC", Limit: len(all) + 1}).Data
}

func (b *InMemoryBackend) targets() (Notifier, Streamer, FunctionInvoker) {
	b.mu.RLock("targets")
	defer b.mu.RUnlock()

	return b.notifier, b.streamer, b.invoker
}

func (b *InMemoryBackend) deliver(j job) error {
	ons, oss, faas := b.targets()
	body := string(j.body)

	switch j.action.ActionType {
	case actionONS:
		if ons == nil {
			return errNoTarget
		}

		return ons.PublishEvent(j.action.TopicID, body)
	case actionOSS:
		if oss == nil {
			return errNoTarget
		}

		return oss.AppendMessage(j.action.StreamID, nil, j.body)
	default:
		if faas == nil {
			return errNoTarget
		}

		ctx, cancel := context.WithTimeout(context.Background(), deliveryTimeout)
		defer cancel()

		return faas.InvokeFunction(ctx, j.action.FunctionID, j.body)
	}
}

func (b *InMemoryBackend) finish(rec *Delivery, err error) {
	b.mu.Lock("finishDelivery")
	defer b.mu.Unlock()

	if err != nil {
		rec.Status, rec.Error = deliveryFailed, err.Error()

		return
	}

	rec.Status = deliveryDelivered
}

// Deliveries returns the delivery log, oldest first.
func (b *InMemoryBackend) Deliveries() []Delivery {
	b.mu.RLock("Deliveries")
	defer b.mu.RUnlock()

	out := make([]Delivery, 0, len(b.deliveries))
	for _, d := range b.deliveries {
		out = append(out, *d)
	}

	return out
}
