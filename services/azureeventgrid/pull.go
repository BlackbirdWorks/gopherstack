package azureeventgrid

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/iso8601"
)

const (
	defaultLockSeconds = 60
	minLockSeconds     = 60
	maxLockSeconds     = 300
	defaultMaxDelivery = 10
	defaultPullTTL     = 7 * 24 * time.Hour
	deliveryModeQueue  = "Queue"
	// MaxReceiveEvents is the largest maxEvents a receive call honours.
	MaxReceiveEvents = 100
)

type pullQueueProps struct {
	EventTimeToLive             string `json:"eventTimeToLive,omitempty"`
	ReceiveLockDurationInSecond int    `json:"receiveLockDurationInSeconds,omitempty"`
	MaxDeliveryCount            int    `json:"maxDeliveryCount,omitempty"`
}

type pullDelivery struct {
	DeliveryMode string         `json:"deliveryMode,omitempty"`
	Queue        pullQueueProps `json:"queue"`
}

type pullFilters struct {
	IncludedEventTypes []string         `json:"includedEventTypes,omitempty"`
	Filters            []AdvancedFilter `json:"filters,omitempty"`
}

type pullProps struct {
	EventDeliverySchema   string       `json:"eventDeliverySchema,omitempty"`
	FiltersConfiguration  pullFilters  `json:"filtersConfiguration"`
	DeliveryConfiguration pullDelivery `json:"deliveryConfiguration"`
}

// Received is one event handed out by a receive call.
type Received struct {
	LockToken     string
	Event         json.RawMessage
	DeliveryCount int
}

// LockFailure reports a lock token an operation could not apply to.
type LockFailure struct {
	LockToken string
	Code      string
	Message   string
}

func (b *InMemoryBackend) pullSubLocked(ns, topicName, name string) (*pullSub, error) {
	_, t, err := b.nsTopicLocked(ns, topicName)
	if err != nil {
		return nil, err
	}

	s, ok := t.Subs[name]
	if !ok {
		return nil, ErrPullSubNotFound
	}

	return s, nil
}

func (s *pullSub) props() map[string]any {
	p := pullProps{
		EventDeliverySchema: SchemaCloudEvent,
		DeliveryConfiguration: pullDelivery{DeliveryMode: deliveryModeQueue, Queue: pullQueueProps{
			EventTimeToLive:             iso8601.Format(time.Duration(s.TTLSeconds) * time.Second),
			ReceiveLockDurationInSecond: s.LockSeconds, MaxDeliveryCount: s.MaxDelivery,
		}},
		FiltersConfiguration: pullFilters{
			IncludedEventTypes: s.Filter.IncludedEventTypes,
			Filters:            s.Filter.AdvancedFilters,
		},
	}

	raw, _ := json.Marshal(p)
	out := map[string]any{}
	_ = json.Unmarshal(raw, &out)
	out[propProvisioning] = StateSucceeded

	return out
}

func parsePullProps(props map[string]any) (pullProps, error) {
	raw, _ := json.Marshal(props)

	var p pullProps
	if err := json.Unmarshal(raw, &p); err != nil {
		return p, invalidSpec("properties are malformed: %v", err)
	}

	if p.DeliveryConfiguration.DeliveryMode != "" && p.DeliveryConfiguration.DeliveryMode != deliveryModeQueue {
		return p, invalidSpec("delivery mode %q is not supported", p.DeliveryConfiguration.DeliveryMode)
	}

	if p.EventDeliverySchema != "" && p.EventDeliverySchema != SchemaCloudEvent {
		return p, invalidSpec("namespace topics only deliver %s", SchemaCloudEvent)
	}

	return p, nil
}

func (s *pullSub) apply(p pullProps) error {
	q := p.DeliveryConfiguration.Queue
	s.LockSeconds, s.MaxDelivery, s.TTLSeconds = q.ReceiveLockDurationInSecond, q.MaxDeliveryCount, int(
		defaultPullTTL/time.Second,
	)

	if s.LockSeconds == 0 {
		s.LockSeconds = defaultLockSeconds
	}

	if s.MaxDelivery == 0 {
		s.MaxDelivery = defaultMaxDelivery
	}

	if q.EventTimeToLive != "" {
		ttl, err := iso8601.Parse(q.EventTimeToLive)
		if err != nil || ttl <= 0 {
			return invalidSpec("eventTimeToLive %q is not a valid duration", q.EventTimeToLive)
		}

		s.TTLSeconds = int(ttl / time.Second)
	}

	if s.LockSeconds < minLockSeconds || s.LockSeconds > maxLockSeconds || s.MaxDelivery < 1 || s.MaxDelivery > 100 {
		return invalidSpec("queue delivery configuration is out of range")
	}

	s.Filter = Filter{
		IncludedEventTypes: p.FiltersConfiguration.IncludedEventTypes,
		AdvancedFilters:    p.FiltersConfiguration.Filters,
	}

	return nil
}

// PutPullSubscription creates or updates a namespace topic subscription.
func (b *InMemoryBackend) PutPullSubscription(
	ns, topicName, name string,
	props map[string]any,
) (map[string]any, error) {
	if !validName(name) {
		return nil, ErrInvalidName
	}

	p, err := parsePullProps(props)
	if err != nil {
		return nil, err
	}

	b.mu.Lock("PutPullSubscription")
	defer b.mu.Unlock()

	_, t, err := b.nsTopicLocked(ns, topicName)
	if err != nil {
		return nil, err
	}

	s, ok := t.Subs[name]
	if !ok {
		s = &pullSub{Name: name, CreatedAt: b.now().UTC()}
	}

	if err = s.apply(p); err != nil {
		return nil, err
	}

	t.Subs[name] = s

	return s.props(), nil
}

// PullSubscription returns the ARM properties and creation time of a namespace topic subscription.
func (b *InMemoryBackend) PullSubscription(ns, topicName, name string) (map[string]any, time.Time, error) {
	b.mu.RLock("PullSubscription")
	defer b.mu.RUnlock()

	s, err := b.pullSubLocked(ns, topicName, name)
	if err != nil {
		return nil, time.Time{}, err
	}

	return s.props(), s.CreatedAt, nil
}

// DeletePullSubscription removes a namespace topic subscription.
func (b *InMemoryBackend) DeletePullSubscription(ns, topicName, name string) error {
	b.mu.Lock("DeletePullSubscription")
	defer b.mu.Unlock()

	_, t, err := b.nsTopicLocked(ns, topicName)
	if err != nil {
		return err
	}

	if _, ok := t.Subs[name]; !ok {
		return ErrPullSubNotFound
	}

	delete(t.Subs, name)

	return nil
}

// PublishNamespace fans CloudEvents out to the matching subscriptions of a namespace topic.
func (b *InMemoryBackend) PublishNamespace(ns, topicName string, events []Event) error {
	b.mu.Lock("PublishNamespace")
	defer b.mu.Unlock()

	_, t, err := b.nsTopicLocked(ns, topicName)
	if err != nil {
		return err
	}

	now := b.now().UTC()

	for _, ev := range events {
		for _, s := range t.Subs {
			s.enqueue(ev, now)
		}
	}

	close(b.pullWake)
	b.pullWake = make(chan struct{})

	return nil
}

func (s *pullSub) enqueue(ev Event, now time.Time) {
	if len(s.Events) >= maxPullEventsPerSub || !s.Filter.Matches(ev.Fields) {
		return
	}

	s.Events = append(s.Events, &pullEvent{
		AvailableAt: now, Event: ev.Raw, ExpiresAt: now.Add(time.Duration(s.TTLSeconds) * time.Second),
	})
}

func (s *pullSub) lockable(now time.Time, limit int) []Received {
	live := s.Events[:0]

	var out []Received

	for _, e := range s.Events {
		if !e.ExpiresAt.After(now) || (e.Count >= s.MaxDelivery && !e.LockedUntil.After(now)) {
			continue
		}

		live = append(live, e)

		if len(out) >= limit || e.AvailableAt.After(now) || e.LockedUntil.After(now) {
			continue
		}

		e.Count++
		e.LockToken, e.LockedUntil = uuid.NewString(), now.Add(time.Duration(s.LockSeconds)*time.Second)
		out = append(out, Received{LockToken: e.LockToken, Event: e.Event, DeliveryCount: e.Count})
	}

	s.Events = live

	return out
}

func (b *InMemoryBackend) receiveOnce(ns, topicName, sub string, limit int) ([]Received, <-chan struct{}, error) {
	b.mu.Lock("Receive")
	defer b.mu.Unlock()

	s, err := b.pullSubLocked(ns, topicName, sub)
	if err != nil {
		return nil, nil, err
	}

	return s.lockable(b.now().UTC(), limit), b.pullWake, nil
}

// Receive locks up to maxEvents available events, waiting up to wait for the first one.
func (b *InMemoryBackend) Receive(
	ctx context.Context, ns, topicName, sub string, maxEvents int, wait time.Duration,
) ([]Received, error) {
	maxEvents = min(max(maxEvents, 1), MaxReceiveEvents)
	timer := time.NewTimer(wait)

	defer timer.Stop()

	for {
		got, wake, err := b.receiveOnce(ns, topicName, sub, maxEvents)
		if err != nil || len(got) > 0 || wait <= 0 {
			return got, err
		}

		select {
		case <-wake:
		case <-timer.C:
			return nil, nil
		case <-ctx.Done():
			return nil, nil
		}
	}
}

type lockAction func(s *pullSub, e *pullEvent, now time.Time)

func (b *InMemoryBackend) withLocks(
	ns, topicName, sub string,
	tokens []string,
	act lockAction,
) ([]string, []LockFailure, error) {
	b.mu.Lock("withLocks")
	defer b.mu.Unlock()

	s, err := b.pullSubLocked(ns, topicName, sub)
	if err != nil {
		return nil, nil, err
	}

	now := b.now().UTC()

	var ok []string

	var failed []LockFailure

	for _, tok := range tokens {
		e := s.byLock(tok, now)
		if e == nil {
			failed = append(
				failed,
				LockFailure{LockToken: tok, Code: outcomeBadRequest, Message: ErrBadLockToken.Error()},
			)

			continue
		}

		act(s, e, now)

		ok = append(ok, tok)
	}

	s.sweepDone()
	close(b.pullWake)
	b.pullWake = make(chan struct{})

	return ok, failed, nil
}

func (s *pullSub) byLock(token string, now time.Time) *pullEvent {
	if token == "" {
		return nil
	}

	for _, e := range s.Events {
		if e.LockToken == token && e.LockedUntil.After(now) {
			return e
		}
	}

	return nil
}

// sweepDone drops events marked done (zero ExpiresAt).
func (s *pullSub) sweepDone() {
	live := s.Events[:0]

	for _, e := range s.Events {
		if !e.ExpiresAt.IsZero() {
			live = append(live, e)
		}
	}

	s.Events = live
}

// Acknowledge removes the locked events.
func (b *InMemoryBackend) Acknowledge(ns, topicName, sub string, tokens []string) ([]string, []LockFailure, error) {
	return b.withLocks(
		ns,
		topicName,
		sub,
		tokens,
		func(_ *pullSub, e *pullEvent, _ time.Time) { e.ExpiresAt = time.Time{} },
	)
}

// Reject removes (dead-letters) the locked events.
func (b *InMemoryBackend) Reject(ns, topicName, sub string, tokens []string) ([]string, []LockFailure, error) {
	return b.Acknowledge(ns, topicName, sub, tokens)
}

// Release unlocks the events, making them available again after delay.
func (b *InMemoryBackend) Release(
	ns, topicName, sub string, tokens []string, delay time.Duration,
) ([]string, []LockFailure, error) {
	return b.withLocks(ns, topicName, sub, tokens, func(_ *pullSub, e *pullEvent, now time.Time) {
		e.LockToken, e.LockedUntil, e.AvailableAt = "", time.Time{}, now.Add(delay)
	})
}

// RenewLock extends the lock of the events by the subscription's lock duration.
func (b *InMemoryBackend) RenewLock(ns, topicName, sub string, tokens []string) ([]string, []LockFailure, error) {
	return b.withLocks(ns, topicName, sub, tokens, func(s *pullSub, e *pullEvent, now time.Time) {
		e.LockedUntil = now.Add(time.Duration(s.LockSeconds) * time.Second)
	})
}
