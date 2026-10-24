package azureeventgrid

import (
	"context"
	"errors"
	"sync"
	"time"
)

const (
	reasonMaxAttempts = "MaxDeliveryAttemptsExceeded"
	reasonTTLExpired  = "TimeToLiveExpired"

	maxParallelDeliveries = 8
)

// retrySchedule is Event Grid's documented retry cadence; the last step repeats.
//
//nolint:gochecknoglobals // static schedule
var retrySchedule = []time.Duration{
	10 * time.Second, 30 * time.Second, time.Minute, 5 * time.Minute, 10 * time.Minute,
	30 * time.Minute, time.Hour, 3 * time.Hour, 6 * time.Hour, 12 * time.Hour,
}

// backoff returns the delay before the next attempt after `failed` failures.
func backoff(failed int) time.Duration {
	if failed < 1 {
		return retrySchedule[0]
	}

	return retrySchedule[min(failed, len(retrySchedule))-1]
}

type outcome struct {
	label     string
	ok        bool
	permanent bool
}

type job struct {
	ds       []*delivery
	topic    string
	sub      string
	topicRID string
	spec     Spec
}

type deadItem struct {
	d      *delivery
	reason string
	job    job
}

// RunDue attempts every delivery whose time has come and returns how many were
// attempted. Network calls run outside the backend lock.
func (b *InMemoryBackend) RunDue(ctx context.Context) int {
	jobs, dead := b.collectDue()
	results := make([]outcome, len(jobs))

	var wg sync.WaitGroup

	sem := make(chan struct{}, maxParallelDeliveries)

	for i := range jobs {
		sem <- struct{}{}

		wg.Add(1)

		go func() {
			defer func() { <-sem; wg.Done() }()

			results[i] = b.attempt(ctx, jobs[i])
		}()
	}

	wg.Wait()

	for i, j := range jobs {
		dead = append(dead, b.settle(j, results[i])...)
	}

	for _, item := range dead {
		b.deadLetter(item)
	}

	return len(jobs)
}

func (b *InMemoryBackend) collectDue() ([]job, []deadItem) {
	var (
		jobs []job
		dead []deadItem
	)

	b.mu.Lock("collectDue")
	defer b.mu.Unlock()

	now := b.now().UTC()

	for _, t := range b.topics {
		for _, s := range t.Subs {
			base := job{topic: t.Name, sub: s.Name, topicRID: t.ResourceID, spec: s.Spec}
			live := s.Pending[:0]

			var ready []*delivery

			for _, d := range s.Pending {
				switch {
				case d.InFlight || d.Due.After(now):
					live = append(live, d)
				case expiredEvent(d, s.Spec, now):
					j := base
					j.ds = []*delivery{d}
					dead = append(dead, deadItem{d: d, reason: reasonTTLExpired, job: j})
				default:
					d.InFlight = true
					live = append(live, d)
					ready = append(ready, d)
				}
			}

			s.Pending = live
			jobs = append(jobs, batches(base, ready)...)
		}
	}

	return jobs, dead
}

// batches groups ready deliveries into jobs: a WebHook subscription sends up to
// maxEventsPerBatch events (and about preferredBatchSizeInKilobytes) per request,
// every other destination one event per delivery.
func batches(base job, ready []*delivery) []job {
	maxEvents, maxBytes := 1, 0
	if base.spec.Destination.EndpointType == EndpointWebHook {
		maxEvents, maxBytes = base.spec.Destination.batchLimits()
	}

	var (
		out  []job
		size int
	)

	cur := make([]*delivery, 0, len(ready))

	flush := func() {
		if len(cur) > 0 {
			j := base
			j.ds = cur
			out = append(out, j)
			cur, size = make([]*delivery, 0, len(ready)), 0
		}
	}

	for _, d := range ready {
		if len(cur) >= maxEvents || (len(cur) > 0 && size+len(d.Event) > maxBytes) {
			flush()
		}

		cur = append(cur, d)
		size += len(d.Event)
	}

	flush()

	return out
}

func expiredEvent(d *delivery, spec Spec, now time.Time) bool {
	ttl := time.Duration(spec.RetryPolicy.EventTimeToLiveInMinutes) * time.Minute

	return now.Sub(d.Published) >= ttl
}

// settle records the result of an attempt for every delivery of the job: success
// drops the events, failure schedules a retry, or reports them for dead-lettering.
func (b *InMemoryBackend) settle(j job, res outcome) []deadItem {
	b.mu.Lock("settle")
	defer b.mu.Unlock()

	now := b.now().UTC()
	s := b.liveSubLocked(j)

	var dead []deadItem

	for _, d := range j.ds {
		d.InFlight, d.LastAttempt, d.LastOutcome = false, now, res.label
		d.Attempts++

		if s == nil {
			continue
		}

		if res.ok {
			s.dropDelivery(d)

			continue
		}

		reason := ""

		switch {
		case res.permanent || d.Attempts >= j.spec.RetryPolicy.MaxDeliveryAttempts:
			reason = reasonMaxAttempts
		case expiredEvent(d, j.spec, now.Add(backoff(d.Attempts))):
			reason = reasonTTLExpired
		}

		if reason == "" {
			d.Due = now.Add(backoff(d.Attempts))

			continue
		}

		s.dropDelivery(d)

		item := j
		item.ds = []*delivery{d}
		dead = append(dead, deadItem{d: d, reason: reason, job: item})
	}

	return dead
}

func (b *InMemoryBackend) liveSubLocked(j job) *subscription {
	t, ok := b.topics[j.topic]
	if !ok {
		return nil
	}

	return t.Subs[j.sub]
}

func (s *subscription) dropDelivery(d *delivery) {
	for i, p := range s.Pending {
		if p == d {
			s.Pending = append(s.Pending[:i], s.Pending[i+1:]...)

			return
		}
	}
}

func (b *InMemoryBackend) attempt(ctx context.Context, j job) outcome {
	switch j.spec.Destination.EndpointType {
	case EndpointWebHook:
		return b.deliverWebhook(ctx, j)
	default:
		return b.deliverTarget(j)
	}
}

func targetOutcome(err error) outcome {
	if err == nil {
		return outcome{ok: true, label: outcomeSuccess}
	}

	if errors.Is(err, ErrTargetUnavailable) {
		return outcome{label: outcomeNotFound}
	}

	return outcome{label: outcomeGeneric}
}
