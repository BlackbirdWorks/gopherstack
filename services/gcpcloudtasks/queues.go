package gcpcloudtasks

import (
	"math"
	"slices"
	"strings"
	"time"

	"cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	maxDispatchesPerSecond = 500
	maxConcurrentLimit     = 5000
	defaultConcurrent      = 1000
	defaultMaxAttempts     = 100
	defaultMaxDoublings    = 16
	defaultMinBackoff      = 100 * time.Millisecond
	defaultMaxBackoff      = time.Hour
	maxBurst               = 100
)

func cloneQueue(q *cloudtaskspb.Queue) *cloudtaskspb.Queue {
	out, _ := proto.Clone(q).(*cloudtaskspb.Queue)

	return out
}

func tsOf(t time.Time) *timestamppb.Timestamp { return timestamppb.New(t.UTC()) }

// defaultRate fills unset rate limits and derives the output-only burst size.
func defaultRate(r *cloudtaskspb.RateLimits) (*cloudtaskspb.RateLimits, error) {
	if r == nil {
		r = &cloudtaskspb.RateLimits{}
	}

	switch {
	case r.GetMaxDispatchesPerSecond() < 0 || r.GetMaxDispatchesPerSecond() > maxDispatchesPerSecond:
		return nil, errInvalid("max_dispatches_per_second must be between 0 and %d.", maxDispatchesPerSecond)
	case r.GetMaxConcurrentDispatches() < 0 || r.GetMaxConcurrentDispatches() > maxConcurrentLimit:
		return nil, errInvalid("max_concurrent_dispatches must be between 0 and %d.", maxConcurrentLimit)
	}

	out := &cloudtaskspb.RateLimits{
		MaxDispatchesPerSecond:  r.GetMaxDispatchesPerSecond(),
		MaxConcurrentDispatches: r.GetMaxConcurrentDispatches(),
	}

	if out.GetMaxDispatchesPerSecond() == 0 {
		out.MaxDispatchesPerSecond = maxDispatchesPerSecond
	}

	if out.GetMaxConcurrentDispatches() == 0 {
		out.MaxConcurrentDispatches = defaultConcurrent
	}

	out.MaxBurstSize = int32(min(max(math.Ceil(out.GetMaxDispatchesPerSecond()), 1), maxBurst))

	return out, nil
}

// defaultRetry fills unset retry settings; zero means unset for every field.
func defaultRetry(r *cloudtaskspb.RetryConfig) (*cloudtaskspb.RetryConfig, error) {
	if r == nil {
		r = &cloudtaskspb.RetryConfig{}
	}

	if r.GetMaxAttempts() < -1 || r.GetMaxDoublings() < 0 {
		return nil, errInvalid("max_attempts must be >= -1 and max_doublings >= 0.")
	}

	for _, d := range []*durationpb.Duration{r.GetMaxRetryDuration(), r.GetMinBackoff(), r.GetMaxBackoff()} {
		if d != nil && d.AsDuration() < 0 {
			return nil, errInvalid("retry durations must not be negative.")
		}
	}

	out := &cloudtaskspb.RetryConfig{
		MaxAttempts:      r.GetMaxAttempts(),
		MaxRetryDuration: r.GetMaxRetryDuration(),
		MinBackoff:       r.GetMinBackoff(),
		MaxBackoff:       r.GetMaxBackoff(),
		MaxDoublings:     r.GetMaxDoublings(),
	}

	if out.GetMaxAttempts() == 0 {
		out.MaxAttempts = defaultMaxAttempts
	}

	if out.GetMinBackoff().AsDuration() == 0 {
		out.MinBackoff = durationpb.New(defaultMinBackoff)
	}

	if out.GetMaxBackoff().AsDuration() == 0 {
		out.MaxBackoff = durationpb.New(defaultMaxBackoff)
	}

	if out.GetMaxDoublings() == 0 {
		out.MaxDoublings = defaultMaxDoublings
	}

	if out.GetMinBackoff().AsDuration() > out.GetMaxBackoff().AsDuration() {
		return nil, errInvalid("min_backoff must not exceed max_backoff.")
	}

	return out, nil
}

// normalizeQueue applies defaults and validation to the mutable fields of q.
func normalizeQueue(q *cloudtaskspb.Queue) error {
	rate, err := defaultRate(q.GetRateLimits())
	if err != nil {
		return err
	}

	retry, err := defaultRetry(q.GetRetryConfig())
	if err != nil {
		return err
	}

	if s := q.GetStackdriverLoggingConfig().GetSamplingRatio(); s < 0 || s > 1 {
		return errInvalid("sampling_ratio must be between 0 and 1.")
	}

	q.RateLimits, q.RetryConfig = rate, retry

	return nil
}

func (b *InMemoryBackend) newQueueLocked(name string, in *cloudtaskspb.Queue) (*queueRec, error) {
	if exp, ok := b.tombstones[name]; ok && exp.After(b.now()) {
		return nil, errPrecondition(
			"The queue cannot be created because a queue with this name existed in the last 7 days. " +
				"Please use a different name.")
	}

	q := cloneQueue(in)
	q.Name, q.State, q.PurgeTime = name, cloudtaskspb.Queue_RUNNING, nil

	if err := normalizeQueue(q); err != nil {
		return nil, err
	}

	rec := &queueRec{pb: q, tasks: make(map[string]*taskRec)}
	b.queues[name] = rec

	return rec, nil
}

// CreateQueue creates a queue under a location.
func (b *InMemoryBackend) CreateQueue(req *cloudtaskspb.CreateQueueRequest) (*cloudtaskspb.Queue, error) {
	if err := parseLocation(req.GetParent()); err != nil {
		return nil, err
	}

	in := req.GetQueue()
	if in == nil {
		return nil, errInvalid("queue must be specified.")
	}

	loc, err := parseQueueName(in.GetName())
	if err != nil {
		return nil, err
	}

	if loc != req.GetParent() {
		return nil, errInvalid("Queue name %q must be under parent %q.", in.GetName(), req.GetParent())
	}

	b.mu.Lock("CreateQueue")
	defer b.mu.Unlock()

	if _, ok := b.queues[in.GetName()]; ok {
		return nil, errExists("Queue already exists.")
	}

	rec, err := b.newQueueLocked(in.GetName(), in)
	if err != nil {
		return nil, err
	}

	return cloneQueue(rec.pb), nil
}

// GetQueue returns a queue.
func (b *InMemoryBackend) GetQueue(name string) (*cloudtaskspb.Queue, error) {
	if _, err := parseQueueName(name); err != nil {
		return nil, err
	}

	b.mu.RLock("GetQueue")
	defer b.mu.RUnlock()

	q, ok := b.queues[name]
	if !ok {
		return nil, errQueueMissing()
	}

	return cloneQueue(q.pb), nil
}

func errQueueMissing() error {
	return errNotFound(
		"Queue does not exist. If you just created the queue, wait at least a minute for the queue to initialize.",
	)
}

// ListQueues lists a location's queues sorted by name.
func (b *InMemoryBackend) ListQueues(req *cloudtaskspb.ListQueuesRequest) ([]*cloudtaskspb.Queue, string, error) {
	if err := parseLocation(req.GetParent()); err != nil {
		return nil, "", err
	}

	match, err := parseQueueFilter(req.GetFilter())
	if err != nil {
		return nil, "", err
	}

	size, err := pageSizeOf(req.GetPageSize())
	if err != nil {
		return nil, "", err
	}

	b.mu.RLock("ListQueues")
	defer b.mu.RUnlock()

	prefix := req.GetParent() + "/queues/"
	names := make([]string, 0, len(b.queues))

	for n, q := range b.queues {
		if strings.HasPrefix(n, prefix) && match(q.pb) {
			names = append(names, n)
		}
	}

	slices.Sort(names)

	all := make([]*cloudtaskspb.Queue, 0, len(names))
	for _, n := range names {
		all = append(all, cloneQueue(b.queues[n].pb))
	}

	pg := page.New(all, req.GetPageToken(), size, defaultPageSize)

	return pg.Data, pg.Next, nil
}

// parseQueueFilter supports AND-joined `state=X`, `name=X` and `name:substring` terms.
func parseQueueFilter(f string) (func(*cloudtaskspb.Queue) bool, error) {
	var preds []func(*cloudtaskspb.Queue) bool

	for term := range strings.SplitSeq(f, " AND ") {
		term = strings.TrimSpace(term)
		if term == "" {
			continue
		}

		p, err := queueTerm(term)
		if err != nil {
			return nil, err
		}

		preds = append(preds, p)
	}

	return func(q *cloudtaskspb.Queue) bool {
		for _, p := range preds {
			if !p(q) {
				return false
			}
		}

		return true
	}, nil
}

func queueTerm(term string) (func(*cloudtaskspb.Queue) bool, error) {
	for _, op := range []string{"=", ":"} {
		field, val, ok := strings.Cut(term, op)
		if !ok {
			continue
		}

		field, val = strings.TrimSpace(field), strings.Trim(strings.TrimSpace(val), `"`)

		switch {
		case field == "state" && op == "=":
			return func(q *cloudtaskspb.Queue) bool { return q.GetState().String() == val }, nil
		case field == "name" && op == "=":
			return func(q *cloudtaskspb.Queue) bool { return q.GetName() == val }, nil
		case field == "name":
			return func(q *cloudtaskspb.Queue) bool { return strings.Contains(q.GetName(), val) }, nil
		}
	}

	return nil, errInvalid("Unsupported filter term %q: use state=, name= or name:.", term)
}

// DeleteQueue removes a queue and its tasks; the name cannot be reused for 7 days.
func (b *InMemoryBackend) DeleteQueue(name string) error {
	if _, err := parseQueueName(name); err != nil {
		return err
	}

	b.mu.Lock("DeleteQueue")
	defer b.mu.Unlock()

	if _, ok := b.queues[name]; !ok {
		return errQueueMissing()
	}

	delete(b.queues, name)
	delete(b.policies, name)
	b.tombstones[name] = b.now().Add(queueTombstoneTTL)

	return nil
}

// PurgeQueue deletes every task in a queue.
func (b *InMemoryBackend) PurgeQueue(name string) (*cloudtaskspb.Queue, error) {
	return b.mutateQueue("PurgeQueue", name, func(q *queueRec, now time.Time) error {
		for tn := range q.tasks {
			b.tombstones[tn] = now.Add(taskTombstoneTTL)
		}

		q.tasks = make(map[string]*taskRec)
		q.pb.PurgeTime = tsOf(now)

		return nil
	})
}

// PauseQueue stops dispatching; tasks can still be added.
func (b *InMemoryBackend) PauseQueue(name string) (*cloudtaskspb.Queue, error) {
	return b.mutateQueue("PauseQueue", name, func(q *queueRec, _ time.Time) error {
		q.pb.State = cloudtaskspb.Queue_PAUSED

		return nil
	})
}

// ResumeQueue restarts dispatching of a paused queue.
func (b *InMemoryBackend) ResumeQueue(name string) (*cloudtaskspb.Queue, error) {
	return b.mutateQueue("ResumeQueue", name, func(q *queueRec, _ time.Time) error {
		if q.pb.GetState() == cloudtaskspb.Queue_DISABLED {
			return errPrecondition("Queue is disabled and cannot be resumed.")
		}

		q.pb.State = cloudtaskspb.Queue_RUNNING

		return nil
	})
}

func (b *InMemoryBackend) mutateQueue(
	op, name string, fn func(*queueRec, time.Time) error,
) (*cloudtaskspb.Queue, error) {
	if _, err := parseQueueName(name); err != nil {
		return nil, err
	}

	b.mu.Lock(op)
	defer b.mu.Unlock()

	q, ok := b.queues[name]
	if !ok {
		return nil, errQueueMissing()
	}

	if err := fn(q, b.now()); err != nil {
		return nil, err
	}

	return cloneQueue(q.pb), nil
}
