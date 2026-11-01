// Package iampolicystream records IAM evaluation events in a bounded ring
// buffer, fans them out to live subscribers and derives least-privilege
// policy suggestions from them.
package iampolicystream

import (
	"context"
	"slices"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	// DefaultCapacity is the ring size used when New is given a non-positive capacity.
	DefaultCapacity = 2000
	subscriberBuf   = 256
)

// Decision values recorded on an Event.
const (
	DecisionAllow = "allow"
	DecisionDeny  = "deny"
)

// Deny-reason values recorded on an Event.
const (
	ReasonIdentityPolicy   = "identity-policy"
	ReasonResourcePolicy   = "resource-policy"
	ReasonPermissionBound  = "permission-boundary"
	ReasonImplicitDeny     = "implicit-deny"
	ReasonUnresolvedCaller = "unresolved-principal"
	ReasonServiceControl   = "service-control-policy"
)

// Deny-kind values recorded on a denied Event.
const (
	DenyExplicit = "explicit"
	DenyImplicit = "implicit"
)

// Policy-type values recorded on a PolicyRef.
const (
	PolicyManaged  = "managed"
	PolicyInline   = "inline"
	PolicyResource = "resource"
	PolicyBoundary = "permission-boundary"
	PolicyIdentity = "identity"
	PolicySCP      = "service-control-policy"
)

// PolicyRef names the policy that produced the deciding statement. Owner is the
// principal (user/NAME, group/NAME, role/NAME) for an inline policy and the
// resource ARN for a resource policy.
type PolicyRef struct {
	Type  string `json:"type"`
	Arn   string `json:"arn,omitempty"`
	Name  string `json:"name,omitempty"`
	Owner string `json:"owner,omitempty"`
}

// BoundaryRef records how the caller's permissions boundary evaluated the
// request. Matched is false when no boundary statement applied.
type BoundaryRef struct {
	Arn          string `json:"arn,omitempty"`
	Sid          string `json:"sid,omitempty"`
	StatementIdx int    `json:"statementIndex"`
	Matched      bool   `json:"matched"`
}

// Event is one evaluated request.
type Event struct {
	Time         time.Time    `json:"timestamp"`
	RequestID    string       `json:"requestId,omitempty"`
	Service      string       `json:"service"`
	Operation    string       `json:"operation"`
	Action       string       `json:"action"`
	Resource     string       `json:"resource"`
	Principal    string       `json:"principal"`
	Decision     string       `json:"decision"`
	Reason       string       `json:"reason,omitempty"`
	MatchedSid   string       `json:"matchedSid,omitempty"`
	MatchedBy    string       `json:"matchedBy,omitempty"`
	DenyType     string       `json:"denyType,omitempty"`
	Policy       *PolicyRef   `json:"policy,omitempty"`
	Boundary     *BoundaryRef `json:"boundary,omitempty"`
	Suggested    *Policy      `json:"suggestedPolicy,omitempty"`
	ConditionKey []string     `json:"conditionKeys,omitempty"`
	Seq          uint64       `json:"seq"`
	StatementIdx int          `json:"matchedStatementIndex"`
	PolicyIdx    int          `json:"matchedPolicyIndex"`
	Enforced     bool         `json:"enforced"`
}

// Recorder is a bounded, concurrency-safe event ring with live fan-out.
type Recorder struct {
	mu      *lockmetrics.RWMutex
	subs    map[uint64]chan Event
	ring    []Event
	cap     int
	start   int
	seq     uint64
	nextSub uint64
	dropped uint64
}

// New returns a Recorder holding at most capacity events.
func New(capacity int) *Recorder {
	if capacity <= 0 {
		capacity = DefaultCapacity
	}

	return &Recorder{
		mu:   lockmetrics.New("iampolicystream.recorder"),
		subs: make(map[uint64]chan Event),
		cap:  capacity,
	}
}

// Record stamps e with a sequence number, stores it and notifies subscribers.
// A subscriber whose buffer is full misses the event rather than blocking.
func (r *Recorder) Record(e Event) {
	if r == nil {
		return
	}

	r.mu.Lock("Record")
	defer r.mu.Unlock()

	r.seq++
	e.Seq = r.seq

	if len(r.ring) < r.cap {
		r.ring = append(r.ring, e)
	} else {
		r.ring[r.start] = e
		r.start = (r.start + 1) % r.cap
	}

	for _, ch := range r.subs {
		select {
		case ch <- e:
		default:
			r.dropped++
		}
	}
}

// Dropped reports how many live deliveries were skipped due to slow subscribers.
func (r *Recorder) Dropped() uint64 {
	r.mu.RLock("Dropped")
	defer r.mu.RUnlock()

	return r.dropped
}

// Snapshot returns the retained events, oldest first, that pass keep (nil keeps all).
func (r *Recorder) Snapshot(keep func(Event) bool) []Event {
	r.mu.RLock("Snapshot")
	defer r.mu.RUnlock()

	out := make([]Event, 0, len(r.ring))

	for i := range r.ring {
		e := r.ring[(r.start+i)%len(r.ring)]
		if keep == nil || keep(e) {
			out = append(out, e)
		}
	}

	return out
}

// Clear discards all retained events.
func (r *Recorder) Clear() {
	r.mu.Lock("Clear")
	defer r.mu.Unlock()

	r.ring = nil
	r.start = 0
}

// Subscribe returns a channel of live events. The subscription ends, and the
// channel is closed, when ctx is done.
func (r *Recorder) Subscribe(ctx context.Context) <-chan Event {
	ch := make(chan Event, subscriberBuf)

	r.mu.Lock("Subscribe")
	r.nextSub++
	id := r.nextSub
	r.subs[id] = ch
	r.mu.Unlock()

	go func() {
		<-ctx.Done()
		r.mu.Lock("Unsubscribe")
		delete(r.subs, id)
		close(ch)
		r.mu.Unlock()
	}()

	return ch
}

// Subscribers reports the number of live subscriptions.
func (r *Recorder) Subscribers() int {
	r.mu.RLock("Subscribers")
	defer r.mu.RUnlock()

	return len(r.subs)
}

// Filter selects events; zero fields match everything.
type Filter struct {
	Since     time.Time
	Decision  string
	Service   string
	Principal string
	AfterSeq  uint64
}

// Match reports whether e passes the filter.
func (f Filter) Match(e Event) bool {
	switch {
	case f.Decision != "" && !equalFold(e.Decision, f.Decision):
		return false
	case f.Service != "" && !equalFold(e.Service, f.Service):
		return false
	case f.Principal != "" && e.Principal != f.Principal:
		return false
	case !f.Since.IsZero() && e.Time.Before(f.Since):
		return false
	case e.Seq <= f.AfterSeq:
		return false
	}

	return true
}

// Policy is an IAM policy document.
type Policy struct {
	Version   string      `json:"Version"`
	Statement []Statement `json:"Statement"`
}

// Statement is one Allow statement of a suggested policy.
type Statement struct {
	Effect   string   `json:"Effect"`
	Action   []string `json:"Action"`
	Resource []string `json:"Resource"`
}

const policyVersion = "2012-10-17"

// StatementFor returns the minimal Allow policy permitting action on resource.
func StatementFor(action, resource string) *Policy {
	if resource == "" {
		resource = "*"
	}

	return &Policy{Version: policyVersion, Statement: []Statement{{
		Effect: "Allow", Action: []string{action}, Resource: []string{resource},
	}}}
}

// Suggestion is a least-privilege policy derived from recorded events.
type Suggestion struct {
	Policy         Policy   `json:"policy"`
	BlockedByDeny  []string `json:"blockedByExplicitDeny,omitempty"`
	EventsConsumed int      `json:"eventsConsumed"`
}

// Suggest builds one Allow statement per distinct resource covering every
// consumed action. Events denied by an explicit Deny or permission boundary
// cannot be fixed by an identity Allow and are listed in BlockedByDeny.
// Allowed events are consumed only when includeAllowed is set.
func Suggest(events []Event, includeAllowed bool) Suggestion {
	byRes := map[string][]string{}
	blocked := map[string]struct{}{}
	n := 0

	for _, e := range events {
		switch {
		case e.Decision == DecisionAllow && !includeAllowed:
			continue
		case e.Decision == DecisionDeny && e.Reason != ReasonImplicitDeny && e.Reason != ReasonUnresolvedCaller:
			blocked[e.Action+" on "+e.Resource] = struct{}{}

			continue
		}

		res := e.Resource
		if res == "" {
			res = "*"
		}

		if !slices.Contains(byRes[res], e.Action) {
			byRes[res] = append(byRes[res], e.Action)
		}

		n++
	}

	s := Suggestion{Policy: Policy{Version: policyVersion, Statement: []Statement{}}, EventsConsumed: n}

	for _, res := range slices.Sorted(mapKeys(byRes)) {
		acts := byRes[res]
		slices.Sort(acts)
		s.Policy.Statement = append(s.Policy.Statement, Statement{
			Effect: "Allow", Action: acts, Resource: []string{res},
		})
	}

	s.BlockedByDeny = slices.Sorted(mapKeys(blocked))

	return s
}
