package gcppubsub

import (
	"cmp"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
)

const (
	dlqAttrCount   = "CloudPubSubDeadLetterSourceDeliveryCount"
	dlqAttrSub     = "CloudPubSubDeadLetterSourceSubscription"
	dlqAttrProject = "CloudPubSubDeadLetterSourceSubscriptionProject"
	dlqAttrTime    = "CloudPubSubDeadLetterSourceTopicPublishTime"

	compactMinAcked = 64
)

func msgSize(m *message) int64 {
	n := int64(len(m.Data))

	for k, v := range m.Attrs {
		n += int64(len(k) + len(v))
	}

	return n
}

// Publish appends messages to a topic and fans them out to its subscriptions.
func (b *InMemoryBackend) Publish(req *pubsubpb.PublishRequest) ([]string, error) {
	msgs := req.GetMessages()
	if len(msgs) == 0 {
		return nil, errInvalid(
			"The value for message_count is too small. You passed 0 in the request, but the minimum value is 1.",
		)
	}

	if len(msgs) > maxPublishBatch {
		return nil, errInvalid(
			"The value for message_count is too large. You passed %d in the request, but the maximum value is %d.",
			len(msgs),
			maxPublishBatch,
		)
	}

	var total int

	for _, m := range msgs {
		if len(m.GetData()) == 0 && len(m.GetAttributes()) == 0 {
			return nil, errInvalid("At least one of 'data' or 'attributes' must be specified.")
		}

		for k := range m.GetAttributes() {
			if k == "" {
				return nil, errInvalid("Attribute keys must be non-empty.")
			}
		}

		total += len(m.GetData())
	}

	if total > maxPublishBytes {
		return nil, errInvalid("Request payload size exceeds the limit: %d bytes.", maxPublishBytes)
	}

	b.mu.Lock("Publish")
	defer b.mu.Unlock()

	t, ok := b.topics[req.GetTopic()]
	if !ok {
		return nil, errNotFound("topics", req.GetTopic())
	}

	if err := b.validateAgainstSchemaLocked(t, msgs); err != nil {
		return nil, err
	}

	b.sweepSubsLocked()

	now := b.now()
	ids := make([]string, 0, len(msgs))
	stored := make([]*message, 0, len(msgs))

	for _, m := range msgs {
		sm := &message{
			ID:          b.nextMsgID(),
			Data:        append([]byte(nil), m.GetData()...),
			Attrs:       maps.Clone(m.GetAttributes()),
			OrderingKey: m.GetOrderingKey(),
			PublishTime: now,
		}
		stored = append(stored, sm)
		ids = append(ids, sm.ID)
	}

	b.fanOutLocked(req.GetTopic(), t, stored)

	return ids, nil
}

func (b *InMemoryBackend) fanOutLocked(topicName string, t *topic, msgs []*message) {
	if d := t.pb.GetMessageRetentionDuration(); d != nil {
		cutoff := b.now().Add(-d.AsDuration())

		t.retained = slices.DeleteFunc(t.retained, func(m *message) bool { return m.PublishTime.Before(cutoff) })
		t.retained = append(t.retained, msgs...)
	}

	for _, s := range b.subs {
		if s.pb.GetTopic() != topicName || s.pb.GetDetached() {
			continue
		}

		if s.deliverLocked(msgs) {
			b.notifyLocked(s)

			if s.pb.GetPushConfig().GetPushEndpoint() != "" {
				b.signalPush()
			}
		}
	}
}

// deliverLocked appends the messages matching the filter and reports whether any did.
func (s *subscription) deliverLocked(msgs []*message) bool {
	var added bool

	for _, m := range msgs {
		if s.filter != nil && !s.filter.eval(m.Attrs) {
			continue
		}

		s.msgs = append(s.msgs, &subMsg{Msg: m})
		added = true
	}

	return added
}

type leaseResult struct {
	wake     <-chan struct{}
	nextWake time.Time
	msgs     []*pubsubpb.ReceivedMessage
}

func (s *subscription) backoff(attempts int32) time.Duration {
	rp := s.pb.GetRetryPolicy()
	if rp == nil {
		return 0
	}

	lo := defaultMinRetryBackoff
	if rp.GetMinimumBackoff() != nil {
		lo = rp.GetMinimumBackoff().AsDuration()
	}

	hi := maxRetryBackoff
	if rp.GetMaximumBackoff() != nil {
		hi = rp.GetMaximumBackoff().AsDuration()
	}

	d := lo

	for i := int32(1); i < attempts && d < hi; i++ {
		d *= 2
	}

	return min(d, hi)
}

func (s *subscription) release(m *subMsg, now time.Time) {
	delete(s.acks, m.ackID)
	m.ackID, m.owner, m.leaseUntil = "", "", time.Time{}
	m.NotBefore = now.Add(s.backoff(m.Attempts))
}

func (s *subscription) expireLeases(now time.Time) {
	for _, m := range s.acks {
		if !m.leaseUntil.After(now) {
			s.release(m, now)
		}
	}
}

func (b *InMemoryBackend) keepsAckedLocked(s *subscription) bool {
	if s.pb.GetRetainAckedMessages() {
		return true
	}

	topicName := s.pb.GetTopic()
	if t, ok := b.topics[topicName]; ok && t.pb.GetMessageRetentionDuration() != nil {
		return true
	}

	for _, sn := range b.snaps {
		if sn.pb.GetTopic() == topicName {
			return true
		}
	}

	return false
}

func (b *InMemoryBackend) compactLocked(s *subscription, now time.Time) {
	retention := defaultRetention
	if d := s.pb.GetMessageRetentionDuration(); d != nil {
		retention = d.AsDuration()
	}

	cutoff := now.Add(-retention)
	keepAcked := b.keepsAckedLocked(s)

	expiredFront := len(s.msgs) > 0 && s.msgs[0].Msg.PublishTime.Before(cutoff)
	ackedDue := !keepAcked && s.acked > 0 && (s.acked >= compactMinAcked || s.acked*2 >= len(s.msgs))

	if !expiredFront && !ackedDue {
		return
	}

	kept := s.msgs[:0]
	s.acked = 0

	for _, m := range s.msgs {
		drop := m.Msg.PublishTime.Before(cutoff) || (m.Acked && !keepAcked)
		if drop {
			if m.ackID != "" {
				delete(s.acks, m.ackID)
			}

			continue
		}

		if m.Acked {
			s.acked++
		}

		kept = append(kept, m)
	}

	clear(s.msgs[len(kept):])
	s.msgs = kept
}

type leaseReq struct {
	owner    string
	deadline time.Duration
	maxBytes int64
	maxMsgs  int
}

type scanState struct {
	blocked map[string]struct{}
	dl      *pubsubpb.DeadLetterPolicy
	ordered bool
}

func (st *scanState) hold(key string) {
	if st.ordered && key != "" {
		st.blocked[key] = struct{}{}
	}
}

func (st *scanState) held(key string) bool {
	_, ok := st.blocked[key]

	return st.ordered && key != "" && ok
}

func (r *leaseResult) wakeAt(t time.Time) {
	if r.nextWake.IsZero() || t.Before(r.nextWake) {
		r.nextWake = t
	}
}

// leaseLocked hands out up to rq.maxMsgs deliverable messages, honouring ordering keys,
// retry backoff and the dead-letter policy.
func (b *InMemoryBackend) leaseLocked(s *subscription, rq leaseReq) leaseResult {
	now := b.now()
	s.lastActive = now
	s.expireLeases(now)
	b.compactLocked(s, now)

	res := leaseResult{wake: s.wake}

	for _, m := range s.acks {
		res.wakeAt(m.leaseUntil)
	}

	st := &scanState{
		ordered: s.pb.GetEnableMessageOrdering(),
		blocked: make(map[string]struct{}),
		dl:      s.pb.GetDeadLetterPolicy(),
	}

	var bytes int64

	for _, m := range s.msgs {
		if m.Acked || st.held(m.Msg.OrderingKey) || b.skipForNow(s, m, now, st, &res) {
			continue
		}

		if len(res.msgs) >= rq.maxMsgs || (rq.maxBytes > 0 && bytes > 0 && bytes+msgSize(m.Msg) > rq.maxBytes) {
			break
		}

		bytes += msgSize(m.Msg)
		res.msgs = append(res.msgs, s.lease(m, now, rq.deadline, rq.owner, st.dl != nil))
		st.hold(m.Msg.OrderingKey)
	}

	return res
}

// skipForNow reports whether m cannot be delivered now: it is leased, backing off or dead-lettered.
func (b *InMemoryBackend) skipForNow(s *subscription, m *subMsg, now time.Time, st *scanState, res *leaseResult) bool {
	key := m.Msg.OrderingKey

	switch {
	case m.ackID != "":
		st.hold(key)

		return true
	case m.NotBefore.After(now):
		st.hold(key)
		res.wakeAt(m.NotBefore)

		return true
	case st.dl != nil && m.Attempts >= st.dl.GetMaxDeliveryAttempts():
		b.deadLetterLocked(s, m)

		return true
	default:
		return false
	}
}

func (s *subscription) lease(
	m *subMsg,
	now time.Time,
	deadline time.Duration,
	owner string,
	withAttempt bool,
) *pubsubpb.ReceivedMessage {
	m.Attempts++
	m.ackID = awsid.NewN("", ackIDLen)
	m.owner = owner
	m.leaseUntil = now.Add(deadline)
	s.acks[m.ackID] = m

	rm := &pubsubpb.ReceivedMessage{
		AckId: m.ackID,
		Message: &pubsubpb.PubsubMessage{
			Data:        m.Msg.Data,
			Attributes:  m.Msg.Attrs,
			MessageId:   m.Msg.ID,
			PublishTime: timestamppb.New(m.Msg.PublishTime),
			OrderingKey: m.Msg.OrderingKey,
		},
	}
	if withAttempt {
		rm.DeliveryAttempt = m.Attempts
	}

	return rm
}

func (b *InMemoryBackend) deadLetterLocked(s *subscription, m *subMsg) {
	m.Acked = true
	s.acked++

	t, ok := b.topics[s.pb.GetDeadLetterPolicy().GetDeadLetterTopic()]
	if !ok {
		return
	}

	attrs := maps.Clone(m.Msg.Attrs)
	if attrs == nil {
		attrs = make(map[string]string, 4) //nolint:mnd // four dead-letter attributes
	}

	attrs[dlqAttrCount] = strconv.Itoa(int(m.Attempts))
	attrs[dlqAttrSub] = s.pb.GetName()
	attrs[dlqAttrProject] = projectOf(s.pb.GetName())
	attrs[dlqAttrTime] = m.Msg.PublishTime.UTC().Format(time.RFC3339Nano)

	dm := &message{
		ID:          b.nextMsgID(),
		Data:        m.Msg.Data,
		Attrs:       attrs,
		OrderingKey: m.Msg.OrderingKey,
		PublishTime: b.now(),
	}

	b.fanOutLocked(s.pb.GetDeadLetterPolicy().GetDeadLetterTopic(), t, []*message{dm})
}

// Acknowledge acknowledges messages; ids that are not currently leased are returned as invalid.
func (b *InMemoryBackend) Acknowledge(name string, ids []string) ([]string, []string, error) {
	b.mu.Lock("Acknowledge")
	defer b.mu.Unlock()

	s, err := b.liveSubLocked(name)
	if err != nil {
		return nil, nil, err
	}

	now := b.now()
	s.lastActive = now
	strict := s.pb.GetEnableExactlyOnceDelivery()

	var acked, invalid []string

	for _, id := range ids {
		m, ok := s.acks[id]
		if !ok || (strict && !m.leaseUntil.After(now)) {
			invalid = append(invalid, id)

			continue
		}

		delete(s.acks, id)
		m.ackID, m.owner, m.leaseUntil = "", "", time.Time{}
		m.Acked = true
		s.acked++

		acked = append(acked, id)
	}

	if len(acked) > 0 {
		b.compactLocked(s, now)
		b.notifyLocked(s)
		b.signalPush()
	}

	return acked, invalid, nil
}

// ModifyAckDeadline extends (or, with zero seconds, releases) the leases of the given ack ids.
func (b *InMemoryBackend) ModifyAckDeadline(name string, ids []string, seconds []int32) ([]string, []string, error) {
	if len(seconds) == 0 {
		return nil, nil, errInvalid("ack deadline seconds must be provided")
	}

	b.mu.Lock("ModifyAckDeadline")
	defer b.mu.Unlock()

	s, err := b.liveSubLocked(name)
	if err != nil {
		return nil, nil, err
	}

	now := b.now()
	s.lastActive = now
	strict := s.pb.GetEnableExactlyOnceDelivery()
	released := false

	var ok, invalid []string

	for i, id := range ids {
		m, found := s.acks[id]
		if !found || (strict && !m.leaseUntil.After(now)) {
			invalid = append(invalid, id)

			continue
		}

		sec := seconds[min(i, len(seconds)-1)]
		if sec == 0 {
			s.release(m, now)

			released = true
		} else {
			m.leaseUntil = now.Add(time.Duration(sec) * time.Second)
		}

		ok = append(ok, id)
	}

	if released {
		b.notifyLocked(s)
		b.signalPush()
	}

	return ok, invalid, nil
}

// Seek rewinds a subscription to a point in time or to a snapshot.
func (b *InMemoryBackend) Seek(req *pubsubpb.SeekRequest) error {
	b.mu.Lock("Seek")
	defer b.mu.Unlock()

	s, err := b.liveSubLocked(req.GetSubscription())
	if err != nil {
		return err
	}

	now := b.now()
	s.lastActive = now

	var unackedAt func(m *message) bool

	switch target := req.GetTarget().(type) {
	case *pubsubpb.SeekRequest_Time:
		at := target.Time.AsTime()
		unackedAt = func(m *message) bool { return !m.PublishTime.Before(at) }

		b.mergeRetainedLocked(s, at)
	case *pubsubpb.SeekRequest_Snapshot:
		sn, snErr := b.liveSnapLocked(target.Snapshot)
		if snErr != nil {
			return snErr
		}

		if sn.pb.GetTopic() != s.pb.GetTopic() {
			return errInvalid(
				"snapshot %s belongs to a different topic than subscription %s",
				target.Snapshot,
				s.pb.GetName(),
			)
		}

		unackedAt = func(m *message) bool {
			_, was := sn.unacked[m.ID]

			return was || m.PublishTime.After(sn.created)
		}
	default:
		return errInvalid("seek requires a time or a snapshot target")
	}

	s.acks = make(map[string]*subMsg)
	s.acked = 0

	for _, m := range s.msgs {
		m.ackID, m.owner, m.leaseUntil = "", "", time.Time{}
		m.NotBefore = time.Time{}
		m.Attempts = 0
		m.Acked = !unackedAt(m.Msg)

		if m.Acked {
			s.acked++
		}
	}

	b.notifyLocked(s)
	b.signalPush()

	return nil
}

func (b *InMemoryBackend) mergeRetainedLocked(s *subscription, at time.Time) {
	t, ok := b.topics[s.pb.GetTopic()]
	if !ok {
		return
	}

	have := make(map[string]struct{}, len(s.msgs))
	for _, m := range s.msgs {
		have[m.Msg.ID] = struct{}{}
	}

	var add []*subMsg

	for _, m := range t.retained {
		if _, dup := have[m.ID]; dup || m.PublishTime.Before(at) {
			continue
		}

		if s.filter != nil && !s.filter.eval(m.Attrs) {
			continue
		}

		add = append(add, &subMsg{Msg: m})
	}

	if len(add) == 0 {
		return
	}

	s.msgs = append(s.msgs, add...)

	slices.SortStableFunc(s.msgs, func(a, c *subMsg) int {
		if d := a.Msg.PublishTime.Compare(c.Msg.PublishTime); d != 0 {
			return d
		}

		return cmp.Or(cmp.Compare(len(a.Msg.ID), len(c.Msg.ID)), strings.Compare(a.Msg.ID, c.Msg.ID))
	})
}

// pull leases up to maxMsgs messages without waiting.
func (b *InMemoryBackend) pull(name string, maxMsgs int) (leaseResult, error) {
	b.mu.Lock("Pull")
	defer b.mu.Unlock()

	s, err := b.liveSubLocked(name)
	if err != nil {
		return leaseResult{}, err
	}

	if s.pb.GetPushConfig().GetPushEndpoint() != "" {
		return leaseResult{}, errPrecondition("subscription %s is a push subscription", name)
	}

	return b.leaseLocked(s, leaseReq{
		maxMsgs: maxMsgs, deadline: time.Duration(s.pb.GetAckDeadlineSeconds()) * time.Second,
	}), nil
}

// streamLease leases messages for a StreamingPull stream within its flow-control budget.
func (b *InMemoryBackend) streamLease(
	name, owner string,
	deadline time.Duration,
	maxOutstanding, maxBytes int64,
) (leaseResult, error) {
	b.mu.Lock("StreamLease")
	defer b.mu.Unlock()

	s, err := b.liveSubLocked(name)
	if err != nil {
		return leaseResult{}, err
	}

	if s.pb.GetPushConfig().GetPushEndpoint() != "" {
		return leaseResult{}, errPrecondition("subscription %s is a push subscription", name)
	}

	budget, byteBudget := s.streamBudget(owner, maxOutstanding, maxBytes)
	if budget <= 0 {
		s.expireLeases(b.now())

		return leaseResult{wake: s.wake}, nil
	}

	return b.leaseLocked(s, leaseReq{maxMsgs: int(budget), maxBytes: byteBudget, deadline: deadline, owner: owner}), nil
}

// streamBudget returns how many messages and bytes a stream may still receive under its flow control.
func (s *subscription) streamBudget(owner string, maxOutstanding, maxBytes int64) (int64, int64) {
	budget, byteBudget := int64(maxPublishBatch), maxBytes

	if maxOutstanding <= 0 && maxBytes <= 0 {
		return budget, byteBudget
	}

	var out, outBytes int64

	for _, m := range s.acks {
		if m.owner == owner {
			out++
			outBytes += msgSize(m.Msg)
		}
	}

	if maxOutstanding > 0 {
		budget = min(budget, maxOutstanding-out)
	}

	if maxBytes > 0 {
		byteBudget = maxBytes - outBytes
		if byteBudget <= 0 {
			budget = 0
		}
	}

	return budget, byteBudget
}

type subProps struct {
	ackDeadline time.Duration
	exactlyOnce bool
	ordering    bool
}

// subProps reports the exactly-once and ordering flags of a subscription.
func (b *InMemoryBackend) subProps(name string) (subProps, error) {
	b.mu.RLock("SubProps")
	defer b.mu.RUnlock()

	s, err := b.liveSubLocked(name)
	if err != nil {
		return subProps{}, err
	}

	return subProps{
		exactlyOnce: s.pb.GetEnableExactlyOnceDelivery(), ordering: s.pb.GetEnableMessageOrdering(),
		ackDeadline: time.Duration(s.pb.GetAckDeadlineSeconds()) * time.Second,
	}, nil
}

func (b *InMemoryBackend) strictAcks(name string) bool {
	b.mu.RLock("StrictAcks")
	defer b.mu.RUnlock()

	s, ok := b.subs[name]

	return ok && s.pb.GetEnableExactlyOnceDelivery()
}
