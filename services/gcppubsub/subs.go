package gcppubsub

import (
	"net/url"
	"slices"
	"strings"

	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func cloneSub(s *pubsubpb.Subscription) *pubsubpb.Subscription { return clone(s) }

func subMutable(field string) bool {
	switch field {
	case "push_config", "ack_deadline_seconds", "retain_acked_messages", "message_retention_duration",
		fieldLabels, "expiration_policy", "dead_letter_policy", "retry_policy", "enable_exactly_once_delivery", "tags":
		return true
	default:
		return false
	}
}

// validateSubLocked checks the user-settable fields of a subscription.
func (b *InMemoryBackend) validateSubLocked(sub *pubsubpb.Subscription) error {
	if err := checkLabels(sub.GetLabels()); err != nil {
		return err
	}

	if sub.GetBigqueryConfig() != nil || sub.GetCloudStorageConfig() != nil || sub.GetBigtableConfig() != nil {
		return errUnsupported("BigQuery, Cloud Storage and Bigtable subscriptions are not supported")
	}

	if len(sub.GetMessageTransforms()) > 0 {
		return errUnsupported("message_transforms are not supported")
	}

	if err := b.validateDeliverySettings(sub); err != nil {
		return err
	}

	if err := durationIn(
		sub.GetMessageRetentionDuration(),
		minSubRetention,
		maxRetention,
		"message_retention_duration",
	); err != nil {
		return err
	}

	if ep := sub.GetExpirationPolicy(); ep != nil && ep.GetTtl() != nil && ep.GetTtl().AsDuration() != 0 &&
		ep.GetTtl().AsDuration() < minExpiration {
		return errInvalid("expiration_policy.ttl must be at least %s or 0", minExpiration)
	}

	return validateRetryPolicy(sub.GetRetryPolicy())
}

func validateRetryPolicy(rp *pubsubpb.RetryPolicy) error {
	if rp == nil {
		return nil
	}

	if err := durationIn(rp.GetMinimumBackoff(), 0, maxRetryBackoff, "retry_policy.minimum_backoff"); err != nil {
		return err
	}

	if err := durationIn(rp.GetMaximumBackoff(), 0, maxRetryBackoff, "retry_policy.maximum_backoff"); err != nil {
		return err
	}

	if rp.GetMinimumBackoff() != nil && rp.GetMaximumBackoff() != nil &&
		rp.GetMinimumBackoff().AsDuration() > rp.GetMaximumBackoff().AsDuration() {
		return errInvalid("retry_policy.minimum_backoff must not exceed maximum_backoff")
	}

	return nil
}

func (b *InMemoryBackend) validateDeliverySettings(sub *pubsubpb.Subscription) error {
	if d := sub.GetAckDeadlineSeconds(); d != 0 && (d < minAckDeadline || d > maxAckDeadline) {
		return errInvalid("ack_deadline_seconds must be between %d and %d", minAckDeadline, maxAckDeadline)
	}

	if err := b.validateDeadLetter(sub.GetDeadLetterPolicy()); err != nil {
		return err
	}

	if pc := sub.GetPushConfig(); pc != nil && pc.GetPushEndpoint() != "" {
		u, err := url.Parse(pc.GetPushEndpoint())
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return errInvalid("push_config.push_endpoint must be an http(s) URL")
		}
	}

	return nil
}

func (b *InMemoryBackend) validateDeadLetter(dl *pubsubpb.DeadLetterPolicy) error {
	if dl == nil {
		return nil
	}

	if err := checkName(dl.GetDeadLetterTopic(), "topics"); err != nil {
		return err
	}

	if n := dl.GetMaxDeliveryAttempts(); n != 0 && (n < minDeliveryAttempts || n > maxDeliveryAttempts) {
		return errInvalid("dead_letter_policy.max_delivery_attempts must be between %d and %d",
			minDeliveryAttempts, maxDeliveryAttempts)
	}

	if _, ok := b.topics[dl.GetDeadLetterTopic()]; !ok {
		return errNotFound("topics", dl.GetDeadLetterTopic())
	}

	return nil
}

func normalizeSub(sub *pubsubpb.Subscription) {
	if sub.GetAckDeadlineSeconds() == 0 {
		sub.AckDeadlineSeconds = defaultAckDeadline
	}

	if sub.GetMessageRetentionDuration() == nil {
		sub.MessageRetentionDuration = durationpb.New(defaultRetention)
	}

	if dl := sub.GetDeadLetterPolicy(); dl != nil && dl.GetMaxDeliveryAttempts() == 0 {
		dl.MaxDeliveryAttempts = minDeliveryAttempts
	}

	if sub.GetExpirationPolicy() == nil {
		sub.ExpirationPolicy = &pubsubpb.ExpirationPolicy{Ttl: durationpb.New(defaultExpiration)}
	}

	sub.State = pubsubpb.Subscription_ACTIVE
}

func (b *InMemoryBackend) viewSubLocked(s *subscription) *pubsubpb.Subscription {
	out := cloneSub(s.pb)

	if t, ok := b.topics[out.GetTopic()]; ok {
		out.TopicMessageRetentionDuration = t.pb.GetMessageRetentionDuration()
	}

	return out
}

func (b *InMemoryBackend) liveSubLocked(name string) (*subscription, error) {
	s, ok := b.subs[name]
	if !ok || s.expired(b.now()) {
		return nil, errNotFound("subscriptions", name)
	}

	return s, nil
}

func (b *InMemoryBackend) sweepSubsLocked() {
	now := b.now()

	for name, s := range b.subs {
		if s.expired(now) {
			b.notifyLocked(s)
			delete(b.subs, name)
			b.dropPolicyLocked(name)
		}
	}
}

// CreateSubscription stores a new subscription.
func (b *InMemoryBackend) CreateSubscription(in *pubsubpb.Subscription) (*pubsubpb.Subscription, error) {
	if err := checkName(in.GetName(), "subscriptions"); err != nil {
		return nil, err
	}

	if err := checkName(in.GetTopic(), "topics"); err != nil && in.GetTopic() != deletedTopic {
		return nil, err
	}

	flt, err := compileFilter(in.GetFilter())
	if err != nil {
		return nil, err
	}

	sub := cloneSub(in)

	b.mu.Lock("CreateSubscription")
	defer b.mu.Unlock()

	b.sweepSubsLocked()

	if _, ok := b.subs[sub.GetName()]; ok {
		return nil, errExists("subscription", sub.GetName())
	}

	if _, ok := b.topics[sub.GetTopic()]; !ok {
		return nil, errNotFound("topics", sub.GetTopic())
	}

	if err = b.validateSubLocked(sub); err != nil {
		return nil, err
	}

	normalizeSub(sub)
	sub.Detached = false
	sub.TopicMessageRetentionDuration = nil

	s := &subscription{
		pb:         sub,
		filter:     flt,
		acks:       make(map[string]*subMsg),
		wake:       make(chan struct{}),
		lastActive: b.now(),
	}
	b.dropPolicyLocked(sub.GetName())
	b.subs[sub.GetName()] = s

	if sub.GetPushConfig().GetPushEndpoint() != "" {
		b.signalPush()
	}

	return b.viewSubLocked(s), nil
}

// GetSubscription returns a subscription.
func (b *InMemoryBackend) GetSubscription(name string) (*pubsubpb.Subscription, error) {
	b.mu.RLock("GetSubscription")
	defer b.mu.RUnlock()

	s, err := b.liveSubLocked(name)
	if err != nil {
		return nil, err
	}

	return b.viewSubLocked(s), nil
}

// UpdateSubscription applies a masked update.
func (b *InMemoryBackend) UpdateSubscription(req *pubsubpb.UpdateSubscriptionRequest) (*pubsubpb.Subscription, error) {
	in := req.GetSubscription()
	if in == nil {
		return nil, errInvalid("subscription must be provided")
	}

	b.mu.Lock("UpdateSubscription")
	defer b.mu.Unlock()

	s, err := b.liveSubLocked(in.GetName())
	if err != nil {
		return nil, err
	}

	next := cloneSub(s.pb)

	if err = applyMask(next, cloneSub(in), req.GetUpdateMask().GetPaths(), subMutable); err != nil {
		return nil, err
	}

	if err = b.validateSubLocked(next); err != nil {
		return nil, err
	}

	normalizeSub(next)

	s.pb = next
	s.lastActive = b.now()
	b.notifyLocked(s)
	b.signalPush()

	return b.viewSubLocked(s), nil
}

// ListSubscriptions lists the subscriptions of a project.
func (b *InMemoryBackend) ListSubscriptions(
	req *pubsubpb.ListSubscriptionsRequest,
) ([]*pubsubpb.Subscription, string, error) {
	if err := checkProject(req.GetProject()); err != nil {
		return nil, "", err
	}

	b.mu.RLock("ListSubscriptions")
	defer b.mu.RUnlock()

	prefix := req.GetProject() + "/subscriptions/"
	now := b.now()

	var all []*pubsubpb.Subscription

	for name, s := range b.subs {
		if strings.HasPrefix(name, prefix) && !s.expired(now) {
			all = append(all, b.viewSubLocked(s))
		}
	}

	slices.SortFunc(all, func(a, c *pubsubpb.Subscription) int { return strings.Compare(a.GetName(), c.GetName()) })

	pg, err := paginate(all, req.GetPageToken(), req.GetPageSize())
	if err != nil {
		return nil, "", err
	}

	return pg.Data, pg.Next, nil
}

// DeleteSubscription removes a subscription.
func (b *InMemoryBackend) DeleteSubscription(name string) error {
	b.mu.Lock("DeleteSubscription")
	defer b.mu.Unlock()

	s, err := b.liveSubLocked(name)
	if err != nil {
		return err
	}

	b.notifyLocked(s)
	delete(b.subs, name)
	b.dropPolicyLocked(name)

	return nil
}

// ModifyPushConfig switches a subscription between pull and push delivery.
func (b *InMemoryBackend) ModifyPushConfig(name string, pc *pubsubpb.PushConfig) error {
	b.mu.Lock("ModifyPushConfig")
	defer b.mu.Unlock()

	s, err := b.liveSubLocked(name)
	if err != nil {
		return err
	}

	next := cloneSub(s.pb)
	next.PushConfig = clone(pc)

	if err = b.validateDeliverySettings(next); err != nil {
		return err
	}

	s.pb = next
	s.lastActive = b.now()
	b.notifyLocked(s)
	b.signalPush()

	return nil
}

// CreateSnapshot captures a subscription's unacknowledged messages.
func (b *InMemoryBackend) CreateSnapshot(req *pubsubpb.CreateSnapshotRequest) (*pubsubpb.Snapshot, error) {
	if err := checkName(req.GetName(), "snapshots"); err != nil {
		return nil, err
	}

	if err := checkLabels(req.GetLabels()); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateSnapshot")
	defer b.mu.Unlock()

	s, err := b.liveSubLocked(req.GetSubscription())
	if err != nil {
		return nil, err
	}

	if _, ok := b.snaps[req.GetName()]; ok {
		return nil, errExists("snapshot", req.GetName())
	}

	if s.pb.GetTopic() == deletedTopic {
		return nil, errPrecondition("the subscription's topic has been deleted")
	}

	now := b.now()
	unacked := make(map[string]struct{})

	for _, m := range s.msgs {
		if !m.Acked {
			unacked[m.Msg.ID] = struct{}{}
		}
	}

	sn := &snapshot{
		created: now,
		unacked: unacked,
		pb: &pubsubpb.Snapshot{
			Name:       req.GetName(),
			Topic:      s.pb.GetTopic(),
			Labels:     req.GetLabels(),
			ExpireTime: timestamppb.New(now.Add(snapshotLifetime)),
		},
	}
	b.dropPolicyLocked(req.GetName())
	b.snaps[req.GetName()] = sn

	return clone(sn.pb), nil
}

func (b *InMemoryBackend) liveSnapLocked(name string) (*snapshot, error) {
	sn, ok := b.snaps[name]
	if !ok || !b.now().Before(sn.pb.GetExpireTime().AsTime()) {
		return nil, errNotFound("snapshots", name)
	}

	return sn, nil
}

// GetSnapshot returns a snapshot.
func (b *InMemoryBackend) GetSnapshot(name string) (*pubsubpb.Snapshot, error) {
	b.mu.RLock("GetSnapshot")
	defer b.mu.RUnlock()

	sn, err := b.liveSnapLocked(name)
	if err != nil {
		return nil, err
	}

	return clone(sn.pb), nil
}

// ListSnapshots lists the snapshots of a project.
func (b *InMemoryBackend) ListSnapshots(req *pubsubpb.ListSnapshotsRequest) ([]*pubsubpb.Snapshot, string, error) {
	if err := checkProject(req.GetProject()); err != nil {
		return nil, "", err
	}

	b.mu.RLock("ListSnapshots")
	defer b.mu.RUnlock()

	prefix := req.GetProject() + "/snapshots/"
	now := b.now()

	var all []*pubsubpb.Snapshot

	for name, sn := range b.snaps {
		if strings.HasPrefix(name, prefix) && now.Before(sn.pb.GetExpireTime().AsTime()) {
			all = append(all, clone(sn.pb))
		}
	}

	slices.SortFunc(all, func(a, c *pubsubpb.Snapshot) int { return strings.Compare(a.GetName(), c.GetName()) })

	pg, err := paginate(all, req.GetPageToken(), req.GetPageSize())
	if err != nil {
		return nil, "", err
	}

	return pg.Data, pg.Next, nil
}

// UpdateSnapshot applies a masked update (labels, expire_time).
func (b *InMemoryBackend) UpdateSnapshot(req *pubsubpb.UpdateSnapshotRequest) (*pubsubpb.Snapshot, error) {
	in := req.GetSnapshot()
	if in == nil {
		return nil, errInvalid("snapshot must be provided")
	}

	b.mu.Lock("UpdateSnapshot")
	defer b.mu.Unlock()

	sn, err := b.liveSnapLocked(in.GetName())
	if err != nil {
		return nil, err
	}

	next := clone(sn.pb)

	src := clone(in)
	if err = applyMask(next, src, req.GetUpdateMask().GetPaths(), snapMutable); err != nil {
		return nil, err
	}

	if err = checkLabels(next.GetLabels()); err != nil {
		return nil, err
	}

	if next.GetExpireTime() == nil || !next.GetExpireTime().AsTime().After(b.now()) ||
		next.GetExpireTime().AsTime().After(sn.created.Add(snapshotLifetime)) {
		return nil, errInvalid("expire_time must be in the future and within %s of snapshot creation", snapshotLifetime)
	}

	sn.pb = next

	return clone(next), nil
}

func snapMutable(field string) bool { return field == fieldLabels || field == "expire_time" }

// DeleteSnapshot removes a snapshot.
func (b *InMemoryBackend) DeleteSnapshot(name string) error {
	b.mu.Lock("DeleteSnapshot")
	defer b.mu.Unlock()

	if _, err := b.liveSnapLocked(name); err != nil {
		return err
	}

	delete(b.snaps, name)
	b.dropPolicyLocked(name)

	return nil
}
