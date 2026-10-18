package gcppubsub

import (
	"slices"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// InMemoryBackend holds all topics, subscriptions, snapshots and schemas under one lock.
type InMemoryBackend struct {
	now      func() time.Time
	mu       *lockmetrics.RWMutex
	topics   map[string]*topic
	subs     map[string]*subscription
	snaps    map[string]*snapshot
	schemas  map[string]*schemaRec
	pushWake chan struct{}
	lastMsg  int64
}

// NewInMemoryBackend creates an empty backend.
func NewInMemoryBackend() *InMemoryBackend {
	b := &InMemoryBackend{
		mu:       lockmetrics.New("gcppubsub"),
		now:      time.Now,
		pushWake: make(chan struct{}, 1),
	}
	b.resetMaps()

	return b
}

func (b *InMemoryBackend) resetMaps() {
	b.topics = make(map[string]*topic)
	b.subs = make(map[string]*subscription)
	b.snaps = make(map[string]*snapshot)
	b.schemas = make(map[string]*schemaRec)
}

// Reset clears all state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	for _, s := range b.subs {
		b.notifyLocked(s)
	}

	b.resetMaps()
	b.lastMsg = 0
}

func (b *InMemoryBackend) nextMsgID() string {
	id := b.now().UnixMicro()
	if id <= b.lastMsg {
		id = b.lastMsg + 1
	}

	b.lastMsg = id

	return strconv.FormatInt(id, 10)
}

func (b *InMemoryBackend) notifyLocked(s *subscription) {
	close(s.wake)
	s.wake = make(chan struct{})
}

func (b *InMemoryBackend) signalPush() {
	select {
	case b.pushWake <- struct{}{}:
	default:
	}
}

const fieldLabels = "labels"

func clone[T proto.Message](m T) T {
	c, ok := proto.Clone(m).(T)
	if !ok {
		var zero T

		return zero
	}

	return c
}

func paginate[T any](all []T, token string, size int32) (page.Page[T], error) {
	if err := page.ValidateToken(token); err != nil {
		return page.Page[T]{}, errInvalid("invalid page token")
	}

	limit := min(int(size), maxPageSize)

	return page.New(all, token, limit, defaultPageSize), nil
}

func applyMask(dst, src proto.Message, paths []string, mutable func(string) bool) error {
	if len(paths) == 0 {
		return errInvalid("update_mask must be provided and non-empty")
	}

	d, s := dst.ProtoReflect(), src.ProtoReflect()

	for _, p := range paths {
		field, _, _ := strings.Cut(p, ".")
		if !mutable(field) {
			return errInvalid("Invalid update_mask path %q: field is unknown or immutable", p)
		}

		fd := d.Descriptor().Fields().ByName(protoreflect.Name(field))
		if fd == nil {
			return errInvalid("Invalid update_mask path %q", p)
		}

		if s.Has(fd) {
			d.Set(fd, s.Get(fd))
		} else {
			d.Clear(fd)
		}
	}

	return nil
}

func checkLabels(labels map[string]string) error {
	if len(labels) > maxLabels {
		return errInvalid("too many labels: at most %d are allowed", maxLabels)
	}

	return nil
}

func durationIn(d *durationpb.Duration, lo, hi time.Duration, what string) error {
	if d == nil {
		return nil
	}

	if v := d.AsDuration(); v < lo || v > hi {
		return errInvalid("%s must be between %s and %s", what, lo, hi)
	}

	return nil
}

// CreateTopic stores a new topic.
func (b *InMemoryBackend) CreateTopic(in *pubsubpb.Topic) (*pubsubpb.Topic, error) {
	if err := checkName(in.GetName(), "topics"); err != nil {
		return nil, err
	}

	if err := b.checkTopicFields(in); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateTopic")
	defer b.mu.Unlock()

	if _, ok := b.topics[in.GetName()]; ok {
		return nil, errExists("topic", in.GetName())
	}

	if err := b.checkSchemaSettingsLocked(in.GetSchemaSettings()); err != nil {
		return nil, err
	}

	t := clone(in)
	b.topics[in.GetName()] = &topic{pb: t}

	return clone(t), nil
}

func (b *InMemoryBackend) checkTopicFields(in *pubsubpb.Topic) error {
	if err := checkLabels(in.GetLabels()); err != nil {
		return err
	}

	if in.GetIngestionDataSourceSettings() != nil {
		return errUnsupported("ingestion_data_source_settings is not supported")
	}

	if len(in.GetMessageTransforms()) > 0 {
		return errUnsupported("message_transforms are not supported")
	}

	return durationIn(in.GetMessageRetentionDuration(), minTopicRetention, maxRetention, "message_retention_duration")
}

func (b *InMemoryBackend) checkSchemaSettingsLocked(ss *pubsubpb.SchemaSettings) error {
	if ss == nil || ss.GetSchema() == "" || ss.GetSchema() == deletedSchema {
		return nil
	}

	if ss.GetEncoding() == pubsubpb.Encoding_ENCODING_UNSPECIFIED {
		return errInvalid("schema_settings.encoding must be JSON or BINARY")
	}

	if _, err := b.resolveSchemaLocked(ss.GetSchema()); err != nil {
		return err
	}

	for _, rev := range []string{ss.GetFirstRevisionId(), ss.GetLastRevisionId()} {
		if rev == "" {
			continue
		}

		if _, err := b.resolveSchemaLocked(ss.GetSchema() + "@" + rev); err != nil {
			return err
		}
	}

	return nil
}

// GetTopic returns a topic.
func (b *InMemoryBackend) GetTopic(name string) (*pubsubpb.Topic, error) {
	b.mu.RLock("GetTopic")
	defer b.mu.RUnlock()

	t, ok := b.topics[name]
	if !ok {
		return nil, errNotFound("topics", name)
	}

	return clone(t.pb), nil
}

func topicMutable(field string) bool {
	switch field {
	case fieldLabels, "message_storage_policy", "kms_key_name", "schema_settings",
		"message_retention_duration", "satisfies_pzs", "tags":
		return true
	default:
		return false
	}
}

// UpdateTopic applies a masked update.
func (b *InMemoryBackend) UpdateTopic(req *pubsubpb.UpdateTopicRequest) (*pubsubpb.Topic, error) {
	in := req.GetTopic()
	if in == nil {
		return nil, errInvalid("topic must be provided")
	}

	b.mu.Lock("UpdateTopic")
	defer b.mu.Unlock()

	t, ok := b.topics[in.GetName()]
	if !ok {
		return nil, errNotFound("topics", in.GetName())
	}

	next := clone(t.pb)

	src := clone(in)
	if err := applyMask(next, src, req.GetUpdateMask().GetPaths(), topicMutable); err != nil {
		return nil, err
	}

	if err := b.checkTopicFields(next); err != nil {
		return nil, err
	}

	if err := b.checkSchemaSettingsLocked(next.GetSchemaSettings()); err != nil {
		return nil, err
	}

	t.pb = next

	return clone(next), nil
}

// ListTopics lists the topics of a project.
func (b *InMemoryBackend) ListTopics(req *pubsubpb.ListTopicsRequest) ([]*pubsubpb.Topic, string, error) {
	if err := checkProject(req.GetProject()); err != nil {
		return nil, "", err
	}

	b.mu.RLock("ListTopics")
	defer b.mu.RUnlock()

	prefix := req.GetProject() + "/topics/"

	var all []*pubsubpb.Topic

	for name, t := range b.topics {
		if strings.HasPrefix(name, prefix) {
			all = append(all, clone(t.pb))
		}
	}

	slices.SortFunc(all, func(a, c *pubsubpb.Topic) int { return strings.Compare(a.GetName(), c.GetName()) })

	pg, err := paginate(all, req.GetPageToken(), req.GetPageSize())
	if err != nil {
		return nil, "", err
	}

	return pg.Data, pg.Next, nil
}

// DeleteTopic removes a topic; its subscriptions remain, pointing at the deleted-topic marker.
func (b *InMemoryBackend) DeleteTopic(name string) error {
	b.mu.Lock("DeleteTopic")
	defer b.mu.Unlock()

	if _, ok := b.topics[name]; !ok {
		return errNotFound("topics", name)
	}

	delete(b.topics, name)

	for _, s := range b.subs {
		if s.pb.GetTopic() == name {
			s.pb.Topic = deletedTopic
		}
	}

	for n, sn := range b.snaps {
		if sn.pb.GetTopic() == name {
			delete(b.snaps, n)
		}
	}

	return nil
}

// ListTopicSubscriptions lists the attached subscriptions of a topic.
func (b *InMemoryBackend) ListTopicSubscriptions(
	req *pubsubpb.ListTopicSubscriptionsRequest,
) ([]string, string, error) {
	b.mu.RLock("ListTopicSubscriptions")
	defer b.mu.RUnlock()

	if _, ok := b.topics[req.GetTopic()]; !ok {
		return nil, "", errNotFound("topics", req.GetTopic())
	}

	now := b.now()

	var all []string

	for name, s := range b.subs {
		if s.pb.GetTopic() == req.GetTopic() && !s.pb.GetDetached() && !s.expired(now) {
			all = append(all, name)
		}
	}

	slices.Sort(all)

	pg, err := paginate(all, req.GetPageToken(), req.GetPageSize())
	if err != nil {
		return nil, "", err
	}

	return pg.Data, pg.Next, nil
}

// ListTopicSnapshots lists the snapshots of a topic.
func (b *InMemoryBackend) ListTopicSnapshots(req *pubsubpb.ListTopicSnapshotsRequest) ([]string, string, error) {
	b.mu.RLock("ListTopicSnapshots")
	defer b.mu.RUnlock()

	if _, ok := b.topics[req.GetTopic()]; !ok {
		return nil, "", errNotFound("topics", req.GetTopic())
	}

	now := b.now()

	var all []string

	for name, sn := range b.snaps {
		if sn.pb.GetTopic() == req.GetTopic() && now.Before(sn.pb.GetExpireTime().AsTime()) {
			all = append(all, name)
		}
	}

	slices.Sort(all)

	pg, err := paginate(all, req.GetPageToken(), req.GetPageSize())
	if err != nil {
		return nil, "", err
	}

	return pg.Data, pg.Next, nil
}

// DetachSubscription detaches a subscription from its topic.
func (b *InMemoryBackend) DetachSubscription(name string) error {
	b.mu.Lock("DetachSubscription")
	defer b.mu.Unlock()

	s, ok := b.subs[name]
	if !ok || s.expired(b.now()) {
		return errNotFound("subscriptions", name)
	}

	s.pb.Detached = true

	return nil
}

func (s *subscription) expired(now time.Time) bool {
	ttl := defaultExpiration

	if ep := s.pb.GetExpirationPolicy(); ep != nil {
		ttl = ep.GetTtl().AsDuration()
		if ep.GetTtl() == nil || ttl == 0 {
			return false
		}
	}

	return now.After(s.lastActive.Add(ttl))
}
