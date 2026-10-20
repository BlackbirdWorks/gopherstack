package azureeventhubs

import (
	"crypto/rand"
	"encoding/base64"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

// InMemoryBackend is the Event Hubs state shared by the ARM, REST and AMQP planes.
type InMemoryBackend struct {
	mu         *lockmetrics.RWMutex
	namespaces map[string]*namespace
	receivers  map[string][]*receiverReg
	now        func() time.Time
}

// NewInMemoryBackend returns an empty backend using the wall clock.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		mu:         lockmetrics.New("azureeventhubs"),
		namespaces: map[string]*namespace{},
		receivers:  map[string][]*receiverReg{},
		now:        time.Now,
	}
}

// Reset drops all state and disconnects every receiver.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	for key := range b.receivers {
		b.kickAllLocked(key)
	}

	b.namespaces = map[string]*namespace{}
}

func validName(n string) bool {
	if n == "" || len(n) > maxNameLen || strings.ContainsAny(n, "/\\?#") {
		return false
	}

	return strings.TrimSpace(n) == n
}

func newKey() string {
	var buf [32]byte

	_, _ = rand.Read(buf[:])

	return base64.StdEncoding.EncodeToString(buf[:])
}

func newPartition() *partition {
	return &partition{notify: make(chan struct{})}
}

func (b *InMemoryBackend) nsLocked(name string) (*namespace, error) {
	ns, ok := b.namespaces[name]
	if !ok {
		return nil, ErrNamespaceNotFound
	}

	return ns, nil
}

func (b *InMemoryBackend) hubLocked(ref HubRef) (*namespace, *hub, error) {
	ns, err := b.nsLocked(ref.Namespace)
	if err != nil {
		return nil, nil, err
	}

	h, ok := ns.Hubs[ref.Hub]
	if !ok {
		return nil, nil, ErrHubNotFound
	}

	return ns, h, nil
}

func (b *InMemoryBackend) partLocked(ref HubRef, pid int) (*hub, *partition, error) {
	_, h, err := b.hubLocked(ref)
	if err != nil {
		return nil, nil, err
	}

	if pid < 0 || pid >= len(h.Partitions) {
		return nil, nil, ErrPartitionNotFound
	}

	return h, h.Partitions[pid], nil
}

// EnsureNamespace creates the namespace (with its root rule) if absent.
func (b *InMemoryBackend) EnsureNamespace(name string) error {
	if !validName(name) {
		return ErrInvalidName
	}

	b.mu.Lock("EnsureNamespace")
	defer b.mu.Unlock()

	if _, ok := b.namespaces[name]; ok {
		return nil
	}

	root := &rule{
		Name: DefaultKeyName, PrimaryKey: DefaultKeyValue, SecondaryKey: DefaultKeyValue,
		Rights: []string{RightListen, RightSend, RightManage},
	}
	b.namespaces[name] = &namespace{
		Name: name, CreatedAt: b.now().UTC(), Hubs: map[string]*hub{}, Rules: map[string]*rule{root.Name: root},
	}

	return nil
}

// NamespaceExists reports whether the namespace exists.
func (b *InMemoryBackend) NamespaceExists(name string) bool {
	b.mu.RLock("NamespaceExists")
	defer b.mu.RUnlock()

	_, ok := b.namespaces[name]

	return ok
}

// DeleteNamespace removes a namespace and everything in it.
func (b *InMemoryBackend) DeleteNamespace(name string) error {
	b.mu.Lock("DeleteNamespace")
	defer b.mu.Unlock()

	ns, err := b.nsLocked(name)
	if err != nil {
		return err
	}

	for hubName := range ns.Hubs {
		b.dropHubReceiversLocked(HubRef{Namespace: name, Hub: hubName})
	}

	delete(b.namespaces, name)

	return nil
}

func normalizeConfig(cfg HubConfig) (HubConfig, error) {
	if cfg.PartitionCount == 0 {
		cfg.PartitionCount = DefaultPartitionCount
	}

	if cfg.Retention == 0 {
		cfg.Retention = DefaultRetention
	}

	if cfg.PartitionCount < 1 || cfg.PartitionCount > MaxPartitionCount {
		return cfg, ErrInvalidPartitionCount
	}

	if cfg.Retention < 0 || cfg.Retention > MaxRetention {
		return cfg, ErrInvalidRetention
	}

	return cfg, nil
}

// CreateHub creates or updates an event hub; the partition count may only grow.
func (b *InMemoryBackend) CreateHub(ref HubRef, cfg HubConfig) (HubInfo, error) {
	cfg, err := normalizeConfig(cfg)
	if err != nil {
		return HubInfo{}, err
	}

	if !validName(ref.Hub) {
		return HubInfo{}, ErrInvalidName
	}

	b.mu.Lock("CreateHub")
	defer b.mu.Unlock()

	ns, err := b.nsLocked(ref.Namespace)
	if err != nil {
		return HubInfo{}, err
	}

	h, exists := ns.Hubs[ref.Hub]
	if !exists {
		if len(ns.Hubs) >= maxEntitiesPerParent {
			return HubInfo{}, ErrInvalidName
		}

		h = &hub{Name: ref.Hub, CreatedAt: b.now().UTC(), ConsumerGroups: map[string]time.Time{}}
		h.ConsumerGroups[DefaultConsumerGroup] = h.CreatedAt
		ns.Hubs[ref.Hub] = h
	}

	if cfg.PartitionCount < len(h.Partitions) {
		return HubInfo{}, ErrPartitionCountShrink
	}

	for len(h.Partitions) < cfg.PartitionCount {
		h.Partitions = append(h.Partitions, newPartition())
	}

	h.Config = cfg

	return hubInfo(ref.Namespace, h), nil
}

func hubInfo(ns string, h *hub) HubInfo {
	ids := make([]string, len(h.Partitions))
	for i := range ids {
		ids[i] = strconv.Itoa(i)
	}

	return HubInfo{Name: h.Name, Namespace: ns, CreatedAt: h.CreatedAt, PartitionIDs: ids, Config: h.Config}
}

// Hub returns an event hub's description.
func (b *InMemoryBackend) Hub(ref HubRef) (HubInfo, error) {
	b.mu.RLock("Hub")
	defer b.mu.RUnlock()

	_, h, err := b.hubLocked(ref)
	if err != nil {
		return HubInfo{}, err
	}

	return hubInfo(ref.Namespace, h), nil
}

// ListHubs returns the namespace's event hubs sorted by name.
func (b *InMemoryBackend) ListHubs(nsName string) ([]HubInfo, error) {
	b.mu.RLock("ListHubs")
	defer b.mu.RUnlock()

	ns, err := b.nsLocked(nsName)
	if err != nil {
		return nil, err
	}

	out := make([]HubInfo, 0, len(ns.Hubs))
	for _, h := range ns.Hubs {
		out = append(out, hubInfo(nsName, h))
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out, nil
}

// DeleteHub removes an event hub and disconnects its receivers.
func (b *InMemoryBackend) DeleteHub(ref HubRef) error {
	b.mu.Lock("DeleteHub")
	defer b.mu.Unlock()

	ns, _, err := b.hubLocked(ref)
	if err != nil {
		return err
	}

	b.dropHubReceiversLocked(ref)
	delete(ns.Hubs, ref.Hub)

	for key, r := range ns.Rules {
		if r.Hub == ref.Hub {
			delete(ns.Rules, key)
		}
	}

	return nil
}

// ResolveHub finds the namespace owning a hub of the given name; AMQP
// addresses carry no namespace, so the first namespace by name wins.
func (b *InMemoryBackend) ResolveHub(name string) (HubRef, bool) {
	b.mu.RLock("ResolveHub")
	defer b.mu.RUnlock()

	names := make([]string, 0, len(b.namespaces))
	for n := range b.namespaces {
		names = append(names, n)
	}

	sort.Strings(names)

	for _, n := range names {
		if _, ok := b.namespaces[n].Hubs[name]; ok {
			return HubRef{Namespace: n, Hub: name}, true
		}
	}

	return HubRef{}, false
}

// CreateConsumerGroup creates a consumer group (idempotent).
func (b *InMemoryBackend) CreateConsumerGroup(ref HubRef, name string) (time.Time, error) {
	if !validName(name) {
		return time.Time{}, ErrInvalidName
	}

	b.mu.Lock("CreateConsumerGroup")
	defer b.mu.Unlock()

	_, h, err := b.hubLocked(ref)
	if err != nil {
		return time.Time{}, err
	}

	if t, ok := h.ConsumerGroups[name]; ok {
		return t, nil
	}

	if len(h.ConsumerGroups) >= maxEntitiesPerParent {
		return time.Time{}, ErrInvalidName
	}

	t := b.now().UTC()
	h.ConsumerGroups[name] = t

	return t, nil
}

// ConsumerGroup returns a consumer group's creation time.
func (b *InMemoryBackend) ConsumerGroup(ref HubRef, name string) (time.Time, error) {
	b.mu.RLock("ConsumerGroup")
	defer b.mu.RUnlock()

	_, h, err := b.hubLocked(ref)
	if err != nil {
		return time.Time{}, err
	}

	t, ok := h.ConsumerGroups[name]
	if !ok {
		return time.Time{}, ErrConsumerGroupNotFound
	}

	return t, nil
}

// ListConsumerGroups returns the hub's consumer group names, sorted.
func (b *InMemoryBackend) ListConsumerGroups(ref HubRef) ([]string, error) {
	b.mu.RLock("ListConsumerGroups")
	defer b.mu.RUnlock()

	_, h, err := b.hubLocked(ref)
	if err != nil {
		return nil, err
	}

	out := make([]string, 0, len(h.ConsumerGroups))
	for n := range h.ConsumerGroups {
		out = append(out, n)
	}

	sort.Strings(out)

	return out, nil
}

// DeleteConsumerGroup removes a consumer group; $Default is protected.
func (b *InMemoryBackend) DeleteConsumerGroup(ref HubRef, name string) error {
	b.mu.Lock("DeleteConsumerGroup")
	defer b.mu.Unlock()

	_, h, err := b.hubLocked(ref)
	if err != nil {
		return err
	}

	if _, ok := h.ConsumerGroups[name]; !ok {
		return ErrConsumerGroupNotFound
	}

	if name == DefaultConsumerGroup {
		return ErrProtectedEntity
	}

	for pid := range h.Partitions {
		b.kickAllLocked(receiverKey(ref, name, pid))
	}

	delete(h.ConsumerGroups, name)

	return nil
}

func validRights(rights []string) bool {
	if len(rights) == 0 {
		return false
	}

	for _, r := range rights {
		if r != RightListen && r != RightSend && r != RightManage {
			return false
		}
	}

	return true
}

func ruleKey(hubName, name string) string {
	if hubName == "" {
		return name
	}

	return hubName + "/" + name
}

func (ns *namespace) scopeLocked(hubName string) error {
	if hubName == "" {
		return nil
	}

	if _, ok := ns.Hubs[hubName]; !ok {
		return ErrHubNotFound
	}

	return nil
}

// PutRule creates or updates an authorization rule scoped to the namespace or,
// when hubName is set, to one event hub; keys survive updates.
func (b *InMemoryBackend) PutRule(nsName, hubName, name string, rights []string) (Rule, error) {
	if !validName(name) {
		return Rule{}, ErrInvalidName
	}

	if !validRights(rights) {
		return Rule{}, errNoSuchRights
	}

	b.mu.Lock("PutRule")
	defer b.mu.Unlock()

	ns, err := b.nsLocked(nsName)
	if err != nil {
		return Rule{}, err
	}

	if scopeErr := ns.scopeLocked(hubName); scopeErr != nil {
		return Rule{}, scopeErr
	}

	key := ruleKey(hubName, name)

	r, ok := ns.Rules[key]
	if !ok {
		if len(ns.Rules) >= maxEntitiesPerParent {
			return Rule{}, ErrInvalidName
		}

		r = &rule{Name: name, Hub: hubName, PrimaryKey: newKey(), SecondaryKey: newKey()}
		ns.Rules[key] = r
	}

	r.Rights = append([]string(nil), rights...)

	return ruleView(r), nil
}

func ruleView(r *rule) Rule {
	return Rule{
		Name: r.Name, PrimaryKey: r.PrimaryKey, SecondaryKey: r.SecondaryKey,
		Rights: append([]string(nil), r.Rights...),
	}
}

// GetRule returns an authorization rule including its keys.
func (b *InMemoryBackend) GetRule(nsName, hubName, name string) (Rule, error) {
	b.mu.RLock("GetRule")
	defer b.mu.RUnlock()

	ns, err := b.nsLocked(nsName)
	if err != nil {
		return Rule{}, err
	}

	r, ok := ns.Rules[ruleKey(hubName, name)]
	if !ok {
		return Rule{}, ErrRuleNotFound
	}

	return ruleView(r), nil
}

// DeleteRule removes an authorization rule; the namespace root rule is protected.
func (b *InMemoryBackend) DeleteRule(nsName, hubName, name string) error {
	b.mu.Lock("DeleteRule")
	defer b.mu.Unlock()

	ns, err := b.nsLocked(nsName)
	if err != nil {
		return err
	}

	key := ruleKey(hubName, name)
	if _, ok := ns.Rules[key]; !ok {
		return ErrRuleNotFound
	}

	if key == DefaultKeyName {
		return ErrProtectedEntity
	}

	delete(ns.Rules, key)

	return nil
}
