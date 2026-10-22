package azureeventgrid

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"net/http"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const webhookTimeout = 30 * time.Second

// InMemoryBackend is the Event Grid state shared by the ARM and data planes.
type InMemoryBackend struct {
	mu             *lockmetrics.RWMutex
	topics         map[string]*topic
	namespaces     map[string]*namespace
	targets        Targets
	httpc          *http.Client
	now            func() time.Time
	kick           chan struct{}
	pullWake       chan struct{}
	validationBase string
}

// NewInMemoryBackend returns an empty backend using the wall clock.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		mu:         lockmetrics.New("azureeventgrid"),
		topics:     map[string]*topic{},
		namespaces: map[string]*namespace{},
		targets:    noTargets{},
		httpc:      newWebhookClient(webhookTimeout),
		now:        time.Now,
		kick:       make(chan struct{}, 1),
		pullWake:   make(chan struct{}),
	}
}

// SetTargets wires the Service Bus, Event Hubs, Storage Queue and Blob sinks.
func (b *InMemoryBackend) SetTargets(t Targets) {
	b.mu.Lock("SetTargets")
	defer b.mu.Unlock()

	b.targets = t
}

// SetValidationBase sets the base URL used in webhook validationUrl values.
func (b *InMemoryBackend) SetValidationBase(base string) {
	b.mu.Lock("SetValidationBase")
	defer b.mu.Unlock()

	b.validationBase = strings.TrimRight(base, "/")
}

// Reset drops all state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.topics = map[string]*topic{}
	b.namespaces = map[string]*namespace{}
}

func validName(n string) bool {
	if n == "" || len(n) > maxNameLen {
		return false
	}

	for _, r := range n {
		ok := r == '-' || r == '_' || (r >= '0' && r <= '9') || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z')
		if !ok {
			return false
		}
	}

	return true
}

func newKey() string {
	var buf [32]byte

	_, _ = rand.Read(buf[:])

	return base64.StdEncoding.EncodeToString(buf[:])
}

func keyEqual(a, b string) bool {
	return a != "" && subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

func (b *InMemoryBackend) topicLocked(name string) (*topic, error) {
	t, ok := b.topics[name]
	if !ok {
		return nil, ErrTopicNotFound
	}

	return t, nil
}

func (t *topic) info() TopicInfo {
	return TopicInfo{CreatedAt: t.CreatedAt, Name: t.Name, ResourceID: t.ResourceID, InputSchema: t.InputSchema}
}

// PutTopic creates a custom topic or updates its resource ID and input schema.
func (b *InMemoryBackend) PutTopic(name, resourceID, inputSchema string) (TopicInfo, error) {
	if !validName(name) {
		return TopicInfo{}, ErrInvalidName
	}

	if inputSchema == "" {
		inputSchema = SchemaEventGrid
	}

	b.mu.Lock("PutTopic")
	defer b.mu.Unlock()

	t, ok := b.topics[name]
	if !ok {
		t = &topic{
			Name: name, CreatedAt: b.now().UTC(), Key1: newKey(), Key2: newKey(),
			Subs: map[string]*subscription{},
		}
		b.topics[name] = t
	}

	t.ResourceID, t.InputSchema = resourceID, inputSchema

	return t.info(), nil
}

// Topic returns the custom topic.
func (b *InMemoryBackend) Topic(name string) (TopicInfo, error) {
	b.mu.RLock("Topic")
	defer b.mu.RUnlock()

	t, err := b.topicLocked(name)
	if err != nil {
		return TopicInfo{}, err
	}

	return t.info(), nil
}

// DeleteTopic removes a custom topic and its event subscriptions.
func (b *InMemoryBackend) DeleteTopic(name string) error {
	b.mu.Lock("DeleteTopic")
	defer b.mu.Unlock()

	if _, err := b.topicLocked(name); err != nil {
		return err
	}

	delete(b.topics, name)

	return nil
}

// TopicKeys returns the topic's access keys.
func (b *InMemoryBackend) TopicKeys(name string) (Keys, error) {
	b.mu.RLock("TopicKeys")
	defer b.mu.RUnlock()

	t, err := b.topicLocked(name)
	if err != nil {
		return Keys{}, err
	}

	return Keys{Key1: t.Key1, Key2: t.Key2}, nil
}

// RegenerateTopicKey replaces "key1" or "key2" (case-insensitive) with a new random key.
func (b *InMemoryBackend) RegenerateTopicKey(name, keyName string) (Keys, error) {
	b.mu.Lock("RegenerateTopicKey")
	defer b.mu.Unlock()

	t, err := b.topicLocked(name)
	if err != nil {
		return Keys{}, err
	}

	if err = regenerate(&t.Key1, &t.Key2, keyName); err != nil {
		return Keys{}, err
	}

	return Keys{Key1: t.Key1, Key2: t.Key2}, nil
}

func regenerate(k1, k2 *string, keyName string) error {
	switch strings.ToLower(keyName) {
	case "key1":
		*k1 = newKey()
	case "key2":
		*k2 = newKey()
	default:
		return ErrInvalidName
	}

	return nil
}

// PutNamespace creates an Event Grid namespace if absent.
func (b *InMemoryBackend) PutNamespace(name string) error {
	if !validName(name) {
		return ErrInvalidName
	}

	b.mu.Lock("PutNamespace")
	defer b.mu.Unlock()

	if _, ok := b.namespaces[name]; !ok {
		b.namespaces[name] = &namespace{
			Name: name, CreatedAt: b.now().UTC(), Key1: newKey(), Key2: newKey(), Topics: map[string]*nsTopic{},
		}
	}

	return nil
}

func (b *InMemoryBackend) nsLocked(name string) (*namespace, error) {
	ns, ok := b.namespaces[name]
	if !ok {
		return nil, ErrNamespaceNotFound
	}

	return ns, nil
}

// NamespaceExists reports whether the namespace exists.
func (b *InMemoryBackend) NamespaceExists(name string) bool {
	b.mu.RLock("NamespaceExists")
	defer b.mu.RUnlock()

	_, ok := b.namespaces[name]

	return ok
}

// DeleteNamespace removes a namespace with its topics.
func (b *InMemoryBackend) DeleteNamespace(name string) error {
	b.mu.Lock("DeleteNamespace")
	defer b.mu.Unlock()

	if _, err := b.nsLocked(name); err != nil {
		return err
	}

	delete(b.namespaces, name)

	return nil
}

// NamespaceKeys returns the namespace's access keys.
func (b *InMemoryBackend) NamespaceKeys(name string) (Keys, error) {
	b.mu.RLock("NamespaceKeys")
	defer b.mu.RUnlock()

	ns, err := b.nsLocked(name)
	if err != nil {
		return Keys{}, err
	}

	return Keys{Key1: ns.Key1, Key2: ns.Key2}, nil
}

// RegenerateNamespaceKey replaces "key1" or "key2" with a new random key.
func (b *InMemoryBackend) RegenerateNamespaceKey(name, keyName string) (Keys, error) {
	b.mu.Lock("RegenerateNamespaceKey")
	defer b.mu.Unlock()

	ns, err := b.nsLocked(name)
	if err != nil {
		return Keys{}, err
	}

	if err = regenerate(&ns.Key1, &ns.Key2, keyName); err != nil {
		return Keys{}, err
	}

	return Keys{Key1: ns.Key1, Key2: ns.Key2}, nil
}

func (b *InMemoryBackend) nsTopicLocked(nsName, name string) (*namespace, *nsTopic, error) {
	ns, err := b.nsLocked(nsName)
	if err != nil {
		return nil, nil, err
	}

	t, ok := ns.Topics[name]
	if !ok {
		return nil, nil, ErrNamespaceTopicNotFound
	}

	return ns, t, nil
}

// PutNamespaceTopic creates a namespace topic if absent.
func (b *InMemoryBackend) PutNamespaceTopic(nsName, name string) (time.Time, error) {
	if !validName(name) {
		return time.Time{}, ErrInvalidName
	}

	b.mu.Lock("PutNamespaceTopic")
	defer b.mu.Unlock()

	ns, err := b.nsLocked(nsName)
	if err != nil {
		return time.Time{}, err
	}

	t, ok := ns.Topics[name]
	if !ok {
		t = &nsTopic{Name: name, CreatedAt: b.now().UTC(), Subs: map[string]*pullSub{}}
		ns.Topics[name] = t
	}

	return t.CreatedAt, nil
}

// NamespaceTopicCreated returns a namespace topic's creation time.
func (b *InMemoryBackend) NamespaceTopicCreated(nsName, name string) (time.Time, error) {
	b.mu.RLock("NamespaceTopicCreated")
	defer b.mu.RUnlock()

	_, t, err := b.nsTopicLocked(nsName, name)
	if err != nil {
		return time.Time{}, err
	}

	return t.CreatedAt, nil
}

// DeleteNamespaceTopic removes a namespace topic and its subscriptions.
func (b *InMemoryBackend) DeleteNamespaceTopic(nsName, name string) error {
	b.mu.Lock("DeleteNamespaceTopic")
	defer b.mu.Unlock()

	ns, _, err := b.nsTopicLocked(nsName, name)
	if err != nil {
		return err
	}

	delete(ns.Topics, name)

	return nil
}
