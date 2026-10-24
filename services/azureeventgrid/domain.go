package azureeventgrid

import (
	"sort"
	"time"
)

type domainTopic struct {
	CreatedAt time.Time                `json:"createdAt"`
	Subs      map[string]*subscription `json:"subs"`
	Name      string                   `json:"name"`
}

type domain struct {
	CreatedAt   time.Time                `json:"createdAt"`
	Subs        map[string]*subscription `json:"subs"`
	Topics      map[string]*domainTopic  `json:"topics"`
	Name        string                   `json:"name"`
	ResourceID  string                   `json:"resourceId"`
	Key1        string                   `json:"key1"`
	Key2        string                   `json:"key2"`
	InputSchema string                   `json:"inputSchema"`
	AutoCreate  bool                     `json:"autoCreate"`
	AutoDelete  bool                     `json:"autoDelete"`
}

// DomainInfo is the exported view of a domain.
type DomainInfo struct {
	CreatedAt   time.Time
	Name        string
	ResourceID  string
	InputSchema string
	AutoCreate  bool
	AutoDelete  bool
}

// DomainTopicInfo is the exported view of a domain topic.
type DomainTopicInfo struct {
	CreatedAt time.Time
	Name      string
}

func (d *domain) topicRID(name string) string { return d.ResourceID + "/topics/" + name }

func (d *domain) info() DomainInfo {
	return DomainInfo{
		CreatedAt: d.CreatedAt, Name: d.Name, ResourceID: d.ResourceID, InputSchema: d.InputSchema,
		AutoCreate: d.AutoCreate, AutoDelete: d.AutoDelete,
	}
}

func (b *InMemoryBackend) domainLocked(name string) (*domain, error) {
	d, ok := b.domains[name]
	if !ok {
		return nil, ErrDomainNotFound
	}

	return d, nil
}

// PutDomain creates a domain or updates its resource ID, input schema and topic
// management flags; a nil flag means true, the Event Grid default.
func (b *InMemoryBackend) PutDomain(
	name, resourceID, inputSchema string, autoCreate, autoDelete *bool,
) (DomainInfo, error) {
	if !validName(name) {
		return DomainInfo{}, ErrInvalidName
	}

	if inputSchema == "" {
		inputSchema = SchemaEventGrid
	}

	b.mu.Lock("PutDomain")
	defer b.mu.Unlock()

	d, ok := b.domains[name]
	if !ok {
		d = &domain{
			Name: name, CreatedAt: b.now().UTC(), Key1: newKey(), Key2: newKey(),
			Subs: map[string]*subscription{}, Topics: map[string]*domainTopic{},
		}
		b.domains[name] = d
	}

	d.ResourceID, d.InputSchema = resourceID, inputSchema
	d.AutoCreate, d.AutoDelete = autoCreate == nil || *autoCreate, autoDelete == nil || *autoDelete

	return d.info(), nil
}

// Domain returns the domain.
func (b *InMemoryBackend) Domain(name string) (DomainInfo, error) {
	b.mu.RLock("Domain")
	defer b.mu.RUnlock()

	d, err := b.domainLocked(name)
	if err != nil {
		return DomainInfo{}, err
	}

	return d.info(), nil
}

// DeleteDomain removes a domain with its topics and event subscriptions.
func (b *InMemoryBackend) DeleteDomain(name string) error {
	b.mu.Lock("DeleteDomain")
	defer b.mu.Unlock()

	if _, err := b.domainLocked(name); err != nil {
		return err
	}

	delete(b.domains, name)

	return nil
}

// DomainKeys returns the domain's access keys.
func (b *InMemoryBackend) DomainKeys(name string) (Keys, error) {
	b.mu.RLock("DomainKeys")
	defer b.mu.RUnlock()

	d, err := b.domainLocked(name)
	if err != nil {
		return Keys{}, err
	}

	return Keys{Key1: d.Key1, Key2: d.Key2}, nil
}

// RegenerateDomainKey replaces "key1" or "key2" (case-insensitive) with a new random key.
func (b *InMemoryBackend) RegenerateDomainKey(name, keyName string) (Keys, error) {
	b.mu.Lock("RegenerateDomainKey")
	defer b.mu.Unlock()

	d, err := b.domainLocked(name)
	if err != nil {
		return Keys{}, err
	}

	if err = regenerate(&d.Key1, &d.Key2, keyName); err != nil {
		return Keys{}, err
	}

	return Keys{Key1: d.Key1, Key2: d.Key2}, nil
}

// PutDomainTopic creates a domain topic if absent.
func (b *InMemoryBackend) PutDomainTopic(domainName, name string) (time.Time, error) {
	if !validName(name) {
		return time.Time{}, ErrInvalidName
	}

	b.mu.Lock("PutDomainTopic")
	defer b.mu.Unlock()

	d, err := b.domainLocked(domainName)
	if err != nil {
		return time.Time{}, err
	}

	return d.ensureTopic(name, b.now().UTC()).CreatedAt, nil
}

func (d *domain) ensureTopic(name string, now time.Time) *domainTopic {
	t, ok := d.Topics[name]
	if !ok {
		t = &domainTopic{Name: name, CreatedAt: now, Subs: map[string]*subscription{}}
		d.Topics[name] = t
	}

	return t
}

// DomainTopic returns a domain topic.
func (b *InMemoryBackend) DomainTopic(domainName, name string) (DomainTopicInfo, error) {
	b.mu.RLock("DomainTopic")
	defer b.mu.RUnlock()

	d, err := b.domainLocked(domainName)
	if err != nil {
		return DomainTopicInfo{}, err
	}

	t, ok := d.Topics[name]
	if !ok {
		return DomainTopicInfo{}, ErrDomainTopicNotFound
	}

	return DomainTopicInfo{Name: t.Name, CreatedAt: t.CreatedAt}, nil
}

// DomainTopics lists a domain's topics sorted by name.
func (b *InMemoryBackend) DomainTopics(domainName string) ([]DomainTopicInfo, error) {
	b.mu.RLock("DomainTopics")
	defer b.mu.RUnlock()

	d, err := b.domainLocked(domainName)
	if err != nil {
		return nil, err
	}

	out := make([]DomainTopicInfo, 0, len(d.Topics))
	for _, t := range d.Topics {
		out = append(out, DomainTopicInfo{Name: t.Name, CreatedAt: t.CreatedAt})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out, nil
}

// DeleteDomainTopic removes a domain topic and its event subscriptions.
func (b *InMemoryBackend) DeleteDomainTopic(domainName, name string) error {
	b.mu.Lock("DeleteDomainTopic")
	defer b.mu.Unlock()

	d, err := b.domainLocked(domainName)
	if err != nil {
		return err
	}

	if _, ok := d.Topics[name]; !ok {
		return ErrDomainTopicNotFound
	}

	delete(d.Topics, name)

	return nil
}

// ensureDomainTopicLocked creates the domain topic of a first subscription when
// the domain auto-creates topics; a self-managed domain needs the topic first.
func (b *InMemoryBackend) ensureDomainTopicLocked(sc Scope) error {
	if sc.Domain == "" || sc.Topic == "" {
		return nil
	}

	d, err := b.domainLocked(sc.Domain)
	if err != nil {
		return err
	}

	if _, ok := d.Topics[sc.Topic]; ok {
		return nil
	}

	if !d.AutoCreate {
		return ErrDomainTopicNotFound
	}

	d.ensureTopic(sc.Topic, b.now().UTC())

	return nil
}

// autoDeleteDomainTopicLocked drops a domain topic left without subscriptions
// when its domain auto-deletes topics.
func (b *InMemoryBackend) autoDeleteDomainTopicLocked(sc Scope) {
	if sc.Domain == "" || sc.Topic == "" {
		return
	}

	d, err := b.domainLocked(sc.Domain)
	if err != nil || !d.AutoDelete {
		return
	}

	if t, ok := d.Topics[sc.Topic]; ok && len(t.Subs) == 0 {
		delete(d.Topics, sc.Topic)
	}
}

// PublishDomain fans events out to the subscriptions of the domain and of the
// domain topic each event names (EventGrid schema: "topic"; CloudEvents: "source").
// Events for a topic without a domain topic reach only domain-scope subscriptions.
func (b *InMemoryBackend) PublishDomain(domainName string, events []Event) error {
	b.mu.Lock("PublishDomain")
	defer b.mu.Unlock()

	d, err := b.domainLocked(domainName)
	if err != nil {
		return err
	}

	now := b.now().UTC()

	for i := range events {
		topicName := domainTopicOf(events[i], d.InputSchema)
		if topicName == "" {
			return invalidEvent("required property '%s' not found in JSON", domainTopicField(d.InputSchema))
		}

		if err = events[i].stamp(d.topicRID(topicName), d.InputSchema); err != nil {
			return err
		}

		for _, s := range d.Subs {
			s.enqueue(events[i], now)
		}

		if t, ok := d.Topics[topicName]; ok {
			for _, s := range t.Subs {
				s.enqueue(events[i], now)
			}
		}
	}

	select {
	case b.kick <- struct{}{}:
	default:
	}

	return nil
}

func domainTopicField(schema string) string {
	if schema == SchemaCloudEvent {
		return "source"
	}

	return "topic"
}

func domainTopicOf(ev Event, schema string) string {
	return strField(ev.Fields, domainTopicField(schema))
}

func (b *InMemoryBackend) publisherSchema(p Publisher) (string, error) {
	if !p.Domain {
		info, err := b.Topic(p.Name)

		return info.InputSchema, err
	}

	info, err := b.Domain(p.Name)

	return info.InputSchema, err
}
