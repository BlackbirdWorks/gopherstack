package ocistreaming

import (
	"net/url"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	minKafkaRetention = 24
	maxKafkaRetention = 168
	defaultPoolName   = "DefaultPool"
)

type kmsKeyIn struct {
	KmsKeyID string `json:"kmsKeyId"`
}

// CreatePoolIn is the CreateStreamPool request body.
type CreatePoolIn struct {
	Kafka   *KafkaSettings   `json:"kafkaSettings"`
	Key     *kmsKeyIn        `json:"customEncryptionKeyDetails"`
	Private *PrivateEndpoint `json:"privateEndpointDetails"`
	Tags
	Name        string `json:"name"`
	Compartment string `json:"compartmentId"`
}

// UpdatePoolIn is the UpdateStreamPool request body.
type UpdatePoolIn struct {
	Name  *string        `json:"name"`
	Kafka *KafkaSettings `json:"kafkaSettings"`
	Key   *kmsKeyIn      `json:"customEncryptionKeyDetails"`
	Tags
}

type keyWire struct {
	KmsKeyID *string `json:"kmsKeyId,omitempty"`
	KeyState string  `json:"keyState"`
}

type poolWire struct {
	TimeCreated    time.Time                 `json:"timeCreated"`
	Kafka          KafkaSettings             `json:"kafkaSettings"`
	Key            keyWire                   `json:"customEncryptionKey"`
	Endpoint       *PrivateEndpoint          `json:"privateEndpointSettings,omitempty"`
	FreeformTags   map[string]string         `json:"freeformTags,omitempty"`
	DefinedTags    map[string]map[string]any `json:"definedTags,omitempty"`
	SecurityAttr   map[string]map[string]any `json:"securityAttributes,omitempty"`
	EndpointFqdn   *string                   `json:"endpointFqdn,omitempty"`
	ID             string                    `json:"id"`
	CompartmentID  string                    `json:"compartmentId"`
	Name           string                    `json:"name"`
	LifecycleState string                    `json:"lifecycleState"`
	IsPrivate      bool                      `json:"isPrivate"`
}

type poolSummaryWire struct {
	TimeCreated    time.Time                 `json:"timeCreated"`
	FreeformTags   map[string]string         `json:"freeformTags,omitempty"`
	DefinedTags    map[string]map[string]any `json:"definedTags,omitempty"`
	SecurityAttr   map[string]map[string]any `json:"securityAttributes,omitempty"`
	ID             string                    `json:"id"`
	CompartmentID  string                    `json:"compartmentId"`
	Name           string                    `json:"name"`
	LifecycleState string                    `json:"lifecycleState"`
	IsPrivate      bool                      `json:"isPrivate"`
}

func poolView(p *Pool, shown, host string) poolWire {
	if shown == "" {
		shown = p.State
	}

	key := keyWire{KeyState: "NONE"}
	if p.KMSKeyID != "" {
		key = keyWire{KmsKeyID: &p.KMSKeyID, KeyState: "ACTIVE"}
	}

	w := poolWire{
		ID: p.ID, CompartmentID: p.CompartmentID, Name: p.Name, LifecycleState: shown, TimeCreated: p.TimeCreated,
		Kafka: p.Kafka, Key: key, IsPrivate: p.Private, Endpoint: p.Endpoint,
		FreeformTags: p.Tags.FreeformTags, DefinedTags: p.Tags.DefinedTags, SecurityAttr: p.Tags.SecurityAttributes,
	}
	if host != "" {
		w.EndpointFqdn = &host
	}

	return w
}

func hostOf(endpoint string) string {
	u, err := url.Parse(endpoint)
	if err != nil {
		return ""
	}

	return u.Host
}

func defaultKafka(in *KafkaSettings) (KafkaSettings, *APIError) {
	k := KafkaSettings{AutoCreateTopicsEnable: new(false), LogRetentionHours: new(minKafkaRetention), NumPartitions: new(1)}
	if in == nil {
		return k, nil
	}

	return mergeKafka(k, in)
}

func mergeKafka(k KafkaSettings, in *KafkaSettings) (KafkaSettings, *APIError) {
	if in.BootstrapServers != nil {
		k.BootstrapServers = in.BootstrapServers
	}

	if in.AutoCreateTopicsEnable != nil {
		k.AutoCreateTopicsEnable = in.AutoCreateTopicsEnable
	}

	if in.LogRetentionHours != nil {
		if *in.LogRetentionHours < minKafkaRetention || *in.LogRetentionHours > maxKafkaRetention {
			return k, errParam("kafkaSettings.logRetentionHours must be between 24 and 168")
		}

		k.LogRetentionHours = in.LogRetentionHours
	}

	if in.NumPartitions != nil {
		if *in.NumPartitions < 1 || *in.NumPartitions > maxPartitions {
			return k, errParam("kafkaSettings.numPartitions must be between 1 and 1000")
		}

		k.NumPartitions = in.NumPartitions
	}

	return k, nil
}

func (b *InMemoryBackend) activePool(id string) (*Pool, *APIError) {
	p, ok := b.pools[id]
	if !ok {
		return nil, errNotFound(id)
	}

	if p.State != stateActive {
		return nil, errState("Stream pool " + id + " is in state " + p.State + "; the operation requires ACTIVE")
	}

	return p, nil
}

func (b *InMemoryBackend) livePool(id string) (*Pool, *APIError) {
	p, ok := b.pools[id]
	if !ok || p.State == stateDeleted || p.State == stateDeleting {
		return nil, errNotFound(id)
	}

	return p, nil
}

// CreatePool creates a stream pool; it is CREATING until the transition elapses.
func (b *InMemoryBackend) CreatePool(in CreatePoolIn, host string) (poolWire, string, string, error) {
	if in.Compartment == "" {
		return poolWire{}, "", "", errMissing("compartmentId is required")
	}

	if aerr := validateName(in.Name); aerr != nil {
		return poolWire{}, "", "", aerr
	}

	kafka, aerr := defaultKafka(in.Kafka)
	if aerr != nil {
		return poolWire{}, "", "", aerr
	}

	b.mu.Lock("CreatePool")
	defer b.mu.Unlock()

	p := &Pool{
		ID: newOCID("streampool"), CompartmentID: in.Compartment, Name: in.Name, State: stateCreating,
		TimeCreated: b.nowUTC(), TransitionAt: b.transitionAt(), Kafka: kafka, Tags: in.Tags,
		Etag: newEtag(), Seq: b.nextSeq(),
	}

	if in.Key != nil {
		if aerr = b.checkKey(in.Key.KmsKeyID); aerr != nil {
			return poolWire{}, "", "", aerr
		}

		p.KMSKeyID = in.Key.KmsKeyID
	}

	if in.Private != nil {
		p.Private, p.Endpoint = true, in.Private
	}

	b.pools[p.ID] = p
	wr := b.newWR("CREATE_STREAM_POOL", p.CompartmentID, "streampool", p.ID, "CREATED",
		"/streampools/"+p.ID, *p.TransitionAt)

	return poolView(p, "", host), p.Etag, wr, nil
}

// GetPool returns one pool.
func (b *InMemoryBackend) GetPool(id, host string) (poolWire, string, error) {
	b.mu.RLock("GetPool")
	defer b.mu.RUnlock()

	p, ok := b.pools[id]
	if !ok {
		return poolWire{}, "", errNotFound(id)
	}

	return poolView(p, "", host), p.Etag, nil
}

// ListPools lists the pools of a compartment.
func (b *InMemoryBackend) ListPools(compartment, id, name, state string, o ListOpts) (page.Page[poolSummaryWire], error) {
	if compartment == "" {
		return page.Page[poolSummaryWire]{}, errMissing("compartmentId is required")
	}

	b.mu.RLock("ListPools")
	defer b.mu.RUnlock()

	var all []*Pool

	for _, p := range b.pools {
		if p.CompartmentID == compartment && (id == "" || p.ID == id) && (name == "" || p.Name == name) &&
			(state == "" || strings.EqualFold(p.State, state)) {
			all = append(all, p)
		}
	}

	pg := paginate(all, o, func(p *Pool) string { return p.Name },
		func(p *Pool) (time.Time, int64) { return p.TimeCreated, p.Seq })

	out := make([]poolSummaryWire, 0, len(pg.Data))
	for _, p := range pg.Data {
		out = append(out, poolSummaryWire{
			ID: p.ID, CompartmentID: p.CompartmentID, Name: p.Name, LifecycleState: p.State,
			TimeCreated: p.TimeCreated, IsPrivate: p.Private, FreeformTags: p.Tags.FreeformTags,
			DefinedTags: p.Tags.DefinedTags, SecurityAttr: p.Tags.SecurityAttributes,
		})
	}

	return page.Page[poolSummaryWire]{Data: out, Next: pg.Next}, nil
}

// UpdatePool changes name, Kafka settings, encryption key and tags.
func (b *InMemoryBackend) UpdatePool(id, ifMatch, host string, in UpdatePoolIn) (poolWire, string, string, error) {
	b.mu.Lock("UpdatePool")
	defer b.mu.Unlock()

	p, aerr := b.activePool(id)
	if aerr != nil {
		return poolWire{}, "", "", aerr
	}

	if aerr = checkEtag(p.Etag, ifMatch); aerr != nil {
		return poolWire{}, "", "", aerr
	}

	if aerr = b.applyPoolUpdate(p, in); aerr != nil {
		return poolWire{}, "", "", aerr
	}

	p.Etag = newEtag()
	wr := b.newWR("UPDATE_STREAM_POOL", p.CompartmentID, "streampool", p.ID, "UPDATED",
		"/streampools/"+p.ID, b.nowUTC())

	return poolView(p, "", host), p.Etag, wr, nil
}

func (b *InMemoryBackend) applyPoolUpdate(p *Pool, in UpdatePoolIn) *APIError {
	var aerr *APIError

	if in.Name != nil {
		if aerr = validateName(*in.Name); aerr != nil {
			return aerr
		}
	}

	kafka := p.Kafka
	if in.Kafka != nil {
		if kafka, aerr = mergeKafka(kafka, in.Kafka); aerr != nil {
			return aerr
		}
	}

	if in.Key != nil {
		if aerr = b.checkKey(in.Key.KmsKeyID); aerr != nil {
			return aerr
		}

		p.KMSKeyID = in.Key.KmsKeyID
	}

	if in.Name != nil {
		p.Name = *in.Name
	}

	p.Kafka = kafka
	p.Tags.merge(in.Tags)

	return nil
}

func (t *Tags) merge(in Tags) {
	if in.FreeformTags != nil {
		t.FreeformTags = in.FreeformTags
	}

	if in.DefinedTags != nil {
		t.DefinedTags = in.DefinedTags
	}

	if in.SecurityAttributes != nil {
		t.SecurityAttributes = in.SecurityAttributes
	}
}

// DeletePool moves an empty pool to DELETING.
func (b *InMemoryBackend) DeletePool(id, ifMatch string) (string, error) {
	b.mu.Lock("DeletePool")
	defer b.mu.Unlock()

	p, aerr := b.activePool(id)
	if aerr != nil {
		return "", aerr
	}

	if aerr = checkEtag(p.Etag, ifMatch); aerr != nil {
		return "", aerr
	}

	for _, s := range b.streams {
		if s.PoolID == id && s.State != stateDeleted {
			return "", errState("Stream pool " + id + " still contains streams")
		}
	}

	p.State, p.TransitionAt, p.Etag = stateDeleting, b.transitionAt(), newEtag()

	return b.newWR("DELETE_STREAM_POOL", p.CompartmentID, "streampool", p.ID, "DELETED",
		"/streampools/"+p.ID, *p.TransitionAt), nil
}

// ChangePoolCompartment moves a pool to another compartment.
func (b *InMemoryBackend) ChangePoolCompartment(id, ifMatch, compartment string) (string, error) {
	if compartment == "" {
		return "", errMissing("compartmentId is required")
	}

	b.mu.Lock("ChangePoolCompartment")
	defer b.mu.Unlock()

	p, aerr := b.activePool(id)
	if aerr != nil {
		return "", aerr
	}

	if aerr = checkEtag(p.Etag, ifMatch); aerr != nil {
		return "", aerr
	}

	p.CompartmentID, p.Etag = compartment, newEtag()

	return b.newWR("UPDATE_STREAM_POOL", compartment, "streampool", p.ID, "UPDATED",
		"/streampools/"+p.ID, b.nowUTC()), nil
}

// defaultPool returns the compartment's DefaultPool, creating it on first use.
func (b *InMemoryBackend) defaultPool(compartment string) *Pool {
	for _, p := range b.pools {
		if p.IsDefault && p.CompartmentID == compartment && p.State != stateDeleted && p.State != stateDeleting {
			return p
		}
	}

	kafka, _ := defaultKafka(nil)
	p := &Pool{
		ID: newOCID("streampool"), CompartmentID: compartment, Name: defaultPoolName, State: stateActive,
		TimeCreated: b.nowUTC(), Kafka: kafka, IsDefault: true, Etag: newEtag(), Seq: b.nextSeq(),
	}
	b.pools[p.ID] = p

	return p
}

func (h *Handler) createPool(q *req) error {
	var in CreatePoolIn
	if err := q.decode(&in); err != nil {
		return err
	}

	p, etag, wr, err := h.Backend.CreatePool(in, hostOf(q.endpoint()))

	return replyWR(q, p, etag, wr, err)
}

func (h *Handler) getPool(q *req) error {
	p, etag, err := h.Backend.GetPool(q.param("streamPoolId"), hostOf(q.endpoint()))

	return reply(q, p, etag, err)
}

func (h *Handler) listPools(q *req) error {
	pg, err := h.Backend.ListPools(q.query("compartmentId"), q.query("id"), q.query("name"),
		q.query("lifecycleState"), q.listOpts())

	return listReply(q, pg, err)
}

func (h *Handler) updatePool(q *req) error {
	var in UpdatePoolIn
	if err := q.decode(&in); err != nil {
		return err
	}

	p, etag, wr, err := h.Backend.UpdatePool(q.param("streamPoolId"), q.ifMatch(), hostOf(q.endpoint()), in)

	return replyWR(q, p, etag, wr, err)
}

func (h *Handler) deletePool(q *req) error {
	wr, err := h.Backend.DeletePool(q.param("streamPoolId"), q.ifMatch())

	return acceptedWR(q, wr, err)
}

type changeCompartmentIn struct {
	CompartmentID string `json:"compartmentId"`
}

func (h *Handler) changePoolCompartment(q *req) error {
	var in changeCompartmentIn
	if err := q.decode(&in); err != nil {
		return err
	}

	wr, err := h.Backend.ChangePoolCompartment(q.param("streamPoolId"), q.ifMatch(), in.CompartmentID)

	return acceptedWR(q, wr, err)
}
