package azuresearch

import (
	"crypto/rand"
	"encoding/hex"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	keyBytes   = 16
	kindPrim   = "primary"
	kindSecond = "secondary"

	authAPIKeyOnly  = "apiKeyOnly"
	authAADOrAPIKey = "aadOrApiKey"
	accessDisabled  = "disabled"
)

// ServiceConfig is the ARM-controlled configuration of a search service.
type ServiceConfig struct {
	SKU                 string `json:"sku"`
	HostingMode         string `json:"hostingMode"`
	PublicNetworkAccess string `json:"publicNetworkAccess"`
	AuthMode            string `json:"authMode"`
	AADFailureMode      string `json:"aadFailureMode,omitempty"`
	ReplicaCount        int    `json:"replicaCount"`
	PartitionCount      int    `json:"partitionCount"`
	DisableLocalAuth    bool   `json:"disableLocalAuth,omitempty"`
}

// QueryKey is a read-only key; Name may be empty.
type QueryKey struct {
	Name string `json:"name"`
	Key  string `json:"key"`
}

// AdminKeys is the admin key pair of a service.
type AdminKeys struct {
	Primary   string
	Secondary string
}

// Service is one search service and everything it contains.
type Service struct {
	Created     time.Time                    `json:"created"`
	Indexes     map[string]*Index            `json:"indexes"`
	Objects     map[string]map[string]Object `json:"objects"`
	Runtime     map[string]*IndexerRuntime   `json:"runtime"`
	Name        string                       `json:"name"`
	Primary     string                       `json:"primary"`
	Secondary   string                       `json:"secondary"`
	QueryKeys   []QueryKey                   `json:"queryKeys"`
	Config      ServiceConfig                `json:"config"`
	NextETagSeq int64                        `json:"nextETagSeq"`
}

// Object is a stored JSON resource (synonym map, data source, indexer, skillset).
type Object = map[string]any

// InMemoryBackend holds every search service; the ARM and data planes share it.
type InMemoryBackend struct {
	mu       *lockmetrics.RWMutex
	services map[string]*Service
	clock    func() time.Time
	blobs    BlobSource
	cosmos   CosmosSource
	tables   TableSource
}

// Option customises a backend.
type Option func(*InMemoryBackend)

// WithClock injects the clock used for timestamps and indexer schedules.
func WithClock(clock func() time.Time) Option { return func(b *InMemoryBackend) { b.clock = clock } }

// NewInMemoryBackend returns an empty backend using the wall clock.
func NewInMemoryBackend(opts ...Option) *InMemoryBackend {
	b := &InMemoryBackend{
		mu: lockmetrics.New("azuresearch"), services: map[string]*Service{}, clock: time.Now,
		blobs: noBlobs{}, cosmos: noCosmos{}, tables: noTables{},
	}

	for _, o := range opts {
		o(b)
	}

	return b
}

func (b *InMemoryBackend) now() time.Time { return b.clock().UTC() }

// SetBlobSource wires the in-process Blob Storage emulator used by azureblob data sources.
func (b *InMemoryBackend) SetBlobSource(s BlobSource) {
	b.mu.Lock("SetBlobSource")
	defer b.mu.Unlock()

	b.blobs = s
}

// SetCosmosSource wires the in-process Cosmos DB emulator used by cosmosdb data sources.
func (b *InMemoryBackend) SetCosmosSource(s CosmosSource) {
	b.mu.Lock("SetCosmosSource")
	defer b.mu.Unlock()

	b.cosmos = s
}

// SetTableSource wires the in-process Table Storage emulator used by azuretable data sources.
func (b *InMemoryBackend) SetTableSource(s TableSource) {
	b.mu.Lock("SetTableSource")
	defer b.mu.Unlock()

	b.tables = s
}

// Reset drops all state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.services = map[string]*Service{}
}

func newKey() string {
	buf := make([]byte, keyBytes)
	_, _ = rand.Read(buf)

	return strings.ToUpper(hex.EncodeToString(buf))
}

func newService(name string, cfg ServiceConfig, now time.Time) *Service {
	return &Service{
		Name: name, Config: cfg, Created: now, Primary: newKey(), Secondary: newKey(),
		QueryKeys: []QueryKey{{Key: newKey()}},
		Indexes:   map[string]*Index{}, Objects: newObjects(), Runtime: map[string]*IndexerRuntime{},
	}
}

func newObjects() map[string]map[string]Object {
	out := map[string]map[string]Object{}
	for _, k := range objectKindList() {
		out[k] = map[string]Object{}
	}

	return out
}

// PutService creates the service or updates its configuration, returning its creation time.
func (b *InMemoryBackend) PutService(name string, cfg ServiceConfig) (time.Time, error) {
	b.mu.Lock("PutService")
	defer b.mu.Unlock()

	if s, ok := b.services[strs.Fold(name)]; ok {
		s.Config = cfg

		return s.Created, nil
	}

	s := newService(name, cfg, b.now())
	b.services[strs.Fold(name)] = s

	return s.Created, nil
}

// DeleteService removes the service and all its content.
func (b *InMemoryBackend) DeleteService(name string) error {
	b.mu.Lock("DeleteService")
	defer b.mu.Unlock()

	if _, ok := b.services[strs.Fold(name)]; !ok {
		return ErrServiceNotFound
	}

	delete(b.services, strs.Fold(name))

	return nil
}

// HasService reports whether a service exists.
func (b *InMemoryBackend) HasService(name string) bool {
	b.mu.RLock("HasService")
	defer b.mu.RUnlock()

	_, ok := b.services[strs.Fold(name)]

	return ok
}

// ServiceNames lists the services, sorted.
func (b *InMemoryBackend) ServiceNames() []string {
	b.mu.RLock("ServiceNames")
	defer b.mu.RUnlock()

	out := make([]string, 0, len(b.services))
	for _, s := range b.services {
		out = append(out, s.Name)
	}

	sort.Strings(out)

	return out
}

func (b *InMemoryBackend) svc(name string) (*Service, error) {
	s, ok := b.services[strs.Fold(name)]
	if !ok {
		return nil, ErrServiceNotFound
	}

	return s, nil
}

// Keys returns the service's admin keys.
func (b *InMemoryBackend) Keys(name string) (AdminKeys, error) {
	b.mu.RLock("Keys")
	defer b.mu.RUnlock()

	s, err := b.svc(name)
	if err != nil {
		return AdminKeys{}, err
	}

	return AdminKeys{Primary: s.Primary, Secondary: s.Secondary}, nil
}

// RegenerateAdminKey replaces the primary or secondary admin key.
func (b *InMemoryBackend) RegenerateAdminKey(name, kind string) (AdminKeys, error) {
	b.mu.Lock("RegenerateAdminKey")
	defer b.mu.Unlock()

	s, err := b.svc(name)
	if err != nil {
		return AdminKeys{}, err
	}

	switch strings.ToLower(kind) {
	case kindPrim:
		s.Primary = newKey()
	case kindSecond:
		s.Secondary = newKey()
	default:
		return AdminKeys{}, ErrKeyKind
	}

	return AdminKeys{Primary: s.Primary, Secondary: s.Secondary}, nil
}

// ListQueryKeys returns the service's query keys; the default key has an empty name.
func (b *InMemoryBackend) ListQueryKeys(name string) ([]QueryKey, error) {
	b.mu.RLock("ListQueryKeys")
	defer b.mu.RUnlock()

	s, err := b.svc(name)
	if err != nil {
		return nil, err
	}

	return append([]QueryKey(nil), s.QueryKeys...), nil
}

// CreateQueryKey adds a named query key.
func (b *InMemoryBackend) CreateQueryKey(name, keyName string) (QueryKey, error) {
	b.mu.Lock("CreateQueryKey")
	defer b.mu.Unlock()

	s, err := b.svc(name)
	if err != nil {
		return QueryKey{}, err
	}

	for _, k := range s.QueryKeys {
		if keyName != "" && strs.Equal(k.Name, keyName) {
			return QueryKey{}, ErrQueryKeyNameTaken
		}
	}

	k := QueryKey{Name: keyName, Key: newKey()}
	s.QueryKeys = append(s.QueryKeys, k)

	return k, nil
}

// DeleteQueryKey removes the query key whose value is key.
func (b *InMemoryBackend) DeleteQueryKey(name, key string) error {
	b.mu.Lock("DeleteQueryKey")
	defer b.mu.Unlock()

	s, err := b.svc(name)
	if err != nil {
		return err
	}

	for i, k := range s.QueryKeys {
		if k.Key == key {
			s.QueryKeys = append(s.QueryKeys[:i], s.QueryKeys[i+1:]...)

			return nil
		}
	}

	return ErrQueryKeyNotFound
}

// keyRole is what a presented api-key allows.
type keyRole int

const (
	roleNone keyRole = iota
	roleQuery
	roleAdmin
)

func (s *Service) roleOf(key string) keyRole {
	if key == "" {
		return roleNone
	}

	if key == s.Primary || key == s.Secondary {
		return roleAdmin
	}

	for _, q := range s.QueryKeys {
		if q.Key == key {
			return roleQuery
		}
	}

	return roleNone
}

// owner returns the service owning key, if any.
func (b *InMemoryBackend) owner(key string) (string, bool) {
	b.mu.RLock("owner")
	defer b.mu.RUnlock()

	for _, s := range b.services {
		if s.roleOf(key) != roleNone {
			return s.Name, true
		}
	}

	return "", false
}
