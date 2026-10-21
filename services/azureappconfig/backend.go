package azureappconfig

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	secretBytes = 32
	idRandBytes = 15
	etagBytes   = 12
)

var keyNames = []struct { //nolint:gochecknoglobals // fixed credential names, read-only
	name     string
	readOnly bool
}{
	{"Primary", false}, {"Secondary", false}, {"Primary Read Only", true}, {"Secondary Read Only", true},
}

// InMemoryBackend holds every configuration store; the ARM and data planes share it.
type InMemoryBackend struct {
	mu     *lockmetrics.RWMutex
	stores map[string]*store
	now    func() time.Time
}

// NewInMemoryBackend returns an empty backend using the wall clock.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{mu: lockmetrics.New("azureappconfig"), stores: map[string]*store{}, now: time.Now}
}

// Reset drops all state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.stores = map[string]*store{}
}

// ValidStoreName reports whether name is a legal store name (5-50 letters, digits, hyphens).
func ValidStoreName(name string) bool {
	if len(name) < minNameLen || len(name) > maxNameLen {
		return false
	}

	for _, r := range name {
		ok := r == '-' || (r >= '0' && r <= '9') || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z')
		if !ok {
			return false
		}
	}

	return true
}

func randomText(n int) string {
	buf := make([]byte, n)

	_, _ = rand.Read(buf)

	return base64.RawURLEncoding.EncodeToString(buf)
}

func newAccessKey(name string, readOnly bool, now time.Time) AccessKey {
	secret := make([]byte, secretBytes)

	_, _ = rand.Read(secret)

	id := hex.EncodeToString(secret[:2]) + "-l0-s0:" + randomText(idRandBytes)

	return AccessKey{
		ID: id, Name: name, Secret: base64.StdEncoding.EncodeToString(secret), ReadOnly: readOnly,
		LastModified: now.UTC(),
	}
}

func newStore(name string, now time.Time) *store {
	s := &store{
		Name: name, Created: now.UTC(), KVs: map[string]*KeyValue{}, Snapshots: map[string]*Snapshot{},
	}

	for _, k := range keyNames {
		s.Keys = append(s.Keys, newAccessKey(k.name, k.readOnly, now))
	}

	return s
}

func (b *InMemoryBackend) storeLocked(name string) (*store, error) {
	s, ok := b.stores[strs.Fold(name)]
	if !ok {
		return nil, ErrStoreNotFound
	}

	return s, nil
}

func (s *store) bump() { s.Seq++ }

// PutStore creates a store, or returns the existing one unchanged.
func (b *InMemoryBackend) PutStore(name string) (StoreInfo, error) {
	if !ValidStoreName(name) {
		return StoreInfo{}, ErrInvalidName
	}

	b.mu.Lock("PutStore")
	defer b.mu.Unlock()

	s, ok := b.stores[strs.Fold(name)]
	if !ok {
		s = newStore(strs.Fold(name), b.now())
		b.stores[s.Name] = s
	}

	return StoreInfo{Name: s.Name, Created: s.Created}, nil
}

// EnsureStore creates the store when missing; it accepts any non-empty name.
func (b *InMemoryBackend) EnsureStore(name string) error {
	if name == "" || len(name) > maxNameLen {
		return ErrInvalidName
	}

	b.mu.Lock("EnsureStore")
	defer b.mu.Unlock()

	if _, ok := b.stores[strs.Fold(name)]; !ok {
		s := newStore(strs.Fold(name), b.now())
		b.stores[s.Name] = s
	}

	return nil
}

// HasStore reports whether the store exists.
func (b *InMemoryBackend) HasStore(name string) bool {
	b.mu.RLock("HasStore")
	defer b.mu.RUnlock()

	_, ok := b.stores[strs.Fold(name)]

	return ok
}

// Store returns the ARM-visible view of a store.
func (b *InMemoryBackend) Store(name string) (StoreInfo, error) {
	b.mu.RLock("Store")
	defer b.mu.RUnlock()

	s, err := b.storeLocked(name)
	if err != nil {
		return StoreInfo{}, err
	}

	return StoreInfo{Name: s.Name, Created: s.Created}, nil
}

// DeleteStore removes a store with all its key-values and snapshots.
func (b *InMemoryBackend) DeleteStore(name string) error {
	b.mu.Lock("DeleteStore")
	defer b.mu.Unlock()

	if _, err := b.storeLocked(name); err != nil {
		return err
	}

	delete(b.stores, strs.Fold(name))

	return nil
}

// AccessKeys returns the store's four credentials.
func (b *InMemoryBackend) AccessKeys(name string) ([]AccessKey, error) {
	b.mu.RLock("AccessKeys")
	defer b.mu.RUnlock()

	s, err := b.storeLocked(name)
	if err != nil {
		return nil, err
	}

	return append([]AccessKey(nil), s.Keys...), nil
}

// RegenerateKey replaces the secret (and id) of the credential with the given id.
func (b *InMemoryBackend) RegenerateKey(name, keyID string) (AccessKey, error) {
	b.mu.Lock("RegenerateKey")
	defer b.mu.Unlock()

	s, err := b.storeLocked(name)
	if err != nil {
		return AccessKey{}, err
	}

	for i, k := range s.Keys {
		if k.ID == keyID {
			s.Keys[i] = newAccessKey(k.Name, k.ReadOnly, b.now())

			return s.Keys[i], nil
		}
	}

	return AccessKey{}, ErrKeyIDNotFound
}

// KeyOwner finds the credential with the given id across all stores.
func (b *InMemoryBackend) KeyOwner(id string) (string, AccessKey, bool) {
	b.mu.RLock("KeyOwner")
	defer b.mu.RUnlock()

	for _, s := range b.stores {
		for _, k := range s.Keys {
			if k.ID == id {
				return s.Name, k, true
			}
		}
	}

	return "", AccessKey{}, false
}

// SyncToken returns the store's current sync token.
func (b *InMemoryBackend) SyncToken(name string) string {
	b.mu.RLock("SyncToken")
	defer b.mu.RUnlock()

	var seq int64

	if s, err := b.storeLocked(name); err == nil {
		seq = s.Seq
	}

	return formatSyncToken(name, seq)
}

func formatSyncToken(name string, seq int64) string {
	sum := sha256.Sum256([]byte(strs.Fold(name)))
	id := hex.EncodeToString(sum[:4])
	val := base64.StdEncoding.EncodeToString([]byte(id))

	return "gs" + id + "=" + val + ";sn=" + itoa(seq)
}
