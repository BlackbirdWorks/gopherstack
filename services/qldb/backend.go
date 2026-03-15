package qldb

import (
	"fmt"
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	// stateActive is the active state for a ledger.
	stateActive = "ACTIVE"

	// defaultPermissionsMode is the default QLDB permissions mode.
	defaultPermissionsMode = "ALLOW_ALL"
)

var (
	// ErrNotFound is returned when a ledger does not exist.
	ErrNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a ledger already exists.
	ErrAlreadyExists = awserr.New("ResourceAlreadyExistsException", awserr.ErrConflict)
	// ErrDeletionProtection is returned when deletion protection is enabled.
	ErrDeletionProtection = awserr.New("ResourcePreconditionNotMetException", awserr.ErrConflict)
)

// Ledger represents a QLDB ledger.
type Ledger struct {
	CreationDateTime  time.Time         `json:"creationDateTime"`
	Tags              map[string]string `json:"tags,omitempty"`
	Name              string            `json:"name"`
	ARN               string            `json:"arn"`
	State             string            `json:"state"`
	PermissionsMode   string            `json:"permissionsMode"`
	AccountID         string            `json:"accountID"`
	Region            string            `json:"region"`
	DeletionProtected bool              `json:"deletionProtected"`
}

// cloneLedger returns a deep copy of l with the Tags map cloned.
func cloneLedger(l *Ledger) *Ledger {
	cp := *l
	cp.Tags = maps.Clone(l.Tags)

	return &cp
}

// InMemoryBackend is an in-memory QLDB backend.
type InMemoryBackend struct {
	ledgers        map[string]*Ledger
	ledgerARNIndex map[string]string // ARN → ledger name
	mu             *lockmetrics.RWMutex
	accountID      string
	region         string
}

// NewInMemoryBackend creates a new in-memory QLDB backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		ledgers:        make(map[string]*Ledger),
		ledgerARNIndex: make(map[string]string),
		accountID:      accountID,
		region:         region,
		mu:             lockmetrics.New("qldb"),
	}
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// CreateLedger creates a new ledger.
func (b *InMemoryBackend) CreateLedger(
	name, permissionsMode string,
	deletionProtected bool,
	tags map[string]string,
) (*Ledger, error) {
	b.mu.Lock("CreateLedger")
	defer b.mu.Unlock()

	if _, ok := b.ledgers[name]; ok {
		return nil, fmt.Errorf("%w: ledger %s already exists", ErrAlreadyExists, name)
	}

	if permissionsMode == "" {
		permissionsMode = defaultPermissionsMode
	}

	ledgerARN := arn.Build("qldb", b.region, b.accountID, "ledger/"+name)
	l := &Ledger{
		Name:              name,
		ARN:               ledgerARN,
		State:             stateActive,
		PermissionsMode:   permissionsMode,
		DeletionProtected: deletionProtected,
		AccountID:         b.accountID,
		Region:            b.region,
		CreationDateTime:  time.Now(),
		Tags:              mergeTags(nil, tags),
	}
	b.ledgers[name] = l
	b.ledgerARNIndex[ledgerARN] = name

	return cloneLedger(l), nil
}

// GetLedger returns a ledger by name.
func (b *InMemoryBackend) GetLedger(name string) (*Ledger, error) {
	b.mu.RLock("GetLedger")
	defer b.mu.RUnlock()

	l, ok := b.ledgers[name]
	if !ok {
		return nil, fmt.Errorf("%w: ledger %s not found", ErrNotFound, name)
	}

	return cloneLedger(l), nil
}

// ListLedgers returns all ledgers.
func (b *InMemoryBackend) ListLedgers() []*Ledger {
	b.mu.RLock("ListLedgers")
	defer b.mu.RUnlock()

	list := make([]*Ledger, 0, len(b.ledgers))
	for _, l := range b.ledgers {
		list = append(list, cloneLedger(l))
	}

	return list
}

// UpdateLedger updates an existing ledger's deletion protection setting.
func (b *InMemoryBackend) UpdateLedger(name string, deletionProtected bool) (*Ledger, error) {
	b.mu.Lock("UpdateLedger")
	defer b.mu.Unlock()

	l, ok := b.ledgers[name]
	if !ok {
		return nil, fmt.Errorf("%w: ledger %s not found", ErrNotFound, name)
	}

	l.DeletionProtected = deletionProtected

	return cloneLedger(l), nil
}

// DeleteLedger deletes a ledger by name.
func (b *InMemoryBackend) DeleteLedger(name string) error {
	b.mu.Lock("DeleteLedger")
	defer b.mu.Unlock()

	l, ok := b.ledgers[name]
	if !ok {
		return fmt.Errorf("%w: ledger %s not found", ErrNotFound, name)
	}

	if l.DeletionProtected {
		return fmt.Errorf("%w: ledger %s has deletion protection enabled", ErrDeletionProtection, name)
	}

	delete(b.ledgers, name)
	delete(b.ledgerARNIndex, l.ARN)

	return nil
}

// TagResource adds or updates tags on a ledger identified by ARN.
func (b *InMemoryBackend) TagResource(resourceARN string, kv map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	name, ok := b.ledgerARNIndex[resourceARN]
	if !ok {
		return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}

	l := b.ledgers[name]
	l.Tags = mergeTags(l.Tags, kv)

	return nil
}

// UntagResource removes specified tag keys from a ledger.
func (b *InMemoryBackend) UntagResource(resourceARN string, keys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	name, ok := b.ledgerARNIndex[resourceARN]
	if !ok {
		return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}

	l := b.ledgers[name]

	for _, k := range keys {
		delete(l.Tags, k)
	}

	return nil
}

// ListTagsForResource returns tags for a ledger identified by ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	name, ok := b.ledgerARNIndex[resourceARN]
	if !ok {
		return nil, fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}

	l := b.ledgers[name]
	result := make(map[string]string, len(l.Tags))
	maps.Copy(result, l.Tags)

	return result, nil
}

// mergeTags merges new tags into existing ones, returning a new map.
func mergeTags(existing, incoming map[string]string) map[string]string {
	result := make(map[string]string, len(existing)+len(incoming))
	maps.Copy(result, existing)
	maps.Copy(result, incoming)

	return result
}
