package ocivault

import (
	"cmp"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/ociauth"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	// Region is the fixed region baked into emulated OCIDs.
	Region           = "us-ashburn-1"
	defaultListLimit = 100
	ocidTailLen      = 52
	prefixLen        = 13

	kmsMinDeletion    = 7 * 24 * time.Hour
	secretMinDeletion = 24 * time.Hour
	maxDeletion       = 30 * 24 * time.Hour
	deletionSlack     = 5 * time.Minute
)

// InMemoryBackend holds every vault, key and secret under one lock.
type InMemoryBackend struct {
	mu      *lockmetrics.RWMutex
	now     func() time.Time
	vaults  map[string]*Vault
	keys    map[string]*Key
	secrets map[string]*Secret
	Auth    *ociauth.Registry
	seq     int64
}

// NewInMemoryBackend creates an empty backend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		mu:      lockmetrics.New("ocivault"),
		now:     time.Now,
		vaults:  map[string]*Vault{},
		keys:    map[string]*Key{},
		secrets: map[string]*Secret{},
		Auth:    ociauth.NewRegistry(),
	}
}

// SetClock injects the time source used for deletion schedules and rotation.
func (b *InMemoryBackend) SetClock(now func() time.Time) {
	b.mu.Lock("SetClock")
	b.now = now
	b.mu.Unlock()
}

// Reset clears all state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	b.vaults, b.keys, b.secrets = map[string]*Vault{}, map[string]*Key{}, map[string]*Secret{}
	b.mu.Unlock()
	b.Auth.Reset()
}

func (b *InMemoryBackend) nowUTC() time.Time { return b.now().UTC() }

func (b *InMemoryBackend) nextSeq() int64 {
	b.seq++

	return b.seq
}

func newEtag() string { return uuid.NewString() }

func newOCID(kind, prefix string) string {
	id := "ocid1." + kind + ".oc1." + Region + "."
	if prefix != "" {
		id += prefix + "."
	}

	return id + awsid.NewN("", ocidTailLen)
}

func checkEtag(current, ifMatch string) *APIError {
	if ifMatch != "" && ifMatch != current {
		return errEtag()
	}

	return nil
}

// deletionTime resolves a requested deletion time against the allowed window (default: the maximum).
func (b *InMemoryBackend) deletionTime(req *time.Time, minWindow time.Duration) (time.Time, *APIError) {
	now := b.nowUTC()
	if req == nil {
		return now.Add(maxDeletion), nil
	}

	t := req.UTC()
	if t.Before(now.Add(minWindow-deletionSlack)) || t.After(now.Add(maxDeletion+deletionSlack)) {
		return t, errParam("timeOfDeletion must be between the minimum retention window and 30 days from now")
	}

	return t, nil
}

// ListOpts carries the shared list query parameters.
type ListOpts struct {
	SortBy string
	Order  string
	Page   string
	Limit  int
}

func (o ListOpts) desc(defaultDesc bool) bool {
	switch strings.ToUpper(o.Order) {
	case "ASC":
		return false
	case "DESC":
		return true
	}

	return defaultDesc
}

func paginate[T any](items []T, o ListOpts, byName func(T) string, byTime func(T) (time.Time, int64)) page.Page[T] {
	desc := o.desc(strings.ToUpper(o.SortBy) != "DISPLAYNAME" && strings.ToUpper(o.SortBy) != "NAME")

	slices.SortStableFunc(items, func(a, c T) int {
		var r int

		if sb := strings.ToUpper(o.SortBy); sb == "DISPLAYNAME" || sb == "NAME" {
			r = cmp.Compare(byName(a), byName(c))
		} else {
			ta, sa := byTime(a)
			tc, sc := byTime(c)
			r = cmp.Or(ta.Compare(tc), cmp.Compare(sa, sc))
		}

		if desc {
			return -r
		}

		return r
	})

	return page.New(items, o.Page, o.Limit, defaultListLimit)
}

func (b *InMemoryBackend) vaultByID(id string) (*Vault, *APIError) {
	v, ok := b.vaults[id]
	if !ok {
		return nil, errNotFound(id)
	}

	return v, nil
}

func (b *InMemoryBackend) activeVault(id string) (*Vault, *APIError) {
	v, aerr := b.vaultByID(id)
	if aerr != nil {
		return nil, aerr
	}

	if v.State != stateActive {
		return nil, errState("Vault " + id + " is in state " + v.State + "; the operation requires ACTIVE")
	}

	return v, nil
}

// VaultIDForHost resolves a per-vault endpoint label ("<prefix>") or OCID to a vault id; with no
// hint a lone non-deleted vault is used.
func (b *InMemoryBackend) VaultIDForHost(hint string) (string, *APIError) {
	b.mu.RLock("VaultIDForHost")
	defer b.mu.RUnlock()

	if hint != "" {
		if _, ok := b.vaults[hint]; ok {
			return hint, nil
		}

		for id, v := range b.vaults {
			if v.Prefix == hint {
				return id, nil
			}
		}

		return "", errNotFound("vault endpoint " + hint)
	}

	var found string

	for id, v := range b.vaults {
		if v.State == stateDeleted {
			continue
		}

		if found != "" {
			return "", errParam("Multiple vaults exist; address one via its management or crypto endpoint host")
		}

		found = id
	}

	if found == "" {
		return "", errNotFound("vault")
	}

	return found, nil
}
