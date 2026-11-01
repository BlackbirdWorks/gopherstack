package gcpresourcemanager

import (
	"crypto/rand"
	"strings"
	"sync/atomic"
	"time"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/protobuf/proto"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpiampolicy"
	"github.com/blackbirdworks/gopherstack/pkgs/gcplro"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	defaultListPage = 100
	numberDigits    = 12
	tagIDDigits     = 15
	maxIDTries      = 64
	opIDDigits      = 16
)

// InMemoryBackend holds Resource Manager and Service Usage state under one lock.
type InMemoryBackend struct {
	clock     atomic.Pointer[func() time.Time]
	mu        *lockmetrics.RWMutex
	org       orgRec
	projects  map[string]*projectRec
	folders   map[string]*folderRec
	tagKeys   map[string]*tagKeyRec
	tagValues map[string]*tagValueRec
	bindings  map[string]*bindingRec
	enabled   map[string]map[string]time.Time
	policies  *gcpiampolicy.Store
	lro       *gcplro.Store
	seed      string
	retention time.Duration
}

// NewInMemoryBackend creates a backend for the default organization, with no projects.
func NewInMemoryBackend() *InMemoryBackend { return NewBackend(DefaultSettings()) }

// NewBackend creates a backend with the organization and seed projects of s.
func NewBackend(s Settings) *InMemoryBackend {
	b := &InMemoryBackend{
		mu:        lockmetrics.New("gcpresourcemanager"),
		policies:  gcpiampolicy.NewStore(),
		lro:       gcplro.New("gcpresourcemanager"),
		retention: s.DeleteRetention,
		seed:      s.SeedProjects,
	}
	b.SetClock(time.Now)
	b.init(s)

	return b
}

func (b *InMemoryBackend) init(s Settings) {
	b.org = orgRec{ID: s.OrganizationID, Domain: s.Domain, Created: b.now()}
	b.projects, b.folders = map[string]*projectRec{}, map[string]*folderRec{}
	b.tagKeys, b.tagValues = map[string]*tagKeyRec{}, map[string]*tagValueRec{}
	b.bindings, b.enabled = map[string]*bindingRec{}, map[string]map[string]time.Time{}

	for id := range strings.SplitSeq(s.SeedProjects, ",") {
		if id = strings.TrimSpace(id); id != "" {
			b.createProjectLocked(id, id, "", nil)
		}
	}
}

// SetClock replaces the clock timestamps and the deletion retention derive from.
func (b *InMemoryBackend) SetClock(now func() time.Time) { b.clock.Store(&now) }

func (b *InMemoryBackend) now() time.Time { return (*b.clock.Load())().UTC() }

// Reset restores the seeded state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	seed := Settings{OrganizationID: b.org.ID, Domain: b.org.Domain, SeedProjects: b.seedIDsLocked()}
	b.policies.Reset()
	b.lro.Reset()
	b.init(seed)
}

func (b *InMemoryBackend) seedIDsLocked() string {
	return b.seed
}

func randDigits(n int) string {
	raw := make([]byte, n)
	_, _ = rand.Read(raw)

	out := make([]byte, n)
	for i, c := range raw {
		out[i] = '0' + c%10 //nolint:mnd // decimal digit
	}

	if out[0] == '0' {
		out[0] = '1'
	}

	return string(out)
}

func uniqueDigits(n int, taken func(string) bool) string {
	for range maxIDTries {
		if id := randDigits(n); !taken(id) {
			return id
		}
	}

	return randDigits(n + 1)
}

// opLocked records a finished long-running operation. Mutations are applied synchronously, so every
// operation is done when it is returned.
func (b *InMemoryBackend) opLocked(prefix string, meta, resp proto.Message) (*longrunningpb.Operation, error) {
	op, err := gcplro.Done("operations/"+prefix+"."+randDigits(opIDDigits), meta, resp)
	if err != nil {
		return nil, errInvalid("record operation: %v", err)
	}

	return b.lro.Put(op), nil
}

// purgeLocked drops projects and folders whose deletion retention has passed.
func (b *InMemoryBackend) purgeLocked() {
	now := b.now()

	for n, p := range b.projects {
		if p.requested() && !now.Before(p.Deleted.Add(b.retention)) {
			b.dropProjectLocked(n)
		}
	}

	for id, f := range b.folders {
		if f.requested() && !now.Before(f.Deleted.Add(b.retention)) {
			delete(b.folders, id)
			b.policies.Drop(kindFolders + "/" + id)
		}
	}
}

func (b *InMemoryBackend) dropProjectLocked(number string) {
	delete(b.projects, number)
	delete(b.enabled, number)
	b.policies.Drop(kindProjects + "/" + number)
}
