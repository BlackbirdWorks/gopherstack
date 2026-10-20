package gcpiam

import (
	"encoding/binary"
	"time"

	"cloud.google.com/go/iam/admin/apiv1/adminpb"
	"cloud.google.com/go/iam/apiv1/iampb"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	defaultAccountPage = 20
	maxAccountPage     = 100
	defaultRolePage    = 300
	defaultPermPage    = 100
	maxBigPage         = 1000
	maxUserKeys        = 10
	accountUndeleteTTL = 30 * 24 * time.Hour
	roleUndeleteTTL    = 7 * 24 * time.Hour
	rolePurgeAfter     = 37 * 24 * time.Hour
	etagLen            = 8
)

// keyRec holds one key's public metadata and material; private is set only for system-managed keys.
type keyRec struct {
	pb      *adminpb.ServiceAccountKey
	cert    []byte
	public  []byte
	private []byte
}

type accountRec struct {
	pb        *adminpb.ServiceAccount
	keys      map[string]*keyRec
	system    *keyRec
	policy    *iampb.Policy
	deletedAt time.Time
}

type roleRec struct {
	pb        *adminpb.Role
	deletedAt time.Time
}

// InMemoryBackend holds service accounts, keys, custom roles and policies under one lock.
type InMemoryBackend struct {
	now      func() time.Time
	mu       *lockmetrics.RWMutex
	accounts map[string]*accountRec
	roles    map[string]*roleRec
	etagSeq  uint64
}

// NewInMemoryBackend creates an empty backend.
func NewInMemoryBackend() *InMemoryBackend {
	b := &InMemoryBackend{mu: lockmetrics.New("gcpiam"), now: time.Now}
	b.resetMaps()

	return b
}

func (b *InMemoryBackend) resetMaps() {
	b.accounts = make(map[string]*accountRec)
	b.roles = make(map[string]*roleRec)
}

// Reset clears all state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.resetMaps()
	b.etagSeq = 0
}

func (b *InMemoryBackend) nextEtag() []byte {
	b.etagSeq++

	raw := make([]byte, etagLen)
	binary.BigEndian.PutUint64(raw, b.etagSeq)

	return raw
}

func pageSizeOf(n int32, maxSize int) (int, error) {
	if n < 0 {
		return 0, errInvalid("page_size must not be negative.")
	}

	return min(int(n), maxSize), nil
}
