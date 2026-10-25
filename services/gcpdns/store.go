package gcpdns

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"strings"
	"time"

	"github.com/miekg/dns"
	dnsapi "google.golang.org/api/dns/v1"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	timeLayout   = "2006-01-02T15:04:05.000Z"
	idFloor      = 1_000_000_000_000_000_000
	idSpan       = 8_000_000_000_000_000_000
	maxZones     = 10000
	maxRRSets    = 10000
	maxChangeOps = 1000
	maxChangeLen = 100000
	maxRRDatas   = 10000
	maxRouting   = 100
	maxLabels    = 64
	maxLabelLen  = 128
)

type zoneRec struct {
	Zone    *dnsapi.ManagedZone                  `json:"zone"`
	RRSets  map[string]*dnsapi.ResourceRecordSet `json:"rrsets"`
	Changes []*dnsapi.Change                     `json:"changes"`
	Ops     []*dnsapi.Operation                  `json:"ops"`
	Keys    []*dnsapi.DnsKey                     `json:"keys"`
	Project string                               `json:"project"`
}

type responseRec struct {
	Policy *dnsapi.ResponsePolicy                `json:"policy"`
	Rules  map[string]*dnsapi.ResponsePolicyRule `json:"rules"`
}

// InMemoryBackend holds all Cloud DNS state under one lock.
type InMemoryBackend struct {
	now      func() time.Time
	mu       *lockmetrics.RWMutex
	zones    map[string]*zoneRec
	policies map[string]*dnsapi.Policy
	respPols map[string]*responseRec
}

// NewInMemoryBackend creates an empty backend.
func NewInMemoryBackend() *InMemoryBackend {
	b := &InMemoryBackend{mu: lockmetrics.New("gcpdns"), now: time.Now}
	b.resetMaps()

	return b
}

func (b *InMemoryBackend) resetMaps() {
	b.zones = map[string]*zoneRec{}
	b.policies = map[string]*dnsapi.Policy{}
	b.respPols = map[string]*responseRec{}
}

// Reset removes all state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.resetMaps()
}

func (b *InMemoryBackend) stamp() string { return b.now().UTC().Format(timeLayout) }

func key(project, name string) string { return project + "/" + name }

func newID() uint64 {
	var buf [8]byte

	_, _ = rand.Read(buf[:])

	return idFloor + binary.BigEndian.Uint64(buf[:])%idSpan
}

func canon(name string) string { return strings.ToLower(dns.Fqdn(name)) }

func clone[T any](v *T) *T {
	if v == nil {
		return nil
	}

	data, err := json.Marshal(v)
	if err != nil {
		return v
	}

	out := new(T)
	if err = json.Unmarshal(data, out); err != nil {
		return v
	}

	return out
}

func cloneAll[T any](in []*T) []*T {
	out := make([]*T, len(in))
	for i, v := range in {
		out[i] = clone(v)
	}

	return out
}

// mergePatch applies a JSON merge patch (RFC 7386) to dst.
func mergePatch(dst, patch map[string]any) {
	for k, v := range patch {
		if v == nil {
			delete(dst, k)

			continue
		}

		pm, isMap := v.(map[string]any)
		dm, dIsMap := dst[k].(map[string]any)

		if isMap && dIsMap {
			mergePatch(dm, pm)

			continue
		}

		dst[k] = v
	}
}

func patched[T any](cur *T, patch map[string]any) (*T, error) {
	data, err := json.Marshal(cur)
	if err != nil {
		return nil, errInvalid("Invalid JSON payload received.")
	}

	var m map[string]any
	if err = json.Unmarshal(data, &m); err != nil {
		return nil, errInvalid("Invalid JSON payload received.")
	}

	mergePatch(m, patch)

	data, err = json.Marshal(m)
	if err != nil {
		return nil, errInvalid("Invalid JSON payload received.")
	}

	out := new(T)
	if err = json.Unmarshal(data, out); err != nil {
		return nil, errInvalid("Invalid JSON payload received: %v", err)
	}

	return out, nil
}

func isDomain(name string) bool {
	_, ok := dns.IsDomainName(name)

	return ok
}
