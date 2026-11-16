package app

import (
	"maps"
	"net/http"
	"strings"
	"sync/atomic"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	cebackend "github.com/blackbirdworks/gopherstack/services/ce"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
)

type s3RequestKey struct {
	bucket, operation string
	tier1             bool
}

type retiredKey struct {
	region, operation string
	tier1             bool
}

type s3RequestCounter struct {
	region string
	n      atomic.Int64
}

// s3RequestMeter counts bucket requests lock-free; region and tags are resolved when the counts are drained.
type s3RequestMeter struct {
	mem           *s3backend.InMemoryBackend
	lock          *lockmetrics.RWMutex
	resourceLevel func() bool
	retired       map[retiredKey]int64
	counters      atomic.Pointer[map[s3RequestKey]*s3RequestCounter]
}

func (m *s3RequestMeter) observe(ev s3backend.RequestEvent) {
	if ev.Operation == "DeleteBucket" && ev.Status < http.StatusMultipleChoices {
		m.retire(ev.Bucket)

		return
	}

	if ev.Method == http.MethodDelete || ev.Status >= http.StatusInternalServerError {
		return
	}

	op := ev.Operation
	if op == "" || op == "Unknown" {
		op = ev.Method
	}

	k := s3RequestKey{
		bucket: ev.Bucket, operation: op,
		tier1: ev.Method == http.MethodPut || ev.Method == http.MethodPost ||
			(ev.Method == http.MethodGet && !ev.HasKey),
	}

	if cur := m.counters.Load(); cur != nil {
		if c, ok := (*cur)[k]; ok {
			c.n.Add(1)

			return
		}
	}

	if c := m.register(k); c != nil {
		c.n.Add(1)
	}
}

func (m *s3RequestMeter) register(k s3RequestKey) *s3RequestCounter {
	region, _, ok := m.mem.BucketMeta(k.bucket)
	if !ok {
		return nil
	}

	m.lock.Lock("register")
	defer m.lock.Unlock()

	var cur map[s3RequestKey]*s3RequestCounter
	if p := m.counters.Load(); p != nil {
		cur = *p
	}

	if c, found := cur[k]; found {
		return c
	}

	next := make(map[s3RequestKey]*s3RequestCounter, len(cur)+1)
	maps.Copy(next, cur)

	c := &s3RequestCounter{region: region}
	next[s3RequestKey{bucket: strings.Clone(k.bucket), operation: k.operation, tier1: k.tier1}] = c
	m.counters.Store(&next)

	return c
}

func (m *s3RequestMeter) retire(bucket string) {
	if m.resourceLevel != nil && m.resourceLevel() {
		return
	}

	m.lock.Lock("retire")
	defer m.lock.Unlock()

	p := m.counters.Load()
	if p == nil {
		return
	}

	next := make(map[s3RequestKey]*s3RequestCounter, len(*p))

	for k, c := range *p {
		if k.bucket != bucket {
			next[k] = c

			continue
		}

		if n := c.n.Swap(0); n > 0 {
			if m.retired == nil {
				m.retired = map[retiredKey]int64{}
			}

			m.retired[retiredKey{region: c.region, operation: k.operation, tier1: k.tier1}] += n
		}
	}

	m.counters.Store(&next)
}

func (m *s3RequestMeter) takeRetired() map[retiredKey]int64 {
	m.lock.Lock("takeRetired")
	defer m.lock.Unlock()

	out := m.retired
	m.retired = nil

	return out
}

func (m *s3RequestMeter) prune(dead []s3RequestKey) {
	m.lock.Lock("prune")
	defer m.lock.Unlock()

	p := m.counters.Load()
	if p == nil {
		return
	}

	next := maps.Clone(*p)
	for _, k := range dead {
		delete(next, k)
	}

	m.counters.Store(&next)
}

func (m *s3RequestMeter) drain() []cebackend.Usage {
	var (
		out  []cebackend.Usage
		dead []s3RequestKey
	)

	for rk, n := range m.takeRetired() {
		out = append(out, s3RequestUsage(s3RequestKey{operation: rk.operation, tier1: rk.tier1}, rk.region, nil, n))
	}

	p := m.counters.Load()
	if p == nil {
		return out
	}

	for k, c := range *p {
		n := c.n.Swap(0)
		region, tags, ok := m.mem.BucketMeta(k.bucket)

		if !ok {
			dead = append(dead, k)
			region, tags = c.region, nil
		}

		if n == 0 {
			continue
		}

		out = append(out, s3RequestUsage(k, region, tags, n))
	}

	if len(dead) > 0 {
		m.prune(dead)
	}

	return out
}

func s3RequestUsage(k s3RequestKey, region string, tags map[string]string, n int64) cebackend.Usage {
	usage, group := s3Tier2UsageType, "S3: Requests - Tier 2"
	if k.tier1 {
		usage, group = s3Tier1UsageType, "S3: Requests - Tier 1"
	}

	return cebackend.Usage{
		Service: ceServiceS3, PriceService: priceS3, UsageType: usage, Operation: k.operation, Region: region,
		ResourceID: k.bucket, ProductFamily: "API Request", UsageTypeGroup: group, Unit: unitRequests,
		PriceMatch: map[string]string{attrUsageType: usage}, Tags: tags, Quantity: float64(n),
	}
}
