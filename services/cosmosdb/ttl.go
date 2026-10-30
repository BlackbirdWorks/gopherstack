package cosmosdb

import (
	"fmt"
	"math"
	"time"
)

const attrTTL = "ttl"

func validateDefaultTTL(ttl *int) error {
	if ttl != nil && (*ttl == 0 || *ttl < -1 || *ttl > math.MaxInt32) {
		return ErrInvalidDefaultTTL
	}

	return nil
}

// validateItemTTL rejects a per-item ttl that is neither -1 nor a positive integer.
func validateItemTTL(body map[string]any) error {
	raw, ok := body[attrTTL]
	if !ok {
		return nil
	}

	n, isNum := toNumber(raw)
	if !isNum {
		return fmt.Errorf("%w: ttl must be -1 or a positive integer", ErrInvalidDocument)
	}

	i, err := n.Int64()
	if err != nil || i == 0 || i < -1 || i > math.MaxInt32 {
		return fmt.Errorf("%w: ttl must be -1 or a positive integer", ErrInvalidDocument)
	}

	return nil
}

// expiry returns when d expires. A container without defaultTtl ignores item ttl; -1 means never.
func (c *storedContainer) expiry(d *storedDocument) (time.Time, bool) {
	if c.DefaultTTL == nil {
		return time.Time{}, false
	}

	ttl := int64(*c.DefaultTTL)

	if raw, ok := d.Body[attrTTL]; ok {
		if n, isNum := toNumber(raw); isNum {
			if i, err := n.Int64(); err == nil {
				ttl = i
			}
		}
	}

	if ttl <= 0 {
		return time.Time{}, false
	}

	return d.Timestamp.Add(time.Duration(ttl) * time.Second), true
}

func (c *storedContainer) alive(d *storedDocument, now time.Time) bool {
	exp, ok := c.expiry(d)

	return !ok || now.Before(exp)
}

// lookup returns the live document at key; an expired one reads as absent.
func (c *storedContainer) lookup(key documentCompositeKey, now time.Time) (*storedDocument, bool) {
	d, ok := c.Documents[key]
	if !ok || !c.alive(d, now) {
		return nil, false
	}

	return d, true
}

// liveDocs returns the sorted live documents. With TTL off it is the cached read-only slice.
func (c *storedContainer) liveDocs(now time.Time) []*storedDocument {
	docs := c.sortedDocs()
	if c.DefaultTTL == nil {
		return docs
	}

	out := make([]*storedDocument, 0, len(docs))

	for _, d := range docs {
		if c.alive(d, now) {
			out = append(out, d)
		}
	}

	return out
}

// SweepExpired physically removes expired documents and returns how many were removed.
func (b *InMemoryBackend) SweepExpired() int {
	b.mu.Lock("SweepExpired")
	defer b.mu.Unlock()

	now := b.now()
	removed := 0

	for _, db := range b.databases {
		for _, c := range db.Containers {
			if c.DefaultTTL == nil {
				continue
			}

			before := removed
			c.keep = b.changeRetention()

			for _, d := range c.sortedDocs() {
				if !c.alive(d, now) {
					c.recordExpired(d, now)
					delete(c.Documents, documentCompositeKey{PartitionKeyJSON: d.PartitionKeyJSON, ID: d.ID})

					removed++
				}
			}

			if removed > before {
				c.sorted.Store(nil)
			}
		}
	}

	return removed
}
