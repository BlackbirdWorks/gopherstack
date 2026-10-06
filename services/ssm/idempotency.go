package ssm

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"
)

// maxIdempotencyRecords caps remembered tokens per region; the oldest are evicted first.
const maxIdempotencyRecords = 1024

// idempotencyRecord remembers the request fingerprint and created resource ID
// for one ClientToken.
type idempotencyRecord struct {
	CreatedAt   time.Time `json:"createdAt,omitzero"`
	Fingerprint string    `json:"fingerprint"`
	ID          string    `json:"id"`
}

func idempotencyFingerprint(req any) string {
	raw, err := json.Marshal(req)
	if err != nil {
		return ""
	}

	var m map[string]any
	if json.Unmarshal(raw, &m) != nil {
		return ""
	}

	delete(m, "ClientToken")

	canonical, err := json.Marshal(m)
	if err != nil {
		return ""
	}

	sum := sha256.Sum256(canonical)

	return hex.EncodeToString(sum[:])
}

// idempotentReplayLocked returns the resource ID a repeated ClientToken maps to, or
// ErrIdempotentParameterMismatch; a token whose resource is gone counts as unused. Caller holds b.mu.
func (b *InMemoryBackend) idempotentReplayLocked(
	region, op, token string, req any, exists func(id string) bool,
) (string, bool, error) {
	if token == "" {
		return "", false, nil
	}

	rec, ok := b.idempotency[region][op+"|"+token]
	if !ok || !exists(rec.ID) {
		return "", false, nil
	}

	if rec.Fingerprint != idempotencyFingerprint(req) {
		return "", false, fmt.Errorf(
			"%w: ClientToken %q was used with different parameters", ErrIdempotentParameterMismatch, token,
		)
	}

	return rec.ID, true, nil
}

func (b *InMemoryBackend) recordIdempotentLocked(region, op, token string, req any, id string) {
	if token == "" {
		return
	}

	recs := b.idempotency[region]
	if recs == nil {
		recs = make(map[string]idempotencyRecord)
		b.idempotency[region] = recs
	}

	key := op + "|" + token
	if _, replaced := recs[key]; !replaced {
		evictOldestIdempotency(recs, maxIdempotencyRecords-1)
	}

	recs[key] = idempotencyRecord{CreatedAt: timeNow(), Fingerprint: idempotencyFingerprint(req), ID: id}
}

func evictOldestIdempotency(recs map[string]idempotencyRecord, keep int) {
	for len(recs) > keep {
		var oldestKey string

		var oldest time.Time

		for k, r := range recs {
			if oldestKey == "" || r.CreatedAt.Before(oldest) {
				oldestKey, oldest = k, r.CreatedAt
			}
		}

		delete(recs, oldestKey)
	}
}
