package ssm

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// idempotencyRecord remembers the request fingerprint and created resource ID
// for one ClientToken.
type idempotencyRecord struct {
	Fingerprint string `json:"fingerprint"`
	ID          string `json:"id"`
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

	if b.idempotency[region] == nil {
		b.idempotency[region] = make(map[string]idempotencyRecord)
	}

	b.idempotency[region][op+"|"+token] = idempotencyRecord{Fingerprint: idempotencyFingerprint(req), ID: id}
}
