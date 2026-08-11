package redshiftdata

import "time"

// clientTokenTTL bounds how long a successful ExecuteStatement/
// BatchExecuteStatement response is cached for idempotent replay by
// ClientToken. ClientToken is documented as ensuring "the idempotency of the
// request" (api_op_ExecuteStatement.go / api_op_BatchExecuteStatement.go),
// and the SDK client auto-generates one on the caller's behalf when omitted
// (see ExecuteStatementInput's idempotency-token defaulting in the same
// files) specifically so its own retry-after-lost-response logic is safe --
// this mirrors the identical trait already handled in
// services/scheduler/idempotency.go, whose 5-minute window this reuses.
const clientTokenTTL = 5 * time.Minute

// idempotentStatement caches a statement Id created by a ClientToken-bearing
// ExecuteStatement/BatchExecuteStatement call.
type idempotentStatement struct {
	expiresAt time.Time
	id        string
}

// clientTokenKey scopes a ClientToken to the operation and region it
// targeted, so a token reused across ExecuteStatement/BatchExecuteStatement
// or across regions can't replay the wrong statement. Returns "" (never a
// cache hit) when no token was supplied.
func clientTokenKey(op, region, clientToken string) string {
	if clientToken == "" {
		return ""
	}

	return op + ":" + region + ":" + clientToken
}

// lookupIdempotentStatement returns the cached statement Id for key, if
// present and unexpired. A blank key always misses.
func (h *Handler) lookupIdempotentStatement(key string) (string, bool) {
	if key == "" {
		return "", false
	}

	res, ok := h.idempotency.Get(key)
	if !ok {
		return "", false
	}

	if time.Now().After(res.expiresAt) {
		h.idempotency.Delete(key)

		return "", false
	}

	return res.id, true
}

// storeIdempotentStatement caches id under key for clientTokenTTL. A blank
// key (no ClientToken supplied) is a no-op.
func (h *Handler) storeIdempotentStatement(key, id string) {
	if key == "" {
		return
	}

	h.idempotency.Set(key, idempotentStatement{id: id, expiresAt: time.Now().Add(clientTokenTTL)})
}
