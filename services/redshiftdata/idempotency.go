package redshiftdata

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/idempotency"
)

// clientTokenTTL is how long a ClientToken replays its statement Id; matches the scheduler's window.
const clientTokenTTL = 5 * time.Minute

const idempotencyEntries = 1024

func newIdempotencyMemo() *idempotency.Memo {
	return idempotency.New("redshiftdata", idempotency.WithLimits(clientTokenTTL, idempotencyEntries))
}

// clientTokenKey scopes a ClientToken to its operation and region; "" when no token was supplied.
func clientTokenKey(op, region, clientToken string) string {
	if clientToken == "" {
		return ""
	}

	return op + ":" + region + ":" + clientToken
}

func (h *Handler) lookupIdempotentStatement(key string) (string, bool) {
	id, ok, _ := h.idem.Lookup("redshiftdata", key, "")

	return id, ok
}

func (h *Handler) storeIdempotentStatement(key, id string) {
	h.idem.Record("redshiftdata", key, "", id)
}
