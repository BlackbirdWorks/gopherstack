package scheduler

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/idempotency"
)

// clientTokenTTL bounds how long a successful CreateSchedule/CreateScheduleGroup
// response is cached for idempotent replay by ClientToken. Real EventBridge
// Scheduler auto-generates a ClientToken when the caller omits one specifically so
// that an SDK's built-in retry-after-lost-response logic can safely re-send the
// same Create* request; a several-minute window covers that retry scenario without
// caching results indefinitely.
const clientTokenTTL = 5 * time.Minute

const idempotencyEntries = 1024

func newIdempotencyMemo() *idempotency.Memo {
	return idempotency.New("scheduler", idempotency.WithLimits(clientTokenTTL, idempotencyEntries))
}

// clientTokenKey scopes a ClientToken to the operation kind and the resource name
// (and, for schedules, group) it targeted, so the cache can't replay the wrong
// resource's ARN if the same ClientToken were ever reused for two different
// resources (not expected in practice -- SDKs generate a random token per call --
// but not disallowed by the API either).
func clientTokenKey(kind, groupName, name, clientToken string) string {
	if clientToken == "" {
		return ""
	}

	return kind + ":" + groupName + "/" + name + ":" + clientToken
}

// lookupIdempotent returns the ARN cached for key; a blank key always misses.
func (h *Handler) lookupIdempotent(key string) (string, bool) {
	arn, ok, _ := h.idem.Lookup("scheduler", key, "")

	return arn, ok
}

// storeIdempotent caches arn under key; a blank key is a no-op.
func (h *Handler) storeIdempotent(key, arn string) { h.idem.Record("scheduler", key, "", arn) }
