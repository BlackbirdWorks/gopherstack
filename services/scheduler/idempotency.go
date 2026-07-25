package scheduler

import "time"

// clientTokenTTL bounds how long a successful CreateSchedule/CreateScheduleGroup
// response is cached for idempotent replay by ClientToken. Real EventBridge
// Scheduler auto-generates a ClientToken when the caller omits one specifically so
// that an SDK's built-in retry-after-lost-response logic can safely re-send the
// same Create* request; a several-minute window covers that retry scenario without
// caching results indefinitely.
const clientTokenTTL = 5 * time.Minute

// idempotentResult is a cached successful Create*'s ARN, keyed by clientTokenKey.
type idempotentResult struct {
	expiresAt time.Time
	arn       string
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

// lookupIdempotent returns the cached ARN for key, if present and unexpired. A
// blank key (no ClientToken supplied) always misses.
func (h *Handler) lookupIdempotent(key string) (string, bool) {
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

	return res.arn, true
}

// storeIdempotent caches arn under key for clientTokenTTL. A blank key (no
// ClientToken supplied) is a no-op.
func (h *Handler) storeIdempotent(key, arn string) {
	if key == "" {
		return
	}

	h.idempotency.Set(key, idempotentResult{arn: arn, expiresAt: time.Now().Add(clientTokenTTL)})
}
