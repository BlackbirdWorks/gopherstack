package azureeventgrid

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

func decodeFields(raw []byte) map[string]any {
	var m map[string]any
	_ = json.Unmarshal(raw, &m, jsonx.V1())

	return m
}

const (
	deadLetterRetryInterval = 5 * time.Minute
	deadLetterGiveUp        = 4 * time.Hour
)

// deadRetry is a dead-letter blob write that failed and is retried until deadLetterGiveUp has passed.
type deadRetry struct {
	Queued      time.Time      `json:"queued"`
	Due         time.Time      `json:"due"`
	Destination Destination    `json:"destination"`
	Blob        string         `json:"blob"`
	Payload     jsontext.Value `json:"payload"`
}

// deadLetterPayload is the blob body: an array holding the event plus the dead-letter fields, with lower-case
// names for CloudEvents (Learn: event-grid/delivery-and-retry, "Delivery event formats").
func deadLetterPayload(item deadItem) ([]byte, error) {
	m := decodeFields(item.d.Event)
	if m == nil {
		m = map[string]any{}
	}

	published := item.d.Published.UTC().Format(time.RFC3339Nano)

	if item.job.spec.EventDeliverySchema == SchemaCloudEvent {
		m["deadletterreason"] = item.reason
		m["deliveryattempts"] = item.d.Attempts
		m["lastdeliveryoutcome"] = item.d.LastOutcome
		m["publishtime"] = published
	} else {
		m["deadLetterReason"] = item.reason
		m["deliveryAttempts"] = item.d.Attempts
		m["lastDeliveryOutcome"] = item.d.LastOutcome
		m["publishTime"] = published
		m["lastDeliveryAttemptTime"] = item.d.LastAttempt.UTC().Format(time.RFC3339Nano)
	}

	return json.Marshal([]any{m}, jsonx.V1())
}

// blobName is <SUBSCRIPTION>/<YYYY>/<M>/<D>/<H>/<id>.json with the subscription upper-cased and the date
// unpadded (Learn: event-grid/manage-event-delivery, "Set dead-letter location").
func blobName(item deadItem, at time.Time) string {
	at = at.UTC()

	return fmt.Sprintf("%s/%d/%d/%d/%d/%s.json",
		strings.ToUpper(item.job.sub), at.Year(), int(at.Month()), at.Day(), at.Hour(), item.d.ID)
}

func (b *InMemoryBackend) putDeadLetter(dl *Destination, blob string, payload []byte) error {
	account := resourceSegment(dl.str(propResourceID), "storageAccounts")

	return b.targetsSnapshot().PutBlob(account, dl.str(propContainer), blob, payload, "application/json")
}

// deadLetter writes the event to the subscription's dead-letter blob container, if one is configured; a failed
// write is retried until deadLetterGiveUp has passed, then the event is dropped.
func (b *InMemoryBackend) deadLetter(item deadItem) {
	dl := item.job.spec.DeadLetterDestination
	if dl == nil {
		return
	}

	payload, err := deadLetterPayload(item)
	if err != nil {
		return
	}

	blob := blobName(item, b.now())
	if b.putDeadLetter(dl, blob, payload) == nil {
		return
	}

	now := b.now()

	b.mu.Lock("deadLetterRetry")
	b.deadRetries = append(b.deadRetries, deadRetry{
		Queued: now, Due: now.Add(deadLetterRetryInterval), Destination: *dl, Blob: blob, Payload: payload,
	})
	b.mu.Unlock()
}

// retryDeadLetters re-attempts the queued blob writes that are due.
func (b *InMemoryBackend) retryDeadLetters() {
	now := b.now()

	b.mu.Lock("retryDeadLetters")
	due := make([]deadRetry, 0, len(b.deadRetries))
	keep := make([]deadRetry, 0, len(b.deadRetries))

	for _, r := range b.deadRetries {
		if r.Due.After(now) {
			keep = append(keep, r)
		} else {
			due = append(due, r)
		}
	}

	b.deadRetries = keep
	b.mu.Unlock()

	for _, r := range due {
		if b.putDeadLetter(&r.Destination, r.Blob, r.Payload) == nil || now.Sub(r.Queued) >= deadLetterGiveUp {
			continue
		}

		r.Due = now.Add(deadLetterRetryInterval)

		b.mu.Lock("retryDeadLetters")
		b.deadRetries = append(b.deadRetries, r)
		b.mu.Unlock()
	}
}
