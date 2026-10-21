package azureeventgrid

import (
	"encoding/json"
	"fmt"
	"time"
)

func decodeFields(raw []byte) map[string]any {
	var m map[string]any
	_ = json.Unmarshal(raw, &m)

	return m
}

func deadLetterPayload(item deadItem) ([]byte, error) {
	m := decodeFields(item.d.Event)
	if m == nil {
		m = map[string]any{}
	}

	m["deadLetterReason"] = item.reason
	m["deliveryAttempts"] = item.d.Attempts
	m["lastDeliveryOutcome"] = item.d.LastOutcome
	m["publishTime"] = item.d.Published.UTC().Format(time.RFC3339Nano)
	m["lastDeliveryAttemptTime"] = item.d.LastAttempt.UTC().Format(time.RFC3339Nano)

	return json.Marshal(m)
}

func blobName(item deadItem, at time.Time) string {
	at = at.UTC()

	return fmt.Sprintf("%s/%s/%04d/%02d/%02d/%02d/%s.json",
		item.job.topic, item.job.sub, at.Year(), int(at.Month()), at.Day(), at.Hour(), item.d.ID)
}

// deadLetter writes the event to the subscription's dead-letter blob container,
// if one is configured; otherwise the event is dropped.
func (b *InMemoryBackend) deadLetter(item deadItem) {
	dl := item.job.spec.DeadLetterDestination
	if dl == nil {
		return
	}

	payload, err := deadLetterPayload(item)
	if err != nil {
		return
	}

	account := resourceSegment(dl.str(propResourceID), "storageAccounts")
	_ = b.targetsSnapshot().
		PutBlob(account, dl.str(propContainer), blobName(item, b.now()), payload, "application/json")
}
