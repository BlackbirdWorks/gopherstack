package azureeventgrid

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const storageAccountScope = "/providers/microsoft.storage/storageaccounts/"

// StorageEvent is a Blob Storage event (Learn: event-grid/event-schema-blob-storage).
type StorageEvent struct {
	Time            time.Time
	Account         string
	Container       string
	Blob            string
	EventType       string
	API             string
	ContentType     string
	ETag            string
	AccessTier      string
	RequestID       string
	ClientRequestID string
	BlobURL         string
	ContentLength   int64
}

func (e StorageEvent) fields(id string, seq uint64) map[string]any {
	data := map[string]any{
		"api": e.API, "requestId": e.RequestID, "contentType": e.ContentType, "blobType": "BlockBlob",
		"url":                e.BlobURL,
		"sequencer":          fmt.Sprintf("%032X", seq),
		"storageDiagnostics": map[string]any{"batchId": uuid.NewString()},
	}

	if e.ClientRequestID != "" {
		data["clientRequestId"] = e.ClientRequestID
	}

	if e.EventType == "Microsoft.Storage.BlobCreated" {
		data["eTag"], data["contentLength"] = e.ETag, e.ContentLength

		if e.AccessTier != "" {
			data["accessTier"] = e.AccessTier
		}
	}

	return map[string]any{
		"id":             id,
		fieldSubject:     "/blobServices/default/containers/" + e.Container + "/blobs/" + e.Blob,
		fieldEventType:   e.EventType,
		fieldEventTime:   e.Time.UTC().Format(time.RFC3339Nano),
		fieldData:        data,
		fieldDataVersion: "",
	}
}

// PublishStorageEvent delivers a blob event to the event subscriptions of the storage account's resource scope.
func (b *InMemoryBackend) PublishStorageEvent(ev StorageEvent) {
	suffix := storageAccountScope + strs.Fold(ev.Account)

	b.mu.Lock("PublishStorageEvent")
	defer b.mu.Unlock()

	now := b.now().UTC()
	queued := false

	for key, a := range b.armScopes {
		if !strings.HasSuffix(key, suffix) || len(a.Subs) == 0 {
			continue
		}

		b.storageSeq++

		id := uuid.NewString()
		e := Event{Fields: ev.fields(id, b.storageSeq), ID: id}

		if err := e.stamp(a.ID, SchemaEventGrid); err != nil {
			continue
		}

		for _, s := range a.Subs {
			s.enqueue(e, now)
		}

		queued = true
	}

	if queued {
		select {
		case b.kick <- struct{}{}:
		default:
		}
	}
}
