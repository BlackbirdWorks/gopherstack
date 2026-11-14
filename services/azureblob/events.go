package azureblob

import (
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
)

// Event types and operation names of the Blob Storage Event Grid source.
const (
	EventBlobCreated = "Microsoft.Storage.BlobCreated"
	EventBlobDeleted = "Microsoft.Storage.BlobDeleted"
)

// BlobEvent describes a committed blob create/replace or a blob delete.
type BlobEvent struct {
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
	ContentLength   int64
}

// BlobEventSink receives blob events after the operation succeeded.
type BlobEventSink interface {
	OnBlobEvent(ev BlobEvent)
}

// serveBlobEvents runs a blob-level operation and reports PutBlob, PutBlockList, CopyBlob and DeleteBlob outcomes.
func (h *Handler) serveBlobEvents(c *echo.Context, account, container, blob string) error {
	r := c.Request()
	op := blobOperationFor(r)

	var prior BlobInfo

	deleting := op == opDeleteBlob && r.URL.Query().Get(querySnapshot) == ""
	if deleting {
		prior, _ = h.Backend.HeadBlobAt(container, blob, "")
	}

	err := h.handleBlobLevel(c, container, blob)
	if err != nil {
		return err
	}

	status := http.StatusOK
	if resp, rerr := echo.UnwrapResponse(c.Response()); rerr == nil && resp.Status != 0 {
		status = resp.Status
	}

	if status >= http.StatusMultipleChoices {
		return nil
	}

	ev := BlobEvent{
		Time: time.Now().UTC(), Account: account, Container: container, Blob: blob, API: op,
		RequestID: uuid.NewString(), ClientRequestID: r.Header.Get("X-Ms-Client-Request-Id"),
	}

	switch {
	case op == opPutBlob || op == opPutBlockList || op == opCopyBlob:
		info, herr := h.Backend.HeadBlobAt(container, blob, "")
		if herr == nil {
			ev.EventType, ev.ContentType, ev.ETag = EventBlobCreated, info.ContentType, info.ETag
			ev.ContentLength, ev.AccessTier = info.ContentLength, info.AccessTier
		}
	case deleting:
		ev.EventType, ev.ContentType = EventBlobDeleted, prior.ContentType
	default:
		return nil
	}

	if ev.EventType != "" {
		h.Events.OnBlobEvent(ev)
	}

	return nil
}
