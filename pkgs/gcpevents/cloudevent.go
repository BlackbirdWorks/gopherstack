package gcpevents

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"cloud.google.com/go/firestore/apiv1/firestorepb"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

const (
	// StorageTypePrefix prefixes Cloud Storage event types.
	StorageTypePrefix = "google.cloud.storage.object.v1."
	// FirestoreTypePrefix prefixes Firestore event types.
	FirestoreTypePrefix = "google.cloud.firestore.document.v1."
	// FirestoreWritten is the suffix matching created, updated and deleted.
	FirestoreWritten = "written"

	contentTypeJSON  = "application/json"
	contentTypeProto = "application/protobuf"
	docSegment       = "/documents/"
	maskField        = protowire.Number(3)
)

// CloudEvent is one event in binary content mode: attributes travel as ce-* headers, the payload as the body.
type CloudEvent struct {
	Extensions  map[string]string
	ID          string
	Source      string
	Type        string
	Subject     string
	Time        string
	ContentType string
	Data        []byte
}

// NewRequest builds the POST that delivers e to url.
func (e CloudEvent) NewRequest(ctx context.Context, url string) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(e.Data))
	if err != nil {
		return nil, fmt.Errorf("build event request: %w", err)
	}

	req.Header.Set("Content-Type", e.ContentType)
	req.Header.Set("Ce-Specversion", "1.0")
	req.Header.Set("Ce-Id", e.ID)
	req.Header.Set("Ce-Type", e.Type)
	req.Header.Set("Ce-Source", e.Source)
	req.Header.Set("Ce-Time", e.Time)

	if e.Subject != "" {
		req.Header.Set("Ce-Subject", e.Subject)
	}

	for k, v := range e.Extensions {
		req.Header.Set("Ce-"+k, v)
	}

	return req, nil
}

func stamp(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

// StorageEvent wraps a Cloud Storage object resource (StorageObjectData JSON) in a CloudEvent of type
// google.cloud.storage.object.v1.<kind>.
func StorageEvent(id, kind, bucket, object string, data []byte, t time.Time) CloudEvent {
	return CloudEvent{
		ID:          id,
		Type:        StorageTypePrefix + kind,
		Source:      "//storage.googleapis.com/projects/_/buckets/" + bucket,
		Subject:     "objects/" + object,
		Time:        stamp(t),
		ContentType: contentTypeJSON,
		Extensions:  map[string]string{"bucket": bucket},
		Data:        data,
	}
}

// FirestoreEvent wraps a document change in a CloudEvent of type google.cloud.firestore.document.v1.<kind>. The
// payload is a google.events.cloud.firestore.v1.DocumentEventData message (value=1, old_value=2, update_mask=3),
// which Eventarc delivers as application/protobuf. The document name is
// projects/{p}/databases/{d}/documents/{path}.
func FirestoreEvent(id, kind string, doc, old *firestorepb.Document, mask []string, t time.Time) (CloudEvent, error) {
	name := doc.GetName()
	if name == "" {
		name = old.GetName()
	}

	db, path, ok := strings.Cut(name, docSegment)
	if !ok {
		return CloudEvent{}, fmt.Errorf("%w: document name %q", ErrPattern, name)
	}

	var body []byte

	for _, f := range []struct {
		d *firestorepb.Document
		n protowire.Number
	}{{doc, 1}, {old, 2}} {
		if f.d == nil {
			continue
		}

		raw, err := proto.Marshal(f.d)
		if err != nil {
			return CloudEvent{}, fmt.Errorf("marshal document: %w", err)
		}

		body = protowire.AppendTag(body, f.n, protowire.BytesType)
		body = protowire.AppendBytes(body, raw)
	}

	if len(mask) > 0 {
		var m []byte
		for _, p := range mask {
			m = protowire.AppendTag(m, 1, protowire.BytesType)
			m = protowire.AppendString(m, p)
		}

		body = protowire.AppendTag(body, maskField, protowire.BytesType)
		body = protowire.AppendBytes(body, m)
	}

	dbName := strings.TrimPrefix(db, "projects/")
	_, database, _ := strings.Cut(dbName, "/databases/")

	return CloudEvent{
		ID:          id,
		Type:        FirestoreTypePrefix + kind,
		Source:      "//firestore.googleapis.com/" + db,
		Subject:     "documents/" + path,
		Time:        stamp(t),
		ContentType: contentTypeProto,
		Extensions:  map[string]string{"document": path, "database": database, "namespace": "(default)"},
		Data:        body,
	}, nil
}
