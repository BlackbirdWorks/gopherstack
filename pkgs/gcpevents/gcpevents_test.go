package gcpevents_test

import (
	"io"
	"net/http"
	"testing"
	"time"

	"cloud.google.com/go/firestore/apiv1/firestorepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpevents"
)

func TestMatchFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name, op, pattern, value string
		want                     bool
	}{
		{"exact", "", "users/alice", "users/alice", true},
		{"exact differs", "", "users/alice", "users/bob", false},
		{"capture one segment", gcpevents.OperatorPathPattern, "users/{id}", "users/alice", true},
		{"capture not two segments", gcpevents.OperatorPathPattern, "users/{id}", "users/alice/orders", false},
		{"capture rest", gcpevents.OperatorPathPattern, "users/{path=**}", "users/alice/orders/1", true},
		{"star within segment", gcpevents.OperatorPathPattern, "logs-*", "logs-prod", true},
		{"star not across segments", gcpevents.OperatorPathPattern, "a/*", "a/b/c", false},
		{"double star", gcpevents.OperatorPathPattern, "a/**", "a/b/c", true},
		{"literal dots are literal", gcpevents.OperatorPathPattern, "a.b", "axb", false},
		{"bad pattern never matches", gcpevents.OperatorPathPattern, "{bad id}", "x", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, gcpevents.MatchFilter(tt.op, tt.pattern, tt.value))
		})
	}
}

func TestCloudEventRequestIsBinaryMode(t *testing.T) {
	t.Parallel()

	at := time.Date(2026, 5, 6, 7, 8, 9, 0, time.UTC)
	ev := gcpevents.StorageEvent("id-1", "finalized", "b", "dir/o.txt", []byte(`{"name":"dir/o.txt"}`), at)

	req, err := ev.NewRequest(t.Context(), "http://example.test/")
	require.NoError(t, err)

	body, err := io.ReadAll(req.Body)
	require.NoError(t, err)

	assert.Equal(t, http.MethodPost, req.Method)
	assert.JSONEq(t, `{"name":"dir/o.txt"}`, string(body))
	assert.Equal(t, "application/json", req.Header.Get("Content-Type"))

	want := map[string]string{
		"Ce-Specversion": "1.0", "Ce-Id": "id-1", "Ce-Type": "google.cloud.storage.object.v1.finalized",
		"Ce-Source": "//storage.googleapis.com/projects/_/buckets/b", "Ce-Subject": "objects/dir/o.txt",
		"Ce-Time": "2026-05-06T07:08:09Z", "Ce-Bucket": "b",
	}
	for k, v := range want {
		assert.Equal(t, v, req.Header.Get(k), k)
	}
}

func TestFirestoreEventPayload(t *testing.T) {
	t.Parallel()

	doc := &firestorepb.Document{
		Name:   "projects/p/databases/(default)/documents/users/alice",
		Fields: map[string]*firestorepb.Value{"n": {ValueType: &firestorepb.Value_IntegerValue{IntegerValue: 2}}},
	}
	old := &firestorepb.Document{Name: doc.GetName()}

	ev, err := gcpevents.FirestoreEvent("i", "updated", doc, old, []string{"n"}, time.Unix(0, 0))
	require.NoError(t, err)

	assert.Equal(t, "google.cloud.firestore.document.v1.updated", ev.Type)
	assert.Equal(t, "//firestore.googleapis.com/projects/p/databases/(default)", ev.Source)
	assert.Equal(t, "documents/users/alice", ev.Subject)
	assert.Equal(t, "application/protobuf", ev.ContentType)
	assert.Equal(t, map[string]string{"document": "users/alice", "database": "(default)", "namespace": "(default)"},
		ev.Extensions)

	// DocumentEventData: value=1, old_value=2, update_mask=3.
	fields := map[int32][]byte{}

	for raw := ev.Data; len(raw) > 0; {
		num, _, l := protowire.ConsumeTag(raw)
		v, l2 := protowire.ConsumeBytes(raw[l:])
		fields[int32(num)] = v
		raw = raw[l+l2:]
	}

	got := &firestorepb.Document{}
	require.NoError(t, proto.Unmarshal(fields[1], got))
	assert.True(t, proto.Equal(doc, got))
	assert.Contains(t, fields, int32(2))

	path, _, _ := protowire.ConsumeTag(fields[3])
	assert.Equal(t, protowire.Number(1), path, "update_mask.field_paths is field 1")
	assert.Contains(t, string(fields[3]), "n")
}

func TestFirestoreEventRejectsForeignName(t *testing.T) {
	t.Parallel()

	_, err := gcpevents.FirestoreEvent("i", "created", &firestorepb.Document{Name: "nope"}, nil, nil, time.Now())
	require.Error(t, err)
}
