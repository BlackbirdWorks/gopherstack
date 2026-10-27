package gcpfunctions_test

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/firestore/apiv1/firestorepb"
	"cloud.google.com/go/functions/apiv2/functionspb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpevents"
	"github.com/blackbirdworks/gopherstack/services/gcpfunctions"
)

type fakeBuckets map[string]bool

func (f fakeBuckets) BucketExists(b string) bool { return f[b] }

const (
	typeFinalized = "google.cloud.storage.object.v1.finalized"
	typeDeleted   = "google.cloud.storage.object.v1.deleted"
	typeCreated   = "google.cloud.firestore.document.v1.created"
	typeUpdated   = "google.cloud.firestore.document.v1.updated"
	typeWritten   = "google.cloud.firestore.document.v1.written"
	docPrefix     = "projects/test-project/databases/(default)/documents/"
)

func filter(attr, value string) *functionspb.EventFilter {
	return &functionspb.EventFilter{Attribute: attr, Value: value}
}

func patternFilter(attr, value string) *functionspb.EventFilter {
	return &functionspb.EventFilter{Attribute: attr, Value: value, Operator: gcpevents.OperatorPathPattern}
}

func triggerFn(e *env, t *testing.T, typ string, retry functionspb.EventTrigger_RetryPolicy,
	filters ...*functionspb.EventFilter,
) *functionspb.Function {
	t.Helper()

	spec := fnSpec(e.upload(t, zipOf(t, nodeFilesFn())))
	spec.EventTrigger = &functionspb.EventTrigger{EventType: typ, EventFilters: filters, RetryPolicy: retry}

	return e.createFunction(t, "trig", spec)
}

func TestTriggers_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		trigger *functionspb.EventTrigger
		wantErr bool
	}{
		{"storage ok", &functionspb.EventTrigger{
			EventType: typeFinalized, EventFilters: []*functionspb.EventFilter{filter("bucket", "b1")},
		}, false},
		{"storage bucket pattern", &functionspb.EventTrigger{
			EventType: typeDeleted, EventFilters: []*functionspb.EventFilter{patternFilter("bucket", "logs-*")},
		}, false},
		{"storage without bucket", &functionspb.EventTrigger{EventType: typeFinalized}, true},
		{"storage unknown bucket", &functionspb.EventTrigger{
			EventType: typeFinalized, EventFilters: []*functionspb.EventFilter{filter("bucket", "nope")},
		}, true},
		{"storage bad attribute", &functionspb.EventTrigger{
			EventType:    typeFinalized,
			EventFilters: []*functionspb.EventFilter{filter("bucket", "b1"), filter("document", "x")},
		}, true},
		{"storage with topic", &functionspb.EventTrigger{
			EventType: typeFinalized, PubsubTopic: topic, EventFilters: []*functionspb.EventFilter{filter("bucket", "b1")},
		}, true},
		{"unknown storage kind", &functionspb.EventTrigger{
			EventType: "google.cloud.storage.object.v1.exploded", EventFilters: []*functionspb.EventFilter{filter("bucket", "b1")},
		}, true},
		{"firestore ok", &functionspb.EventTrigger{
			EventType: typeWritten, EventFilters: []*functionspb.EventFilter{
				filter("database", "(default)"), patternFilter("document", "users/{id}"),
			},
		}, false},
		{"firestore without database", &functionspb.EventTrigger{
			EventType: typeCreated, EventFilters: []*functionspb.EventFilter{patternFilter("document", "users/{id}")},
		}, true},
		{"firestore bad pattern", &functionspb.EventTrigger{
			EventType: typeCreated, EventFilters: []*functionspb.EventFilter{
				filter("database", "(default)"), patternFilter("document", "users/{bad id}"),
			},
		}, true},
		{"firestore pattern on database", &functionspb.EventTrigger{
			EventType: typeCreated, EventFilters: []*functionspb.EventFilter{patternFilter("database", "(default)")},
		}, true},
		{"firestore bad operator", &functionspb.EventTrigger{
			EventType: typeCreated, EventFilters: []*functionspb.EventFilter{
				filter("database", "(default)"), {Attribute: "document", Value: "a", Operator: "regex"},
			},
		}, true},
		{"audit log unsupported", &functionspb.EventTrigger{EventType: "google.cloud.audit.log.v1.written"}, true},
		{"pubsub with filters", &functionspb.EventTrigger{
			EventType: gcpfunctions.EventTypePubSub, PubsubTopic: topic,
			EventFilters: []*functionspb.EventFilter{filter("bucket", "b1")},
		}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, true)
			e.handler.Backend.SetBucketLookup(fakeBuckets{"b1": true})

			spec := fnSpec(e.upload(t, zipOf(t, nodeFilesFn())))
			spec.EventTrigger = tt.trigger

			err := e.createErr(t, "val", spec)
			if tt.wantErr {
				assert.Equal(t, codes.InvalidArgument, status.Code(err), "%v", err)

				return
			}

			assert.NoError(t, err)
		})
	}
}

func (e *env) waitRequests(t *testing.T, n int) []received {
	t.Helper()
	require.Eventually(t, func() bool { return len(e.rt.requests()) >= n }, 10*time.Second, 5*time.Millisecond)

	return e.rt.requests()
}

func TestStorageTrigger_Delivery(t *testing.T) {
	t.Parallel()

	const object = `{"bucket":"b1","name":"dir/a.txt","generation":"7","size":"5","contentType":"text/plain"}`

	e := newEnv(t, true)
	triggerFn(e, t, typeFinalized, 0, filter("bucket", "b1"))

	b := e.handler.Backend
	at := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)

	b.HandleStorageEvent(gcpfunctions.StorageEvent{Type: "deleted", Bucket: "b1", Name: "x", Data: []byte(`{}`), Time: at})
	b.HandleStorageEvent(gcpfunctions.StorageEvent{Type: "finalized", Bucket: "other", Name: "x", Data: []byte(`{}`), Time: at})
	b.HandleStorageEvent(gcpfunctions.StorageEvent{
		Type: "finalized", Bucket: "b1", Name: "dir/a.txt", Data: []byte(object), Time: at,
	})

	reqs := e.waitRequests(t, 1)
	require.Len(t, reqs, 1, "wrong type and wrong bucket must not be delivered")

	h := reqs[0].Headers
	assert.Equal(t, "1.0", h["ce-specversion"])
	assert.Equal(t, typeFinalized, h["ce-type"])
	assert.Equal(t, "//storage.googleapis.com/projects/_/buckets/b1", h["ce-source"])
	assert.Equal(t, "objects/dir/a.txt", h["ce-subject"])
	assert.Equal(t, "b1", h["ce-bucket"])
	assert.Equal(t, "2026-03-04T05:06:07Z", h["ce-time"])
	assert.NotEmpty(t, h["ce-id"])
	assert.Equal(t, "application/json", h["content-type"])
	assert.JSONEq(t, object, reqs[0].Body)
}

func TestStorageTrigger_BucketPattern(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	triggerFn(e, t, typeFinalized, 0, patternFilter("bucket", "logs-*"))

	b := e.handler.Backend
	b.HandleStorageEvent(gcpfunctions.StorageEvent{Type: "finalized", Bucket: "data", Name: "x", Data: []byte(`{}`)})
	b.HandleStorageEvent(gcpfunctions.StorageEvent{Type: "finalized", Bucket: "logs-prod", Name: "x", Data: []byte(`{}`)})

	reqs := e.waitRequests(t, 1)
	require.Len(t, reqs, 1)
	assert.Equal(t, "logs-prod", reqs[0].Headers["ce-bucket"])
}

func docEvent(kind, path string, old, cur map[string]string) gcpfunctions.DocumentEvent {
	mk := func(f map[string]string) *firestorepb.Document {
		if f == nil {
			return nil
		}

		d := &firestorepb.Document{Name: docPrefix + path, Fields: map[string]*firestorepb.Value{}}
		for k, v := range f {
			d.Fields[k] = &firestorepb.Value{ValueType: &firestorepb.Value_StringValue{StringValue: v}}
		}

		return d
	}

	ev := gcpfunctions.DocumentEvent{Kind: kind, Old: mk(old), New: mk(cur), Time: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)}
	if kind == "updated" {
		ev.UpdateMask = []string{"name"}
	}

	return ev
}

// fieldDoc decodes field number n of a DocumentEventData message as a Firestore document.
func fieldDoc(t *testing.T, body string, n protowire.Number) *firestorepb.Document {
	t.Helper()

	raw := []byte(body)
	for len(raw) > 0 {
		num, typ, l := protowire.ConsumeTag(raw)
		require.GreaterOrEqual(t, l, 0)
		raw = raw[l:]

		v, l := protowire.ConsumeBytes(raw)
		require.GreaterOrEqual(t, l, 0)
		require.Equal(t, protowire.BytesType, typ)

		raw = raw[l:]

		if num == n {
			d := &firestorepb.Document{}
			require.NoError(t, proto.Unmarshal(v, d))

			return d
		}
	}

	return nil
}

func TestFirestoreTrigger_Delivery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		typ      string
		filters  []*functionspb.EventFilter
		event    gcpfunctions.DocumentEvent
		want     bool
		wantType string
	}{
		{"created matches pattern", typeCreated,
			[]*functionspb.EventFilter{filter("database", "(default)"), patternFilter("document", "users/{id}")},
			docEvent("created", "users/alice", nil, map[string]string{"name": "Alice"}), true, typeCreated},
		{"created ignores updates", typeCreated,
			[]*functionspb.EventFilter{filter("database", "(default)")},
			docEvent("updated", "users/alice", map[string]string{"name": "A"}, map[string]string{"name": "B"}), false, ""},
		{"updated carries old value", typeUpdated,
			[]*functionspb.EventFilter{filter("database", "(default)")},
			docEvent("updated", "users/alice", map[string]string{"name": "A"}, map[string]string{"name": "B"}), true, typeUpdated},
		{"written sees deletes", typeWritten,
			[]*functionspb.EventFilter{filter("database", "(default)")},
			docEvent("deleted", "users/alice", map[string]string{"name": "A"}, nil), true, typeWritten},
		{"pattern is one segment", typeWritten,
			[]*functionspb.EventFilter{filter("database", "(default)"), patternFilter("document", "users/{id}")},
			docEvent("created", "users/alice/orders/1", nil, map[string]string{"a": "b"}), false, ""},
		{"multi segment pattern", typeWritten,
			[]*functionspb.EventFilter{filter("database", "(default)"), patternFilter("document", "users/{path=**}")},
			docEvent("created", "users/alice/orders/1", nil, map[string]string{"a": "b"}), true, typeWritten},
		{"exact document", typeWritten,
			[]*functionspb.EventFilter{filter("database", "(default)"), filter("document", "users/bob")},
			docEvent("created", "users/alice", nil, map[string]string{"a": "b"}), false, ""},
		{"other database", typeWritten,
			[]*functionspb.EventFilter{filter("database", "analytics")},
			docEvent("created", "users/alice", nil, map[string]string{"a": "b"}), false, ""},
		{"namespace default", typeWritten,
			[]*functionspb.EventFilter{filter("database", "(default)"), filter("namespace", "(default)")},
			docEvent("created", "users/alice", nil, map[string]string{"a": "b"}), true, typeWritten},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, true)
			triggerFn(e, t, tt.typ, 0, tt.filters...)
			e.handler.Backend.HandleDocumentEvent(tt.event)

			if !tt.want {
				e.handler.Backend.Drain()
				assert.Empty(t, e.rt.requests())

				return
			}

			reqs := e.waitRequests(t, 1)
			require.Len(t, reqs, 1)

			h := reqs[0].Headers
			assert.Equal(t, tt.wantType, h["ce-type"])
			assert.Equal(t, "//firestore.googleapis.com/projects/test-project/databases/(default)", h["ce-source"])
			assert.True(t, strings.HasPrefix(h["ce-subject"], "documents/"), h["ce-subject"])
			assert.Equal(t, strings.TrimPrefix(h["ce-subject"], "documents/"), h["ce-document"])
			assert.Equal(t, "(default)", h["ce-database"])
			assert.Equal(t, "application/protobuf", h["content-type"])

			value, old := fieldDoc(t, reqs[0].Body, 1), fieldDoc(t, reqs[0].Body, 2)
			if tt.event.New != nil {
				assert.True(t, proto.Equal(tt.event.New, value))
			} else {
				assert.Nil(t, value)
			}

			if tt.event.Old != nil {
				assert.True(t, proto.Equal(tt.event.Old, old))
			}
		})
	}
}

func TestEventTrigger_Retry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		retry functionspb.EventTrigger_RetryPolicy
		want  []time.Duration
	}{
		{"retry backs off exponentially", functionspb.EventTrigger_RETRY_POLICY_RETRY,
			[]time.Duration{10 * time.Second, 20 * time.Second, 40 * time.Second}},
		{"do not retry gives up", functionspb.EventTrigger_RETRY_POLICY_DO_NOT_RETRY, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, true)
			clk := newFakeClock()
			e.handler.Backend.SetClock(clk)
			e.rt.reply.Store(http.StatusInternalServerError)

			triggerFn(e, t, typeFinalized, tt.retry, filter("bucket", "b1"))
			e.handler.Backend.HandleStorageEvent(gcpfunctions.StorageEvent{
				Type: "finalized", Bucket: "b1", Name: "o", Data: []byte(`{}`),
			})

			e.waitRequests(t, 1)

			if tt.want == nil {
				e.handler.Backend.Drain()
				assert.Empty(t, clk.durations())
				assert.Len(t, e.rt.requests(), 1)

				return
			}

			for i, d := range tt.want {
				clk.waitNew(t, 1)
				require.Equal(t, d, clk.durations()[i])
				clk.advance(d)
				e.waitRequests(t, i+2)
			}

			e.rt.reply.Store(http.StatusOK)
			clk.waitNew(t, 1)
			clk.advance(80 * time.Second)

			reqs := e.waitRequests(t, len(tt.want)+2)
			assert.Len(t, reqs, len(tt.want)+2)

			ids := map[string]bool{}
			for _, r := range reqs {
				ids[r.Headers["ce-id"]] = true
			}

			assert.Len(t, ids, 1, "redelivery keeps the event id")
			assert.Len(t, clk.durations(), len(tt.want)+1, "a delivered event is not retried again")
		})
	}
}

func TestEventTrigger_RetryStopsWhenRetentionEnds(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	clk := newFakeClock()
	e.handler.Backend.SetClock(clk)
	e.handler.Backend.SetRetryPolicy(10*time.Second, 10*time.Second, 25*time.Second)
	e.rt.reply.Store(http.StatusInternalServerError)

	triggerFn(e, t, typeFinalized, functionspb.EventTrigger_RETRY_POLICY_RETRY, filter("bucket", "b1"))
	e.handler.Backend.HandleStorageEvent(gcpfunctions.StorageEvent{Type: "finalized", Bucket: "b1", Name: "o", Data: []byte(`{}`)})

	for i := range 2 {
		e.waitRequests(t, i+1)
		clk.waitNew(t, 1)
		clk.advance(10 * time.Second)
	}

	e.waitRequests(t, 3)
	e.handler.Backend.Drain()
	assert.Zero(t, clk.pending(true))
	assert.Len(t, e.rt.requests(), 3, "no redelivery past the retention window")
}
