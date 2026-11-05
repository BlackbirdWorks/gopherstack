package gcpeventarc_test

import (
	"bytes"
	"encoding/json"
	"testing"

	"cloud.google.com/go/eventarc/apiv1/eventarcpb"
	"cloud.google.com/go/firestore/apiv1/firestorepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

const fsParent = "projects/" + project + "/databases/(default)/documents"

func fsFilters(typ string, extra ...*eventarcpb.EventFilter) []*eventarcpb.EventFilter {
	return append([]*eventarcpb.EventFilter{
		typeFilter("google.cloud.firestore.document.v1." + typ),
		{Attribute: "database", Value: "(default)"},
		{Attribute: "namespace", Value: "(default)"},
	}, extra...)
}

func (e *env) createDoc(t *testing.T, collection, id, value string) {
	t.Helper()

	_, err := e.fs.CreateDocument(&firestorepb.CreateDocumentRequest{
		Parent: fsParent, CollectionId: collection, DocumentId: id,
		Document: &firestorepb.Document{Fields: map[string]*firestorepb.Value{
			"v": {ValueType: &firestorepb.Value_StringValue{StringValue: value}},
		}},
	})
	require.NoError(t, err)
}

func (e *env) updateDoc(t *testing.T, collection, id, value string) {
	t.Helper()

	_, err := e.fs.UpdateDocument(&firestorepb.UpdateDocumentRequest{
		Document: &firestorepb.Document{
			Name: fsParent + "/" + collection + "/" + id,
			Fields: map[string]*firestorepb.Value{
				"v": {ValueType: &firestorepb.Value_StringValue{StringValue: value}},
			},
		},
	})
	require.NoError(t, err)
}

func TestFirestoreDelivery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		act         func(t *testing.T, e *env)
		name        string
		trigType    string
		contentType string
		wantType    string
		wantSubject string
		wantCT      string
		extra       []*eventarcpb.EventFilter
		skip        int
	}{
		{
			name: "created with a document path pattern, protobuf by default", trigType: "created",
			extra: []*eventarcpb.EventFilter{
				{Attribute: "document", Value: "users/{id}", Operator: "match-path-pattern"},
			},
			act: func(t *testing.T, e *env) {
				t.Helper()
				e.createDoc(t, "orders", "o1", "skipped")
				e.createDoc(t, "users", "alice", "hello")
			},
			wantType: "google.cloud.firestore.document.v1.created", wantSubject: "documents/users/alice",
			wantCT: "application/protobuf",
		},
		{
			name: "written fires for an update and reports the mask as JSON", trigType: "written",
			contentType: "application/json",
			act: func(t *testing.T, e *env) {
				t.Helper()
				e.createDoc(t, "users", "bob", "one")
				e.updateDoc(t, "users", "bob", "two")
			},
			wantType: "google.cloud.firestore.document.v1.written", wantSubject: "documents/users/bob",
			wantCT: "application/json", skip: 1,
		},
		{
			name: "updated ignores the create", trigType: "updated", contentType: "application/json",
			act: func(t *testing.T, e *env) {
				t.Helper()
				e.createDoc(t, "users", "carol", "one")
				e.updateDoc(t, "users", "carol", "two")
			},
			wantType: "google.cloud.firestore.document.v1.updated", wantSubject: "documents/users/carol",
			wantCT: "application/json",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.createTrigger(t, "fs-trigger", &eventarcpb.Trigger{
				EventFilters:         fsFilters(tt.trigType, tt.extra...),
				Destination:          httpDestination(e.dest.srv.URL + "/fs"),
				EventDataContentType: tt.contentType,
			})

			tt.act(t, e)

			got := e.dest.next(t)

			for range tt.skip {
				if next := e.dest.next(t); bytes.Contains(next.body, []byte("updateMask")) {
					got = next
				}
			}

			assert.Equal(t, tt.wantType, got.header.Get("Ce-Type"))
			assert.Equal(t, tt.wantSubject, got.header.Get("Ce-Subject"))
			assert.Equal(t, tt.wantCT, got.header.Get("Content-Type"))
			assert.Equal(t, "(default)", got.header.Get("Ce-Database"))
			assert.Equal(
				t,
				"//firestore.googleapis.com/projects/"+project+"/databases/(default)",
				got.header.Get("Ce-Source"),
			)
			assert.NotEmpty(t, got.body)

			if tt.wantCT != "application/json" {
				return
			}

			var data struct {
				Value      map[string]any `json:"value"`
				UpdateMask struct {
					FieldPaths []string `json:"fieldPaths"`
				} `json:"updateMask"`
			}

			require.NoError(t, json.Unmarshal(got.body, &data))
			assert.Contains(t, data.Value["name"], tt.wantSubject[len("documents/"):])

			if tt.trigType != "created" {
				require.NotNil(t, data.Value)
				assert.Equal(t, []string{"v"}, data.UpdateMask.FieldPaths)
			}
		})
	}
}

func TestFirestoreProtobufBody(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.createTrigger(t, "fs-proto", &eventarcpb.Trigger{
		EventFilters: fsFilters("created"), Destination: httpDestination(e.dest.srv.URL),
	})
	e.createDoc(t, "users", "dave", "v1")

	got := e.dest.next(t)

	// DocumentEventData: value = field 1 (length-delimited Document).
	require.NotEmpty(t, got.body)
	require.Equal(t, byte(0x0a), got.body[0])

	var doc firestorepb.Document

	n, sz := int(got.body[1]), 2
	require.Less(t, n, 128, "single-byte length for this small document")
	require.NoError(t, proto.Unmarshal(got.body[sz:sz+n], &doc))
	assert.Equal(t, fsParent+"/users/dave", doc.GetName())
}

func TestFirestoreTriggerValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		ct      string
		filters []*eventarcpb.EventFilter
		wantOK  bool
	}{
		{name: "valid", filters: fsFilters("created"), wantOK: true},
		{name: "database required", filters: []*eventarcpb.EventFilter{
			typeFilter("google.cloud.firestore.document.v1.created"), {Attribute: "namespace", Value: "(default)"},
		}},
		{name: "protobuf allowed", filters: fsFilters("written"), ct: "application/protobuf", wantOK: true},
		{name: "unknown event kind", filters: fsFilters("exploded")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			_, err := e.client.CreateTrigger(t.Context(), &eventarcpb.CreateTriggerRequest{
				Parent: parent, TriggerId: "fs-validate",
				Trigger: &eventarcpb.Trigger{
					EventFilters: tt.filters, Destination: httpDestination(e.dest.srv.URL), EventDataContentType: tt.ct,
				},
			})
			assert.Equal(t, tt.wantOK, err == nil)
		})
	}
}
