package gcpeventarc

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"strings"
	"time"

	"cloud.google.com/go/eventarc/apiv1/eventarcpb"
	"cloud.google.com/go/firestore/apiv1/firestorepb"
	"cloud.google.com/go/logging/apiv2/loggingpb"
	"github.com/google/uuid"
	"google.golang.org/genproto/googleapis/cloud/audit"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpevents"
	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

// HandleLogEntry delivers a Cloud Audit Logs entry (protoPayload AuditLog) to the triggers on
// google.cloud.audit.log.v1.written whose serviceName, methodName and resourceName filters it satisfies.
// Entries without an AuditLog payload are ignored.
func (b *InMemoryBackend) HandleLogEntry(e *loggingpb.LogEntry) {
	pp := e.GetProtoPayload()
	if pp == nil {
		return
	}

	var al audit.AuditLog
	if err := pp.UnmarshalTo(&al); err != nil {
		return
	}

	project, _ := auditLogParts(e.GetLogName())
	if project == "" {
		return
	}

	subject := al.GetServiceName() + "/" + al.GetResourceName()
	id := e.GetInsertId()

	if id == "" {
		id = uuid.NewString()
	}

	at := e.GetTimestamp().AsTime()
	ext := map[string]string{
		"servicename": al.GetServiceName(), "methodname": al.GetMethodName(), "resourcename": al.GetResourceName(),
	}

	b.publishEvent(fanout{
		project: project,
		types:   []string{typeAuditLog},
		attrs: map[string]string{
			attrServiceName: al.GetServiceName(), attrMethodName: al.GetMethodName(),
			attrResourceName: al.GetResourceName(),
		},
		message: func() map[string]any {
			data, _ := protojson.Marshal(e)

			return eventMessage(id, auditSource(al.GetServiceName(), project), typeAuditLog, subject, at, ext, data)
		},
		build: func(_ *eventarcpb.Trigger, typ string) (map[string]string, []byte, error) {
			data, err := protojson.Marshal(e)
			if err != nil {
				return nil, nil, err //nolint:wrapcheck // message built by the emulator
			}

			return ceAttrs(id, auditSource(al.GetServiceName(), project), typ, subject, at, ext), data, nil
		},
	})
}

// auditLogParts splits projects/{p}/logs/cloudaudit.googleapis.com%2F{id} into the project and short id.
func auditLogParts(logName string) (string, string) {
	rest, ok := strings.CutPrefix(logName, "projects/")
	if !ok {
		return "", ""
	}

	project, log, _ := strings.Cut(rest, "/logs/")
	id, ok := strings.CutPrefix(log, "cloudaudit.googleapis.com%2F")

	if !ok || project == "" {
		return "", ""
	}

	return project, id
}

// DocumentEvent is a committed Firestore document change; Kind is created, updated or deleted.
type DocumentEvent struct {
	Time       time.Time
	Old        *firestorepb.Document
	New        *firestorepb.Document
	Kind       string
	UpdateMask []string
}

// HandleDocumentEvent delivers a Firestore document change to the created/updated/deleted trigger of its kind
// and to written triggers whose database, namespace and document filters it satisfies.
func (b *InMemoryBackend) HandleDocumentEvent(ev DocumentEvent) {
	name := ev.New.GetName()
	if name == "" {
		name = ev.Old.GetName()
	}

	db, path, ok := strings.Cut(name, "/documents/")
	if !ok {
		return
	}

	project, _, _ := strings.Cut(strings.TrimPrefix(db, "projects/"), "/")
	_, database, _ := strings.Cut(db, "/databases/")
	id := uuid.NewString()

	b.publishEvent(fanout{
		project: project,
		types:   []string{firestoreType(ev.Kind), firestoreType(firestoreWritten)},
		attrs: map[string]string{
			attrDatabase: database, attrNamespace: defaultFirestoreNS, attrDocument: path,
		},
		message: func() map[string]any {
			data, _ := documentEventJSON(ev)

			return eventMessage(id, firestoreSourceBase+db, firestoreType(ev.Kind), "documents/"+path, ev.Time,
				map[string]string{"database": database, "namespace": defaultFirestoreNS, "document": path}, data)
		},
		build: func(t *eventarcpb.Trigger, typ string) (map[string]string, []byte, error) {
			ce, err := gcpevents.FirestoreEvent(id, strings.TrimPrefix(typ, typeFirestorePrefix), ev.New, ev.Old,
				ev.UpdateMask, ev.Time)
			if err != nil {
				return nil, nil, err //nolint:wrapcheck // document name checked above
			}

			data := ce.Data
			if contentTypeFor(t) == contentTypeJSON {
				if data, err = documentEventJSON(ev); err != nil {
					return nil, nil, err
				}
			}

			return ceAttrs(id, firestoreSourceBase+db, typ, ce.Subject, ev.Time, ce.Extensions), data, nil
		},
	})
}

// documentEventJSON renders google.events.cloud.firestore.v1.DocumentEventData as JSON.
func documentEventJSON(ev DocumentEvent) ([]byte, error) {
	out := map[string]any{}

	for k, d := range map[string]*firestorepb.Document{"value": ev.New, "oldValue": ev.Old} {
		if d == nil {
			continue
		}

		raw, err := protojson.Marshal(d)
		if err != nil {
			return nil, err //nolint:wrapcheck // message built by the emulator
		}

		out[k] = jsontext.Value(raw)
	}

	if len(ev.UpdateMask) > 0 {
		out["updateMask"] = map[string]any{"fieldPaths": ev.UpdateMask}
	}

	return json.Marshal(out, jsonx.V1())
}

// auditSource is the documented audit-log ce-source //SERVICE_NAME/projects/PROJECT_ID
// (docs.cloud.google.com/eventarc/docs/cloudevents).
func auditSource(service, project string) string { return "//" + service + "/projects/" + project }
