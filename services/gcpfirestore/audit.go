package gcpfirestore

import (
	"time"

	"cloud.google.com/go/firestore/apiv1/firestorepb"
	"google.golang.org/protobuf/proto"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpaudit"
)

func auditPolicy() gcpaudit.Policy {
	w := gcpaudit.DataWrite

	return gcpaudit.Policy{
		ServiceName: "firestore.googleapis.com",
		Data: map[string]gcpaudit.Kind{
			"Commit": w, "BatchWrite": w, "CreateDocument": w, "UpdateDocument": w, "DeleteDocument": w,
		},
		NameFunc: func(req, resp proto.Message) string {
			switch r := req.(type) {
			case *firestorepb.CommitRequest:
				return firstWriteName(r.GetWrites(), r.GetDatabase())
			case *firestorepb.BatchWriteRequest:
				return firstWriteName(r.GetWrites(), r.GetDatabase())
			case *firestorepb.DeleteDocumentRequest:
				return r.GetName()
			case *firestorepb.UpdateDocumentRequest:
				return r.GetDocument().GetName()
			}

			if d, ok := resp.(*firestorepb.Document); ok {
				return d.GetName()
			}

			return ""
		},
	}
}

func firstWriteName(writes []*firestorepb.Write, database string) string {
	if len(writes) > 0 {
		if n := writes[0].GetUpdate().GetName(); n != "" {
			return n
		}

		if n := writes[0].GetDelete(); n != "" {
			return n
		}
	}

	return database
}

func (b *InMemoryBackend) clockNow() time.Time {
	b.mu.RLock("clockNow")
	defer b.mu.RUnlock()

	return b.now()
}
