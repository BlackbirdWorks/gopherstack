package gcpfunctions_test

import (
	"testing"
	"time"

	"cloud.google.com/go/logging/apiv2/loggingpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/cloud/audit"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpevents"
)

func auditEntry(t *testing.T, project, service, method, resource string) *loggingpb.LogEntry {
	t.Helper()

	payload, err := anypb.New(&audit.AuditLog{ServiceName: service, MethodName: method, ResourceName: resource})
	require.NoError(t, err)

	return &loggingpb.LogEntry{
		LogName:   "projects/" + project + "/logs/cloudaudit.googleapis.com%2Factivity",
		InsertId:  "entry-1",
		Timestamp: timestamppb.New(time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)),
		Payload:   &loggingpb.LogEntry_ProtoPayload{ProtoPayload: payload},
	}
}

func TestAuditTrigger_Delivery(t *testing.T) {
	t.Parallel()

	const (
		svc    = "storage.googleapis.com"
		method = "storage.buckets.create"
		proj   = "test-project"
		bkt    = "projects/_/buckets/b1"
	)

	tests := []struct {
		name     string
		project  string
		service  string
		method   string
		resource string
		matches  bool
	}{
		{name: "match", project: proj, service: svc, method: method, resource: bkt, matches: true},
		{name: "other method", project: proj, service: svc, method: "storage.buckets.delete", resource: bkt},
		{name: "other service", project: proj, service: "bigquery.googleapis.com", method: method, resource: bkt},
		{name: "other project", project: "elsewhere", service: svc, method: method, resource: bkt},
		{name: "resource pattern", project: proj, service: svc, method: method, resource: bkt + "/objects/x"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, true)
			triggerFn(t, e, gcpevents.AuditLogType, 0,
				filter("serviceName", svc), filter("methodName", method),
				patternFilter("resourceName", "projects/_/buckets/{b}"),
			)

			b := e.handler.Backend
			b.HandleLogEntry(auditEntry(t, tt.project, tt.service, tt.method, tt.resource))

			if !tt.matches {
				// a matching entry afterwards proves the first was skipped rather than slow
				b.HandleLogEntry(auditEntry(t, proj, svc, method, "projects/_/buckets/control"))

				reqs := e.waitRequests(t, 1)
				require.Len(t, reqs, 1)
				assert.Equal(t, "projects/_/buckets/control", reqs[0].Headers["ce-resourcename"])

				return
			}

			reqs := e.waitRequests(t, 1)
			require.Len(t, reqs, 1)

			h := reqs[0].Headers
			assert.Equal(t, gcpevents.AuditLogType, h["ce-type"])
			assert.Equal(t, "//storage.googleapis.com/projects/test-project", h["ce-source"])
			assert.Equal(t, "storage.googleapis.com/projects/_/buckets/b1", h["ce-subject"])
			assert.Equal(t, method, h["ce-methodname"])
			assert.Equal(t, "entry-1", h["ce-id"])
			assert.Contains(t, reqs[0].Body, "storage.buckets.create")
		})
	}
}
