package gcpeventarc_test

import (
	"encoding/json/v2"
	"testing"

	"cloud.google.com/go/eventarc/apiv1/eventarcpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

func auditFilters(service, method string, extra ...*eventarcpb.EventFilter) []*eventarcpb.EventFilter {
	return append([]*eventarcpb.EventFilter{
		typeFilter("google.cloud.audit.log.v1.written"),
		{Attribute: "serviceName", Value: service},
		{Attribute: "methodName", Value: method},
	}, extra...)
}

func TestAuditLogDelivery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		act          func(t *testing.T, e *env)
		name         string
		method       string
		wantResource string
		wantLog      string
		extra        []*eventarcpb.EventFilter
	}{
		{
			name: "bucket create with resource pattern", method: "storage.buckets.create",
			extra: []*eventarcpb.EventFilter{{
				Attribute: "resourceName", Value: "projects/_/buckets/audit-*", Operator: "match-path-pattern",
			}},
			act: func(t *testing.T, e *env) {
				t.Helper()
				require.NoError(t, e.gcs.Bucket("other").Create(t.Context(), project, nil))
				require.NoError(t, e.gcs.Bucket("audit-one").Create(t.Context(), project, nil))
			},
			wantResource: "projects/_/buckets/audit-one", wantLog: "activity",
		},
		{
			name: "object create is a data access write", method: "storage.objects.create",
			act: func(t *testing.T, e *env) {
				t.Helper()
				require.NoError(t, e.gcs.Bucket("data-bkt").Create(t.Context(), project, nil))

				w := e.gcs.Bucket("data-bkt").Object("a/b.txt").NewWriter(t.Context())
				_, err := w.Write([]byte("x"))
				require.NoError(t, err)
				require.NoError(t, w.Close())
			},
			wantResource: "projects/_/buckets/data-bkt/objects/a/b.txt", wantLog: "data_access",
		},
		{
			name: "bucket delete skips the earlier create", method: "storage.buckets.delete",
			act: func(t *testing.T, e *env) {
				t.Helper()
				require.NoError(t, e.gcs.Bucket("gone").Create(t.Context(), project, nil))
				require.NoError(t, e.gcs.Bucket("gone").Delete(t.Context()))
			},
			wantResource: "projects/_/buckets/gone", wantLog: "activity",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.createTrigger(t, "audit-trigger", &eventarcpb.Trigger{
				EventFilters: auditFilters("storage.googleapis.com", tt.method, tt.extra...),
				Destination:  httpDestination(e.dest.srv.URL + "/audit"),
			})

			tt.act(t, e)

			got := e.dest.next(t)
			assert.Equal(t, "/audit", got.path)
			assert.Equal(t, "google.cloud.audit.log.v1.written", got.header.Get("Ce-Type"))
			assert.Equal(t, "//storage.googleapis.com/projects/"+project, got.header.Get("Ce-Source"))
			assert.Equal(t, "storage.googleapis.com/"+tt.wantResource, got.header.Get("Ce-Subject"))
			assert.Equal(t, tt.method, got.header.Get("Ce-Methodname"))
			assert.Equal(t, tt.wantResource, got.header.Get("Ce-Resourcename"))
			assert.Equal(t, "application/json", got.header.Get("Content-Type"))

			var entry struct {
				ProtoPayload struct {
					Type         string `json:"@type"`
					MethodName   string `json:"methodName"`
					ServiceName  string `json:"serviceName"`
					ResourceName string `json:"resourceName"`
					AuthInfo     struct {
						Principal string `json:"principalEmail"`
					} `json:"authenticationInfo"`
				} `json:"protoPayload"`
				LogName string `json:"logName"`
			}

			require.NoError(t, json.Unmarshal(got.body, &entry, jsonx.V1()))
			assert.Equal(t, "type.googleapis.com/google.cloud.audit.AuditLog", entry.ProtoPayload.Type)
			assert.Equal(t, tt.method, entry.ProtoPayload.MethodName)
			assert.Equal(t, "storage.googleapis.com", entry.ProtoPayload.ServiceName)
			assert.Equal(t, tt.wantResource, entry.ProtoPayload.ResourceName)
			assert.NotEmpty(t, entry.ProtoPayload.AuthInfo.Principal)
			assert.Contains(t, entry.LogName, "cloudaudit.googleapis.com%2F"+tt.wantLog)
		})
	}
}

func TestAuditLogEntriesAreStored(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	require.NoError(t, e.gcs.Bucket("stored").Create(t.Context(), project, nil))

	entries, _, err := e.logs.ListLogEntries(listAuditRequest())
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "projects/"+project+"/logs/cloudaudit.googleapis.com%2Factivity", entries[0].GetLogName())
	assert.Equal(t, "gcs_bucket", entries[0].GetResource().GetType())
}

func TestAuditTriggerValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		filters []*eventarcpb.EventFilter
		want    codes.Code
	}{
		{"valid", auditFilters("storage.googleapis.com", "storage.buckets.create"), codes.OK},
		{"method required", []*eventarcpb.EventFilter{
			typeFilter(
				"google.cloud.audit.log.v1.written",
			), {Attribute: "serviceName", Value: "storage.googleapis.com"},
		}, codes.InvalidArgument},
		{"unknown attribute", auditFilters("storage.googleapis.com", "m",
			&eventarcpb.EventFilter{Attribute: "bucket", Value: "b"}), codes.InvalidArgument},
		{"path pattern on method", []*eventarcpb.EventFilter{
			typeFilter("google.cloud.audit.log.v1.written"),
			{Attribute: "serviceName", Value: "s"},
			{Attribute: "methodName", Value: "m*", Operator: "match-path-pattern"},
		}, codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			_, err := e.client.CreateTrigger(t.Context(), &eventarcpb.CreateTriggerRequest{
				Parent: parent, TriggerId: "audit-validate",
				Trigger: &eventarcpb.Trigger{EventFilters: tt.filters, Destination: httpDestination(e.dest.srv.URL)},
			})
			assert.Equal(t, tt.want, code(err))
		})
	}
}
