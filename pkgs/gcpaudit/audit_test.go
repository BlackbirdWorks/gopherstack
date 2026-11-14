package gcpaudit_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/logging/apiv2/loggingpb"
	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/cloud/audit"
	"google.golang.org/grpc"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpaudit"
)

type sink struct {
	entries []*loggingpb.LogEntry
	mu      sync.Mutex
}

func (s *sink) WriteLogEntries(_ context.Context, req *loggingpb.WriteLogEntriesRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.entries = append(s.entries, req.GetEntries()...)

	return nil
}

func TestRecordEntry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantLog      string
		wantResource string
		rec          gcpaudit.Record
	}{
		{
			name: "admin activity", wantLog: "projects/p/logs/cloudaudit.googleapis.com%2Factivity",
			rec: gcpaudit.Record{
				Project: "p", ServiceName: "s.googleapis.com", MethodName: "s.Create", ResourceName: "projects/p/x/1",
				Kind: gcpaudit.Activity, Permission: "s.x.create",
			},
			wantResource: "audited_resource",
		},
		{
			name: "data access", wantLog: "projects/p/logs/cloudaudit.googleapis.com%2Fdata_access",
			rec:          gcpaudit.Record{Project: "p", ServiceName: "s", MethodName: "m", Kind: gcpaudit.DataRead},
			wantResource: "audited_resource",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.rec.Time = time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)

			e, err := tt.rec.Entry()
			require.NoError(t, err)
			assert.Equal(t, tt.wantLog, e.GetLogName())
			assert.Equal(t, tt.wantResource, e.GetResource().GetType())
			assert.Equal(t, tt.rec.Time, e.GetTimestamp().AsTime())
			assert.Len(t, e.GetInsertId(), 16)

			var al audit.AuditLog
			require.NoError(t, e.GetProtoPayload().UnmarshalTo(&al))
			assert.Equal(t, tt.rec.MethodName, al.GetMethodName())
			assert.Equal(t, gcpaudit.DefaultPrincipal, al.GetAuthenticationInfo().GetPrincipalEmail())
			assert.Equal(t, tt.rec.Permission != "", len(al.GetAuthorizationInfo()) == 1)
		})
	}
}

func TestEmitterWithoutSinkOrProject(t *testing.T) {
	t.Parallel()

	var em gcpaudit.Emitter

	em.Emit(t.Context(), gcpaudit.Record{Project: "p"})
	assert.False(t, em.Enabled())

	s := &sink{}
	em.SetSink(s)
	assert.True(t, em.Enabled())

	em.Emit(t.Context(), gcpaudit.Record{})
	assert.Empty(t, s.entries, "a record without a project has no log to go to")

	em.Emit(t.Context(), gcpaudit.Record{Project: "p", ServiceName: "s", MethodName: "m"})
	assert.Len(t, s.entries, 1)

	em.SetSink(nil)
	assert.False(t, em.Enabled())
}

func TestUnaryInterceptorClassification(t *testing.T) {
	t.Parallel()

	policy := gcpaudit.Policy{
		ServiceName:   "secretmanager.googleapis.com",
		AdminPrefixes: []string{"Create", "Delete"},
		Data:          map[string]gcpaudit.Kind{"AccessSecretVersion": gcpaudit.DataRead},
	}

	tests := []struct {
		name       string
		method     string
		wantLog    string
		wantLogged bool
	}{
		{
			name:       "admin prefix",
			method:     "/google.cloud.secretmanager.v1.SecretManagerService/CreateSecret",
			wantLogged: true,
			wantLog:    "activity",
		},
		{
			name: "data method", method: "/google.cloud.secretmanager.v1.SecretManagerService/AccessSecretVersion",
			wantLogged: true, wantLog: "data_access",
		},
		{name: "read is not audited", method: "/google.cloud.secretmanager.v1.SecretManagerService/GetSecret"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var em gcpaudit.Emitter

			s := &sink{}
			em.SetSink(s)

			icpt := em.UnaryInterceptor(policy, time.Now)
			req := &secretmanagerpb.CreateSecretRequest{Parent: "projects/p"}
			_, err := icpt(t.Context(), req, &grpc.UnaryServerInfo{FullMethod: tt.method},
				func(context.Context, any) (any, error) {
					return &secretmanagerpb.Secret{Name: "projects/p/secrets/x"}, nil
				})
			require.NoError(t, err)

			if !tt.wantLogged {
				assert.Empty(t, s.entries)

				return
			}

			require.Len(t, s.entries, 1)
			assert.Contains(t, s.entries[0].GetLogName(), tt.wantLog)

			var al audit.AuditLog
			require.NoError(t, s.entries[0].GetProtoPayload().UnmarshalTo(&al))
			assert.Equal(t, "projects/p/secrets/x", al.GetResourceName())
			assert.Equal(t, gcpaudit.MethodName(tt.method), al.GetMethodName())
		})
	}
}

func TestProjectOf(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "p", gcpaudit.ProjectOf("projects/p/secrets/s"))
	assert.Empty(t, gcpaudit.ProjectOf("folders/1"))
}
