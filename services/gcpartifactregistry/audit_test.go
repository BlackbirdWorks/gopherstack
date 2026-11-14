package gcpartifactregistry_test

import (
	"context"
	"sync"
	"testing"

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"
	"cloud.google.com/go/logging/apiv2/loggingpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/cloud/audit"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpaudit"
)

type auditSink struct {
	entries []*loggingpb.LogEntry
	mu      sync.Mutex
}

func (s *auditSink) WriteLogEntries(_ context.Context, req *loggingpb.WriteLogEntriesRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.entries = append(s.entries, req.GetEntries()...)

	return nil
}

func (s *auditSink) logged() []*loggingpb.LogEntry {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]*loggingpb.LogEntry(nil), s.entries...)
}

func TestAuditLogsRepositoryChanges(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	sink := &auditSink{}
	e.handler.Backend.Audit.SetSink(sink)

	e.createRepo(t, "audited", genericRepo())

	op, err := e.client.DeleteRepository(
		t.Context(),
		&artifactregistrypb.DeleteRepositoryRequest{Name: repoName("audited")},
	)
	require.NoError(t, err)
	require.NoError(t, op.Wait(t.Context()))

	methods := make([]string, 0, len(sink.logged()))

	for _, en := range sink.logged() {
		var al audit.AuditLog
		require.NoError(t, en.GetProtoPayload().UnmarshalTo(&al))
		assert.Equal(t, "artifactregistry.googleapis.com", al.GetServiceName())
		assert.Equal(t, gcpaudit.LogName(project, gcpaudit.Activity), en.GetLogName())

		methods = append(methods, al.GetMethodName())
	}

	assert.Equal(t, []string{
		"google.devtools.artifactregistry.v1.ArtifactRegistry.CreateRepository",
		"google.devtools.artifactregistry.v1.ArtifactRegistry.DeleteRepository",
	}, methods)
}
