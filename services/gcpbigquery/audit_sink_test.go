package gcpbigquery_test

import (
	"context"
	"sync"
	"testing"

	"cloud.google.com/go/logging/apiv2/loggingpb"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/cloud/audit"
)

type auditSink struct {
	mu      sync.Mutex
	entries []*loggingpb.LogEntry
}

func (s *auditSink) WriteLogEntries(_ context.Context, req *loggingpb.WriteLogEntriesRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.entries = append(s.entries, req.GetEntries()...)

	return nil
}

func (s *auditSink) logs(t *testing.T) []*audit.AuditLog {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]*audit.AuditLog, 0, len(s.entries))

	for _, e := range s.entries {
		var al audit.AuditLog
		require.NoError(t, e.GetProtoPayload().UnmarshalTo(&al))
		out = append(out, &al)
	}

	return out
}

func (s *auditSink) logNames() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]string, 0, len(s.entries))
	for _, e := range s.entries {
		out = append(out, e.GetLogName())
	}

	return out
}
