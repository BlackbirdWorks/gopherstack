// Package gcpaudit lets GCP emulators record Cloud Audit Logs entries (protoPayload AuditLog) in the
// Logging emulator, which Eventarc triggers on google.cloud.audit.log.v1.written then match.
package gcpaudit

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"cloud.google.com/go/logging/apiv2/loggingpb"
	"github.com/google/uuid"
	"google.golang.org/genproto/googleapis/api/monitoredres"
	"google.golang.org/genproto/googleapis/cloud/audit"
	ltype "google.golang.org/genproto/googleapis/logging/type"
	"google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Kind selects the audit log a record is written to.
type Kind int

const (
	// Activity is an Admin Activity record (log id cloudaudit.googleapis.com/activity).
	Activity Kind = iota
	// DataWrite is a Data Access record for a data write (log id cloudaudit.googleapis.com/data_access).
	DataWrite
	// DataRead is a Data Access record for a data read.
	DataRead
)

// DefaultPrincipal is the caller recorded when no identity is known: emulator requests are unauthenticated.
const DefaultPrincipal = "emulator@gopherstack.iam.gserviceaccount.com"

// Sink receives log entries; *gcplogging.InMemoryBackend satisfies it.
type Sink interface {
	WriteLogEntries(ctx context.Context, req *loggingpb.WriteLogEntriesRequest) error
}

// Record is one audited API call.
type Record struct {
	Time         time.Time
	Resource     *monitoredres.MonitoredResource
	Request      *structpb.Struct
	Response     *structpb.Struct
	Failure      *status.Status
	Project      string
	ServiceName  string
	MethodName   string
	ResourceName string
	Principal    string
	Permission   string
	Kind         Kind
}

// Emitter writes Records to a Sink. The zero value is ready to use and drops records until a sink is set.
type Emitter struct {
	sink atomic.Pointer[Sink]
}

// SetSink directs records to s; nil stops recording.
func (e *Emitter) SetSink(s Sink) {
	if s == nil {
		e.sink.Store(nil)

		return
	}

	e.sink.Store(&s)
}

// Enabled reports whether a sink is set.
func (e *Emitter) Enabled() bool { return e.sink.Load() != nil }

// LogID returns the audit log id of kind, e.g. cloudaudit.googleapis.com/activity.
func LogID(k Kind) string {
	if k == Activity {
		return "cloudaudit.googleapis.com/activity"
	}

	return "cloudaudit.googleapis.com/data_access"
}

// LogName returns projects/{project}/logs/{escaped id} for kind.
func LogName(project string, k Kind) string {
	return "projects/" + project + "/logs/" + url.PathEscape(LogID(k))
}

// Entry builds the LogEntry for r.
func (r Record) Entry() (*loggingpb.LogEntry, error) {
	when := r.Time
	if when.IsZero() {
		when = time.Now()
	}

	principal := r.Principal
	if principal == "" {
		principal = DefaultPrincipal
	}

	payload := &audit.AuditLog{
		ServiceName:        r.ServiceName,
		MethodName:         r.MethodName,
		ResourceName:       r.ResourceName,
		AuthenticationInfo: &audit.AuthenticationInfo{PrincipalEmail: principal},
		Status:             r.Failure,
		Request:            r.Request,
		Response:           r.Response,
		RequestMetadata:    &audit.RequestMetadata{CallerIp: "127.0.0.1"},
	}

	if r.Permission != "" {
		payload.AuthorizationInfo = []*audit.AuthorizationInfo{{
			Resource: r.ResourceName, Permission: r.Permission, Granted: true,
		}}
	}

	packed, err := anypb.New(payload)
	if err != nil {
		return nil, fmt.Errorf("pack audit log: %w", err)
	}

	res := r.Resource
	if res == nil {
		res = &monitoredres.MonitoredResource{
			Type:   "audited_resource",
			Labels: map[string]string{"service": r.ServiceName, "method": r.MethodName},
		}
	}

	sev := ltype.LogSeverity_NOTICE
	if r.Kind != Activity {
		sev = ltype.LogSeverity_INFO
	}

	return &loggingpb.LogEntry{
		LogName:   LogName(r.Project, r.Kind),
		Resource:  res,
		Timestamp: timestamppb.New(when),
		Severity:  sev,
		InsertId:  strings.ReplaceAll(uuid.NewString(), "-", "")[:16],
		Payload:   &loggingpb.LogEntry_ProtoPayload{ProtoPayload: packed},
	}, nil
}

// Emit records r; it is a no-op without a sink or a project. Call it outside backend locks.
func (e *Emitter) Emit(ctx context.Context, r Record) {
	sink := e.sink.Load()
	if sink == nil || r.Project == "" {
		return
	}

	entry, err := r.Entry()
	if err != nil {
		return
	}

	_ = (*sink).WriteLogEntries(ctx, &loggingpb.WriteLogEntriesRequest{Entries: []*loggingpb.LogEntry{entry}})
}
