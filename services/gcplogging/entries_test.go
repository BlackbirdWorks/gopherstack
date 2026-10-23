package gcplogging_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"cloud.google.com/go/logging"
	"cloud.google.com/go/logging/apiv2/loggingpb"
	"cloud.google.com/go/logging/logadmin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/genproto/googleapis/api/monitoredres"
	ltype "google.golang.org/genproto/googleapis/logging/type"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func global() *monitoredres.MonitoredResource {
	return &monitoredres.MonitoredResource{Type: "global", Labels: map[string]string{"project_id": testProject}}
}

func textEntry(logID, text string, sev ltype.LogSeverity, ts time.Time) *loggingpb.LogEntry {
	return &loggingpb.LogEntry{
		LogName:   proj + "/logs/" + logID,
		Severity:  sev,
		Timestamp: timestamppb.New(ts),
		Payload:   &loggingpb.LogEntry_TextPayload{TextPayload: text},
	}
}

func (e *env) write(t *testing.T, entries ...*loggingpb.LogEntry) {
	t.Helper()

	_, err := e.raw(t).
		WriteLogEntries(t.Context(), &loggingpb.WriteLogEntriesRequest{Resource: global(), Entries: entries})
	require.NoError(t, err)
}

func (e *env) list(t *testing.T, req *loggingpb.ListLogEntriesRequest) []*loggingpb.LogEntry {
	t.Helper()

	if len(req.GetResourceNames()) == 0 {
		req.ResourceNames = []string{proj}
	}

	var out []*loggingpb.LogEntry

	it := e.raw(t).ListLogEntries(t.Context(), req)

	for {
		ent, err := it.Next()
		if errors.Is(err, iterator.Done) {
			return out
		}

		require.NoError(t, err)

		out = append(out, ent)
	}
}

func texts(entries []*loggingpb.LogEntry) []string {
	out := make([]string, len(entries))
	for i, e := range entries {
		out[i] = e.GetTextPayload()
	}

	return out
}

func TestClientWriteAndAdminRead(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	// CommonResource skips GCE metadata detection, whose pooled HTTP connection outlives the test.
	lg := e.logger(t).Logger("app", logging.CommonResource(&monitoredres.MonitoredResource{
		Type: "project", Labels: map[string]string{"project_id": testProject},
	}), logging.CommonLabels(map[string]string{"env": "it"}))
	now := time.Now()

	lg.Log(logging.Entry{Payload: "plain text", Severity: logging.Info, Timestamp: now.Add(-3 * time.Second)})
	lg.Log(logging.Entry{
		Payload: map[string]any{"msg": "disk full", "pct": 99}, Severity: logging.Error,
		Labels: map[string]string{"team": "sre"}, Timestamp: now.Add(-2 * time.Second),
	})
	lg.Log(logging.Entry{Payload: "later", Severity: logging.Warning, Timestamp: now.Add(-1 * time.Second)})
	require.NoError(t, lg.Flush())

	admin := e.admin(t)

	tests := []struct {
		name   string
		filter string
		opts   []logadmin.EntriesOption
		want   int
	}{
		{"all", `logName="projects/test-project/logs/app"`, nil, 3},
		{"severity", `logName="projects/test-project/logs/app" AND severity>=WARNING`, nil, 2},
		{"json field", `jsonPayload.msg:"disk"`, nil, 1},
		{"merged label", `labels.env="it" AND labels.team="sre"`, nil, 1},
		{"common label on all", `labels.env="it" AND logName="projects/test-project/logs/app"`, nil, 3},
		{
			"newest first",
			`logName="projects/test-project/logs/app"`,
			[]logadmin.EntriesOption{logadmin.NewestFirst()},
			3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			it := admin.Entries(
				t.Context(),
				append([]logadmin.EntriesOption{logadmin.Filter(tt.filter)}, tt.opts...)...)

			var got []*logging.Entry

			for {
				ent, err := it.Next()
				if errors.Is(err, iterator.Done) {
					break
				}

				require.NoError(t, err)

				got = append(got, ent)
			}

			assert.Len(t, got, tt.want)
		})
	}

	it := admin.Entries(
		t.Context(),
		logadmin.Filter(`logName="projects/test-project/logs/app"`),
		logadmin.NewestFirst(),
	)
	first, err := it.Next()
	require.NoError(t, err)
	assert.Equal(t, "later", first.Payload)
	assert.Equal(t, logging.Warning, first.Severity)
	assert.Equal(t, "project", first.Resource.GetType())
}

func TestWriteDefaults(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	raw := e.raw(t)

	_, err := raw.WriteLogEntries(t.Context(), &loggingpb.WriteLogEntriesRequest{
		LogName:  proj + "/logs/defaults",
		Resource: &monitoredres.MonitoredResource{Type: "gce_instance", Labels: map[string]string{"zone": "z"}},
		Labels:   map[string]string{"a": "request", "b": "request"},
		Entries: []*loggingpb.LogEntry{
			{Payload: &loggingpb.LogEntry_TextPayload{TextPayload: "one"}, Labels: map[string]string{"b": "entry"}},
			{Payload: &loggingpb.LogEntry_TextPayload{TextPayload: "two"}, InsertId: "fixed-id"},
		},
	})
	require.NoError(t, err)

	got := e.list(t, &loggingpb.ListLogEntriesRequest{})
	require.Len(t, got, 2)

	first := got[0]
	assert.Equal(t, proj+"/logs/defaults", first.GetLogName())
	assert.Equal(t, "gce_instance", first.GetResource().GetType())
	assert.Equal(t, map[string]string{"a": "request", "b": "entry"}, first.GetLabels())
	assert.NotEmpty(t, first.GetInsertId())
	assert.Equal(t, e.clk.now().Unix(), first.GetTimestamp().GetSeconds())
	assert.Equal(t, e.clk.now().Unix(), first.GetReceiveTimestamp().GetSeconds())
	assert.Equal(t, "fixed-id", got[1].GetInsertId())
	assert.Equal(t, ltype.LogSeverity_DEFAULT, first.GetSeverity())
}

func TestWriteValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		req  *loggingpb.WriteLogEntriesRequest
		name string
		code codes.Code
	}{
		{
			&loggingpb.WriteLogEntriesRequest{Resource: global(), Entries: []*loggingpb.LogEntry{{}}},
			"no log name",
			codes.InvalidArgument,
		},
		{&loggingpb.WriteLogEntriesRequest{
			LogName: "bogus", Resource: global(), Entries: []*loggingpb.LogEntry{{}},
		}, "bad log name", codes.InvalidArgument},
		{&loggingpb.WriteLogEntriesRequest{
			LogName: proj + "/logs/x", Entries: []*loggingpb.LogEntry{{}},
		}, "no resource", codes.InvalidArgument},
		{&loggingpb.WriteLogEntriesRequest{LogName: proj + "/logs/x", Resource: global()}, "empty ok", codes.OK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := newEnv(t, false).raw(t).WriteLogEntries(t.Context(), tt.req)
			assert.Equal(t, tt.code, status.Code(err))
		})
	}
}

func TestPartialSuccess(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		wantTexts []string
		partial   bool
	}{
		{"all or nothing", nil, false},
		{"partial writes valid", []string{"good-1", "good-2"}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, false)
			now := time.Now()
			bad := textEntry("p", "bad", 0, now)
			bad.LogName = "nonsense"

			_, err := e.raw(t).WriteLogEntries(t.Context(), &loggingpb.WriteLogEntriesRequest{
				Resource: global(), PartialSuccess: tt.partial,
				Entries: []*loggingpb.LogEntry{textEntry("p", "good-1", 0, now), bad, textEntry("p", "good-2", 0, now)},
			})
			require.Error(t, err)
			assert.Equal(t, codes.InvalidArgument, status.Code(err))

			if tt.partial {
				var found *loggingpb.WriteLogEntriesPartialErrors

				for _, d := range status.Convert(err).Details() {
					if pe, ok := d.(*loggingpb.WriteLogEntriesPartialErrors); ok {
						found = pe
					}
				}

				require.NotNil(t, found)
				assert.Contains(t, found.GetLogEntryErrors(), int32(1))
			}

			assert.ElementsMatch(t, tt.wantTexts, texts(e.list(t, &loggingpb.ListLogEntriesRequest{})))
		})
	}
}

func TestDryRunAndDedupe(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	raw := e.raw(t)
	ent := textEntry("d", "once", 0, e.clk.now())
	ent.InsertId = "same"

	_, err := raw.WriteLogEntries(t.Context(), &loggingpb.WriteLogEntriesRequest{
		Resource: global(), DryRun: true, Entries: []*loggingpb.LogEntry{ent},
	})
	require.NoError(t, err)
	assert.Empty(t, e.list(t, &loggingpb.ListLogEntriesRequest{}))

	e.write(t, ent)
	e.write(t, ent)
	assert.Equal(t, []string{"once"}, texts(e.list(t, &loggingpb.ListLogEntriesRequest{})))
}

func TestOrderAndPagination(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	base := e.clk.now().Add(-time.Hour)

	for i := range 5 {
		e.write(t, textEntry("page", fmt.Sprintf("e%d", i), 0, base.Add(time.Duration(i)*time.Minute)))
	}

	tests := []struct {
		name  string
		order string
		want  []string
	}{
		{"default asc", "", []string{"e0", "e1", "e2", "e3", "e4"}},
		{"asc", "timestamp asc", []string{"e0", "e1", "e2", "e3", "e4"}},
		{"desc", "timestamp desc", []string{"e4", "e3", "e2", "e1", "e0"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := e.list(t, &loggingpb.ListLogEntriesRequest{OrderBy: tt.order, PageSize: 2})
			assert.Equal(t, tt.want, texts(got))
		})
	}

	_, err := e.raw(t).ListLogEntries(t.Context(), &loggingpb.ListLogEntriesRequest{
		ResourceNames: []string{proj}, OrderBy: "severity",
	}).Next()
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestDefaultWindowAndRetention(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	old := e.clk.now().Add(-72 * time.Hour)
	e.write(t, textEntry("w", "old", 0, old), textEntry("w", "new", 0, e.clk.now()))

	assert.Equal(t, []string{"new"}, texts(e.list(t, &loggingpb.ListLogEntriesRequest{})))
	assert.ElementsMatch(t, []string{"old", "new"}, texts(e.list(t, &loggingpb.ListLogEntriesRequest{
		Filter: `timestamp>="2029-12-01T00:00:00Z"`,
	})))

	e.clk.advance(31 * 24 * time.Hour)
	assert.Empty(t, e.list(t, &loggingpb.ListLogEntriesRequest{Filter: `timestamp>="2029-12-01T00:00:00Z"`}))
}

func TestListLogsAndDeleteLog(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	e.write(t, textEntry("alpha", "a", 0, e.clk.now()), textEntry("beta%2Fsub", "b", 0, e.clk.now()))
	raw := e.raw(t)

	var names []string

	it := raw.ListLogs(t.Context(), &loggingpb.ListLogsRequest{Parent: proj})

	for {
		n, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}

		require.NoError(t, err)

		names = append(names, n)
	}

	assert.Equal(t, []string{proj + "/logs/alpha", proj + "/logs/beta%2Fsub"}, names)

	require.NoError(t, raw.DeleteLog(t.Context(), &loggingpb.DeleteLogRequest{LogName: proj + "/logs/alpha"}))
	assert.Equal(t, []string{"b"}, texts(e.list(t, &loggingpb.ListLogEntriesRequest{})))
	assert.Equal(
		t,
		codes.NotFound,
		status.Code(raw.DeleteLog(t.Context(), &loggingpb.DeleteLogRequest{LogName: proj + "/logs/alpha"})),
	)
}

func TestMonitoredResourceDescriptors(t *testing.T) {
	t.Parallel()

	it := newEnv(t, false).admin(t).ResourceDescriptors(t.Context())

	seen := map[string]bool{}

	for {
		d, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}

		require.NoError(t, err)

		seen[d.GetType()] = true
	}

	for _, want := range []string{"global", "gce_instance", "cloud_run_revision", "cloud_function", "gcs_bucket"} {
		assert.True(t, seen[want], want)
	}
}

func TestTailEntries(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	stream, err := e.raw(t).TailLogEntries(t.Context())
	require.NoError(t, err)
	require.NoError(
		t,
		stream.Send(&loggingpb.TailLogEntriesRequest{ResourceNames: []string{proj}, Filter: `severity>=ERROR`}),
	)

	// The first server message is only sent once the subscription exists, so write until one arrives.
	got := make(chan *loggingpb.TailLogEntriesResponse, 1)

	go func() {
		if resp, rerr := stream.Recv(); rerr == nil {
			got <- resp
		}
	}()

	deadline := time.After(10 * time.Second)

	for {
		e.write(t, textEntry("tail", "quiet", ltype.LogSeverity_INFO, e.clk.now()),
			textEntry("tail", "loud", ltype.LogSeverity_ERROR, e.clk.now()))

		select {
		case resp := <-got:
			require.NotEmpty(t, resp.GetEntries())

			for _, ent := range resp.GetEntries() {
				assert.Equal(t, "loud", ent.GetTextPayload())
			}

			return
		case <-time.After(50 * time.Millisecond):
		case <-deadline:
			t.Fatal("no tailed entry received")
		}
	}
}

func TestStructPayloadRoundTrip(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	payload, err := structpb.NewStruct(map[string]any{"a": map[string]any{"b": "c"}, "n": 3})
	require.NoError(t, err)

	e.write(t, &loggingpb.LogEntry{
		LogName: proj + "/logs/s", Timestamp: timestamppb.New(e.clk.now()),
		Payload:     &loggingpb.LogEntry_JsonPayload{JsonPayload: payload},
		HttpRequest: &ltype.HttpRequest{RequestMethod: "GET", Status: 502},
	})

	got := e.list(
		t,
		&loggingpb.ListLogEntriesRequest{
			Filter: `jsonPayload.a.b="c" AND httpRequest.status>=500 AND httpRequest.requestMethod=GET`,
		},
	)
	require.Len(t, got, 1)
	assert.Equal(t, "c", got[0].GetJsonPayload().GetFields()["a"].GetStructValue().GetFields()["b"].GetStringValue())
}
