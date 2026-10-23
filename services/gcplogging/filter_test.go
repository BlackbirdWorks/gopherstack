package gcplogging_test

import (
	"testing"
	"time"

	"cloud.google.com/go/logging/apiv2/loggingpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/api/monitoredres"
	ltype "google.golang.org/genproto/googleapis/logging/type"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/services/gcplogging"
)

func filterEntry(t *testing.T) *loggingpb.LogEntry {
	t.Helper()

	payload, err := structpb.NewStruct(map[string]any{
		"message": "Disk almost FULL",
		"code":    float64(503),
		"user":    map[string]any{"name": "ann", "tags": []any{"a", "b"}},
	})
	require.NoError(t, err)

	return &loggingpb.LogEntry{
		LogName: "projects/p/logs/syslog",
		Resource: &monitoredres.MonitoredResource{
			Type:   "gce_instance",
			Labels: map[string]string{"zone": "us-central1-a"},
		},
		Severity:  ltype.LogSeverity_ERROR,
		Timestamp: timestamppb.New(time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)),
		Labels:    map[string]string{"env": "prod", "compute.googleapis.com/resource_name": "vm-1"},
		InsertId:  "abc",
		Payload:   &loggingpb.LogEntry_JsonPayload{JsonPayload: payload},
	}
}

func TestFilterMatching(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		filter string
		want   bool
	}{
		{"empty", ``, true},
		{"resource type", `resource.type="gce_instance"`, true},
		{"resource type no", `resource.type="gae_app"`, false},
		{"log name", `logName="projects/p/logs/syslog"`, true},
		{"log id", `LOG_ID("syslog")`, true},
		{"log id no", `LOG_ID("other")`, false},
		{"severity gte", `severity>=WARNING`, true},
		{"severity gt", `severity>ERROR`, false},
		{"severity eq", `severity=ERROR`, true},
		{"severity lt", `severity<CRITICAL`, true},
		{"label eq", `labels.env="prod"`, true},
		{"label quoted key", `labels."compute.googleapis.com/resource_name"="vm-1"`, true},
		{"label ne", `labels.env!="prod"`, false},
		{"label missing ne", `labels.nope!="x"`, true},
		{"timestamp gte", `timestamp>="2030-01-01T00:00:00Z"`, true},
		{"timestamp lt", `timestamp<"2030-01-02T00:00:00Z"`, false},
		{"timestamp date only", `timestamp>"2030-01-02"`, true},
		{"json has", `jsonPayload.message:"disk"`, true},
		{"json eq case insensitive", `jsonPayload.message="disk almost full"`, true},
		{"json number gt", `jsonPayload.code>500`, true},
		{"json number lt", `jsonPayload.code<500`, false},
		{"json nested", `jsonPayload.user.name="ann"`, true},
		{"json array any", `jsonPayload.user.tags="b"`, true},
		{"json array index", `jsonPayload.user.tags[1]="b"`, true},
		{"json array index no", `jsonPayload.user.tags[0]="b"`, false},
		{"regex", `jsonPayload.message=~"^Disk .* FULL$"`, true},
		{"regex no", `jsonPayload.message=~"^disk"`, false},
		{"not regex", `jsonPayload.message!~"^disk"`, true},
		{"has any", `jsonPayload:"ann"`, true},
		{"exists", `jsonPayload.user.name:*`, true},
		{"exists no", `jsonPayload.nothing:*`, false},
		{"and", `severity>=ERROR AND resource.type="gce_instance"`, true},
		{"implicit and", `severity>=ERROR resource.type="gae_app"`, false},
		{"or", `resource.type="gae_app" OR severity=ERROR`, true},
		{"not", `NOT severity=ERROR`, false},
		{"minus", `-severity=ERROR`, false},
		{"parens", `(severity=ERROR OR severity=INFO) AND labels.env="prod"`, true},
		{"or binds tighter than and", `severity=ERROR AND resource.type="gae_app" OR labels.env="prod"`, true},
		{"value group", `resource.labels.zone:("us-central1" OR "europe")`, true},
		{"value group no", `resource.labels.zone=("europe-west1-b" OR "asia")`, false},
		{"bare text", `"almost full"`, true},
		{"bare text no", `"healthy"`, false},
		{"search fn", `SEARCH("ann")`, true},
		{"sample all", `sample(insertId, 1)`, true},
		{"sample none", `sample(insertId, 0)`, false},
		{"comment", "severity=ERROR -- trailing note\n AND labels.env=\"prod\"", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := gcplogging.MatchFilter(tt.filter, filterEntry(t))
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestFilterErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		filter string
	}{
		{"unterminated string", `labels.env="prod`},
		{"missing paren", `(severity=ERROR`},
		{"extra paren", `severity=ERROR)`},
		{"dangling and", `severity=ERROR AND`},
		{"unknown function", `bogus("x")`},
		{"bad regex", `jsonPayload.message=~"("`},
		{"bad sample", `sample(insertId, 7)`},
		{"missing value", `severity=`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := gcplogging.MatchFilter(tt.filter, filterEntry(t))
			require.Error(t, err)
		})
	}
}
