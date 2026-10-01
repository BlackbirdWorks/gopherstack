package eventbridge //nolint:testpackage // needs buildEventEnvelopeMap.

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBuildEventEnvelopeMap_MatchesJSONRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		entry EventEntry
	}{
		{
			name:  "object detail",
			entry: EventEntry{Source: "s", DetailType: "d", Detail: `{"a":{"b":[1,"x",true,null]}}`},
		},
		{name: "null detail", entry: EventEntry{Source: "s", DetailType: "d", Detail: `null`}},
		{name: "array detail", entry: EventEntry{Source: "s", DetailType: "d", Detail: `[1,2]`}},
		{name: "invalid detail", entry: EventEntry{Source: "s", DetailType: "d", Detail: `nope`}},
		{name: "empty detail", entry: EventEntry{Source: "s", DetailType: "d"}},
		{
			name: "bus and resources",
			entry: EventEntry{
				Source: "s", DetailType: "d", Detail: `{"n":1.5}`, EventBusName: "bus", Resources: []string{"r1", "r2"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var viaJSON map[string]any
			require.NoError(t, json.Unmarshal([]byte(buildEventEnvelope(tt.entry)), &viaJSON))
			require.Equal(t, viaJSON, buildEventEnvelopeMap(tt.entry))
		})
	}
}

func TestFilterArchivedEvents_Pattern(t *testing.T) {
	t.Parallel()

	events := []EventEntry{
		{Source: "a", DetailType: "T", Detail: `{"id":1}`},
		{Source: "b", DetailType: "T", Detail: `{"id":2}`},
	}

	tests := []struct {
		name    string
		pattern string
		want    int
	}{
		{name: "no pattern", pattern: "", want: 2},
		{name: "source match", pattern: `{"source":["a"]}`, want: 1},
		{name: "detail numeric", pattern: `{"detail":{"id":[{"numeric":[">",1]}]}}`, want: 1},
		{name: "invalid pattern", pattern: `{`, want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			be := NewInMemoryBackend()
			t.Cleanup(be.Close)
			be.archivedEventsStore(be.region)["arc"] = events

			got := be.filterArchivedEvents(be.region, "arc", tt.pattern, time.Time{}, time.Time{})
			require.Len(t, got, tt.want)
		})
	}
}
