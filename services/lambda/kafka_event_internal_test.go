package lambda

import (
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func kafkaTestRecord(topic string, partition int32, offset int64, value string) KafkaRecord {
	return KafkaRecord{
		Topic:         topic,
		Partition:     partition,
		Offset:        offset,
		Timestamp:     time.UnixMilli(1545084650987),
		TimestampType: "CREATE_TIME",
		Key:           []byte("k"),
		Value:         []byte(value),
		Headers:       []KafkaHeader{{Key: "headerKey", Value: []byte("hv")}},
	}
}

func TestBuildKafkaEventPayload(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantGroups map[string]int
		name       string
		source     string
		sourceARN  string
		recs       []KafkaRecord
		wantARN    bool
	}{
		{
			name:       "self managed single partition",
			source:     kafkaEventSourceSelfManaged,
			recs:       []KafkaRecord{kafkaTestRecord("mytopic", 0, 15, "hello")},
			wantGroups: map[string]int{"mytopic-0": 1},
		},
		{
			name:      "msk carries event source arn",
			source:    kafkaEventSourceMSK,
			sourceARN: "arn:aws:kafka:us-east-1:123456789012:cluster/c/uuid",
			wantARN:   true,
			recs: []KafkaRecord{
				kafkaTestRecord("t", 0, 1, "a"),
				kafkaTestRecord("t", 1, 2, "b"),
				kafkaTestRecord("t", 1, 3, "c"),
			},
			wantGroups: map[string]int{"t-0": 1, "t-1": 2},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			raw, err := buildKafkaEventPayload(tt.source, tt.sourceARN, "b1:9092,b2:9092", tt.recs)
			require.NoError(t, err)

			var ev map[string]any

			require.NoError(t, json.Unmarshal(raw, &ev))
			assert.Equal(t, tt.source, ev["eventSource"])
			assert.Equal(t, "b1:9092,b2:9092", ev["bootstrapServers"])

			_, hasARN := ev["eventSourceArn"]
			assert.Equal(t, tt.wantARN, hasARN)

			records, ok := ev["records"].(map[string]any)
			require.True(t, ok)
			require.Len(t, records, len(tt.wantGroups))

			for group, n := range tt.wantGroups {
				assert.Len(t, records[group], n)
			}

			first, ok := records[tt.recs[0].Topic+"-0"].([]any)[0].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, tt.recs[0].Topic, first["topic"])
			assert.InDelta(t, 1545084650987.0, first["timestamp"], 0)
			assert.Equal(t, "CREATE_TIME", first["timestampType"])
			assert.Equal(t, base64.StdEncoding.EncodeToString([]byte("k")), first["key"])
			assert.Equal(t, base64.StdEncoding.EncodeToString(tt.recs[0].Value), first["value"])
			assert.Equal(
				t,
				[]any{map[string]any{"headerKey": []any{104.0, 118.0}}},
				first["headers"],
			)
		})
	}
}

func TestKafkaFilter(t *testing.T) {
	t.Parallel()

	pattern := func(p string) *FilterCriteria { return &FilterCriteria{Filters: []Filter{{Pattern: p}}} }

	tests := []struct {
		name        string
		filter      *FilterCriteria
		value       []byte
		wantMatched int
	}{
		{"no filter passes all", nil, []byte("anything"), 1},
		{
			"json prefix match",
			pattern(`{"value":{"device_ID":[{"prefix":"AB"}]}}`),
			[]byte(`{"device_ID":"AB1234"}`),
			1,
		},
		{
			"json prefix mismatch",
			pattern(`{"value":{"device_ID":[{"prefix":"AB"}]}}`),
			[]byte(`{"device_ID":"ZZ1234"}`),
			0,
		},
		{
			"plain string anything-but drops",
			pattern(`{"value":[{"anything-but":["error"]}]}`),
			[]byte("error"),
			0,
		},
		{
			"plain string anything-but keeps",
			pattern(`{"value":[{"anything-but":["error"]}]}`),
			[]byte("fine"),
			1,
		},
		{
			"non utf8 fails value filter",
			pattern(`{"value":[{"anything-but":["x"]}]}`),
			[]byte{0xff, 0xfe},
			0,
		},
		{"metadata topic filter", pattern(`{"topic":["mytopic"]}`), []byte{0xff, 0xfe}, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			matched, dropped := splitKafkaByFilter(
				tt.filter,
				[]KafkaRecord{kafkaTestRecord("mytopic", 0, 0, string(tt.value))},
			)
			assert.Len(t, matched, tt.wantMatched)
			assert.Len(t, dropped, 1-tt.wantMatched)
		})
	}
}

func TestSplitKafkaByPayload(t *testing.T) {
	t.Parallel()

	big := string(make([]byte, 1000))

	tests := []struct {
		name       string
		count      int
		limit      int
		wantChunks int
	}{
		{"empty", 0, 5000, 0},
		{"fits one chunk", 3, 100000, 1},
		{"splits when over limit", 6, 3500, 3},
		{"single oversized record still delivered", 1, 10, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			recs := make([]KafkaRecord, tt.count)
			for i := range recs {
				recs[i] = kafkaTestRecord("t", 0, int64(i), big)
			}

			chunks := splitKafkaByPayload(recs, tt.limit)
			require.Len(t, chunks, tt.wantChunks)

			total := 0
			for _, c := range chunks {
				total += len(c)
			}

			assert.Equal(t, tt.count, total)
		})
	}
}

func TestKafkaSourceBootstrap(t *testing.T) {
	t.Parallel()

	tests := []struct {
		src  *SelfManagedEventSource
		name string
		want []string
	}{
		{nil, "nil source", nil},
		{&SelfManagedEventSource{}, "no endpoints", nil},
		{
			&SelfManagedEventSource{
				Endpoints: map[string][]string{
					kafkaBootstrapEndpointKey: {"a:9092, b:9092", "c:9092"},
				},
			},
			"split and trimmed",
			[]string{"a:9092", "b:9092", "c:9092"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, kafkaSourceBootstrap(tt.src))
		})
	}
}
