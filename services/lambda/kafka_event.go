package lambda

import (
	"encoding/base64"
	"encoding/json"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	kafkaEventSourceSelfManaged = "SelfManagedKafka"
	kafkaEventSourceMSK         = "aws:kafka"
	kafkaBootstrapEndpointKey   = "KAFKA_BOOTSTRAP_SERVERS"
	kafkaMaxPayloadBytes        = 6 * 1024 * 1024
	kafkaRecordOverheadBytes    = 256
	kafkaBase64Num              = 4
	kafkaBase64Den              = 3
)

// KafkaHeader is one record header; Value is delivered as a JSON byte array.
type KafkaHeader struct {
	Key   string
	Value []byte
}

// KafkaRecord is one Kafka message handed to the ESM batcher.
type KafkaRecord struct {
	Timestamp     time.Time
	raw           any
	Topic         string
	TimestampType string
	Key           []byte
	Value         []byte
	Headers       []KafkaHeader
	Offset        int64
	// HighWatermark is the partition's log end offset when the record was fetched; 0 when unknown.
	HighWatermark int64
	Partition     int32
}

type kafkaEventRecord struct {
	Topic         string             `json:"topic"`
	TimestampType string             `json:"timestampType"`
	Key           *string            `json:"key,omitempty"`
	Value         *string            `json:"value,omitempty"`
	Headers       []map[string][]int `json:"headers"`
	Offset        int64              `json:"offset"`
	Timestamp     int64              `json:"timestamp"`
	Partition     int32              `json:"partition"`
}

type kafkaEvent struct {
	Records          map[string][]kafkaEventRecord `json:"records"`
	EventSource      string                        `json:"eventSource"`
	EventSourceARN   string                        `json:"eventSourceArn,omitempty"`
	BootstrapServers string                        `json:"bootstrapServers"`
}

func b64Ptr(b []byte) *string {
	if b == nil {
		return nil
	}

	s := base64.StdEncoding.EncodeToString(b)

	return &s
}

// buildKafkaEventPayload shapes records into the documented Kafka event payload.
func buildKafkaEventPayload(eventSource, eventSourceARN, bootstrap string, recs []KafkaRecord) ([]byte, error) {
	ev := kafkaEvent{
		EventSource:      eventSource,
		EventSourceARN:   eventSourceARN,
		BootstrapServers: bootstrap,
		Records:          make(map[string][]kafkaEventRecord),
	}

	for _, r := range recs {
		headers := make([]map[string][]int, 0, len(r.Headers))
		for _, h := range r.Headers {
			v := make([]int, len(h.Value))
			for i, b := range h.Value {
				v[i] = int(b)
			}

			headers = append(headers, map[string][]int{h.Key: v})
		}

		key := r.Topic + "-" + strconv.Itoa(int(r.Partition))
		ev.Records[key] = append(ev.Records[key], kafkaEventRecord{
			Topic:         r.Topic,
			Partition:     r.Partition,
			Offset:        r.Offset,
			Timestamp:     r.Timestamp.UnixMilli(),
			TimestampType: r.TimestampType,
			Key:           b64Ptr(r.Key),
			Value:         b64Ptr(r.Value),
			Headers:       headers,
		})
	}

	return json.Marshal(ev)
}

// kafkaFilterView is the record shape FilterCriteria is matched against; only "value" is a data key.
func kafkaFilterView(r KafkaRecord) map[string]any {
	view := map[string]any{
		"topic":         r.Topic,
		"partition":     float64(r.Partition),
		"offset":        float64(r.Offset),
		"timestamp":     float64(r.Timestamp.UnixMilli()),
		"timestampType": r.TimestampType,
	}

	if !utf8.Valid(r.Value) {
		return view
	}

	if obj, ok := parseJSONObjectBytes(r.Value); ok {
		view["value"] = obj
	} else {
		view["value"] = string(r.Value)
	}

	return view
}

// splitKafkaByFilter partitions recs into those matching fc and those dropped.
func splitKafkaByFilter(fc *FilterCriteria, recs []KafkaRecord) ([]KafkaRecord, []KafkaRecord) {
	if fc == nil || len(fc.Filters) == 0 {
		return recs, nil
	}

	var matched, dropped []KafkaRecord

	for _, r := range recs {
		if eventFilterMatches(fc, kafkaFilterView(r)) {
			matched = append(matched, r)
		} else {
			dropped = append(dropped, r)
		}
	}

	return matched, dropped
}

func kafkaRecordSize(r KafkaRecord) int {
	raw := len(r.Key) + len(r.Value)
	for _, h := range r.Headers {
		raw += len(h.Value)
	}

	return raw*kafkaBase64Num/kafkaBase64Den + len(r.Topic) + kafkaRecordOverheadBytes
}

// splitKafkaByPayload chunks recs so each chunk's encoded size stays under limit.
func splitKafkaByPayload(recs []KafkaRecord, limit int) [][]KafkaRecord {
	var (
		chunks [][]KafkaRecord
		size   int
	)

	cur := make([]KafkaRecord, 0, len(recs))

	for _, r := range recs {
		sz := kafkaRecordSize(r)
		if len(cur) > 0 && size+sz > limit {
			chunks = append(chunks, cur)
			cur, size = make([]KafkaRecord, 0, len(recs)), 0
		}

		cur = append(cur, r)
		size += sz
	}

	if len(cur) > 0 {
		chunks = append(chunks, cur)
	}

	return chunks
}

// kafkaSourceBootstrap returns the self-managed bootstrap servers, comma separated.
func kafkaSourceBootstrap(src *SelfManagedEventSource) []string {
	if src == nil {
		return nil
	}

	var out []string

	for _, e := range src.Endpoints[kafkaBootstrapEndpointKey] {
		for p := range strings.SplitSeq(e, ",") {
			if p = strings.TrimSpace(p); p != "" {
				out = append(out, p)
			}
		}
	}

	return out
}
