package gcppubsub

import (
	"time"

	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
)

// SetClock replaces the backend clock.
func (b *InMemoryBackend) SetClock(now func() time.Time) {
	b.mu.Lock("SetClock")
	defer b.mu.Unlock()

	b.now = now
}

// CompileFilter exposes the filter compiler; the returned func evaluates attributes.
func CompileFilter(s string) (func(map[string]string) bool, error) {
	f, err := compileFilter(s)
	if err != nil {
		return nil, err
	}

	if f == nil {
		return func(map[string]string) bool { return true }, nil
	}

	return f.eval, nil
}

// ValidateData exposes schema payload validation (type: 1 protobuf, 2 avro; enc: 1 JSON, 2 binary).
func ValidateData(typ int32, def string, enc int32, data []byte) bool {
	return validateData(pubsubpb.Schema_Type(typ), def, pubsubpb.Encoding(enc), data)
}

// PullMessages leases up to n messages without waiting.
func (b *InMemoryBackend) PullMessages(name string, n int) ([]*pubsubpb.ReceivedMessage, error) {
	res, err := b.pull(name, n)

	return res.msgs, err
}
