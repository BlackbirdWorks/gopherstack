package gcppubsub

import (
	"context"
	"net/http"
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

// SetResourceLimits lowers the per-project resource limits.
func (b *InMemoryBackend) SetResourceLimits(topics, subs, snapshots, schemas int) {
	b.mu.Lock("SetResourceLimits")
	defer b.mu.Unlock()

	b.limits = resourceLimits{topics: topics, subs: subs, snapshots: snapshots, schemas: schemas}
}

// BuildPushRequest builds the HTTP request a push delivery would send.
func (b *InMemoryBackend) BuildPushRequest(
	ctx context.Context, sub, endpoint string, cfg *pubsubpb.PushConfig, rm *pubsubpb.ReceivedMessage,
) (*http.Request, error) {
	return b.buildPushRequest(ctx, pushJob{sub: sub, endpoint: endpoint, cfg: cfg}, rm)
}

// SendPush delivers one message with the production push client and reports whether it was acknowledged.
func (b *InMemoryBackend) SendPush(
	ctx context.Context, sub, endpoint string, cfg *pubsubpb.PushConfig, rm *pubsubpb.ReceivedMessage,
) bool {
	return b.sendPush(ctx, newPushClient(), pushJob{sub: sub, endpoint: endpoint, cfg: cfg}, rm)
}

// PushClientUsesProxy reports whether the push client consults proxy settings.
func PushClientUsesProxy() bool {
	tr, ok := newPushClient().Transport.(*http.Transport)

	return !ok || tr.Proxy != nil
}
