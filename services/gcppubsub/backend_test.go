package gcppubsub_test

import (
	"testing"
	"time"

	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/blackbirdworks/gopherstack/services/gcppubsub"
)

type fakeClock struct{ now time.Time }

func (c *fakeClock) Now() time.Time          { return c.now }
func (c *fakeClock) Advance(d time.Duration) { c.now = c.now.Add(d) }

type bk struct {
	*gcppubsub.InMemoryBackend
	clock *fakeClock
}

func newBK(t *testing.T, mut func(*pubsubpb.Subscription)) (bk, string) {
	t.Helper()

	b := bk{
		InMemoryBackend: gcppubsub.NewInMemoryBackend(),
		clock:           &fakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)},
	}
	b.SetClock(b.clock.Now)

	_, err := b.CreateTopic(&pubsubpb.Topic{Name: proj + "/topics/topic1"})
	require.NoError(t, err)

	sub := &pubsubpb.Subscription{Name: proj + "/subscriptions/sub1", Topic: proj + "/topics/topic1"}
	if mut != nil {
		mut(sub)
	}

	_, err = b.CreateSubscription(sub)
	require.NoError(t, err)

	return b, sub.GetName()
}

func (b bk) publish(t *testing.T) {
	t.Helper()

	_, err := b.Publish(&pubsubpb.PublishRequest{
		Topic: proj + "/topics/topic1", Messages: []*pubsubpb.PubsubMessage{{Data: []byte("x")}},
	})
	require.NoError(t, err)
}

func (b bk) pull(t *testing.T, sub string) []*pubsubpb.ReceivedMessage {
	t.Helper()

	msgs, err := b.PullMessages(sub, 10)
	require.NoError(t, err)

	return msgs
}

func TestLeaseExpiryRedelivers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		advance   time.Duration
		wantAgain bool
	}{
		{"before deadline", 5 * time.Second, false},
		{"after deadline", 11 * time.Second, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, sub := newBK(t, nil)
			b.publish(t)

			first := b.pull(t, sub)
			require.Len(t, first, 1)

			b.clock.Advance(tt.advance)

			again := b.pull(t, sub)
			if !tt.wantAgain {
				assert.Empty(t, again)

				return
			}

			require.Len(t, again, 1)
			assert.Equal(t, first[0].GetMessage().GetMessageId(), again[0].GetMessage().GetMessageId())
			assert.NotEqual(t, first[0].GetAckId(), again[0].GetAckId())
		})
	}
}

func TestRetryPolicyBackoff(t *testing.T) {
	t.Parallel()

	b, sub := newBK(t, func(s *pubsubpb.Subscription) {
		s.RetryPolicy = &pubsubpb.RetryPolicy{
			MinimumBackoff: durationpb.New(10 * time.Second),
			MaximumBackoff: durationpb.New(40 * time.Second),
		}
	})
	b.publish(t)

	for i, wait := range []time.Duration{10 * time.Second, 20 * time.Second, 40 * time.Second, 40 * time.Second} {
		msgs := b.pull(t, sub)
		require.Len(t, msgs, 1, "attempt %d", i+1)

		_, _, err := b.ModifyAckDeadline(sub, []string{msgs[0].GetAckId()}, []int32{0})
		require.NoError(t, err)

		assert.Empty(t, b.pull(t, sub), "backoff must hold the message back")

		b.clock.Advance(wait - time.Second)
		assert.Empty(t, b.pull(t, sub))
		b.clock.Advance(time.Second)
	}
}

func TestRetentionAndExpiration(t *testing.T) {
	t.Parallel()

	b, sub := newBK(t, func(s *pubsubpb.Subscription) {
		s.MessageRetentionDuration = durationpb.New(20 * time.Minute)
		s.ExpirationPolicy = &pubsubpb.ExpirationPolicy{Ttl: durationpb.New(48 * time.Hour)}
	})
	b.publish(t)

	b.clock.Advance(21 * time.Minute)
	assert.Empty(t, b.pull(t, sub), "message older than retention is dropped")

	b.clock.Advance(49 * time.Hour)
	_, err := b.GetSubscription(sub)
	assert.Equal(t, codes.NotFound, status.Code(err), "inactive subscription expires")
}

func TestAcknowledgeInvalidIDs(t *testing.T) {
	t.Parallel()

	b, sub := newBK(t, nil)
	b.publish(t)

	msgs := b.pull(t, sub)
	acked, invalid, err := b.Acknowledge(sub, []string{msgs[0].GetAckId(), "bogus"})
	require.NoError(t, err)
	assert.Equal(t, []string{msgs[0].GetAckId()}, acked)
	assert.Equal(t, []string{"bogus"}, invalid)

	_, _, err = b.Acknowledge(proj+"/subscriptions/none", []string{"a"})
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestSnapshotExpiry(t *testing.T) {
	t.Parallel()

	b, sub := newBK(t, nil)

	_, err := b.CreateSnapshot(&pubsubpb.CreateSnapshotRequest{Name: proj + "/snapshots/snap1", Subscription: sub})
	require.NoError(t, err)

	b.clock.Advance(6 * 24 * time.Hour)
	_, err = b.GetSnapshot(proj + "/snapshots/snap1")
	require.NoError(t, err)

	b.clock.Advance(25 * time.Hour)
	_, err = b.GetSnapshot(proj + "/snapshots/snap1")
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestValidateData(t *testing.T) {
	t.Parallel()

	const (
		avro  = 2
		proto = 1
		jsonE = 1
		binE  = 2
	)

	rec := `{"type":"record","name":"R","fields":[{"name":"n","type":"int"},{"name":"s","type":"string"},` +
		`{"name":"o","type":["null","long"],"default":null}]}`

	tests := []struct {
		name string
		def  string
		data []byte
		typ  int32
		enc  int32
		want bool
	}{
		{"avro json ok", rec, []byte(`{"n":1,"s":"a"}`), avro, jsonE, true},
		{"avro json union", rec, []byte(`{"n":1,"s":"a","o":{"long":5}}`), avro, jsonE, true},
		{"avro json wrong type", rec, []byte(`{"n":"1","s":"a"}`), avro, jsonE, false},
		{"avro json missing field", rec, []byte(`{"n":1}`), avro, jsonE, false},
		{"avro json trailing", rec, []byte(`{"n":1,"s":"a"} {}`), avro, jsonE, false},
		{"avro binary ok", rec, []byte{2, 2, 'a', 0}, avro, binE, true},
		{"avro binary union long", rec, []byte{2, 2, 'a', 2, 10}, avro, binE, true},
		{"avro binary truncated", rec, []byte{2, 4, 'a'}, avro, binE, false},
		{"avro binary trailing", rec, []byte{2, 2, 'a', 0, 9}, avro, binE, false},
		{"avro enum", `{"type":"enum","name":"E","symbols":["A","B"]}`, []byte(`"B"`), avro, jsonE, true},
		{"avro enum bad", `{"type":"enum","name":"E","symbols":["A","B"]}`, []byte(`"C"`), avro, jsonE, false},
		{"avro array", `{"type":"array","items":"int"}`, []byte(`[1,2]`), avro, jsonE, true},
		{"avro array binary", `{"type":"array","items":"int"}`, []byte{4, 2, 4, 0}, avro, binE, true},
		{
			"proto json object",
			`syntax = "proto3"; message M { string a = 1; }`,
			[]byte(`{"a":"b"}`),
			proto,
			jsonE,
			true,
		},
		{"proto json not object", `syntax = "proto3"; message M { string a = 1; }`, []byte(`[1]`), proto, jsonE, false},
		{"proto wire ok", `syntax = "proto3"; message M { string a = 1; }`, []byte{0x0a, 0x01, 'x'}, proto, binE, true},
		{
			"proto wire bad",
			`syntax = "proto3"; message M { string a = 1; }`,
			[]byte{0x0a, 0x09, 'x'},
			proto,
			binE,
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, gcppubsub.ValidateData(tt.typ, tt.def, tt.enc, tt.data))
		})
	}
}
