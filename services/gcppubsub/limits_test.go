package gcppubsub_test

import (
	"fmt"
	"strings"
	"testing"

	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestPublishAttributeLimits(t *testing.T) {
	t.Parallel()

	many := make(map[string]string)
	for i := range 101 {
		many[fmt.Sprintf("k%d", i)] = "v"
	}

	tests := []struct {
		attrs map[string]string
		name  string
		want  codes.Code
	}{
		{name: "ok", attrs: map[string]string{strings.Repeat("k", 256): strings.Repeat("v", 1024)}, want: codes.OK},
		{name: "hundred", attrs: map[string]string{"a": "b"}, want: codes.OK},
		{name: "too many", attrs: many, want: codes.InvalidArgument},
		{name: "long key", attrs: map[string]string{strings.Repeat("k", 257): "v"}, want: codes.InvalidArgument},
		{name: "long value", attrs: map[string]string{"k": strings.Repeat("v", 1025)}, want: codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, _ := newBK(t, nil)
			_, err := b.Publish(&pubsubpb.PublishRequest{
				Topic:    proj + "/topics/topic1",
				Messages: []*pubsubpb.PubsubMessage{{Data: []byte("x"), Attributes: tt.attrs}},
			})
			assert.Equal(t, tt.want, status.Code(err))
		})
	}
}

func TestAckRequestSizeLimit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		size int
		want codes.Code
	}{
		{name: "at limit", size: 512 << 10, want: codes.OK},
		{name: "over limit", size: 512<<10 + 1, want: codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, sub := newBK(t, nil)
			ids := []string{strings.Repeat("a", tt.size)}

			_, _, err := b.Acknowledge(sub, ids)
			assert.Equal(t, tt.want, status.Code(err), "ack")

			_, _, err = b.ModifyAckDeadline(sub, ids, []int32{10})
			assert.Equal(t, tt.want, status.Code(err), "modack")
		})
	}
}

func TestSchemaLimits(t *testing.T) {
	t.Parallel()

	const def = `{"type":"record","name":"R","fields":[{"name":"n","type":"int"}]}`

	t.Run("definition size", func(t *testing.T) {
		t.Parallel()

		b, _ := newBK(t, nil)
		big := def + strings.Repeat(" ", 300<<10)
		_, err := b.CreateSchema(&pubsubpb.CreateSchemaRequest{
			Parent: proj, SchemaId: "sch1", Schema: &pubsubpb.Schema{Type: pubsubpb.Schema_AVRO, Definition: big},
		})
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
	})

	t.Run("revisions", func(t *testing.T) {
		t.Parallel()

		b, _ := newBK(t, nil)
		_, err := b.CreateSchema(&pubsubpb.CreateSchemaRequest{
			Parent: proj, SchemaId: "sch1", Schema: &pubsubpb.Schema{Type: pubsubpb.Schema_AVRO, Definition: def},
		})
		require.NoError(t, err)

		for i := 1; i < 20; i++ {
			_, err = b.CommitSchema(&pubsubpb.CommitSchemaRequest{
				Name: proj + "/schemas/sch1", Schema: &pubsubpb.Schema{Type: pubsubpb.Schema_AVRO, Definition: def},
			})
			require.NoError(t, err, "revision %d", i+1)
		}

		_, err = b.CommitSchema(&pubsubpb.CommitSchemaRequest{
			Name: proj + "/schemas/sch1", Schema: &pubsubpb.Schema{Type: pubsubpb.Schema_AVRO, Definition: def},
		})
		assert.Equal(t, codes.ResourceExhausted, status.Code(err))
	})
}

func TestProjectResourceLimits(t *testing.T) {
	t.Parallel()

	const lim = 5

	t.Run("topics", func(t *testing.T) {
		t.Parallel()

		b, _ := newBK(t, nil)
		b.SetResourceLimits(lim, lim, lim, lim)

		for i := 1; i < lim; i++ {
			_, err := b.CreateTopic(&pubsubpb.Topic{Name: fmt.Sprintf("%s/topics/t%05d", proj, i)})
			require.NoError(t, err)
		}

		_, err := b.CreateTopic(&pubsubpb.Topic{Name: proj + "/topics/overflow"})
		assert.Equal(t, codes.ResourceExhausted, status.Code(err))

		_, err = b.CreateTopic(&pubsubpb.Topic{Name: "projects/other/topics/fine"})
		assert.NoError(t, err, "limit is per project")
	})

	t.Run("snapshots", func(t *testing.T) {
		t.Parallel()

		b, sub := newBK(t, nil)
		b.SetResourceLimits(lim, lim, lim, lim)

		for i := range lim {
			_, err := b.CreateSnapshot(&pubsubpb.CreateSnapshotRequest{
				Name: fmt.Sprintf("%s/snapshots/s%05d", proj, i), Subscription: sub,
			})
			require.NoError(t, err)
		}

		_, err := b.CreateSnapshot(
			&pubsubpb.CreateSnapshotRequest{Name: proj + "/snapshots/overflow", Subscription: sub},
		)
		assert.Equal(t, codes.ResourceExhausted, status.Code(err))
	})

	t.Run("subscriptions", func(t *testing.T) {
		t.Parallel()

		b, _ := newBK(t, nil)
		b.SetResourceLimits(lim, lim, lim, lim)

		for i := 1; i < lim; i++ {
			_, err := b.CreateSubscription(&pubsubpb.Subscription{
				Name: fmt.Sprintf("%s/subscriptions/s%05d", proj, i), Topic: proj + "/topics/topic1",
			})
			require.NoError(t, err)
		}

		_, err := b.CreateSubscription(
			&pubsubpb.Subscription{Name: proj + "/subscriptions/overflow", Topic: proj + "/topics/topic1"},
		)
		assert.Equal(t, codes.ResourceExhausted, status.Code(err))
	})

	t.Run("schemas", func(t *testing.T) {
		t.Parallel()

		b, _ := newBK(t, nil)
		b.SetResourceLimits(lim, lim, lim, 2)

		def := `{"type":"record","name":"R","fields":[{"name":"n","type":"int"}]}`

		for _, id := range []string{"sch1", "sch2", "sch3"} {
			_, err := b.CreateSchema(&pubsubpb.CreateSchemaRequest{
				Parent: proj, SchemaId: id, Schema: &pubsubpb.Schema{Type: pubsubpb.Schema_AVRO, Definition: def},
			})
			if id == "sch3" {
				assert.Equal(t, codes.ResourceExhausted, status.Code(err))
			} else {
				require.NoError(t, err)
			}
		}
	})
}
