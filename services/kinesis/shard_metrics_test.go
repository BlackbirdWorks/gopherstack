package kinesis_test

import (
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/cwmetric"
	"github.com/blackbirdworks/gopherstack/services/kinesis"
)

func TestShardLevelMetrics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantShard map[string]bool
		name      string
		enhanced  []string
	}{
		{name: "disabled", wantShard: map[string]bool{"IncomingBytes": false, "OutgoingBytes": false}},
		{
			name:     "selected",
			enhanced: []string{"IncomingBytes"},
			wantShard: map[string]bool{
				"IncomingBytes": true, "IncomingRecords": false, "OutgoingBytes": false,
			},
		},
		{
			name:     "all",
			enhanced: []string{"ALL"},
			wantShard: map[string]bool{
				"IncomingBytes": true, "IncomingRecords": true, "OutgoingBytes": true,
				"OutgoingRecords": true, "IteratorAgeMilliseconds": true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				shardSeen, streamSeen, shardID := collectKinesisMetrics(t, tt.enhanced)

				assert.True(t, streamSeen["OutgoingBytes"])
				assert.True(t, streamSeen["OutgoingRecords"])

				for name, want := range tt.wantShard {
					if want {
						assert.Equal(t, shardID, shardSeen[name], name)
					} else {
						assert.NotContains(t, shardSeen, name)
					}
				}
			})
		})
	}
}

func collectKinesisMetrics(t *testing.T, enhanced []string) (map[string]string, map[string]bool, string) {
	t.Helper()

	backend := kinesis.NewInMemoryBackend()

	var (
		mu     sync.Mutex
		points []cwmetric.Point
	)

	backend.SetMetricEmitter(cwmetric.EmitterFunc(func(p cwmetric.Point) error {
		mu.Lock()
		defer mu.Unlock()

		points = append(points, p)

		return nil
	}))

	ctx := t.Context()
	require.NoError(t, backend.CreateStream(ctx, &kinesis.CreateStreamInput{StreamName: "s", ShardCount: 1}))
	time.Sleep(streamSettleWait)

	if enhanced != nil {
		_, err := backend.EnableEnhancedMonitoring(ctx, &kinesis.EnableEnhancedMonitoringInput{
			StreamName: "s", ShardLevelMetrics: enhanced,
		})
		require.NoError(t, err)
	}

	put, err := backend.PutRecord(ctx, &kinesis.PutRecordInput{
		StreamName: "s", PartitionKey: "pk", Data: []byte("hello"),
	})
	require.NoError(t, err)

	it, err := backend.GetShardIterator(ctx, &kinesis.GetShardIteratorInput{
		StreamName: "s", ShardID: put.ShardID, ShardIteratorType: "TRIM_HORIZON",
	})
	require.NoError(t, err)

	_, err = backend.GetRecords(ctx, &kinesis.GetRecordsInput{ShardIterator: it.ShardIterator})
	require.NoError(t, err)

	mu.Lock()
	defer mu.Unlock()

	shardSeen := map[string]string{}
	streamSeen := map[string]bool{}

	for _, p := range points {
		hasShard := false

		for _, d := range p.Dimensions {
			if d.Name == "ShardId" {
				hasShard = true
				shardSeen[p.Name] = d.Value
			}
		}

		if !hasShard {
			streamSeen[p.Name] = true
		}
	}

	return shardSeen, streamSeen, put.ShardID
}
