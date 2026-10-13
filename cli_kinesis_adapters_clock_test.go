package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	kinesisbackend "github.com/blackbirdworks/gopherstack/services/kinesis"
)

const (
	skewStreamName = "clock-skew-stream"
	skewStreamARN  = "arn:aws:kinesis:us-east-1:000000000000:stream/" + skewStreamName
)

func newSkewedKinesis(t *testing.T, skew time.Duration) (*kinesisbackend.InMemoryBackend, *kinesisFakeClock) {
	t.Helper()

	ctx := context.Background()
	clock := newKinesisFakeClock(time.Now().Add(skew))
	bk := kinesisbackend.NewInMemoryBackendWithConfig("000000000000", "us-east-1").WithClock(clock.Now)

	require.NoError(t, bk.CreateStream(ctx, &kinesisbackend.CreateStreamInput{
		StreamName: skewStreamName,
		ShardCount: 1,
	}))
	clock.Advance(kinesisStreamSettleWait)

	_, err := bk.PutRecord(ctx, &kinesisbackend.PutRecordInput{
		StreamName: skewStreamName, PartitionKey: "pk", Data: []byte("payload"),
	})
	require.NoError(t, err)

	return bk, clock
}

// skewReader drives one in-process consumer end to end and returns the first read error.
type skewReader func(bk *kinesisbackend.InMemoryBackend) (int, error)

func readViaStreamReader(bk *kinesisbackend.InMemoryBackend) (int, error) {
	a := &kinesisStreamReaderAdapter{backend: bk}

	shards, err := a.ListShards(skewStreamARN)
	if err != nil || len(shards) != 1 {
		return 0, err
	}

	it, err := a.GetShardIterator(skewStreamARN, shards[0])
	if err != nil {
		return 0, err
	}

	recs, _, err := a.GetRecords(it, 10)

	return len(recs), err
}

func readViaLambdaReader(bk *kinesisbackend.InMemoryBackend) (int, error) {
	a := &kinesisReaderAdapter{backend: bk}

	shards, err := a.GetShardIDs(skewStreamARN)
	if err != nil || len(shards) != 1 {
		return 0, err
	}

	it, err := a.GetShardIterator(skewStreamARN, shards[0], "TRIM_HORIZON", "")
	if err != nil {
		return 0, err
	}

	recs, next, err := a.GetRecords(it, 10)
	if err != nil {
		return 0, err
	}

	_, _, err = a.GetRecords(next, 10)

	return len(recs), err
}

func readViaPipesReader(bk *kinesisbackend.InMemoryBackend) (int, error) {
	a := &pipesKinesisReaderAdapter{backend: bk}

	shards, err := a.GetShardIDs(skewStreamARN)
	if err != nil || len(shards) != 1 {
		return 0, err
	}

	it, err := a.GetShardIterator(skewStreamARN, shards[0], "TRIM_HORIZON", "")
	if err != nil {
		return 0, err
	}

	recs, next, err := a.GetRecords(it, 10)
	if err != nil {
		return 0, err
	}

	_, _, err = a.GetRecords(next, 10)

	return len(recs), err
}

// Shard iterators must expire on the backend clock, not wall time: a backend
// driven by a clock far from the real one still hands out usable iterators.
func TestKinesisInProcessReaders_UseBackendClockForIterators(t *testing.T) {
	t.Parallel()

	readers := map[string]skewReader{
		"stream_reader": readViaStreamReader,
		"lambda_reader": readViaLambdaReader,
		"pipes_reader":  readViaPipesReader,
	}

	for _, skew := range []time.Duration{-time.Hour, time.Hour} {
		for name, read := range readers {
			t.Run(name+"_"+skew.String(), func(t *testing.T) {
				t.Parallel()

				bk, _ := newSkewedKinesis(t, skew)

				n, err := read(bk)
				require.NoError(t, err)
				assert.Equal(t, 1, n)
			})
		}
	}
}

func TestKinesisInProcessReaders_IteratorExpiresOnBackendClock(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		advance time.Duration
		wantErr bool
	}{
		{name: "within_ttl", advance: time.Minute},
		{name: "past_ttl", advance: 6 * time.Minute, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk, clock := newSkewedKinesis(t, -time.Hour)
			a := &kinesisStreamReaderAdapter{backend: bk}

			it, err := a.GetShardIterator(skewStreamARN, "shardId-000000000000")
			require.NoError(t, err)

			clock.Advance(tt.advance)

			_, _, err = a.GetRecords(it, 10)
			if tt.wantErr {
				require.ErrorIs(t, err, kinesisbackend.ErrShardIteratorExpired)

				return
			}

			require.NoError(t, err)
		})
	}
}
