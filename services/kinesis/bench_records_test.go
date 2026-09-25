package kinesis_test

// Benchmarks PutRecords/GetRecords at realistic batch sizes, driven directly
// against the backend (no HTTP handler) to isolate backend cost.

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kinesis"
)

func benchCreateActiveStream(b *testing.B, name string, shardCount int) *kinesis.InMemoryBackend {
	b.Helper()

	bk := kinesis.NewInMemoryBackend()
	err := bk.CreateStream(b.Context(), &kinesis.CreateStreamInput{
		StreamName: name,
		ShardCount: shardCount,
	})
	require.NoError(b, err)

	return bk
}

// BenchmarkPutRecords_500 measures a 500-record PutRecords call (the AWS
// max), the batch shape that drives PutRecord's per-record stream-lock and
// channel-delivery-scan cost 500 times over.
func BenchmarkPutRecords_500(b *testing.B) {
	const batchSize = 500

	bk := benchCreateActiveStream(b, "bench-putrecords", 4)

	records := make([]kinesis.PutRecordsEntry, batchSize)
	for i := range batchSize {
		records[i] = kinesis.PutRecordsEntry{
			PartitionKey: fmt.Sprintf("pk-%d", i),
			Data:         []byte("some record payload data"),
		}
	}
	input := &kinesis.PutRecordsInput{StreamName: "bench-putrecords", Records: records}

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		out, err := bk.PutRecords(b.Context(), input)
		if err != nil {
			b.Fatalf("PutRecords: %v", err)
		}
		if out.FailedRecordCount != 0 {
			b.Fatalf("PutRecords: %d failed records", out.FailedRecordCount)
		}
	}
}

// BenchmarkGetRecords_10k measures GetRecords against a shard already
// holding 10k records, returning a full page (the default/typical limit).
func BenchmarkGetRecords_10k(b *testing.B) {
	const recordCount = 10000

	bk := benchCreateActiveStream(b, "bench-getrecords", 1)

	for i := range recordCount {
		_, err := bk.PutRecord(b.Context(), &kinesis.PutRecordInput{
			StreamName:   "bench-getrecords",
			PartitionKey: fmt.Sprintf("pk-%d", i),
			Data:         []byte("some record payload data"),
		})
		require.NoError(b, err)
	}

	streams, err := bk.ListStreams(b.Context(), &kinesis.ListStreamsInput{})
	require.NoError(b, err)
	require.NotEmpty(b, streams.StreamNames)

	shards, err := bk.ListShards(b.Context(), &kinesis.ListShardsInput{StreamName: "bench-getrecords"})
	require.NoError(b, err)
	require.NotEmpty(b, shards.Shards)

	itOut, err := bk.GetShardIterator(b.Context(), &kinesis.GetShardIteratorInput{
		StreamName:        "bench-getrecords",
		ShardID:           shards.Shards[0].ShardID,
		ShardIteratorType: "TRIM_HORIZON",
	})
	require.NoError(b, err)

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		out, getErr := bk.GetRecords(b.Context(), &kinesis.GetRecordsInput{
			ShardIterator: itOut.ShardIterator,
			Limit:         1000,
		})
		if getErr != nil {
			b.Fatalf("GetRecords: %v", getErr)
		}
		if len(out.Records) == 0 {
			b.Fatal("GetRecords: no records returned")
		}
	}
}
