package main

import (
	"context"
	"testing"

	kinesisbackend "github.com/blackbirdworks/gopherstack/services/kinesis"
	lambdabackend "github.com/blackbirdworks/gopherstack/services/lambda"
)

// TestCWLogsSubscriptionDeliverer_Routing verifies the deliverer routes an
// encoded CloudWatch Logs payload to the correct backend based on the
// destination ARN service component.
func TestCWLogsSubscriptionDeliverer_Routing(t *testing.T) {
	t.Parallel()

	t.Run("kinesis destination receives the payload", func(t *testing.T) {
		t.Parallel()

		kb := kinesisbackend.NewInMemoryBackend()
		if err := kb.CreateStream(&kinesisbackend.CreateStreamInput{StreamName: "logs", ShardCount: 1}); err != nil {
			t.Fatalf("CreateStream: %v", err)
		}

		d := &cwlogsSubscriptionDeliverer{kinesis: kb}
		arn := "arn:aws:kinesis:us-east-1:000000000000:stream/logs"
		payload := []byte("encoded-cwlogs-batch")

		if err := d.DeliverLogEvents(context.Background(), arn, payload); err != nil {
			t.Fatalf("DeliverLogEvents: %v", err)
		}

		got := readKinesisRecords(t, kb, "logs")
		if len(got) != 1 {
			t.Fatalf("record count = %d, want 1", len(got))
		}

		if string(got[0]) != string(payload) {
			t.Fatalf("record = %q, want %q", got[0], payload)
		}
	})

	t.Run("lambda destination dispatches to lambda backend", func(t *testing.T) {
		t.Parallel()

		lb := lambdabackend.NewInMemoryBackend(nil, nil, lambdabackend.Settings{}, "000000000000", "us-east-1")
		d := &cwlogsSubscriptionDeliverer{lambda: lb}
		// Function does not exist: routing reached the lambda backend, which
		// surfaces the missing-function error — proving the dispatch path.
		arn := "arn:aws:lambda:us-east-1:000000000000:function:no-such-fn"

		err := d.DeliverLogEvents(context.Background(), arn, []byte("batch"))
		if err == nil {
			t.Fatal("expected an error invoking a non-existent function")
		}
	})

	t.Run("unknown service is a no-op", func(t *testing.T) {
		t.Parallel()

		d := &cwlogsSubscriptionDeliverer{}
		arn := "arn:aws:logs:us-east-1:000000000000:log-group:other"

		if err := d.DeliverLogEvents(context.Background(), arn, []byte("batch")); err != nil {
			t.Fatalf("expected no-op for unknown service, got %v", err)
		}
	})

	t.Run("nil target backend is a no-op", func(t *testing.T) {
		t.Parallel()

		d := &cwlogsSubscriptionDeliverer{} // kinesis nil
		arn := "arn:aws:kinesis:us-east-1:000000000000:stream/logs"

		if err := d.DeliverLogEvents(context.Background(), arn, []byte("batch")); err != nil {
			t.Fatalf("expected no-op when backend is nil, got %v", err)
		}
	})
}

// readKinesisRecords reads all records from the single shard of a stream.
func readKinesisRecords(t *testing.T, kb *kinesisbackend.InMemoryBackend, stream string) [][]byte {
	t.Helper()

	iter, err := kb.GetShardIterator(&kinesisbackend.GetShardIteratorInput{
		StreamName:        stream,
		ShardID:           "shardId-000000000000",
		ShardIteratorType: "TRIM_HORIZON",
	})
	if err != nil {
		t.Fatalf("GetShardIterator: %v", err)
	}

	out, err := kb.GetRecords(&kinesisbackend.GetRecordsInput{ShardIterator: iter.ShardIterator, Limit: 100})
	if err != nil {
		t.Fatalf("GetRecords: %v", err)
	}

	records := make([][]byte, 0, len(out.Records))
	for _, r := range out.Records {
		records = append(records, r.Data)
	}

	return records
}
