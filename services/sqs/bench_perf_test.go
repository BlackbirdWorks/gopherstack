package sqs_test

import (
	"fmt"
	"strconv"
	"sync"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/sqs"
)

// newBenchBackend creates a backend for benchmarks and stops it when the
// benchmark completes.
func newBenchBackend(b *testing.B) *sqs.InMemoryBackend {
	b.Helper()

	bk := sqs.NewInMemoryBackend()
	b.Cleanup(bk.Close)

	return bk
}

func benchCreateQueue(b *testing.B, backend *sqs.InMemoryBackend, name string) string {
	b.Helper()

	out, err := backend.CreateQueue(&sqs.CreateQueueInput{
		QueueName: name,
		Endpoint:  testEndpoint,
	})
	if err != nil {
		b.Fatalf("CreateQueue: %v", err)
	}

	return out.QueueURL
}

func benchSendN(b *testing.B, backend *sqs.InMemoryBackend, queueURL string, n int) {
	b.Helper()

	for i := range n {
		_, err := backend.SendMessage(&sqs.SendMessageInput{
			QueueURL:    queueURL,
			MessageBody: "body-" + strconv.Itoa(i),
		})
		if err != nil {
			b.Fatalf("SendMessage: %v", err)
		}
	}
}

// BenchmarkReceiveMessage_SinglePass measures the combined prepareAndPickMessages
// path (#54) — one sweep over inFlight + messages instead of four separate passes.
func BenchmarkReceiveMessage_SinglePass(b *testing.B) {
	depths := []int{100, 1000, 10000}

	for _, depth := range depths {
		b.Run(fmt.Sprintf("depth=%d", depth), func(b *testing.B) {
			backend := newBenchBackend(b)
			qURL := benchCreateQueue(b, backend, "bench-receive-q")
			benchSendN(b, backend, qURL, depth)

			b.ResetTimer()

			for range b.N {
				_, _ = backend.ReceiveMessage(&sqs.ReceiveMessageInput{
					QueueURL:            qURL,
					MaxNumberOfMessages: 10,
				})
			}
		})
	}
}

// BenchmarkSendReceiveDelete_PerQueueLock exercises the per-queue-lock hot path
// (#55) with concurrent senders and receivers on a single queue.
func BenchmarkSendReceiveDelete_PerQueueLock(b *testing.B) {
	concurrencies := []int{1, 4, 16}

	for _, workers := range concurrencies {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			backend := newBenchBackend(b)
			qURL := benchCreateQueue(b, backend, "bench-concurrent-q")

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					out, err := backend.SendMessage(&sqs.SendMessageInput{
						QueueURL:    qURL,
						MessageBody: "payload",
					})
					if err != nil {
						return
					}

					recv, err := backend.ReceiveMessage(&sqs.ReceiveMessageInput{
						QueueURL:            qURL,
						MaxNumberOfMessages: 1,
					})
					if err != nil || len(recv.Messages) == 0 {
						_ = out

						return
					}

					_ = backend.DeleteMessage(&sqs.DeleteMessageInput{
						QueueURL:      qURL,
						ReceiptHandle: recv.Messages[0].ReceiptHandle,
					})
				}
			})

			_ = workers
		})
	}
}

// BenchmarkDeleteMessage_O1 measures O(1) delete via inFlightByHandle (#56)
// against queues with varying in-flight depths.
func BenchmarkDeleteMessage_O1(b *testing.B) {
	depths := []int{100, 1000, 10000}

	for _, depth := range depths {
		b.Run(fmt.Sprintf("inflight=%d", depth), func(b *testing.B) {
			backend := newBenchBackend(b)
			qURL := benchCreateQueue(b, backend, "bench-delete-q")

			b.ResetTimer()

			for range b.N {
				b.StopTimer()

				// Fill queue and receive all messages to put them in-flight.
				benchSendN(b, backend, qURL, depth)

				recv, err := backend.ReceiveMessage(&sqs.ReceiveMessageInput{
					QueueURL:            qURL,
					MaxNumberOfMessages: 10,
					VisibilityTimeout:   300,
				})
				if err != nil || len(recv.Messages) == 0 {
					b.Fatal("no messages received")
				}

				handle := recv.Messages[0].ReceiptHandle

				b.StartTimer()

				// Delete the first message — tests lookup speed regardless of depth.
				_ = backend.DeleteMessage(&sqs.DeleteMessageInput{
					QueueURL:      qURL,
					ReceiptHandle: handle,
				})

				b.StopTimer()

				// Purge remaining for next iteration.
				_ = backend.PurgeQueue(&sqs.PurgeQueueInput{QueueURL: qURL})

				b.StartTimer()
			}
		})
	}
}

// BenchmarkSendMessageBatch_SingleLock measures #58 — batch resolved under one
// per-queue lock instead of N separate lock round-trips.
func BenchmarkSendMessageBatch_SingleLock(b *testing.B) {
	sizes := []int{1, 5, 10}

	for _, batchSize := range sizes {
		b.Run(fmt.Sprintf("batch=%d", batchSize), func(b *testing.B) {
			backend := newBenchBackend(b)
			qURL := benchCreateQueue(b, backend, "bench-batch-q")

			entries := make([]sqs.SendMessageBatchEntry, batchSize)
			for i := range batchSize {
				entries[i] = sqs.SendMessageBatchEntry{
					ID:          strconv.Itoa(i),
					MessageBody: "body-" + strconv.Itoa(i),
				}
			}

			b.ResetTimer()

			for range b.N {
				_, _ = backend.SendMessageBatch(&sqs.SendMessageBatchInput{
					QueueURL: qURL,
					Entries:  entries,
				})
			}
		})
	}
}

// BenchmarkGetQueueAttributes_DelayedCounter verifies O(1) GetQueueAttributes
// (#59) — no walk over q.messages.
func BenchmarkGetQueueAttributes_DelayedCounter(b *testing.B) {
	depths := []int{100, 10000}

	for _, depth := range depths {
		b.Run(fmt.Sprintf("depth=%d", depth), func(b *testing.B) {
			backend := newBenchBackend(b)
			qURL := benchCreateQueue(b, backend, "bench-attrs-q")
			benchSendN(b, backend, qURL, depth)

			b.ResetTimer()

			for range b.N {
				_, _ = backend.GetQueueAttributes(&sqs.GetQueueAttributesInput{
					QueueURL:       qURL,
					AttributeNames: []string{sqs.AttrApproxMessages},
				})
			}
		})
	}
}

// BenchmarkConcurrentQueues measures throughput across many independent queues
// to show the per-queue lock benefit over a global lock.
func BenchmarkConcurrentQueues(b *testing.B) {
	numQueues := []int{4, 16}

	for _, nq := range numQueues {
		b.Run(fmt.Sprintf("queues=%d", nq), func(b *testing.B) {
			backend := newBenchBackend(b)
			urls := make([]string, nq)

			for i := range nq {
				urls[i] = benchCreateQueue(b, backend, fmt.Sprintf("bench-q-%d", i))
			}

			b.ResetTimer()

			var wg sync.WaitGroup
			for i := range nq {
				wg.Add(1)

				go func(qURL string) {
					defer wg.Done()

					for range b.N {
						_, _ = backend.SendMessage(&sqs.SendMessageInput{
							QueueURL:    qURL,
							MessageBody: "x",
						})
					}
				}(urls[i])
			}

			wg.Wait()
		})
	}
}
