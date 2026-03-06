package sqs_test

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/services/sqs"
)

func newBackendWithQueue(t *testing.T, queueName string) (*sqs.InMemoryBackend, string) {
	t.Helper()

	backend := sqs.NewInMemoryBackend()
	out, err := backend.CreateQueue(&sqs.CreateQueueInput{
		QueueName: queueName,
		Endpoint:  "localhost",
	})
	require.NoError(t, err)

	return backend, out.QueueURL
}

func TestChangeMessageVisibilityBatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr         error
		name            string
		queueName       string
		queueURL        string
		wantFailIDs     []string
		wantFailFaults  []bool
		wantFailCodes   []string
		entryIDs        []string
		wantSuccessIDs  []string
		extraEntries    []sqs.ChangeMessageVisibilityBatchRequestEntry
		messageBodies   []string
		batchVisTimeout int
		recvVisTimeout  int
		recvMax         int
		reReceiveMax    int
		reReceiveVis    int
		wantReReceive   int
		initLogger      bool
		checkReReceive  bool
	}{
		{
			name:            "Success",
			initLogger:      true,
			queueName:       "test-vis-batch",
			messageBodies:   []string{"msg-one", "msg-two"},
			recvMax:         2,
			recvVisTimeout:  30,
			entryIDs:        []string{"e1", "e2"},
			batchVisTimeout: 0,
			wantSuccessIDs:  []string{"e1", "e2"},
			checkReReceive:  true,
			reReceiveMax:    2,
			reReceiveVis:    30,
			wantReReceive:   2,
		},
		{
			name:            "PartialFailure",
			queueName:       "test-vis-batch-partial",
			messageBodies:   []string{"hello"},
			recvMax:         1,
			recvVisTimeout:  30,
			entryIDs:        []string{"good"},
			batchVisTimeout: 0,
			extraEntries: []sqs.ChangeMessageVisibilityBatchRequestEntry{
				{ID: "bad", ReceiptHandle: "invalid-handle", VisibilityTimeout: 0},
			},
			wantSuccessIDs: []string{"good"},
			wantFailIDs:    []string{"bad"},
			wantFailCodes:  []string{"MessageNotInflight"},
			wantFailFaults: []bool{true},
		},
		{
			name:     "QueueNotFound",
			queueURL: "http://localhost/000000000000/nonexistent",
			extraEntries: []sqs.ChangeMessageVisibilityBatchRequestEntry{
				{ID: "e1", ReceiptHandle: "handle", VisibilityTimeout: 0},
			},
			wantErr: sqs.ErrQueueNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if tt.initLogger {
				_ = logger.NewLogger(slog.LevelDebug)
			}

			var backend *sqs.InMemoryBackend

			queueURL := tt.queueURL
			if tt.queueName != "" {
				var url string
				backend, url = newBackendWithQueue(t, tt.queueName)
				queueURL = url
			} else {
				backend = sqs.NewInMemoryBackend()
			}

			// Send messages.
			for _, body := range tt.messageBodies {
				_, err := backend.SendMessage(&sqs.SendMessageInput{QueueURL: queueURL, MessageBody: body})
				require.NoError(t, err)
			}

			// Receive messages so they become in-flight.
			var received []*sqs.Message
			if tt.recvMax > 0 {
				rcv, err := backend.ReceiveMessage(&sqs.ReceiveMessageInput{
					QueueURL:            queueURL,
					MaxNumberOfMessages: tt.recvMax,
					VisibilityTimeout:   tt.recvVisTimeout,
				})
				require.NoError(t, err)
				require.Len(t, rcv.Messages, len(tt.entryIDs))
				received = rcv.Messages
			}

			// Build batch entries from received messages + extra entries.
			var entries []sqs.ChangeMessageVisibilityBatchRequestEntry
			for i, id := range tt.entryIDs {
				entries = append(entries, sqs.ChangeMessageVisibilityBatchRequestEntry{
					ID:                id,
					ReceiptHandle:     received[i].ReceiptHandle,
					VisibilityTimeout: tt.batchVisTimeout,
				})
			}
			entries = append(entries, tt.extraEntries...)

			out, err := backend.ChangeMessageVisibilityBatch(&sqs.ChangeMessageVisibilityBatchInput{
				QueueURL: queueURL,
				Entries:  entries,
			})

			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)

			// Check successful entries.
			assert.Len(t, out.Successful, len(tt.wantSuccessIDs))
			successIDs := make([]string, 0, len(out.Successful))
			for _, s := range out.Successful {
				successIDs = append(successIDs, s.ID)
			}
			assert.ElementsMatch(t, tt.wantSuccessIDs, successIDs)

			// Check failed entries.
			if len(tt.wantFailIDs) == 0 {
				assert.Empty(t, out.Failed)
			} else {
				require.Len(t, out.Failed, len(tt.wantFailIDs))
				for i, f := range out.Failed {
					assert.Equal(t, tt.wantFailIDs[i], f.ID)
					assert.Equal(t, tt.wantFailCodes[i], f.Code)
					assert.Equal(t, tt.wantFailFaults[i], f.SenderFault)
				}
			}

			// Optionally verify messages are receivable again.
			if tt.checkReReceive {
				rcv2, rerr := backend.ReceiveMessage(&sqs.ReceiveMessageInput{
					QueueURL:            queueURL,
					MaxNumberOfMessages: tt.reReceiveMax,
					VisibilityTimeout:   tt.reReceiveVis,
				})
				require.NoError(t, rerr)
				assert.Len(t, rcv2.Messages, tt.wantReReceive)
			}
		})
	}
}
