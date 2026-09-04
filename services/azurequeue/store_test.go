package azurequeue_test

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azurequeue"
)

// sequentialIDs returns an idFunc seam that yields id-1, id-2, ... in order,
// for deterministic PopReceipt/MessageID assertions.
func sequentialIDs() func() string {
	n := 0

	return func() string {
		n++

		return "id-" + strconv.Itoa(n)
	}
}

func TestInMemoryBackend_QueueCreateListDelete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "create_list_delete"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := azurequeue.NewInMemoryBackend()

			created, err := b.CreateQueue("q1")
			require.NoError(t, err)
			assert.True(t, created, tt.name)

			// Re-creating a queue with the same (nonexistent) metadata is
			// idempotent: created is false, err is nil.
			created, err = b.CreateQueue("q1")
			require.NoError(t, err)
			assert.False(t, created, tt.name)

			queues := b.ListQueues()
			require.Len(t, queues, 1)
			assert.Equal(t, "q1", queues[0].Name)

			require.NoError(t, b.DeleteQueue("q1"))
			assert.Empty(t, b.ListQueues())
			assert.ErrorIs(t, b.DeleteQueue("q1"), azurequeue.ErrQueueNotFound)
		})
	}
}

func TestInMemoryBackend_ListQueuesSortedByName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		queues []string
		want   []string
	}{
		{name: "sorted", queues: []string{"c", "a", "b"}, want: []string{"a", "b", "c"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := azurequeue.NewInMemoryBackend()
			for _, name := range tt.queues {
				_, err := b.CreateQueue(name)
				require.NoError(t, err)
			}

			got := make([]string, 0, len(tt.want))
			for _, qi := range b.ListQueues() {
				got = append(got, qi.Name)
			}

			assert.Equal(t, tt.want, got, tt.name)
		})
	}
}

func TestInMemoryBackend_MissingQueueErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		op   func(b *azurequeue.InMemoryBackend) error
		name string
	}{
		{name: "put_message", op: func(b *azurequeue.InMemoryBackend) error {
			_, err := b.PutMessage("missing", "x", 0, 0)

			return err
		}},
		{name: "get_messages", op: func(b *azurequeue.InMemoryBackend) error {
			_, err := b.GetMessages("missing", 1, 0)

			return err
		}},
		{name: "peek_messages", op: func(b *azurequeue.InMemoryBackend) error {
			_, err := b.PeekMessages("missing", 1)

			return err
		}},
		{name: "delete_message", op: func(b *azurequeue.InMemoryBackend) error {
			return b.DeleteMessage("missing", "id", "pr")
		}},
		{name: "update_message", op: func(b *azurequeue.InMemoryBackend) error {
			_, err := b.UpdateMessage("missing", "id", "pr", 0, nil)

			return err
		}},
		{name: "clear_messages", op: func(b *azurequeue.InMemoryBackend) error {
			return b.ClearMessages("missing")
		}},
		{name: "delete_queue", op: func(b *azurequeue.InMemoryBackend) error {
			return b.DeleteQueue("missing")
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := azurequeue.NewInMemoryBackend()
			err := tt.op(b)

			require.Error(t, err, tt.name)
			assert.ErrorIs(t, err, azurequeue.ErrQueueNotFound, tt.name)
		})
	}
}

func TestInMemoryBackend_PutGetPeekMessage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		text string
	}{
		{name: "roundtrip", text: "hello queue"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := azurequeue.NewInMemoryBackend()
			_, err := b.CreateQueue("q1")
			require.NoError(t, err)

			info, err := b.PutMessage("q1", tt.text, 0, 0)
			require.NoError(t, err)
			assert.NotEmpty(t, info.ID, tt.name)
			assert.NotEmpty(t, info.PopReceipt, tt.name)
			assert.True(t, info.ExpirationTime.After(info.InsertionTime), tt.name)

			peeked, err := b.PeekMessages("q1", 10)
			require.NoError(t, err)
			require.Len(t, peeked, 1, tt.name)
			assert.Equal(t, tt.text, peeked[0].Text, tt.name)
			assert.Empty(t, peeked[0].PopReceipt, "peek must not assign a pop receipt")
			assert.Equal(t, int64(0), peeked[0].DequeueCount, tt.name)

			got, err := b.GetMessages("q1", 10, 30*time.Second)
			require.NoError(t, err)
			require.Len(t, got, 1, tt.name)
			assert.Equal(t, tt.text, got[0].Text, tt.name)
			assert.NotEmpty(t, got[0].PopReceipt, tt.name)
			assert.Equal(t, int64(1), got[0].DequeueCount, tt.name)
		})
	}
}

func TestInMemoryBackend_GetMessages_HidesUntilVisibilityTimeoutElapses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "dequeued_message_invisible_then_visible_again"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			now := base

			b := azurequeue.NewInMemoryBackend()
			azurequeue.SetNowFunc(b, func() time.Time { return now })

			_, err := b.CreateQueue("q1")
			require.NoError(t, err)
			_, err = b.PutMessage("q1", "x", 0, 0)
			require.NoError(t, err)

			got, err := b.GetMessages("q1", 10, 30*time.Second)
			require.NoError(t, err)
			require.Len(t, got, 1, tt.name)

			// Immediately after dequeue, the message is hidden: a second Get
			// (and Peek) should return nothing.
			got2, err := b.GetMessages("q1", 10, 30*time.Second)
			require.NoError(t, err)
			assert.Empty(t, got2, tt.name)

			peeked, err := b.PeekMessages("q1", 10)
			require.NoError(t, err)
			assert.Empty(t, peeked, tt.name)

			// Advance past the visibility timeout: the message becomes
			// visible again.
			now = base.Add(31 * time.Second)

			got3, err := b.GetMessages("q1", 10, 30*time.Second)
			require.NoError(t, err)
			require.Len(t, got3, 1, tt.name)
			assert.Equal(t, int64(2), got3[0].DequeueCount, tt.name)
			assert.NotEqual(t, got[0].PopReceipt, got3[0].PopReceipt, "pop receipt must rotate on each dequeue")
		})
	}
}

func TestInMemoryBackend_DeleteMessage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "delete_with_valid_pop_receipt"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := azurequeue.NewInMemoryBackend()
			_, err := b.CreateQueue("q1")
			require.NoError(t, err)
			_, err = b.PutMessage("q1", "x", 0, 0)
			require.NoError(t, err)

			got, err := b.GetMessages("q1", 10, 30*time.Second)
			require.NoError(t, err)
			require.Len(t, got, 1, tt.name)

			require.NoError(t, b.DeleteMessage("q1", got[0].ID, got[0].PopReceipt))

			peeked, err := b.PeekMessages("q1", 10)
			require.NoError(t, err)
			assert.Empty(t, peeked, tt.name)
		})
	}
}

func TestInMemoryBackend_DeleteMessage_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr    error
		name       string
		messageID  string
		popReceipt string
	}{
		{
			name:       "unknown_message_id",
			messageID:  "does-not-exist",
			popReceipt: "any",
			wantErr:    azurequeue.ErrMessageNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := azurequeue.NewInMemoryBackend()
			_, err := b.CreateQueue("q1")
			require.NoError(t, err)

			err = b.DeleteMessage("q1", tt.messageID, tt.popReceipt)
			assert.ErrorIs(t, err, tt.wantErr, tt.name)
		})
	}
}

func TestInMemoryBackend_DeleteMessage_PopReceiptMismatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "stale_pop_receipt_rejected"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := azurequeue.NewInMemoryBackend()
			_, err := b.CreateQueue("q1")
			require.NoError(t, err)
			info, err := b.PutMessage("q1", "x", 0, 0)
			require.NoError(t, err)

			err = b.DeleteMessage("q1", info.ID, "wrong-pop-receipt")
			assert.ErrorIs(t, err, azurequeue.ErrPopReceiptMismatch, tt.name)
		})
	}
}

func TestInMemoryBackend_UpdateMessage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		newText *string
		name    string
	}{
		{name: "visibility_only", newText: nil},
		// new(expr) (Go 1.26+) returns a pointer to a copy of expr -- this is
		// not new(T) called with a value instead of a type; golangci-lint's
		// modernize check flags the old func(){s:=v;return &s}() idiom in
		// favor of exactly this form.
		{name: "replaces_text", newText: new("new text")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := azurequeue.NewInMemoryBackend()
			_, err := b.CreateQueue("q1")
			require.NoError(t, err)
			info, err := b.PutMessage("q1", "original", 0, 0)
			require.NoError(t, err)

			updated, err := b.UpdateMessage("q1", info.ID, info.PopReceipt, time.Minute, tt.newText)
			require.NoError(t, err, tt.name)
			assert.NotEqual(t, info.PopReceipt, updated.PopReceipt, "pop receipt must rotate on update")

			// The stale pop receipt from Put must no longer work.
			err = b.DeleteMessage("q1", info.ID, info.PopReceipt)
			require.ErrorIs(t, err, azurequeue.ErrPopReceiptMismatch, tt.name)

			require.NoError(t, b.DeleteMessage("q1", info.ID, updated.PopReceipt), tt.name)
		})
	}
}

func TestInMemoryBackend_UpdateMessage_PopReceiptMismatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "wrong_pop_receipt_rejected"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := azurequeue.NewInMemoryBackend()
			_, err := b.CreateQueue("q1")
			require.NoError(t, err)
			info, err := b.PutMessage("q1", "x", 0, 0)
			require.NoError(t, err)

			_, err = b.UpdateMessage("q1", info.ID, "wrong", time.Minute, nil)
			assert.ErrorIs(t, err, azurequeue.ErrPopReceiptMismatch, tt.name)
		})
	}
}

func TestInMemoryBackend_ClearMessages(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "clear_removes_all"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := azurequeue.NewInMemoryBackend()
			_, err := b.CreateQueue("q1")
			require.NoError(t, err)
			_, err = b.PutMessage("q1", "a", 0, 0)
			require.NoError(t, err)
			_, err = b.PutMessage("q1", "b", 0, 0)
			require.NoError(t, err)

			require.NoError(t, b.ClearMessages("q1"))

			peeked, err := b.PeekMessages("q1", 10)
			require.NoError(t, err)
			assert.Empty(t, peeked, tt.name)
		})
	}
}

func TestInMemoryBackend_GetMessages_RespectsNumOfMessagesAndOrder(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		put  []string
		want []string
		n    int
	}{
		{name: "fifo_limited", put: []string{"a", "b", "c"}, n: 2, want: []string{"a", "b"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := azurequeue.NewInMemoryBackend()
			_, err := b.CreateQueue("q1")
			require.NoError(t, err)

			for _, text := range tt.put {
				_, putErr := b.PutMessage("q1", text, 0, 0)
				require.NoError(t, putErr)
			}

			got, err := b.GetMessages("q1", tt.n, 30*time.Second)
			require.NoError(t, err)

			gotTexts := make([]string, len(got))
			for i, m := range got {
				gotTexts[i] = m.Text
			}

			assert.Equal(t, tt.want, gotTexts, tt.name)
		})
	}
}

func TestInMemoryBackend_SweepExpired(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "expired_messages_removed_others_kept"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			now := base

			b := azurequeue.NewInMemoryBackend()
			azurequeue.SetNowFunc(b, func() time.Time { return now })

			_, err := b.CreateQueue("q1")
			require.NoError(t, err)

			_, err = b.PutMessage("q1", "expires-soon", 0, time.Second)
			require.NoError(t, err)
			_, err = b.PutMessage("q1", "survives", 0, time.Hour)
			require.NoError(t, err)

			now = base.Add(2 * time.Second)

			removed := azurequeue.SweepExpired(b, now)
			assert.Equal(t, 1, removed, tt.name)

			peeked, err := b.PeekMessages("q1", 10)
			require.NoError(t, err)
			require.Len(t, peeked, 1, tt.name)
			assert.Equal(t, "survives", peeked[0].Text, tt.name)
		})
	}
}

func TestInMemoryBackend_Reset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "reset_clears_all"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := azurequeue.NewInMemoryBackend()
			_, err := b.CreateQueue("q1")
			require.NoError(t, err)
			_, err = b.PutMessage("q1", "x", 0, 0)
			require.NoError(t, err)

			b.Reset()

			assert.Empty(t, b.ListQueues(), tt.name)
		})
	}
}

func TestInMemoryBackend_SetIDFunc(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "deterministic_ids"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := azurequeue.NewInMemoryBackend()
			azurequeue.SetIDFunc(b, sequentialIDs())

			_, err := b.CreateQueue("q1")
			require.NoError(t, err)

			info, err := b.PutMessage("q1", "x", 0, 0)
			require.NoError(t, err)
			assert.Equal(t, "id-1", info.ID, tt.name)
			assert.Equal(t, "id-2", info.PopReceipt, tt.name)
		})
	}
}

// TestInMemoryBackend_DeleteAndUpdateMessage_RejectExpiredMessage is a
// regression test: findMessageLocked must not return (and let
// DeleteMessage/UpdateMessage mutate) a message that has reached its
// expiration instant, even before the Janitor has swept it. Uses the
// injected clock seam -- no sleeps -- to check both the exact expiry instant
// and just after it.
func TestInMemoryBackend_DeleteAndUpdateMessage_RejectExpiredMessage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		advance time.Duration
	}{
		{name: "exactly_at_expiry", advance: time.Second},
		{name: "just_after_expiry", advance: time.Second + time.Millisecond},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			now := base

			b := azurequeue.NewInMemoryBackend()
			azurequeue.SetNowFunc(b, func() time.Time { return now })

			_, err := b.CreateQueue("q1")
			require.NoError(t, err)

			info, err := b.PutMessage("q1", "x", 0, time.Second)
			require.NoError(t, err)

			now = base.Add(tt.advance)

			err = b.DeleteMessage("q1", info.ID, info.PopReceipt)
			require.ErrorIs(t, err, azurequeue.ErrMessageNotFound, tt.name)

			_, err = b.UpdateMessage("q1", info.ID, info.PopReceipt, time.Minute, nil)
			assert.ErrorIs(t, err, azurequeue.ErrMessageNotFound, tt.name)
		})
	}
}

// TestInMemoryBackend_DeleteAndUpdateMessage_NotYetExpiredSucceeds pins the
// boundary from the other side: a message one instant before its expiration
// is still a normal, mutable message.
func TestInMemoryBackend_DeleteAndUpdateMessage_NotYetExpiredSucceeds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "just_before_expiry"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			now := base

			b := azurequeue.NewInMemoryBackend()
			azurequeue.SetNowFunc(b, func() time.Time { return now })

			_, err := b.CreateQueue("q1")
			require.NoError(t, err)

			info, err := b.PutMessage("q1", "x", 0, time.Second)
			require.NoError(t, err)

			now = base.Add(999 * time.Millisecond)

			_, err = b.UpdateMessage("q1", info.ID, info.PopReceipt, time.Minute, nil)
			require.NoError(t, err, tt.name)
		})
	}
}

// TestInMemoryBackend_GetMessages_InvalidNumOfMessages and its Peek sibling
// are regression tests: a negative numOfMessages previously reached make()
// unchecked, which panics on a negative capacity; zero and over-max values
// silently bypassed the documented [MinNumOfMessages, MaxNumOfMessages]
// bounds. Both must now be rejected with ErrOutOfRangeQueryParam instead.
func TestInMemoryBackend_GetMessages_InvalidNumOfMessages(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		numOfMessages int
	}{
		{name: "negative", numOfMessages: -1},
		{name: "zero", numOfMessages: 0},
		{name: "over_max", numOfMessages: azurequeue.MaxNumOfMessages + 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := azurequeue.NewInMemoryBackend()
			_, err := b.CreateQueue("q1")
			require.NoError(t, err)

			_, err = b.GetMessages("q1", tt.numOfMessages, time.Second)
			assert.ErrorIs(t, err, azurequeue.ErrOutOfRangeQueryParam, tt.name)
		})
	}
}

func TestInMemoryBackend_PeekMessages_InvalidNumOfMessages(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		numOfMessages int
	}{
		{name: "negative", numOfMessages: -1},
		{name: "zero", numOfMessages: 0},
		{name: "over_max", numOfMessages: azurequeue.MaxNumOfMessages + 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := azurequeue.NewInMemoryBackend()
			_, err := b.CreateQueue("q1")
			require.NoError(t, err)

			_, err = b.PeekMessages("q1", tt.numOfMessages)
			assert.ErrorIs(t, err, azurequeue.ErrOutOfRangeQueryParam, tt.name)
		})
	}
}
