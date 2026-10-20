package azureservicebus_test

import (
	"encoding/json"
	"math"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azureservicebus"
)

func seedQueue(t *testing.T, bodies ...string) *azureservicebus.Handler {
	t.Helper()

	h := newTestHandler(t)
	require.Equal(t, http.StatusCreated, doRequest(t, h, http.MethodPut, "/q", nil, nil).Code)

	for _, body := range bodies {
		rec := doRequest(t, h, http.MethodPost, "/q/messages", []byte(body), map[string]string{"Color": "blue"})
		require.Equal(t, http.StatusCreated, rec.Code)
	}

	return h
}

type peekResp struct {
	CustomProperties map[string]string `json:"CustomProperties"`
	BrokerProperties struct {
		DeadLetterReason string `json:"DeadLetterReason"`
		SequenceNumber   int64  `json:"SequenceNumber"`
		DeliveryCount    int64  `json:"DeliveryCount"`
	} `json:"BrokerProperties"`
}

func peek(t *testing.T, h *azureservicebus.Handler, path string) []peekResp {
	t.Helper()

	rec := doRequest(t, h, http.MethodGet, path, nil, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out []peekResp
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	return out
}

func TestHandler_PeekMessages(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
		want int
	}{
		{"default count one", "/q/messages", 1},
		{"max messages", "/q/messages?maxMessages=10", 3},
		{"from sequence", "/q/messages?maxMessages=10&fromSequenceNumber=2", 2},
		{"past end", "/q/messages?maxMessages=10&fromSequenceNumber=99", 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := seedQueue(t, "a", "b", "c")
			got := peek(t, h, tt.path)
			assert.Len(t, got, tt.want)

			again := peek(t, h, tt.path)
			assert.Len(t, again, tt.want)

			for _, m := range got {
				assert.Zero(t, m.BrokerProperties.DeliveryCount)
				assert.Equal(t, "blue", m.CustomProperties["Color"])
			}

			// peek never locks: a peek-lock still gets the first message.
			rec := doRequest(t, h, http.MethodPost, "/q/messages/head", nil, nil)
			assert.Equal(t, http.StatusCreated, rec.Code)
			assert.Equal(t, "blue", rec.Header().Get("Color"))
		})
	}
}

func TestHandler_ReceiveAndDelete(t *testing.T) {
	t.Parallel()

	h := seedQueue(t, "a")

	rec := doRequest(t, h, http.MethodDelete, "/q/messages/head", nil, nil)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "a", rec.Body.String())
	assert.Equal(t, "blue", rec.Header().Get("Color"))

	rec = doRequest(t, h, http.MethodDelete, "/q/messages/head", nil, nil)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	assert.Empty(t, peek(t, h, "/q/messages"))
	assert.Equal(t, http.StatusNotFound, doRequest(t, h, http.MethodDelete, "/nope/messages/head", nil, nil).Code)
}

func TestHandler_DeadLetterMessage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		header     string
		wantStatus int
		wantDL     bool
	}{
		{
			"reason and description",
			`{"DeadLetterReason":"bad","DeadLetterErrorDescription":"why"}`, http.StatusOK, true,
		},
		{"reason only", `{"DeadLetterReason":"bad"}`, http.StatusOK, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := seedQueue(t, "a")
			lock := doRequest(t, h, http.MethodPost, "/q/messages/head", nil, nil)
			require.Equal(t, http.StatusCreated, lock.Code)

			loc := lock.Header().Get("Location")
			rec := doRequest(t, h, http.MethodPut, loc, nil, map[string]string{"BrokerProperties": tt.header})
			require.Equal(t, tt.wantStatus, rec.Code)

			assert.Empty(t, peek(t, h, "/q/messages"))

			dl := peek(t, h, "/q/$DeadLetterQueue/messages")
			require.Len(t, dl, 1)
			assert.Equal(t, "bad", dl[0].BrokerProperties.DeadLetterReason)

			got := doRequest(t, h, http.MethodDelete, "/q/$DeadLetterQueue/messages/head", nil, nil)
			require.Equal(t, http.StatusOK, got.Code)
			assert.Equal(t, "bad", got.Header().Get("Deadletterreason"))

			// stale lock now gone
			again := doRequest(t, h, http.MethodPut, loc, nil, map[string]string{"BrokerProperties": tt.header})
			assert.Equal(t, http.StatusGone, again.Code)
		})
	}
}

func TestHandler_DeadLetterFromDeadLetterQueue(t *testing.T) {
	t.Parallel()

	h := seedQueue(t, "a")
	lock := doRequest(t, h, http.MethodPost, "/q/messages/head", nil, nil)
	loc := lock.Header().Get("Location")
	require.Equal(
		t,
		http.StatusOK,
		doRequest(
			t,
			h,
			http.MethodPut,
			loc,
			nil,
			map[string]string{"BrokerProperties": `{"DeadLetterReason":"x"}`},
		).Code,
	)

	dlLock := doRequest(t, h, http.MethodPost, "/q/$DeadLetterQueue/messages/head", nil, nil)
	require.Equal(t, http.StatusCreated, dlLock.Code)

	rec := doRequest(t, h, http.MethodPut, dlLock.Header().Get("Location"), nil,
		map[string]string{"BrokerProperties": `{"DeadLetterReason":"y"}`})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_QueueMessageCounts(t *testing.T) {
	t.Parallel()

	h := seedQueue(t, "a", "b")
	lock := doRequest(t, h, http.MethodPost, "/q/messages/head", nil, nil)
	doRequest(t, h, http.MethodPut, lock.Header().Get("Location"), nil,
		map[string]string{"BrokerProperties": `{"DeadLetterReason":"x"}`})

	rec := doRequest(t, h, http.MethodGet, "/q", nil, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, "<ActiveMessageCount>1</ActiveMessageCount>")
	assert.Contains(t, body, "<DeadLetterMessageCount>1</DeadLetterMessageCount>")
	assert.Contains(t, body, "<MessageCount>2</MessageCount>")
}

func TestBackend_AutoDeadLetterReason(t *testing.T) {
	t.Parallel()

	b := newQueueBackend(t)
	_, err := b.CreateQueue("q2", azureservicebus.EntityConfig{MaxDeliveryCount: 1})
	require.NoError(t, err)

	ref := azureservicebus.EntityRef{Queue: "q2"}
	_, err = b.Send(ref, azureservicebus.NewMessage{Body: []byte("x")})
	require.NoError(t, err)

	info, err := b.PeekLock(ref, false, 0)
	require.NoError(t, err)
	require.NoError(t, b.Abandon(ref, false, info.MessageID, info.LockToken))

	dl, err := b.Peek(ref, true, 0, 10)
	require.NoError(t, err)
	require.Len(t, dl, 1)
	assert.Equal(t, azureservicebus.ReasonMaxDeliveryCountExceeded, dl[0].DeadLetterReason)
}

func TestPeekCountBounds(t *testing.T) {
	t.Parallel()

	const sent = 300

	tests := []struct {
		name  string
		count int
		want  int
	}{
		{"huge count capped", math.MaxInt, 256},
		{"negative count empty", -5, 0},
		{"within cap", 10, 10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newQueueBackend(t)
			ref := azureservicebus.EntityRef{Queue: "q"}

			for range sent {
				_, err := b.Send(ref, azureservicebus.NewMessage{Body: []byte("x")})
				require.NoError(t, err)
			}

			got, err := b.Peek(ref, false, 0, tt.count)
			require.NoError(t, err)
			assert.Len(t, got, tt.want)
		})
	}
}
