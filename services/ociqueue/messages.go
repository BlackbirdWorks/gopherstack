package ociqueue

import (
	"context"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	maxBatch        = 20
	maxContentBytes = 128 * 1024
	maxBatchBytes   = 512 * 1024
	maxChannelID    = 64
	defaultGetLimit = 1
	maxGetLimit     = 20
)

type metaIn struct {
	ChannelID        *string           `json:"channelId"`
	CustomProperties map[string]string `json:"customProperties"`
}

type putEntryIn struct {
	Content  *string `json:"content"`
	Metadata *metaIn `json:"metadata"`
}

type putMessagesIn struct {
	Messages []putEntryIn `json:"messages"`
}

type PutMessageWire struct {
	ExpireAfter time.Time `json:"expireAfter"`
	ID          int64     `json:"id"`
}

type metaWire struct {
	CustomProperties map[string]string `json:"customProperties,omitempty"`
	ChannelID        string            `json:"channelId"`
}

type GetMessageWire struct {
	VisibleAfter  time.Time `json:"visibleAfter"`
	ExpireAfter   time.Time `json:"expireAfter"`
	CreatedAt     time.Time `json:"createdAt"`
	Metadata      *metaWire `json:"metadata,omitempty"`
	Content       string    `json:"content"`
	Receipt       string    `json:"receipt"`
	ID            int64     `json:"id"`
	DeliveryCount int       `json:"deliveryCount"`
}

// GetParams carries the GetMessages query parameters; nil fields fall back to the queue defaults.
type GetParams struct {
	Visibility    *int
	Timeout       *int
	Limit         *int
	ChannelFilter string
}

type UpdatedWire struct {
	VisibleAfter time.Time `json:"visibleAfter"`
	ID           int64     `json:"id"`
}

type resultEntryWire struct {
	ID           *int64     `json:"id,omitempty"`
	VisibleAfter *time.Time `json:"visibleAfter,omitempty"`
	ErrorCode    *int       `json:"errorCode,omitempty"`
	ErrorMessage *string    `json:"errorMessage,omitempty"`
}

type BatchResultWire struct {
	Entries        []resultEntryWire `json:"entries"`
	ServerFailures int               `json:"serverFailures"`
	ClientFailures int               `json:"clientFailures"`
}

type receiptEntryIn struct {
	Visibility *int   `json:"visibilityInSeconds"`
	Receipt    string `json:"receipt"`
}

type receiptsIn struct {
	Entries []receiptEntryIn `json:"entries"`
}

type statsWire struct {
	VisibleMessages  int64 `json:"visibleMessages"`
	InFlightMessages int64 `json:"inFlightMessages"`
	SizeInBytes      int64 `json:"sizeInBytes"`
}

type QueueStatsWire struct {
	Queue statsWire `json:"queue"`
	DLQ   statsWire `json:"dlq"`
}

// dataQueue returns a queue that accepts data plane calls; the caller holds the write lock.
func (b *InMemoryBackend) dataQueue(id string) (*Queue, *APIError) {
	b.settleID(id)

	q, aerr := b.liveQueue(id)
	if aerr != nil {
		return nil, aerr
	}

	if q.State == stateCreating || q.State == stateDeleting {
		return nil, errState("Queue " + id + " is in state " + q.State + "; the operation requires ACTIVE")
	}

	return q, nil
}

func validatePut(in putMessagesIn) *APIError {
	if len(in.Messages) == 0 {
		return errMissing("messages is required")
	}

	if len(in.Messages) > maxBatch {
		return errParam("a request carries at most 20 messages")
	}

	total := 0

	for _, m := range in.Messages {
		if m.Content == nil {
			return errMissing("every message requires content")
		}

		if len(*m.Content) > maxContentBytes {
			return errParam("a message content is limited to 128 KiB")
		}

		total += len(*m.Content)

		if m.Metadata != nil {
			if m.Metadata.ChannelID == nil || *m.Metadata.ChannelID == "" {
				return errMissing("metadata.channelId is required when metadata is set")
			}

			if len(*m.Metadata.ChannelID) > maxChannelID {
				return errParam("channelId must be at most 64 characters")
			}
		}
	}

	if total > maxBatchBytes {
		return errParam("a request carries at most 512 KiB of message content")
	}

	return nil
}

// PutMessages appends messages to a queue.
func (b *InMemoryBackend) PutMessages(queueID string, in putMessagesIn) ([]PutMessageWire, error) {
	if aerr := validatePut(in); aerr != nil {
		return nil, aerr
	}

	b.mu.Lock("PutMessages")
	defer b.mu.Unlock()

	q, aerr := b.dataQueue(queueID)
	if aerr != nil {
		return nil, aerr
	}

	now := b.nowUTC()
	out := make([]PutMessageWire, 0, len(in.Messages))

	for _, e := range in.Messages {
		m := &Message{
			ID: q.NextMsgID, Content: *e.Content, Created: now, VisibleAfter: now,
			Expire: now.Add(time.Duration(q.Retention) * time.Second),
		}
		q.NextMsgID++

		if e.Metadata != nil {
			m.Channel, m.Props = *e.Metadata.ChannelID, e.Metadata.CustomProperties
		}

		q.Messages = append(q.Messages, m)
		out = append(out, PutMessageWire{ID: m.ID, ExpireAfter: m.Expire})
	}

	q.notify()

	return out, nil
}

func matchChannel(m *Message, filter string) bool { return filter == "" || m.Channel == filter }

func (m *Message) view() GetMessageWire {
	w := GetMessageWire{
		ID: m.ID, Content: m.Content, Receipt: m.Receipt, DeliveryCount: m.Deliveries,
		VisibleAfter: m.VisibleAfter, ExpireAfter: m.Expire, CreatedAt: m.Created,
	}
	if m.Channel != "" {
		w.Metadata = &metaWire{ChannelID: m.Channel, CustomProperties: m.Props}
	}

	return w
}

type pollResult struct {
	wake    <-chan struct{}
	after   func(time.Duration) <-chan time.Time
	msgs    []GetMessageWire
	until   time.Duration
	timeout int
}

// pollOnce delivers up to limit visible messages, or reports how long until the next one becomes visible.
func (b *InMemoryBackend) pollOnce(queueID string, p GetParams) (pollResult, error) {
	b.mu.Lock("GetMessages")
	defer b.mu.Unlock()

	q, aerr := b.dataQueue(queueID)
	if aerr != nil {
		return pollResult{}, aerr
	}

	vis, limit := def(p.Visibility, q.Visibility), def(p.Limit, defaultGetLimit)
	res := pollResult{timeout: def(p.Timeout, q.Timeout), after: b.after, wake: q.waiter(), until: -1}
	now := b.nowUTC()
	res.msgs = []GetMessageWire{}

	for _, m := range q.Messages {
		if !matchChannel(m, p.ChannelFilter) {
			continue
		}

		if m.VisibleAfter.After(now) {
			if d := m.VisibleAfter.Sub(now); res.until < 0 || d < res.until {
				res.until = d
			}

			continue
		}

		if len(res.msgs) >= limit {
			continue
		}

		m.Deliveries++
		m.Receipt = uuid.NewString()
		m.VisibleAfter = now.Add(time.Duration(vis) * time.Second)
		res.msgs = append(res.msgs, m.view())
	}

	if len(res.msgs) > 0 {
		q.notify()
	}

	return res, nil
}

// GetMessages returns visible messages, long polling until one arrives, timeout elapses, ctx or stop ends.
func (b *InMemoryBackend) GetMessages(
	ctx context.Context, stop <-chan struct{}, queueID string, p GetParams,
) ([]GetMessageWire, error) {
	if aerr := firstErr(
		inRange("visibilityInSeconds", p.Visibility, 0, maxVisibility),
		inRange("timeoutInSeconds", p.Timeout, 0, maxPollTimeout),
		inRange("limit", p.Limit, 1, maxGetLimit),
	); aerr != nil {
		return nil, aerr
	}

	var deadline <-chan time.Time

	for {
		res, err := b.pollOnce(queueID, p)
		if err != nil {
			return nil, err
		}

		if len(res.msgs) > 0 || res.timeout == 0 {
			return res.msgs, nil
		}

		if deadline == nil {
			deadline = res.after(time.Duration(res.timeout) * time.Second)
		}

		var visible <-chan time.Time
		if res.until >= 0 {
			visible = res.after(res.until)
		}

		select {
		case <-res.wake:
		case <-visible:
		case <-deadline:
			return res.msgs, nil
		case <-ctx.Done():
			return res.msgs, nil
		case <-stop:
			return res.msgs, nil
		}
	}
}

func findReceipt(q *Queue, receipt string) *Message {
	if receipt == "" {
		return nil
	}

	for _, m := range q.Messages {
		if m.Receipt == receipt {
			return m
		}
	}

	return nil
}

func errReceipt() *APIError {
	return errNotFound("message receipt")
}

func (b *InMemoryBackend) setVisibility(q *Queue, receipt string, vis int) (*Message, *APIError) {
	m := findReceipt(q, receipt)
	if m == nil {
		return nil, errReceipt()
	}

	m.VisibleAfter = b.nowUTC().Add(time.Duration(vis) * time.Second)
	q.notify()

	return m, nil
}

// UpdateMessage changes the visibility of one in-flight message.
func (b *InMemoryBackend) UpdateMessage(queueID, receipt string, vis *int) (UpdatedWire, error) {
	if vis == nil {
		return UpdatedWire{}, errMissing("visibilityInSeconds is required")
	}

	if aerr := inRange("visibilityInSeconds", vis, 0, maxVisibility); aerr != nil {
		return UpdatedWire{}, aerr
	}

	b.mu.Lock("UpdateMessage")
	defer b.mu.Unlock()

	q, aerr := b.dataQueue(queueID)
	if aerr != nil {
		return UpdatedWire{}, aerr
	}

	m, aerr := b.setVisibility(q, receipt, *vis)
	if aerr != nil {
		return UpdatedWire{}, aerr
	}

	return UpdatedWire{ID: m.ID, VisibleAfter: m.VisibleAfter}, nil
}

func checkEntries(n int) *APIError {
	if n == 0 {
		return errMissing("entries is required")
	}

	if n > maxBatch {
		return errParam("a request carries at most 20 entries")
	}

	return nil
}

func failEntry(res *BatchResultWire, msg string) {
	code := http.StatusNotFound
	res.ClientFailures++
	res.Entries = append(res.Entries, resultEntryWire{ErrorCode: &code, ErrorMessage: &msg})
}

// UpdateMessages changes the visibility of several in-flight messages; each entry reports its own result.
func (b *InMemoryBackend) UpdateMessages(queueID string, in receiptsIn) (BatchResultWire, error) {
	if aerr := checkEntries(len(in.Entries)); aerr != nil {
		return BatchResultWire{}, aerr
	}

	for _, e := range in.Entries {
		if e.Visibility == nil {
			return BatchResultWire{}, errMissing("every entry requires visibilityInSeconds")
		}

		if aerr := inRange("visibilityInSeconds", e.Visibility, 0, maxVisibility); aerr != nil {
			return BatchResultWire{}, aerr
		}
	}

	b.mu.Lock("UpdateMessages")
	defer b.mu.Unlock()

	q, aerr := b.dataQueue(queueID)
	if aerr != nil {
		return BatchResultWire{}, aerr
	}

	res := BatchResultWire{Entries: make([]resultEntryWire, 0, len(in.Entries))}

	for _, e := range in.Entries {
		m, ae := b.setVisibility(q, e.Receipt, *e.Visibility)
		if ae != nil {
			failEntry(&res, "The message receipt is invalid or the message no longer exists")

			continue
		}

		res.Entries = append(res.Entries, resultEntryWire{ID: &m.ID, VisibleAfter: &m.VisibleAfter})
	}

	return res, nil
}

func (q *Queue) remove(receipt string) bool {
	i := slices.IndexFunc(q.Messages, func(m *Message) bool { return receipt != "" && m.Receipt == receipt })
	if i < 0 {
		return false
	}

	q.Messages = slices.Delete(q.Messages, i, i+1)

	return true
}

// DeleteMessage deletes one message by receipt.
func (b *InMemoryBackend) DeleteMessage(queueID, receipt string) error {
	b.mu.Lock("DeleteMessage")
	defer b.mu.Unlock()

	q, aerr := b.dataQueue(queueID)
	if aerr != nil {
		return aerr
	}

	if !q.remove(receipt) {
		return errReceipt()
	}

	q.notify()

	return nil
}

// DeleteMessages deletes several messages by receipt; each entry reports its own result.
func (b *InMemoryBackend) DeleteMessages(queueID string, in receiptsIn) (BatchResultWire, error) {
	if aerr := checkEntries(len(in.Entries)); aerr != nil {
		return BatchResultWire{}, aerr
	}

	b.mu.Lock("DeleteMessages")
	defer b.mu.Unlock()

	q, aerr := b.dataQueue(queueID)
	if aerr != nil {
		return BatchResultWire{}, aerr
	}

	res := BatchResultWire{Entries: make([]resultEntryWire, 0, len(in.Entries))}

	for _, e := range in.Entries {
		if !q.remove(e.Receipt) {
			failEntry(&res, "The message receipt is invalid or the message no longer exists")

			continue
		}

		res.Entries = append(res.Entries, resultEntryWire{})
	}

	q.notify()

	return res, nil
}

func tally(msgs []*Message, now time.Time, channel string) statsWire {
	var s statsWire

	for _, m := range msgs {
		if !matchChannel(m, channel) {
			continue
		}

		if m.VisibleAfter.After(now) {
			s.InFlightMessages++
		} else {
			s.VisibleMessages++
		}

		s.SizeInBytes += int64(len(m.Content))
	}

	return s
}

// GetStats counts visible and in-flight messages and their size, for the queue and its dead letter queue.
func (b *InMemoryBackend) GetStats(queueID, channel string) (QueueStatsWire, error) {
	b.mu.Lock("GetStats")
	defer b.mu.Unlock()

	q, aerr := b.dataQueue(queueID)
	if aerr != nil {
		return QueueStatsWire{}, aerr
	}

	now := b.nowUTC()

	return QueueStatsWire{Queue: tally(q.Messages, now, channel), DLQ: tally(q.DLQ, now, channel)}, nil
}

// ListChannels lists the channels that currently hold messages.
func (b *InMemoryBackend) ListChannels(queueID, filter string, o ListOpts) (page.Page[string], error) {
	b.mu.Lock("ListChannels")
	defer b.mu.Unlock()

	q, aerr := b.dataQueue(queueID)
	if aerr != nil {
		return page.Page[string]{}, aerr
	}

	seen := map[string]bool{}

	for _, m := range append(slices.Clone(q.Messages), q.DLQ...) {
		if m.Channel != "" && (filter == "" || strings.Contains(m.Channel, filter)) {
			seen[m.Channel] = true
		}
	}

	all := make([]string, 0, len(seen))
	for c := range seen {
		all = append(all, c)
	}

	slices.Sort(all)

	return page.New(all, o.Page, o.Limit, defaultListLimit), nil
}

func queryInt(q *req, key string) *int {
	v := q.query(key)
	if v == "" {
		return nil
	}

	n, err := strconv.Atoi(v)
	if err != nil {
		n = -1
	}

	return &n
}

func (h *Handler) putMessages(q *req) error {
	var in putMessagesIn
	if err := q.decode(&in); err != nil {
		return err
	}

	out, err := h.Backend.PutMessages(q.param("queueId"), in)

	return reply(q, map[string]any{"messages": out}, "", err)
}

func (h *Handler) getMessages(q *req) error {
	out, err := h.Backend.GetMessages(q.r.Context(), h.stopCh(), q.param("queueId"), GetParams{
		Visibility: queryInt(q, "visibilityInSeconds"), Timeout: queryInt(q, "timeoutInSeconds"),
		Limit: queryInt(q, "limit"), ChannelFilter: q.query("channelFilter"),
	})

	return reply(q, map[string]any{"messages": out}, "", err)
}

func (h *Handler) updateMessage(q *req) error {
	var in struct {
		Visibility *int `json:"visibilityInSeconds"`
	}

	if err := q.decode(&in); err != nil {
		return err
	}

	out, err := h.Backend.UpdateMessage(q.param("queueId"), q.param("messageReceipt"), in.Visibility)

	return reply(q, out, "", err)
}

func (h *Handler) updateMessages(q *req) error {
	var in receiptsIn
	if err := q.decode(&in); err != nil {
		return err
	}

	out, err := h.Backend.UpdateMessages(q.param("queueId"), in)

	return reply(q, out, "", err)
}

func (h *Handler) deleteMessage(q *req) error {
	if err := h.Backend.DeleteMessage(q.param("queueId"), q.param("messageReceipt")); err != nil {
		return err
	}

	q.w.WriteHeader(http.StatusNoContent)

	return nil
}

func (h *Handler) deleteMessages(q *req) error {
	var in receiptsIn
	if err := q.decode(&in); err != nil {
		return err
	}

	out, err := h.Backend.DeleteMessages(q.param("queueId"), in)

	return reply(q, out, "", err)
}

func (h *Handler) getStats(q *req) error {
	out, err := h.Backend.GetStats(q.param("queueId"), q.query("channelId"))

	return reply(q, out, "", err)
}

func (h *Handler) listChannels(q *req) error {
	pg, err := h.Backend.ListChannels(q.param("queueId"), q.query("channelFilter"), q.listOpts())
	if err != nil {
		return err
	}

	q.nextPage(pg.Next)

	return q.json(http.StatusOK, map[string]any{itemsKey: pg.Data}, "")
}
