package ocinotifications

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

const (
	maxMessageBytes = 64 * 1024
	msgJSON         = "JSON"
	msgRawText      = "RAW_TEXT"
	maxBackoff      = 32 * time.Minute
	immediateTries  = 2
	maxErrBody      = 256
	maxBackoffShift = 5
)

// PublishIn is the PublishMessage request body.
type PublishIn struct {
	Title *string `json:"title"`
	Body  *string `json:"body"`
}

type PublishWire struct {
	TimeStamp time.Time `json:"timeStamp"`
	MessageID string    `json:"messageId"`
}

// ErrEndpointStatus marks a delivery attempt that the endpoint answered with a non-2xx status.
var ErrEndpointStatus = errors.New("endpoint answered")

// DeliveryView is a delivery record as reported by the admin endpoint.
type DeliveryView = Delivery

func isHTTPProtocol(p string) bool { return p == protoHTTPS || p == protoCustom }

// recordsOnly reports protocols whose delivery the emulator records without sending anything.
func recordsOnly(p string) bool {
	return p == protoEmail || p == protoSMS || p == protoSlack || p == protoPagerDuty
}

func (b *InMemoryBackend) newDelivery(s *Subscription, t *Topic, kind string) *Delivery {
	now := b.nowUTC()
	d := &Delivery{
		ID: uuid.NewString(), Kind: kind, TopicID: t.ID, TopicName: t.Name, SubscriptionID: s.ID, Protocol: s.Protocol,
		Endpoint: s.Endpoint, Published: now, NextAttempt: now, Status: deliveryPending,
		Deadline: now.Add(time.Duration(s.MaxRetryMs) * time.Millisecond), UnsubscribeURL: unsubscribeURL(s),
		Seq: b.nextSeq(),
	}
	b.deliveries[d.ID] = d

	return d
}

// finalize marks a delivery as complete without an attempt, for protocols that are only recorded.
func (b *InMemoryBackend) finalize(d *Delivery, status, reason string) {
	d.Status, d.LastError, d.Completed = status, reason, b.nowUTC()
}

func (b *InMemoryBackend) route(d *Delivery) {
	switch {
	case isHTTPProtocol(d.Protocol):
	case d.Protocol == protoFunctions && b.invoker != nil && d.Kind == kindMessage:
	case d.Protocol == protoFunctions && d.Kind == kindConfirmation:
		b.finalize(d, deliveryRecorded, "")
	case d.Protocol == protoFunctions:
		b.finalize(d, deliveryRecorded, "no OCI Functions backend is wired; delivery recorded only")
	case recordsOnly(d.Protocol):
		b.finalize(d, deliveryRecorded, "")
	}

	b.trim()
}

func (b *InMemoryBackend) trim() {
	if len(b.deliveries) <= maxKeptDeliveries {
		return
	}

	done := make([]*Delivery, 0, len(b.deliveries))

	for _, d := range b.deliveries {
		if d.Status != deliveryPending {
			done = append(done, d)
		}
	}

	slices.SortFunc(done, func(a, c *Delivery) int { return int(a.Seq - c.Seq) })

	for _, d := range done {
		if len(b.deliveries) <= maxKeptDeliveries {
			return
		}

		delete(b.deliveries, d.ID)
	}
}

// queueConfirmation creates the confirmation delivery for a PENDING subscription.
func (b *InMemoryBackend) queueConfirmation(s *Subscription, t *Topic) {
	d := b.newDelivery(s, t, kindConfirmation)
	d.ConfirmationURL = b.confirmationURL(s)
	d.MessageType = msgJSON
	body, _ := json.Marshal(map[string]string{ //nolint:errchkjson // static shape
		"SubscriptionId": s.ID, "ConfirmationURL": d.ConfirmationURL,
		"Message": "You have chosen to subscribe to the topic " + t.Name +
			". To confirm the subscription, visit the ConfirmationURL included in this message.",
	})
	d.Body = string(body)
	d.Title = "Confirm your subscription to " + t.Name
	b.route(d)
}

// cancelConfirmations discards confirmation deliveries still waiting to be sent.
func (b *InMemoryBackend) cancelConfirmations(subID string) {
	for _, d := range b.deliveries {
		if d.SubscriptionID == subID && d.Kind == kindConfirmation && d.Status == deliveryPending {
			b.finalize(d, deliveryDiscarded, "superseded")
		}
	}
}

// PublishMessage fans a message out to every ACTIVE subscription of a topic.
func (b *InMemoryBackend) PublishMessage(topicID, messageType string, in PublishIn) (PublishWire, error) {
	if in.Body == nil {
		return PublishWire{}, errMissing("body is required")
	}

	if messageType == "" {
		messageType = msgRawText
	}

	if messageType != msgJSON && messageType != msgRawText {
		return PublishWire{}, errParam("messageType must be JSON or RAW_TEXT")
	}

	title := ""
	if in.Title != nil {
		title = *in.Title
	}

	if len(*in.Body)+len(title) > maxMessageBytes {
		return PublishWire{}, errParam("a message is limited to 64 KB")
	}

	if messageType == msgJSON && !json.Valid([]byte(*in.Body)) {
		return PublishWire{}, errParam("body must be valid JSON when messageType is JSON")
	}

	b.mu.Lock("PublishMessage")
	defer b.mu.Unlock()

	t, aerr := b.activeTopic(topicID)
	if aerr != nil {
		return PublishWire{}, aerr
	}

	res := PublishWire{MessageID: uuid.NewString(), TimeStamp: b.nowUTC()}

	for _, s := range b.topicSubs(t.ID) {
		if s.State != subActive {
			continue
		}

		d := b.newDelivery(s, t, kindMessage)
		d.MessageID, d.Title, d.Body, d.MessageType = res.MessageID, title, *in.Body, messageType
		b.route(d)
	}

	b.Kick()

	return res, nil
}

// Deliveries lists delivery records, optionally filtered by topic and subscription, oldest first.
func (b *InMemoryBackend) Deliveries(topicID, subscriptionID string) []DeliveryView {
	b.mu.RLock("Deliveries")
	defer b.mu.RUnlock()

	var out []DeliveryView

	for _, d := range b.deliveries {
		if (topicID == "" || d.TopicID == topicID) && (subscriptionID == "" || d.SubscriptionID == subscriptionID) {
			out = append(out, *d)
		}
	}

	slices.SortFunc(out, func(a, c DeliveryView) int { return int(a.Seq - c.Seq) })

	return out
}

// NextDue reports how long until the earliest pending delivery is due; negative when none is waiting.
func (b *InMemoryBackend) NextDue() (time.Duration, func(time.Duration) <-chan time.Time) {
	b.mu.RLock("NextDue")
	defer b.mu.RUnlock()

	now, wait := b.nowUTC(), time.Duration(-1)

	for _, d := range b.deliveries {
		if d.Status != deliveryPending || d.inflight || !b.deliverable(d) {
			continue
		}

		if w := max(d.NextAttempt.Sub(now), 0); wait < 0 || w < wait {
			wait = w
		}
	}

	return wait, b.after
}

// deliverable reports whether a pending delivery has a live route; the caller holds the lock.
func (b *InMemoryBackend) deliverable(d *Delivery) bool {
	return isHTTPProtocol(d.Protocol) || (d.Protocol == protoFunctions && b.invoker != nil)
}

// DeliverDue starts an attempt for every pending delivery whose time has come, using spawn to run each one.
func (b *InMemoryBackend) DeliverDue(ctx context.Context, spawn func(func())) {
	b.mu.Lock("DeliverDue")

	now := b.nowUTC()

	var due []*Delivery

	for _, d := range b.deliveries {
		if d.Status == deliveryPending && !d.inflight && b.deliverable(d) && !d.NextAttempt.After(now) {
			d.inflight = true
			due = append(due, d)
		}
	}

	slices.SortFunc(due, func(a, c *Delivery) int { return int(a.Seq - c.Seq) })

	invoker, client := b.invoker, b.client
	b.mu.Unlock()

	for _, d := range due {
		snap := *d

		spawn(func() { b.attempt(ctx, client, invoker, d.ID, snap) })
	}
}

// Header names as Oracle documents them; set directly on the map so the exact casing reaches the wire.
const (
	hdrMessageID      = "X-OCI-NS-MessageId"
	hdrTopicOcid      = "X-OCI-NS-TopicOcid"
	hdrTopicName      = "X-OCI-NS-TopicName"
	hdrMessageType    = "X-OCI-NS-MessageType"
	hdrUnsubscribeURL = "X-OCI-NS-UnsubscribeURL"
	hdrConfirmURL     = "X-OCI-NS-ConfirmationURL"
	hdrSubscriptionID = "X-OCI-NS-SubscriptionId"
)

func (snap Delivery) headers() http.Header {
	ct := "text/plain; charset=UTF-8"
	if snap.MessageType == msgJSON {
		ct = "application/json"
	}

	h := http.Header{
		"Content-Type":    {ct},
		hdrTopicOcid:      {snap.TopicID},
		hdrTopicName:      {snap.TopicName},
		hdrUnsubscribeURL: {snap.UnsubscribeURL},
	}

	if snap.Kind == kindConfirmation {
		h[hdrMessageType] = []string{"SubscriptionConfirmation"}
		h[hdrConfirmURL] = []string{snap.ConfirmationURL}
		h[hdrSubscriptionID] = []string{snap.SubscriptionID}

		return h
	}

	h[hdrMessageType] = []string{"Notification"}
	h[hdrMessageID] = []string{snap.MessageID}

	return h
}

func send(ctx context.Context, client *http.Client, snap Delivery) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, snap.Endpoint, bytes.NewReader([]byte(snap.Body)))
	if err != nil {
		return 0, fmt.Errorf("build request: %w", err)
	}

	req.Header = snap.headers()

	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("post: %w", err)
	}

	defer resp.Body.Close()

	msg, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrBody))
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return resp.StatusCode, fmt.Errorf(
			"%w %d: %s",
			ErrEndpointStatus,
			resp.StatusCode,
			strings.TrimSpace(string(msg)),
		)
	}

	return resp.StatusCode, nil
}

func (b *InMemoryBackend) attempt(
	ctx context.Context, client *http.Client, invoker FunctionInvoker, id string, snap Delivery,
) {
	var (
		status int
		err    error
	)

	if snap.Protocol == protoFunctions {
		err = invoker(ctx, snap.Endpoint, snap.headers(), []byte(snap.Body))
	} else {
		status, err = send(ctx, client, snap)
	}

	b.mu.Lock("DeliveryResult")

	d, ok := b.deliveries[id]
	if !ok {
		b.mu.Unlock()

		return
	}

	d.inflight = false

	if ctx.Err() != nil {
		b.mu.Unlock()

		return
	}

	d.Attempts++
	d.LastStatus = status

	now := b.nowUTC()

	switch {
	case err == nil:
		d.Status, d.Completed, d.LastError = deliveryDelivered, now, ""
	case !now.Before(d.Deadline):
		d.Status, d.Completed, d.LastError = deliveryDiscarded, now, err.Error()
	default:
		d.LastError, d.NextAttempt = err.Error(), now.Add(backoff(d.Attempts))
	}

	b.mu.Unlock()
	b.Kick()
}

// backoff is the wait after a failed attempt: two immediate tries, then 1, 2, 4, 8, 16 and 32 minutes (capped).
func backoff(attempts int) time.Duration {
	if attempts < immediateTries {
		return 0
	}

	return min(time.Minute<<min(attempts-immediateTries, maxBackoffShift), maxBackoff)
}

// workerLoop dispatches deliveries as they come due until stop or ctx ends.
func (b *InMemoryBackend) workerLoop(ctx context.Context, stop <-chan struct{}, wg *sync.WaitGroup) {
	for {
		var timer <-chan time.Time

		if wait, after := b.NextDue(); wait >= 0 {
			timer = after(wait)
		}

		select {
		case <-b.kick:
		case <-timer:
		case <-stop:
			return
		case <-ctx.Done():
			return
		}

		b.DeliverDue(ctx, func(f func()) { wg.Go(f) })
	}
}

func (h *Handler) publishMessage(q *req) error {
	var in PublishIn
	if err := q.decode(&in); err != nil {
		return err
	}

	out, err := h.Backend.PublishMessage(q.param("topicId"), strings.ToUpper(q.r.Header.Get("Messagetype")), in)
	if err != nil {
		return err
	}

	return q.json(http.StatusOK, out, "")
}
