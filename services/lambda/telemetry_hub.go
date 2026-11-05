package lambda

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	deliveryInitialBackoff = 25 * time.Millisecond
	deliveryMaxBackoff     = time.Second
	deliveryBudget         = 10 * time.Second
	deliveryHTTPTimeout    = 5 * time.Second
	finalDeliveryBudget    = 750 * time.Millisecond
	hubCloseWait           = 3 * time.Second
	jsonObjectPrefix       = "{"
)

var (
	errSubscribedViaOtherAPI = errors.New("extension is already subscribed via the other telemetry API")
	errHubClosed             = errors.New("telemetry hub is closed")
	errDelivery              = errors.New("telemetry delivery failed")
)

// hubEvent is one platform/function/extension event before it is rendered for a subscriber.
type hubEvent struct {
	at       time.Time
	telRec   any
	logsRec  any
	category string
	telType  string
	logsType string
	line     string
	// logsMinSchema limits the Logs API rendering to subscribers at that schema version or newer.
	logsMinSchema string
	// initPhase events are replayed to subscribers that join after they fired.
	initPhase bool
}

func (e hubEvent) render(s *subscriber) (wireEvent, bool) {
	if !s.sub.types[e.category] {
		return wireEvent{}, false
	}

	switch e.category {
	case typeFunction, typeExtension:
		return wireEvent{Time: eventTime(e.at), Type: e.category, Record: s.lineRecord(e.line)}, true
	}

	if s.sub.api == apiLogs {
		if e.logsType == "" || (e.logsMinSchema != "" && s.sub.schema < e.logsMinSchema) {
			return wireEvent{}, false
		}

		return wireEvent{Time: eventTime(e.at), Type: e.logsType, Record: e.logsRec}, true
	}

	if e.telType == "" {
		return wireEvent{}, false
	}

	return wireEvent{Time: eventTime(e.at), Type: e.telType, Record: e.telRec}, true
}

// subscriber buffers and delivers events to one extension's listener.
type subscriber struct {
	hub      *telemetryHub
	in       chan []byte
	stop     chan struct{}
	flushReq chan chan struct{}
	done     chan struct{}
	extName  string
	target   string
	sub      subscription
	droppedN atomic.Int64
	droppedB atomic.Int64
	jsonLogs bool
	stopOnce sync.Once
	conn     net.Conn
}

// lineRecord renders a log line: a string, or the parsed object for JSON-format logs on schema 2022-12-13+.
func (s *subscriber) lineRecord(line string) any {
	if s.jsonLogs && s.sub.api == apiTelemetry && s.sub.schema >= schemaTelemetry20221213 &&
		strings.HasPrefix(strings.TrimSpace(line), jsonObjectPrefix) {
		var obj map[string]any
		if json.Unmarshal([]byte(line), &obj) == nil {
			return obj
		}
	}

	return line
}

// offer queues an event, counting it as dropped when the subscriber has fallen too far behind.
func (s *subscriber) offer(ev hubEvent) {
	we, ok := ev.render(s)
	if !ok {
		return
	}

	raw, err := json.Marshal(we)
	if err != nil {
		return
	}

	select {
	case s.in <- raw:
	default:
		s.droppedN.Add(1)
		s.droppedB.Add(int64(len(raw)))
	}
}

func (s *subscriber) droppedEvent() ([]byte, bool) {
	n, b := s.droppedN.Swap(0), s.droppedB.Swap(0)
	if n == 0 || !s.sub.types[typePlatform] {
		return nil, false
	}

	var we wireEvent

	if s.sub.api == apiLogs {
		we = wireEvent{Type: "platform.logsDropped", Record: recLogsDropped{
			Reason: logsDropReason, DroppedRecords: int(n), DroppedBytes: int(b),
		}}
	} else {
		we = wireEvent{Type: "platform.logsDropped", Record: recLogsDropped{
			Reason: dropReason, DroppedRecords: int(n), DroppedBytes: int(b),
		}}
	}

	we.Time = eventTime(time.Now())
	raw, _ := json.Marshal(we)

	return raw, true
}

func (s *subscriber) run() {
	defer close(s.done)

	var (
		batch  [][]byte
		size   int
		timer  *time.Timer
		timerC <-chan time.Time
	)

	flush := func(final bool) {
		if timer != nil {
			timer.Stop()
			timer, timerC = nil, nil
		}

		if raw, ok := s.droppedEvent(); ok {
			batch = append(batch, raw)
		}

		if len(batch) > 0 {
			s.deliver(batch, final)
		}

		batch, size = nil, 0
	}

	add := func(raw []byte) {
		batch = append(batch, raw)
		size += len(raw)

		if len(batch) == 1 {
			timer = time.NewTimer(time.Duration(s.sub.buf.timeoutMs) * time.Millisecond)
			timerC = timer.C
		}

		if len(batch) >= s.sub.buf.maxItems || size >= s.sub.buf.maxBytes {
			flush(false)
		}
	}

	drain := func() {
		for {
			select {
			case raw := <-s.in:
				add(raw)
			default:
				return
			}
		}
	}

	for {
		select {
		case raw := <-s.in:
			add(raw)
		case <-timerC:
			flush(false)
		case ack := <-s.flushReq:
			drain()
			flush(true)
			close(ack)
		case <-s.stop:
			drain()
			flush(true)
			s.closeConn()

			return
		}
	}
}

func (s *subscriber) closeConn() {
	if s.conn != nil {
		_ = s.conn.Close()
		s.conn = nil
	}
}

// deliver sends one batch, retrying with backoff; a final delivery gets one short budget.
func (s *subscriber) deliver(batch [][]byte, final bool) {
	budget := deliveryBudget
	if final {
		budget = finalDeliveryBudget
	}

	deadline := time.Now().Add(budget)
	backoff := deliveryInitialBackoff

	for {
		err := s.send(batch)
		if err == nil {
			return
		}

		if !time.Now().Add(backoff).Before(deadline) {
			s.droppedN.Add(int64(len(batch)))

			for _, b := range batch {
				s.droppedB.Add(int64(len(b)))
			}

			return
		}

		t := time.NewTimer(backoff)

		select {
		case <-t.C:
		case <-s.stop:
			t.Stop()

			if !final {
				deadline = time.Now().Add(finalDeliveryBudget)
			}
		}

		backoff = min(backoff*2, deliveryMaxBackoff)
	}
}

func (s *subscriber) send(batch [][]byte) error {
	if s.hub.sink != nil {
		return s.hub.sink(batch)
	}

	if s.sub.dest.protocol == protoTCP {
		return s.sendTCP(batch)
	}

	body := make([]byte, 0, 2+len(batch))
	body = append(body, '[')

	for i, b := range batch {
		if i > 0 {
			body = append(body, ',')
		}

		body = append(body, b...)
	}

	body = append(body, ']')

	ctx, cancel := context.WithTimeout(context.Background(), deliveryHTTPTimeout)
	defer cancel()

	u := "http://" + s.target + s.sub.dest.path

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("%w: %w", errDelivery, err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := s.hub.client.Do(req)
	if err != nil {
		return fmt.Errorf("%w: %w", errDelivery, err)
	}

	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("%w: status %d", errDelivery, resp.StatusCode)
	}

	return nil
}

func (s *subscriber) sendTCP(batch [][]byte) error {
	if s.conn == nil {
		c, err := (&net.Dialer{Timeout: deliveryHTTPTimeout}).DialContext(context.Background(), "tcp", s.target)
		if err != nil {
			return fmt.Errorf("%w: %w", errDelivery, err)
		}

		s.conn = c
	}

	var buf bytes.Buffer

	for _, b := range batch {
		buf.Write(b)
		buf.WriteByte('\n')
	}

	_ = s.conn.SetWriteDeadline(time.Now().Add(deliveryHTTPTimeout))

	if _, err := s.conn.Write(buf.Bytes()); err != nil {
		s.closeConn()

		return fmt.Errorf("%w: %w", errDelivery, err)
	}

	return nil
}

func (s *subscriber) shutdown() { s.stopOnce.Do(func() { close(s.stop) }) }

// telemetryHub fans platform events and log lines out to the extensions subscribed to them.
type telemetryHub struct {
	mu        *lockmetrics.RWMutex
	client    *http.Client
	subs      map[string]*subscriber
	ph        *phaseTracker
	history   []hubEvent
	sink      func(batch [][]byte) error
	startLogs func()
	jsonLogs  bool
	logsOn    bool
	closed    bool
}

func newTelemetryHub() *telemetryHub {
	return &telemetryHub{
		mu:     lockmetrics.New("lambda.telemetry"),
		client: httpclient.New(deliveryHTTPTimeout),
		subs:   make(map[string]*subscriber),
		ph:     newPhaseTracker(),
	}
}

func (h *telemetryHub) setJSONLogs(v bool) {
	h.mu.Lock("setJSONLogs")
	h.jsonLogs = v
	h.mu.Unlock()
}

// setLogSource registers the function that starts following the container's output; it runs once a
// subscriber wants function or extension lines.
func (h *telemetryHub) setLogSource(start func()) {
	h.mu.Lock("setLogSource")

	h.startLogs = start
	run := h.logsOn

	h.mu.Unlock()

	if run && start != nil {
		start()
	}
}

// hasPlatformSubscribers reports whether anyone receives platform events.
func (h *telemetryHub) hasPlatformSubscribers() bool {
	h.mu.RLock("hasPlatformSubscribers")
	defer h.mu.RUnlock()

	for _, s := range h.subs {
		if s.sub.types[typePlatform] {
			return true
		}
	}

	return false
}

func (h *telemetryHub) publish(ev hubEvent) {
	if ev.at.IsZero() {
		ev.at = time.Now()
	}

	h.mu.Lock("publish")
	defer h.mu.Unlock()

	if h.closed {
		return
	}

	if ev.initPhase {
		h.history = append(h.history, ev)
	}

	for _, s := range h.subs {
		s.offer(ev)
	}
}

// subscribe registers the extension's subscription (idempotent per extension and API).
func (h *telemetryHub) subscribe(extID, extName, containerHost string, sub subscription) error {
	h.mu.Lock("subscribe")

	if h.closed {
		h.mu.Unlock()

		return errHubClosed
	}

	if old, ok := h.subs[extID]; ok {
		h.mu.Unlock()

		if old.sub.api != sub.api {
			return errSubscribedViaOtherAPI
		}

		return nil
	}

	s := &subscriber{
		hub: h, sub: sub, extName: extName, jsonLogs: h.jsonLogs,
		target:   sub.dest.target(containerHost),
		in:       make(chan []byte, sub.buf.maxItems),
		stop:     make(chan struct{}),
		flushReq: make(chan chan struct{}),
		done:     make(chan struct{}),
	}
	h.subs[extID] = s

	for _, ev := range h.history {
		s.offer(ev)
	}

	s.offer(subscriptionEvent(extName, sub))

	wantLines := sub.types[typeFunction] || sub.types[typeExtension]
	start := h.startLogs

	if wantLines {
		h.logsOn = true
	}

	h.mu.Unlock()

	go s.run()

	if wantLines && start != nil {
		start()
	}

	return nil
}

func subscriptionEvent(name string, sub subscription) hubEvent {
	rec := recSubscription{Name: name, State: "Subscribed", Types: sub.typeList()}

	return hubEvent{
		at: time.Now(), category: typePlatform,
		telType: "platform.telemetrySubscription", telRec: rec,
		logsType: "platform.logsSubscription", logsRec: rec,
	}
}

// flush delivers everything buffered so far, waiting for the deliveries (bounded by ctx).
func (h *telemetryHub) flush(ctx context.Context) {
	h.mu.RLock("flush")

	subs := make([]*subscriber, 0, len(h.subs))
	for _, s := range h.subs {
		subs = append(subs, s)
	}

	h.mu.RUnlock()

	for _, s := range subs {
		ack := make(chan struct{})

		select {
		case s.flushReq <- ack:
		case <-s.done:
			continue
		case <-ctx.Done():
			return
		}

		select {
		case <-ack:
		case <-ctx.Done():
			return
		}
	}
}

// close stops all subscribers after a last delivery attempt.
func (h *telemetryHub) close(ctx context.Context) {
	h.mu.Lock("close")

	if h.closed {
		h.mu.Unlock()

		return
	}

	h.closed = true

	subs := make([]*subscriber, 0, len(h.subs))
	for _, s := range h.subs {
		subs = append(subs, s)
	}

	h.mu.Unlock()

	for _, s := range subs {
		s.shutdown()
	}

	t := time.NewTimer(hubCloseWait)
	defer t.Stop()

	for _, s := range subs {
		select {
		case <-s.done:
		case <-t.C:
			h.client.CloseIdleConnections()
			h.mu.Close()

			return
		case <-ctx.Done():
			h.mu.Close()

			return
		}
	}

	h.client.CloseIdleConnections()
	h.mu.Close()
}
