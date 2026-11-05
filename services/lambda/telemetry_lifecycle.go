package lambda

import (
	"context"
	"encoding/json"
	"math"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	statusSuccess = "success"
	statusFailure = "failure"
	statusError   = "error"
	statusTimeout = "timeout"

	initTypeOnDemand = "on-demand"
	phaseInit        = "init"

	traceType        = "X-Amzn-Trace-Id"
	spanIDLen        = 16
	stateReady       = "Ready"
	spanResponseLat  = "responseLatency"
	spanResponseDur  = "responseDuration"
	memProbeTimeout  = 2 * time.Second
	logsMinRuntimeEv = schemaLogs20210318
)

// hubIdentity is the function identity stamped into platform events.
type hubIdentity struct {
	functionName string
	version      string
	instanceID   string
	memoryMB     int
}

type invocationTrack struct {
	started   time.Time
	finished  time.Time
	tracing   *telTracing
	status    string
	errorType string
}

// phaseTracker records init and per-invocation timing so platform events carry real durations.
type phaseTracker struct {
	mu          *lockmetrics.RWMutex
	invs        map[string]*invocationTrack
	reportQueue []string
	memProbe    func(ctx context.Context) (int64, bool)
	id          hubIdentity
	initAt      time.Time
	initMs      *float64
	initDone    bool
	coldStart   bool
}

func newPhaseTracker() *phaseTracker {
	return &phaseTracker{
		mu:   lockmetrics.New("lambda.telemetry.phase"),
		invs: make(map[string]*invocationTrack),
		id:   hubIdentity{instanceID: uuid.NewString()},
	}
}

func (t *phaseTracker) setIdentity(id hubIdentity) {
	t.mu.Lock("setIdentity")

	if id.instanceID == "" {
		id.instanceID = t.id.instanceID
	}

	t.id = id
	t.mu.Unlock()
}

// setMemoryProbe installs the reader behind maxMemoryUsedMB.
func (t *phaseTracker) setMemoryProbe(p func(ctx context.Context) (int64, bool)) {
	t.mu.Lock("setMemoryProbe")
	t.memProbe = p
	t.mu.Unlock()
}

// initStart records the start of the Init phase.
func (h *telemetryHub) initStart(now time.Time) {
	h.ph.mu.Lock("initStart")

	h.ph.initAt = now
	id := h.ph.id

	h.ph.mu.Unlock()

	rec := recInitStart{
		InitializationType: initTypeOnDemand, Phase: phaseInit,
		FunctionName: id.functionName, FunctionVersion: id.version,
		InstanceID: id.instanceID, InstanceMaxMemory: id.memoryMB,
	}

	h.publish(hubEvent{
		at: now, category: typePlatform, initPhase: true,
		telType: "platform.initStart", telRec: rec,
	})
}

// initFinished ends the Init phase once, on the runtime's first Next or an init error.
func (h *telemetryHub) initFinished(now time.Time, errType string) {
	h.ph.mu.Lock("initFinished")

	if h.ph.initDone || h.ph.initAt.IsZero() {
		h.ph.mu.Unlock()

		return
	}

	h.ph.initDone = true
	h.ph.coldStart = true
	dur := now.Sub(h.ph.initAt)
	ms := roundMs(dur)

	if errType == "" {
		h.ph.initMs = &ms
	}

	h.ph.mu.Unlock()

	status := statusSuccess
	if errType != "" {
		status = statusFailure
	}

	h.publish(hubEvent{
		at: now, category: typePlatform, initPhase: true,
		telType: "platform.initRuntimeDone",
		telRec: recInitRuntimeDone{
			InitializationType: initTypeOnDemand, Phase: phaseInit, Status: status, ErrorType: errType,
		},
	})
	h.publish(hubEvent{
		at: now, category: typePlatform, initPhase: true,
		telType: "platform.initReport",
		telRec: recInitReport{
			InitializationType: initTypeOnDemand, Status: status, Phase: phaseInit, ErrorType: errType,
			Metrics: recInitMetrics{DurationMs: ms},
		},
	})
}

func newTracing(value string) *telTracing {
	if value == "" {
		return nil
	}

	return &telTracing{
		SpanID: strings.ReplaceAll(uuid.NewString(), "-", "")[:spanIDLen], Type: traceType, Value: value,
	}
}

// invokeStarted records the hand-off of an invocation to the runtime.
func (h *telemetryHub) invokeStarted(reqID, trace string, now time.Time) {
	tr := &invocationTrack{started: now, tracing: newTracing(trace)}

	h.ph.mu.Lock("invokeStarted")
	h.ph.invs[reqID] = tr
	version := h.ph.id.version
	h.ph.mu.Unlock()

	h.publish(hubEvent{
		at: now, category: typePlatform,
		telType: "platform.start", telRec: recStart{RequestID: reqID, Version: version, Tracing: tr.tracing},
		logsType: "platform.start", logsRec: recRequestID{RequestID: reqID},
	})
}

// functionErrorType reads errorType from a Runtime API error body or header value.
func functionErrorType(header string, body []byte) string {
	if header != "" {
		return header
	}

	var e struct {
		ErrorType string `json:"errorType"`
	}

	if json.Unmarshal(body, &e) == nil {
		return e.ErrorType
	}

	return ""
}

// runtimeDone publishes platform.runtimeDone for a response or error the runtime posted.
func (h *telemetryHub) runtimeDone(reqID string, isErr bool, errType string, produced int, readStart, readEnd time.Time) {
	status := statusSuccess
	if isErr {
		status = statusError
	}

	h.finishRuntime(reqID, status, errType, produced, readStart, readEnd)
}

func (h *telemetryHub) finishRuntime(
	reqID, status, errType string, produced int, readStart, readEnd time.Time,
) {
	h.ph.mu.Lock("runtimeDone")

	tr, ok := h.ph.invs[reqID]
	if !ok {
		h.ph.mu.Unlock()

		return
	}

	tr.finished, tr.status, tr.errorType = readEnd, status, errType
	h.ph.reportQueue = append(h.ph.reportQueue, reqID)
	started, tracing := tr.started, tr.tracing

	h.ph.mu.Unlock()

	rec := recRuntimeDone{
		RequestID: reqID, Status: status, ErrorType: errType, Tracing: tracing,
		Metrics: &recRuntimeDoneMetrics{DurationMs: roundMs(readEnd.Sub(started)), ProducedBytes: produced},
		Spans: []telSpan{
			{Name: spanResponseLat, Start: eventTime(started), DurationMs: roundMs(readStart.Sub(started))},
			{Name: spanResponseDur, Start: eventTime(readStart), DurationMs: roundMs(readEnd.Sub(readStart))},
		},
	}

	logsStatus := status
	if status == statusError {
		logsStatus = statusFailure
	}

	h.publish(hubEvent{
		at: readEnd, category: typePlatform,
		telType: "platform.runtimeDone", telRec: rec,
		logsType: "platform.runtimeDone", logsRec: recLogsRuntimeDone{RequestID: reqID, Status: logsStatus},
		logsMinSchema: logsMinRuntimeEv,
	})
	h.publish(hubEvent{
		at: readEnd, category: typePlatform,
		logsType: "platform.end", logsRec: recRequestID{RequestID: reqID},
	})
}

// timedOut publishes runtimeDone and report for an invocation that exceeded its deadline.
func (h *telemetryHub) timedOut(ctx context.Context, reqID string, now time.Time) {
	h.finishRuntime(reqID, statusTimeout, "", 0, now, now)
	h.invokePhaseEnded(ctx, now)
}

// invokePhaseEnded publishes platform.report for invocations whose runtime and extensions are done.
func (h *telemetryHub) invokePhaseEnded(ctx context.Context, now time.Time) {
	h.ph.mu.Lock("invokePhaseEnded")

	queue := h.ph.reportQueue
	h.ph.reportQueue = nil

	probe := h.ph.memProbe
	memMB := h.ph.id.memoryMB

	type item struct {
		tr   invocationTrack
		id   string
		cold *float64
	}

	items := make([]item, 0, len(queue))

	for _, id := range queue {
		tr, ok := h.ph.invs[id]
		if !ok {
			continue
		}

		it := item{tr: *tr, id: id}
		if h.ph.coldStart {
			it.cold, h.ph.coldStart = h.ph.initMs, false
		}

		items = append(items, it)

		delete(h.ph.invs, id)
	}

	h.ph.mu.Unlock()

	if len(items) == 0 {
		return
	}

	var usedMB int

	if probe != nil && h.hasPlatformSubscribers() {
		pctx, cancel := context.WithTimeout(ctx, memProbeTimeout)
		if b, ok := probe(pctx); ok {
			usedMB = int(math.Ceil(float64(b) / bytesPerMB))
		}

		cancel()
	}

	for _, it := range items {
		dur := it.tr.finished.Sub(it.tr.started)
		metrics := recReportMetrics{
			DurationMs: roundMs(dur), BilledDuration: int64(math.Ceil(float64(dur) / float64(time.Millisecond))),
			MemorySizeMB: memMB, MaxMemoryUsedMB: usedMB, InitDurationMs: it.cold,
		}
		rec := recReport{
			RequestID: it.id, Status: it.tr.status, ErrorType: it.tr.errorType,
			Metrics: metrics, Tracing: it.tr.tracing,
		}

		h.publish(hubEvent{
			at: now, category: typePlatform,
			telType: "platform.report", telRec: rec,
			logsType: "platform.report", logsRec: logsReport{RequestID: it.id, Metrics: metrics},
		})
	}
}

// logsReport is the Logs API platform.report record, which has no status or spans.
type logsReport struct {
	Metrics   recReportMetrics `json:"metrics"`
	RequestID string           `json:"requestId"`
}

// extensionReady publishes platform.extension when an extension finished its init.
func (h *telemetryHub) extensionReady(name string, events []string) {
	if events == nil {
		events = []string{}
	}

	rec := recExtension{Name: name, State: stateReady, Events: events}

	h.publish(hubEvent{
		category: typePlatform, initPhase: true,
		telType: "platform.extension", telRec: rec,
		logsType: "platform.extension", logsRec: rec,
	})
}

// logLine publishes one function or extension output line.
func (h *telemetryHub) logLine(category, line string) {
	h.publish(hubEvent{category: category, line: line})
}
