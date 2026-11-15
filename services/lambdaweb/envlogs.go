package lambdaweb

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/cwmetric"
	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const (
	logTimeLayout = "2006-01-02T15:04:05.000Z07:00"
	dropReason    = "Some logs were dropped because the log group could not accept them"
)

// levelOrder lists log levels in severity order.
func levelOrder() []string {
	return []string{"TRACE", levelDebug, levelInfo, levelWarn, levelError, "FATAL"}
}

func levelRank(level string) int {
	if level == "" {
		level = levelInfo
	}

	return slices.Index(levelOrder(), strings.ToUpper(level))
}

type appEvent struct {
	Message      any    `json:"message"`
	Timestamp    string `json:"timestamp"`
	Level        string `json:"level"`
	RequestID    string `json:"requestId,omitempty"`
	EndpointName string `json:"endpointName,omitempty"`
	Region       string `json:"region"`
}

type systemEvent struct {
	Record    any    `json:"record"`
	Timestamp string `json:"timestamp"`
	Level     string `json:"level"`
	Type      string `json:"type"`
	Region    string `json:"region"`
}

// shipper writes one environment's application and system log events to its log group and the .local companion.
type shipper struct {
	sink           LogSink
	now            func() time.Time
	live           func() (reqID, endpoint string)
	metrics        cwmetric.Emitter
	groups         [2]string
	region         string
	home           string
	stream         string
	appLevel       int
	sysLevel       int
	droppedRecords int
	droppedBytes   int
	mu             sync.Mutex
	ready          bool
}

func newShipper(
	sink LogSink, metrics cwmetric.Emitter, now func() time.Time, rev *Revision, serve, stream string,
) *shipper {
	group := logGroupOf(rev)
	lc := rev.ServiceConfig.Logging

	appLevel, sysLevel := levelRank(levelInfo), levelRank(levelInfo)
	if lc != nil {
		// Node.js has no FATAL console method, so FATAL filters like ERROR.
		appLevel = min(levelRank(lc.ApplicationLogLevel), levelRank(levelError))
		sysLevel = levelRank(lc.SystemLogLevel)
	}

	return &shipper{
		sink: sink, metrics: metrics, now: now, region: serve, home: rev.Region, stream: stream,
		appLevel: appLevel, sysLevel: sysLevel,
		groups: [2]string{group, group + ".local"},
	}
}

// groupRegion is where group i lives: the centralized group in the home Region, its .local companion where the
// environment runs.
func (s *shipper) groupRegion(i int) string {
	if i == 0 {
		return s.home
	}

	return s.region
}

func (s *shipper) ensure(ctx context.Context) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.ready {
		return true
	}

	for i, g := range s.groups {
		if err := s.sink.EnsureLogGroupAndStream(ctx, s.groupRegion(i), g, s.stream); err != nil {
			return false
		}
	}

	s.ready = true

	return true
}

func (s *shipper) deliver(ctx context.Context, line []byte) bool {
	ok := true

	for i, g := range s.groups {
		if err := s.sink.PutLogLines(ctx, s.groupRegion(i), g, s.stream, []string{string(line)}); err != nil {
			ok = false
		}
	}

	return ok
}

func (s *shipper) addDrop(bytes int) {
	s.mu.Lock()
	s.droppedRecords++
	s.droppedBytes += bytes
	s.mu.Unlock()
}

func (s *shipper) takeDrops() (int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	n, b := s.droppedRecords, s.droppedBytes
	s.droppedRecords, s.droppedBytes = 0, 0

	return n, b
}

// put delivers one event. Events the log group cannot take are counted, and the next delivered event is preceded
// by a platform.logsDropped warning carrying the counts since the last delivery.
func (s *shipper) put(line []byte) {
	if s == nil || s.sink == nil {
		return
	}

	ctx := context.Background()
	if !s.ensure(ctx) {
		s.addDrop(len(line))

		return
	}

	if n, bytes := s.takeDrops(); n > 0 {
		warn := s.systemLine(levelWarn, "platform.logsDropped", map[string]any{
			"reason":         dropReason,
			"droppedRecords": n,
			"droppedBytes":   bytes,
		})
		if warn != nil && !s.deliver(ctx, warn) {
			s.mu.Lock()
			s.droppedRecords += n
			s.droppedBytes += bytes
			s.mu.Unlock()
		}
	}

	if !s.deliver(ctx, line) {
		s.addDrop(len(line))
	}
}

func (s *shipper) stamp() string { return s.now().UTC().Format(logTimeLayout) }

// runtimeLine is a console line the runtime shim tagged with its level and request.
type runtimeLine struct {
	Message      any    `json:"message"`
	Level        string `json:"level"`
	RequestID    string `json:"requestId"`
	EndpointName string `json:"endpointName"`
	Marker       int    `json:"__lambdaweb"`
}

func parseRuntimeLine(line string) (runtimeLine, bool) {
	if !strings.HasPrefix(line, `{"__lambdaweb":`) {
		return runtimeLine{}, false
	}

	var rl runtimeLine
	if json.Unmarshal([]byte(line), &rl, jsonx.V1()) != nil || rl.Marker != 1 {
		return runtimeLine{}, false
	}

	return rl, true
}

// application ships one line of container output. A shim-tagged line keeps its console level and request; any
// other line maps stderr to ERROR and stdout to INFO.
func (s *shipper) application(stderr bool, line string) {
	if s == nil {
		return
	}

	if points, ok := parseEMF(line); ok {
		s.shipEMF(line, points)

		return
	}

	if rl, ok := parseRuntimeLine(line); ok {
		s.shipEvent(appEvent{
			Timestamp: s.stamp(), Level: rl.Level, Message: rl.Message, Region: s.region,
			RequestID: rl.RequestID, EndpointName: rl.EndpointName,
		})

		return
	}

	level := levelInfo
	if stderr {
		level = levelError
	}

	if levelRank(level) < s.appLevel {
		return
	}

	ev := appEvent{Timestamp: s.stamp(), Level: level, Message: parseMessage(line), Region: s.region}
	if s.live != nil {
		ev.RequestID, ev.EndpointName = s.live()
	}

	s.shipEvent(ev)
}

func (s *shipper) shipEvent(ev appEvent) {
	if levelRank(ev.Level) < s.appLevel {
		return
	}

	if b, err := json.Marshal(ev, jsonx.V1()); err == nil {
		s.put(b)
	}
}

// parseMessage keeps a JSON object line structured, as console methods given an object do.
func parseMessage(line string) any {
	trimmed := strings.TrimSpace(line)
	if strings.HasPrefix(trimmed, "{") {
		var obj map[string]any
		if json.Unmarshal([]byte(trimmed), &obj, jsonx.V1()) == nil {
			return obj
		}
	}

	return line
}

// system ships one platform.* event when its level passes systemLogLevel.
func (s *shipper) system(typ, level string, record any) {
	if s == nil || levelRank(level) < s.sysLevel {
		return
	}

	if b := s.systemLine(level, typ, record); b != nil {
		s.put(b)
	}
}

func (s *shipper) systemLine(level, typ string, record any) []byte {
	b, err := json.Marshal(
		systemEvent{Timestamp: s.stamp(), Level: level, Type: typ, Region: s.region, Record: record},
		jsonx.V1(),
	)
	if err != nil {
		return nil
	}

	return b
}

// lineSplitter turns a byte stream into lines.
type lineSplitter struct {
	emit    func(line string)
	partial []byte
}

func (w *lineSplitter) Write(p []byte) (int, error) {
	w.partial = append(w.partial, p...)

	for {
		i := bytes.IndexByte(w.partial, '\n')
		if i < 0 {
			break
		}

		if line := strings.TrimRight(string(w.partial[:i]), "\r"); line != "" {
			w.emit(line)
		}

		w.partial = w.partial[i+1:]
	}

	return len(p), nil
}

func (w *lineSplitter) flush() {
	if len(w.partial) > 0 {
		w.emit(string(w.partial))
		w.partial = nil
	}
}

func logGroupOf(rev *Revision) string {
	if lg := rev.ServiceConfig.Logging; lg != nil && lg.LogGroup != "" {
		return lg.LogGroup
	}

	return "/aws/lambda/web/" + rev.FunctionName
}

// shipEMF delivers an embedded-metric-format line as written (CloudWatch extracts from the unwrapped object) and
// publishes its metrics in the Region that served the request and in the home Region.
func (s *shipper) shipEMF(line string, points []cwmetric.Point) {
	s.put([]byte(line))

	if s.metrics == nil || s.sink == nil {
		return
	}

	for _, p := range points {
		p.Region = s.region
		_ = s.metrics.EmitMetric(p)

		if s.home != s.region {
			p.Region = s.home
			_ = s.metrics.EmitMetric(p)
		}
	}
}
