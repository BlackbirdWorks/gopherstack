package lambdaweb

import (
	"bytes"
	"context"
	"encoding/json"
	"slices"
	"strings"
	"sync"
	"time"
)

const logTimeLayout = "2006-01-02T15:04:05.000Z07:00"

// Log levels in severity order.
var levelOrder = []string{"TRACE", "DEBUG", "INFO", "WARN", "ERROR", "FATAL"}

func levelRank(level string) int {
	if level == "" {
		level = defaultLogLevel
	}

	return slices.Index(levelOrder, strings.ToUpper(level))
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
	sink     LogSink
	now      func() time.Time
	live     func() (reqID, endpoint string)
	region   string
	stream   string
	appLevel int
	sysLevel int
	groups   [2]string
	mu       sync.Mutex
	ready    bool
}

func newShipper(sink LogSink, now func() time.Time, rev *Revision, stream string) *shipper {
	group := logGroupOf(rev)
	lc := rev.ServiceConfig.Logging

	appLevel, sysLevel := levelRank(defaultLogLevel), levelRank(defaultLogLevel)
	if lc != nil {
		// Node.js has no FATAL console method, so FATAL filters like ERROR.
		appLevel = min(levelRank(lc.ApplicationLogLevel), levelRank("ERROR"))
		sysLevel = levelRank(lc.SystemLogLevel)
	}

	return &shipper{
		sink: sink, now: now, region: rev.Region, stream: stream, appLevel: appLevel, sysLevel: sysLevel,
		groups: [2]string{group, group + ".local"},
	}
}

func (s *shipper) ensure(ctx context.Context) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.ready {
		return true
	}

	for _, g := range s.groups {
		if err := s.sink.EnsureLogGroupAndStream(ctx, s.region, g, s.stream); err != nil {
			return false
		}
	}

	s.ready = true

	return true
}

func (s *shipper) put(line []byte) {
	if s == nil || s.sink == nil {
		return
	}

	ctx := context.Background()
	if !s.ensure(ctx) {
		return
	}

	for _, g := range s.groups {
		_ = s.sink.PutLogLines(ctx, s.region, g, s.stream, []string{string(line)})
	}
}

func (s *shipper) stamp() string { return s.now().UTC().Format(logTimeLayout) }

// application ships one line of container output; stderr maps to ERROR and stdout to INFO.
func (s *shipper) application(stderr bool, line string) {
	if s == nil {
		return
	}

	level := "INFO"
	if stderr {
		level = "ERROR"
	}

	if levelRank(level) < s.appLevel {
		return
	}

	ev := appEvent{Timestamp: s.stamp(), Level: level, Message: parseMessage(line), Region: s.region}
	if s.live != nil {
		ev.RequestID, ev.EndpointName = s.live()
	}

	if b, err := json.Marshal(ev); err == nil {
		s.put(b)
	}
}

// parseMessage keeps a JSON object line structured, as console methods given an object do.
func parseMessage(line string) any {
	trimmed := strings.TrimSpace(line)
	if strings.HasPrefix(trimmed, "{") {
		var obj map[string]any
		if json.Unmarshal([]byte(trimmed), &obj) == nil {
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

	b, err := json.Marshal(systemEvent{Timestamp: s.stamp(), Level: level, Type: typ, Region: s.region, Record: record})
	if err == nil {
		s.put(b)
	}
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
