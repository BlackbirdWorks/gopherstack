package lambda

import (
	"bufio"
	"context"
	"encoding/json/v2"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

func sub(t *testing.T, h *extHarness, api, id, body string) (int, map[string]string) {
	t.Helper()

	path := telemetryPath
	if api == apiLogs {
		path = logsPath
	}

	hdr := map[string]string{}
	if id != "" {
		hdr[headerExtensionID] = id
	}

	status, _, raw := h.do(t, http.MethodPut, path, hdr, body)

	var out map[string]string

	_ = json.Unmarshal([]byte(raw), &out, jsonx.V1())

	if status == http.StatusOK {
		assert.Equal(t, `"OK"`, raw)
	}

	return status, out
}

func withDest(schema, typ, dest string) string {
	return `{"schemaVersion":"` + schema + `","types":["` + typ + `"],"destination":` + dest + `}`
}

func withBuffering(buf string) string {
	return `{"schemaVersion":"2022-07-01","types":["platform"],"buffering":` + buf + `,"destination":` + goodDest + `}`
}

const noPortDest = `{"protocol":"HTTP","URI":"http://sandbox.localdomain"}`

const goodDest = `{"protocol":"HTTP","URI":"http://sandbox.localdomain:8080/x"}`

func TestTelemetrySubscribeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		api        string
		body       string
		wantType   string
		wantMsg    string
		wantStatus int
	}{
		{
			"telemetry ok", apiTelemetry,
			`{"schemaVersion":"2022-07-01","types":["platform"],"destination":` + goodDest + `}`,
			"", "", http.StatusOK,
		},
		{
			"telemetry all buffering", apiTelemetry,
			`{"schemaVersion":"2025-01-29","types":["platform","function","extension"],` +
				`"buffering":{"maxItems":10000,"maxBytes":1048576,"timeoutMs":25},"destination":` + goodDest + `}`,
			"", "", http.StatusOK,
		},
		{
			"telemetry tcp", apiTelemetry,
			withDest("2022-12-13", "function", `{"protocol":"TCP","URI":"tcp://sandbox.localdomain:7000"}`),
			"", "", http.StatusOK,
		},
		{
			"missing schema", apiTelemetry, `{"types":["platform"],"destination":` + goodDest + `}`,
			"ValidationError", "schemaVersion is required", http.StatusBadRequest,
		},
		{
			"bad schema", apiTelemetry, `{"schemaVersion":"1999","types":["platform"],"destination":` + goodDest + `}`,
			"ValidationError", "unsupported schemaVersion", http.StatusBadRequest,
		},
		{
			"empty types", apiTelemetry, `{"schemaVersion":"2022-07-01","types":[],"destination":` + goodDest + `}`,
			"ValidationError", "types should not be empty", http.StatusBadRequest,
		},
		{
			"bad type", apiTelemetry, `{"schemaVersion":"2022-07-01","types":["logs"],"destination":` + goodDest + `}`,
			"ValidationError", `unsupported type "logs"`, http.StatusBadRequest,
		},
		{
			"no port", apiTelemetry,
			`{"schemaVersion":"2022-07-01","types":[],"destination":` + noPortDest + `}`,
			"ValidationError", "URI port is not provided; types should not be empty", http.StatusBadRequest,
		},
		{
			"https", apiTelemetry,
			withDest("2022-07-01", "platform", `{"protocol":"HTTP","URI":"https://sandbox.localdomain:8080"}`),
			"ValidationError", "scheme must be http", http.StatusBadRequest,
		},
		{
			"reserved port", apiTelemetry,
			withDest("2022-07-01", "platform", `{"protocol":"HTTP","URI":"http://sandbox.localdomain:9001"}`),
			"ValidationError", "9001 is reserved", http.StatusBadRequest,
		},
		{
			"remote host", apiTelemetry,
			withDest("2022-07-01", "platform", `{"protocol":"HTTP","URI":"http://example.com:80"}`),
			"ValidationError", "inside the execution environment", http.StatusBadRequest,
		},
		{
			"bad protocol", apiTelemetry,
			withDest("2022-07-01", "platform", `{"protocol":"UDP","URI":"udp://sandbox.localdomain:80"}`),
			"ValidationError", "must be HTTP or TCP", http.StatusBadRequest,
		},
		{
			"no destination", apiTelemetry, `{"schemaVersion":"2022-07-01","types":["platform"]}`,
			"ValidationError", "destination is required", http.StatusBadRequest,
		},
		{
			"maxItems low", apiTelemetry,
			withBuffering(`{"maxItems":999}`),
			"ValidationError", "buffering.maxItems must be between 1000 and 10000", http.StatusBadRequest,
		},
		{
			"maxBytes high", apiTelemetry,
			withBuffering(`{"maxBytes":1048577}`),
			"ValidationError", "buffering.maxBytes must be between 262144 and 1048576", http.StatusBadRequest,
		},
		{
			"timeout low", apiTelemetry,
			withBuffering(`{"timeoutMs":24}`),
			"ValidationError", "buffering.timeoutMs must be between 25 and 30000", http.StatusBadRequest,
		},
		{
			"timeout high", apiTelemetry,
			withBuffering(`{"timeoutMs":30001}`),
			"ValidationError", "buffering.timeoutMs must be between", http.StatusBadRequest,
		},
		{"malformed", apiTelemetry, `{`, "ValidationError", "malformed", http.StatusBadRequest},
		{
			"logs ok default schema", apiLogs, `{"types":["platform","function"],"destination":` + goodDest + `}`,
			"", "", http.StatusOK,
		},
		{
			"logs 2021 schema", apiLogs,
			`{"schemaVersion":"2021-03-18","types":["platform"],"destination":` + goodDest + `}`, "", "", http.StatusOK,
		},
		{
			"logs telemetry schema", apiLogs,
			`{"schemaVersion":"2022-07-01","types":["platform"],"destination":` + goodDest + `}`,
			"Logs.ValidationError", "unsupported schemaVersion", http.StatusBadRequest,
		},
		{
			"logs no port", apiLogs,
			`{"types":[],"destination":{"protocol":"HTTP","URI":"http://sandbox.localdomain"}}`,
			"Logs.ValidationError", "URI port is not provided; types should not be empty", http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newExtHarness(t)
			id := h.register(t, "tel-ext", "INVOKE")

			status, out := sub(t, h, tt.api, id, tt.body)
			assert.Equal(t, tt.wantStatus, status)

			if tt.wantStatus != http.StatusOK {
				assert.Equal(t, tt.wantType, out["errorType"])
				assert.Contains(t, out["errorMessage"], tt.wantMsg)
			}
		})
	}
}

func TestTelemetrySubscribeIdentity(t *testing.T) {
	t.Parallel()

	body := `{"schemaVersion":"2022-07-01","types":["platform"],"destination":` + goodDest + `}`

	tests := []struct {
		run        func(t *testing.T, h *extHarness) (int, map[string]string)
		name       string
		wantType   string
		wantStatus int
	}{
		{func(t *testing.T, h *extHarness) (int, map[string]string) {
			t.Helper()

			return sub(t, h, apiTelemetry, "", body)
		}, "missing id", "ValidationError", http.StatusBadRequest},
		{func(t *testing.T, h *extHarness) (int, map[string]string) {
			t.Helper()

			return sub(t, h, apiTelemetry, "nope", body)
		}, "unknown id", "Extension.InvalidExtensionID", http.StatusForbidden},
		{func(t *testing.T, h *extHarness) (int, map[string]string) {
			t.Helper()

			id := h.register(t, "e")
			h.srv.exts.awaitInit(t.Context())

			return sub(t, h, apiTelemetry, id, body)
		}, "after init", "Extension.RegistrationClosed", http.StatusForbidden},
		{func(t *testing.T, h *extHarness) (int, map[string]string) {
			t.Helper()

			id := h.register(t, "e")
			first, _ := sub(t, h, apiTelemetry, id, body)
			require.Equal(t, http.StatusOK, first)

			return sub(t, h, apiLogs, id, `{"types":["platform"],"destination":`+goodDest+`}`)
		}, "other api", "Logs.ValidationError", http.StatusBadRequest},
		{func(t *testing.T, h *extHarness) (int, map[string]string) {
			t.Helper()

			id := h.register(t, "e")
			first, _ := sub(t, h, apiTelemetry, id, body)
			require.Equal(t, http.StatusOK, first)

			return sub(t, h, apiTelemetry, id, body)
		}, "idempotent", "", http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newExtHarness(t)
			status, out := tt.run(t, h)
			assert.Equal(t, tt.wantStatus, status)
			assert.Equal(t, tt.wantType, out["errorType"])
		})
	}

	t.Run("method", func(t *testing.T) {
		t.Parallel()

		h := newExtHarness(t)
		status, _, _ := h.do(t, http.MethodPost, telemetryPath, nil, "")
		assert.Equal(t, http.StatusMethodNotAllowed, status)
	})
}

func TestInternalExtensionShutdownRejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		extName    string
		external   []string
		events     []string
		wantStatus int
	}{
		{
			"internal with shutdown",
			"internal",
			[]string{"ext-bin"},
			[]string{"INVOKE", "SHUTDOWN"},
			http.StatusBadRequest,
		},
		{"internal invoke only", "internal", []string{"ext-bin"}, []string{"INVOKE"}, http.StatusOK},
		{"external with shutdown", "ext-bin", []string{"ext-bin"}, []string{"INVOKE", "SHUTDOWN"}, http.StatusOK},
		{"no external files", "anything", nil, []string{"SHUTDOWN"}, http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newExtHarness(t)
			h.srv.exts.setExternal(tt.external)

			b, _ := json.Marshal(map[string]any{"events": tt.events}, jsonx.V1())
			status, _, body := h.do(t, http.MethodPost, "/2020-01-01/extension/register",
				map[string]string{headerExtensionName: tt.extName}, string(b))
			assert.Equal(t, tt.wantStatus, status)

			if tt.wantStatus != http.StatusOK {
				assert.Contains(t, body, "Extension.InvalidEventType")
			}
		})
	}
}

func TestTelemetryBatching(t *testing.T) {
	t.Parallel()

	line := func(n int) hubEvent { return hubEvent{category: typeFunction, line: strings.Repeat("x", n)} }

	tests := []struct {
		name        string
		buffering   string
		publish     []hubEvent
		wantBatches []int
		wait        time.Duration
	}{
		{"timeout flush", `{"timeoutMs":100}`, []hubEvent{line(1), line(1), line(1)}, []int{3}, 100 * time.Millisecond},
		{"before timeout", `{"timeoutMs":100}`, []hubEvent{line(1)}, nil, 99 * time.Millisecond},
		{"max items", `{"maxItems":1000,"timeoutMs":30000}`, repeat(line(1), 1000), []int{1000}, 0},
		{"below max items", `{"maxItems":1000,"timeoutMs":30000}`, repeat(line(1), 999), nil, 0},
		{"max bytes", `{"maxBytes":262144,"timeoutMs":30000}`, repeat(line(10000), 27), []int{27}, 0},
		{"below max bytes", `{"maxBytes":262144,"timeoutMs":30000}`, repeat(line(10000), 20), nil, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				var (
					mu      sync.Mutex
					batches []int
				)

				hub := newTelemetryHub()
				hub.sink = func(b [][]byte) error {
					mu.Lock()
					batches = append(batches, len(b))
					mu.Unlock()

					return nil
				}

				req := `{"schemaVersion":"2022-07-01","types":["function"],"buffering":` + tt.buffering +
					`,"destination":` + goodDest + `}`
				var parsed subscribeRequest

				require.NoError(t, json.Unmarshal([]byte(req), &parsed, jsonx.V1()))

				s, err := validateSubscription(apiTelemetry, &parsed)
				require.NoError(t, err)
				require.NoError(t, hub.subscribe("e1", "ext", "", s))

				synctest.Wait()

				for _, ev := range tt.publish {
					hub.publish(ev)
				}

				synctest.Wait()
				time.Sleep(tt.wait)
				synctest.Wait()

				mu.Lock()
				got := append([]int(nil), batches...)
				mu.Unlock()

				assert.Equal(t, tt.wantBatches, got)

				hub.close(context.Background())
			})
		})
	}
}

func repeat(ev hubEvent, n int) []hubEvent {
	out := make([]hubEvent, n)
	for i := range out {
		out[i] = ev
	}

	return out
}

func TestTelemetryEventSchemas(t *testing.T) {
	t.Parallel()

	tr := &telTracing{
		SpanID: "54565fb41ac79632", Type: "X-Amzn-Trace-Id",
		Value: "Root=1-62e900b2-710d76f009d6e7785905449a;Parent=0efbd19962d95b05;Sampled=1",
	}
	initMs := 397.68
	metrics := recReportMetrics{
		BilledDuration: 694, DurationMs: 693.92, InitDurationMs: &initMs, MaxMemoryUsedMB: 84, MemorySizeMB: 128,
	}

	tests := []struct {
		rec  any
		name string
		typ  string
		want string
	}{
		{recInitStart{
			InitializationType: "on-demand", Phase: "init", FunctionName: "myFunction", FunctionVersion: "$LATEST",
			InstanceID: "82561ce0-53dd-47d1-90e0-c8f5e063e62e", InstanceMaxMemory: 256,
		}, "initStart", "platform.initStart", `{"initializationType":"on-demand","phase":"init",
			"functionName":"myFunction","functionVersion":"$LATEST",
			"instanceId":"82561ce0-53dd-47d1-90e0-c8f5e063e62e","instanceMaxMemory":256}`},
		{recInitRuntimeDone{
			InitializationType: "on-demand", Status: "success",
			Spans: []telSpan{{Name: "someTimeSpan", Start: "2022-06-02T12:02:33.913Z", DurationMs: 70.5}},
		}, "initRuntimeDone", "platform.initRuntimeDone", `{"initializationType":"on-demand","status":"success",
			"spans":[{"name":"someTimeSpan","start":"2022-06-02T12:02:33.913Z","durationMs":70.5}]}`},
		{recInitReport{
			InitializationType: "on-demand", Status: "success", Phase: "init",
			Metrics: recInitMetrics{DurationMs: 125.33},
			Spans:   []telSpan{{Name: "someTimeSpan", Start: "2022-06-02T12:02:33.913Z", DurationMs: 90.1}},
		}, "initReport", "platform.initReport", `{"initializationType":"on-demand","status":"success","phase":"init",
			"metrics":{"durationMs":125.33},
			"spans":[{"name":"someTimeSpan","start":"2022-06-02T12:02:33.913Z","durationMs":90.1}]}`},
		{
			recStart{RequestID: "6d68ca91-49c9-448d-89b8-7ca3e6dc66aa", Version: "$LATEST", Tracing: tr},
			"start", "platform.start", `{"requestId":"6d68ca91-49c9-448d-89b8-7ca3e6dc66aa","version":"$LATEST",
			"tracing":{"spanId":"54565fb41ac79632","type":"X-Amzn-Trace-Id",
			"value":"Root=1-62e900b2-710d76f009d6e7785905449a;Parent=0efbd19962d95b05;Sampled=1"}}`,
		},
		{recRuntimeDone{
			RequestID: "6d68ca91-49c9-448d-89b8-7ca3e6dc66aa", Status: "success", Tracing: tr,
			Spans:   []telSpan{{Name: "someTimeSpan", Start: "2022-08-02T12:01:23.521Z", DurationMs: 80}},
			Metrics: &recRuntimeDoneMetrics{DurationMs: 140, ProducedBytes: 16},
		}, "runtimeDone", "platform.runtimeDone", `{"requestId":"6d68ca91-49c9-448d-89b8-7ca3e6dc66aa",
			"status":"success","tracing":{"spanId":"54565fb41ac79632","type":"X-Amzn-Trace-Id",
			"value":"Root=1-62e900b2-710d76f009d6e7785905449a;Parent=0efbd19962d95b05;Sampled=1"},
			"spans":[{"name":"someTimeSpan","start":"2022-08-02T12:01:23.521Z","durationMs":80}],
			"metrics":{"durationMs":140,"producedBytes":16}}`},
		{
			recReport{RequestID: "6d68ca91-49c9-448d-89b8-7ca3e6dc66aa", Status: "success", Metrics: metrics},
			"report", "platform.report", `{"metrics":{"billedDurationMs":694,"durationMs":693.92,
			"initDurationMs":397.68,"maxMemoryUsedMB":84,"memorySizeMB":128},
			"requestId":"6d68ca91-49c9-448d-89b8-7ca3e6dc66aa","status":"success"}`,
		},
		{
			recExtension{Name: "my-telemetry-extension", State: "Ready", Events: []string{"INVOKE", "SHUTDOWN"}},
			"extension", "platform.extension",
			`{"events":["INVOKE","SHUTDOWN"],"name":"my-telemetry-extension","state":"Ready"}`,
		},
		{
			recSubscription{
				Name:  "my-telemetry-extension",
				State: "Subscribed",
				Types: []string{"platform", "function"},
			},
			"telemetrySubscription", "platform.telemetrySubscription",
			`{"name":"my-telemetry-extension","state":"Subscribed","types":["platform","function"]}`,
		},
		{
			recLogsDropped{
				Reason:         dropReason,
				DroppedRecords: 123,
				DroppedBytes:   12345,
			},
			"logsDropped", "platform.logsDropped",
			`{"droppedBytes":12345,"droppedRecords":123,
			"reason":"` + dropReason + `"}`,
		},
		{
			recRequestID{RequestID: "6f7f0961f83442118a7af6fe80b88d56"},
			"logs start", "platform.start",
			`{"requestId":"6f7f0961f83442118a7af6fe80b88d56"}`,
		},
		{
			recLogsRuntimeDone{RequestID: "6f7f0961f83442118a7af6fe80b88", Status: "failure"},
			"logs runtimeDone",
			"platform.runtimeDone", `{"requestId":"6f7f0961f83442118a7af6fe80b88","status":"failure"}`,
		},
		{logsReport{RequestID: "6f7f0961f83442118a7af6fe80b88d56", Metrics: recReportMetrics{
			DurationMs: 101.51, BilledDuration: 300, MemorySizeMB: 512, MaxMemoryUsedMB: 33,
			InitDurationMs: new(116.67),
		}}, "logs report", "platform.report", `{"requestId":"6f7f0961f83442118a7af6fe80b88d56",
			"metrics":{"durationMs":101.51,"billedDurationMs":300,"memorySizeMB":512,"maxMemoryUsedMB":33,
			"initDurationMs":116.67}}`},
		{"[INFO] Hello world, I am a function!", "function text", "function", `"[INFO] Hello world, I am a function!"`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			at := time.Date(2022, 10, 12, 0, 0, 15, 64*int(time.Millisecond), time.UTC)
			raw, err := json.Marshal(wireEvent{Time: eventTime(at), Type: tt.typ, Record: tt.rec}, jsonx.V1())
			require.NoError(t, err)

			want := fmt.Sprintf(`{"time":"2022-10-12T00:00:15.064Z","type":%q,"record":%s}`, tt.typ, tt.want)
			assert.JSONEq(t, want, string(raw))
		})
	}
}

func TestTelemetryLogRecords(t *testing.T) {
	t.Parallel()

	jsonLine := `{"timestamp":"2022-10-12T00:03:50.000Z","level":"INFO","requestId":"79b4f56e","message":"Hello"}`

	tests := []struct {
		name     string
		api      string
		schema   string
		line     string
		jsonLogs bool
		wantJSON bool
	}{
		{"text format", apiTelemetry, schemaTelemetry20221213, jsonLine, false, false},
		{"json new schema", apiTelemetry, schemaTelemetry20221213, jsonLine, true, true},
		{"json 2025 schema", apiTelemetry, schemaTelemetry20250129, jsonLine, true, true},
		{"json old schema", apiTelemetry, schemaTelemetry20220701, jsonLine, true, false},
		{"json mode plain line", apiTelemetry, schemaTelemetry20221213, "hello", true, false},
		{"logs api always string", apiLogs, schemaLogs20200815, jsonLine, true, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := &subscriber{jsonLogs: tt.jsonLogs, sub: subscription{api: tt.api, schema: tt.schema}}
			rec := s.lineRecord(tt.line)

			if tt.wantJSON {
				assert.IsType(t, map[string]any{}, rec)

				return
			}

			assert.Equal(t, tt.line, rec)
		})
	}
}

// listener collects what the emulator delivers to an extension's local listener.
type listener struct {
	addr   string
	events []wireEvent
	mu     sync.Mutex
}

func (l *listener) add(evs []wireEvent) {
	l.mu.Lock()
	l.events = append(l.events, evs...)
	l.mu.Unlock()
}

func (l *listener) types() []string {
	l.mu.Lock()
	defer l.mu.Unlock()

	out := make([]string, 0, len(l.events))
	for _, e := range l.events {
		out = append(out, e.Type)
	}

	return out
}

func (l *listener) find(typ string) (wireEvent, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, e := range l.events {
		if e.Type == typ {
			return e, true
		}
	}

	return wireEvent{}, false
}

func newHTTPListener(t *testing.T) *listener {
	t.Helper()

	l := &listener{}
	ts := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		var evs []wireEvent

		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &evs, jsonx.V1())
		l.add(evs)
	}))
	t.Cleanup(ts.Close)

	l.addr = strings.TrimPrefix(ts.URL, "http://")

	return l
}

func newTCPListener(t *testing.T) *listener {
	t.Helper()

	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })

	l := &listener{addr: ln.Addr().String()}

	go func() {
		for {
			c, acceptErr := ln.Accept()
			if acceptErr != nil {
				return
			}

			go func() {
				defer c.Close()

				sc := bufio.NewScanner(c)
				for sc.Scan() {
					var e wireEvent
					if json.Unmarshal(sc.Bytes(), &e, jsonx.V1()) == nil {
						l.add([]wireEvent{e})
					}
				}
			}()
		}
	}()

	return l
}

func TestTelemetryDeliveryLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		api       string
		proto     string
		schema    string
		types     string
		wantOrder []string
		jsonLogs  bool
	}{
		{
			"telemetry http", apiTelemetry, protoHTTP, schemaTelemetry20221213, `["platform","function","extension"]`,
			[]string{
				"platform.initStart", "platform.telemetrySubscription", "platform.extension",
				"platform.initRuntimeDone", "platform.initReport", "platform.start", "function", "extension",
				"platform.runtimeDone", "platform.report",
			},
			true,
		},
		{
			"telemetry tcp", apiTelemetry, protoTCP, schemaTelemetry20220701, `["platform","function"]`,
			[]string{
				"platform.initStart", "platform.telemetrySubscription", "platform.extension",
				"platform.initRuntimeDone", "platform.initReport", "platform.start", "function",
				"platform.runtimeDone", "platform.report",
			},
			false,
		},
		{
			"logs http", apiLogs, protoHTTP, schemaLogs20210318, `["platform","function","extension"]`,
			[]string{
				"platform.logsSubscription", "platform.extension", "platform.start", "function", "extension",
				"platform.runtimeDone", "platform.end", "platform.report",
			},
			false,
		},
		{
			"logs http old schema", apiLogs, protoHTTP, schemaLogs20200815, `["platform"]`,
			[]string{
				"platform.logsSubscription",
				"platform.extension",
				"platform.start",
				"platform.end",
				"platform.report",
			},
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newExtHarness(t)
			lis := newHTTPListener(t)
			uri := "http://sandbox.localdomain:" + lis.addr[strings.LastIndex(lis.addr, ":")+1:] + "/telemetry"

			if tt.proto == protoTCP {
				lis = newTCPListener(t)
				uri = "tcp://sandbox.localdomain:" + lis.addr[strings.LastIndex(lis.addr, ":")+1:]
			}

			h.srv.tel.ph.setIdentity(hubIdentity{functionName: "ext-fn", version: "$LATEST", memoryMB: 128})
			h.srv.tel.setJSONLogs(tt.jsonLogs)
			h.srv.tel.initStart(time.Now())
			h.srv.exts.setExternal([]string{"tel-ext"})

			id := h.register(t, "tel-ext", "INVOKE")
			body := fmt.Sprintf(`{"schemaVersion":%q,"types":%s,"buffering":{"timeoutMs":25},`+
				`"destination":{"protocol":%q,"URI":%q}}`, tt.schema, tt.types, tt.proto, uri)

			status, out := sub(t, h, tt.api, id, body)
			require.Equal(t, http.StatusOK, status, out)

			firstNext := h.nextAsync(t, id)

			ctx := t.Context()
			done := make(chan struct{})

			go func() {
				defer close(done)

				_, _, _, _ = h.srv.invoke(ctx, []byte(`{}`), "", time.Minute)
			}()

			_, hdr, _, err := h.doRaw(ctx, http.MethodGet, "/2018-06-01/runtime/invocation/next", nil, "")
			require.NoError(t, err)

			reqID := hdr.Get("Lambda-Runtime-Aws-Request-Id")
			require.NotEmpty(t, reqID)
			<-firstNext

			h.srv.tel.logLine(typeFunction, `{"timestamp":"t","level":"INFO","message":"hi"}`)
			h.srv.tel.logLine(typeExtension, "ext says hi")

			status, _, _ = h.do(t, http.MethodPost, "/2018-06-01/runtime/invocation/"+reqID+"/response", nil, `"ok"`)
			require.Equal(t, http.StatusAccepted, status)
			<-done

			// the extension and the runtime both ask for more: the Invoke phase ends
			_ = h.nextAsync(t, id)

			go func() {
				_, _, _, _ = h.doRaw(ctx, http.MethodGet, "/2018-06-01/runtime/invocation/next", nil, "")
			}()

			require.Eventually(t, func() bool {
				_, ok := lis.find("platform.report")

				return ok
			}, 10*time.Second, 10*time.Millisecond, "got %v", lis.types())

			assert.Equal(t, tt.wantOrder, lis.types())

			if rd, ok := lis.find("platform.runtimeDone"); ok && tt.api == apiTelemetry {
				raw, _ := json.Marshal(rd.Record, jsonx.V1())
				assert.Contains(t, string(raw), `"status":"success"`)
				assert.Contains(t, string(raw), `"producedBytes":4`)
			}

			if fn, ok := lis.find("function"); ok && tt.jsonLogs && tt.api == apiTelemetry {
				assert.IsType(t, map[string]any{}, fn.Record)
			}

			if rep, ok := lis.find("platform.report"); ok {
				raw, _ := json.Marshal(rep.Record, jsonx.V1())
				assert.Contains(t, string(raw), `"memorySizeMB":128`)
				assert.Contains(t, string(raw), `"billedDurationMs"`)
				assert.Contains(t, string(raw), `"initDurationMs"`)
			}
		})
	}
}

func TestTelemetryTimeoutReport(t *testing.T) {
	t.Parallel()

	h := newExtHarness(t)
	lis := newHTTPListener(t)
	h.srv.tel.initStart(time.Now())

	id := h.register(t, "tel-ext", "INVOKE")
	port := lis.addr[strings.LastIndex(lis.addr, ":")+1:]
	status, out := sub(t, h, apiTelemetry, id, `{"schemaVersion":"2022-07-01","types":["platform"],`+
		`"buffering":{"timeoutMs":25},"destination":{"protocol":"HTTP","URI":"http://sandbox.localdomain:`+port+`"}}`)
	require.Equal(t, http.StatusOK, status, out)

	_ = h.nextAsync(t, id)

	go func() {
		_, _, _, _ = h.doRaw(t.Context(), http.MethodGet, "/2018-06-01/runtime/invocation/next", nil, "")
	}()

	_, _, _, err := h.srv.invoke(t.Context(), []byte(`{}`), "", 150*time.Millisecond)
	require.ErrorIs(t, err, ErrInvocationTimeout)

	require.Eventually(t, func() bool {
		_, ok := lis.find("platform.report")

		return ok
	}, 10*time.Second, 10*time.Millisecond, "got %v", lis.types())

	rd, ok := lis.find("platform.runtimeDone")
	require.True(t, ok)

	raw, _ := json.Marshal(rd.Record, jsonx.V1())
	assert.Contains(t, string(raw), `"status":"timeout"`)
}

func TestLineSplitter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		write []string
		want  []string
	}{
		{"whole lines", []string{"a\nb\n"}, []string{"function:a", "function:b"}},
		{"split across writes", []string{"he", "llo\nwor", "ld\n"}, []string{"function:hello", "function:world"}},
		{"crlf", []string{"a\r\n"}, []string{"function:a"}},
		{
			"extension marker",
			[]string{extensionLogMarker + "from ext\nfrom fn\n"},
			[]string{"extension:from ext", "function:from fn"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var got []string

			hub := newTelemetryHub()
			hub.sink = func([][]byte) error { return nil }

			// capture through a catch-all subscriber
			var parsed subscribeRequest

			require.NoError(t, json.Unmarshal([]byte(`{"schemaVersion":"2022-07-01","types":["function","extension"],`+
				`"buffering":{"timeoutMs":25},"destination":`+goodDest+`}`), &parsed, jsonx.V1()))

			s, err := validateSubscription(apiTelemetry, &parsed)
			require.NoError(t, err)

			var mu sync.Mutex

			hub.sink = func(b [][]byte) error {
				mu.Lock()
				defer mu.Unlock()

				for _, raw := range b {
					var e wireEvent
					_ = json.Unmarshal(raw, &e, jsonx.V1())

					if r, ok := e.Record.(string); ok && (e.Type == typeFunction || e.Type == typeExtension) {
						got = append(got, e.Type+":"+r)
					}
				}

				return nil
			}

			require.NoError(t, hub.subscribe("x", "ext", "", s))

			ls := &lineSplitter{hub: hub}
			for _, w := range tt.write {
				_, _ = ls.Write([]byte(w))
			}

			hub.flush(t.Context())

			mu.Lock()
			defer mu.Unlock()

			assert.Equal(t, tt.want, got)

			hub.close(context.Background())
		})
	}
}

func TestExtensionReadyPublishedBeforeInitGate(t *testing.T) {
	t.Parallel()

	reg := newExtensionRegistry()
	reg.hub = newTelemetryHub()
	t.Cleanup(reg.close)

	_, id, status, rerr := reg.register("tel-ext", []string{extEventInvoke}, false)
	require.Nil(t, rerr)
	require.Equal(t, http.StatusOK, status)

	reg.setExternal([]string{"tel-ext"})

	reg.hub.mu.Lock("test")

	go func() { _, _ = reg.next(t.Context(), id) }()

	select {
	case <-reg.ready:
		t.Error("init gate opened before platform.extension was published")
	case <-time.After(200 * time.Millisecond):
	}

	reg.hub.mu.Unlock()
	<-reg.ready
}
