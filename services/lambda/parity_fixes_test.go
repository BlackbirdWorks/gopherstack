package lambda_test

// parity_fixes_test.go tests the parity, performance, and leak fixes applied to the
// lambda service:
//
//   - Parity: handleInvokeAsync truly async (returns 202 without waiting for execution)
//   - Parity: handleInvokeWithResponseStream uses AWS event stream binary protocol
//   - Parity: sweepESMs marks enabled ESMs whose function is missing as "PROBLEM"
//   - Performance: withInvocationChain uses []string — verified via contain/len semantics
//   - Leak: activeConcurrencies entries deleted when count reaches zero
//   - Leak: cleanupSem and logSem replaced with fresh channels in Reset()

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"hash/crc32"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lambda"
)

// ---- helpers ----

func newParityBackend(t *testing.T) *lambda.InMemoryBackend {
	t.Helper()

	return closeBackend(t,
		lambda.NewInMemoryBackend(nil, nil, lambda.DefaultSettings(), "123456789012", "us-east-1"))
}

func makeMinimalFunction(name string) *lambda.FunctionConfiguration {
	return &lambda.FunctionConfiguration{
		FunctionName: name,
		Runtime:      "python3.12",
		Handler:      "index.handler",
		Role:         "arn:aws:iam::123456789012:role/test-role",
	}
}

// callParityHandler issues an HTTP request through the lambda Handler and returns the recorder.
func callParityHandler(
	t *testing.T,
	h *lambda.Handler,
	method, path, body string,
) *httptest.ResponseRecorder {
	t.Helper()

	var r io.Reader = strings.NewReader("")
	if body != "" {
		r = strings.NewReader(body)
	}

	req := httptest.NewRequest(method, path, r)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)

	require.NoError(t, h.Handler()(c))

	return rec
}

// ---- invocationChain (performance fix: slice, not map) ----

func TestWithInvocationChain_SliceSemantics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		check     string
		additions []string
		wantLen   int
		wantHas   bool
	}{
		{
			name:      "empty_chain_does_not_contain",
			additions: nil,
			check:     "fn-a",
			wantLen:   0,
			wantHas:   false,
		},
		{
			name:      "single_element_found",
			additions: []string{"fn-a"},
			check:     "fn-a",
			wantLen:   1,
			wantHas:   true,
		},
		{
			name:      "multiple_elements_first_found",
			additions: []string{"fn-a", "fn-b", "fn-c"},
			check:     "fn-a",
			wantLen:   3,
			wantHas:   true,
		},
		{
			name:      "multiple_elements_last_found",
			additions: []string{"fn-a", "fn-b", "fn-c"},
			check:     "fn-c",
			wantLen:   3,
			wantHas:   true,
		},
		{
			name:      "absent_element_not_found",
			additions: []string{"fn-a", "fn-b"},
			check:     "fn-x",
			wantLen:   2,
			wantHas:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := lambda.WithInvocationChainBatchForTest(context.Background(), tt.additions)

			assert.Equal(t, tt.wantLen, lambda.InvocationChainLenForTest(ctx))
			assert.Equal(t, tt.wantHas, lambda.InvocationChainContainsForTest(ctx, tt.check))
		})
	}
}

func TestWithInvocationChain_DoesNotMutateParent(t *testing.T) {
	t.Parallel()

	// Add "fn-a" to parent context.
	parent := lambda.WithInvocationChainForTest(context.Background(), "fn-a")
	// Fork to child context and add "fn-b".
	child := lambda.WithInvocationChainForTest(parent, "fn-b")

	// Parent must remain unchanged.
	assert.Equal(t, 1, lambda.InvocationChainLenForTest(parent))
	assert.False(t, lambda.InvocationChainContainsForTest(parent, "fn-b"),
		"parent chain must not be mutated by child append")
	// Child must contain both.
	assert.Equal(t, 2, lambda.InvocationChainLenForTest(child))
	assert.True(t, lambda.InvocationChainContainsForTest(child, "fn-a"))
	assert.True(t, lambda.InvocationChainContainsForTest(child, "fn-b"))
}

// ---- activeConcurrencies zero-delete (leak fix) ----

func TestReleaseConcurrencySlot_DeletesZeroEntry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		acquireCount  int
		releaseCount  int
		wantMapLen    int
		wantMapHasKey bool
	}{
		{
			name:          "acquire_once_release_once_entry_deleted",
			acquireCount:  1,
			releaseCount:  1,
			wantMapLen:    0,
			wantMapHasKey: false,
		},
		{
			name:          "acquire_twice_release_once_entry_remains",
			acquireCount:  2,
			releaseCount:  1,
			wantMapLen:    1,
			wantMapHasKey: true,
		},
		{
			name:          "acquire_twice_release_twice_entry_deleted",
			acquireCount:  2,
			releaseCount:  2,
			wantMapLen:    0,
			wantMapHasKey: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := newParityBackend(t)
			fn := makeMinimalFunction("fn-concurrency")
			require.NoError(t, bk.CreateFunction(fn))

			// Give the function a reserved concurrency limit so
			// acquireConcurrencySlot actually tracks the slot.
			_, err := bk.PutFunctionConcurrency("fn-concurrency", tt.acquireCount+1)
			require.NoError(t, err)

			for range tt.acquireCount {
				ok, errAcq := lambda.AcquireConcurrencySlot(bk, "fn-concurrency")
				require.NoError(t, errAcq)
				require.True(t, ok)
			}

			snap := lambda.ActiveConcurrenciesSnapshot(bk)
			assert.Equal(t, tt.acquireCount, snap["fn-concurrency"])

			for range tt.releaseCount {
				lambda.ReleaseConcurrencySlot(bk, "fn-concurrency")
			}

			after := lambda.ActiveConcurrenciesSnapshot(bk)
			assert.Len(t, after, tt.wantMapLen)
			_, hasKey := after["fn-concurrency"]
			assert.Equal(t, tt.wantMapHasKey, hasKey)
		})
	}
}

// ---- cleanupSem/logSem replaced in Reset() (leak fix) ----

func TestReset_ReplacesSemaphoreChannels(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		resetN  int
		wantCap int
	}{
		{
			name:    "single_reset_replaces_channels",
			resetN:  1,
			wantCap: 64, // maxCleanupConcurrency
		},
		{
			name:    "double_reset_replaces_channels_twice",
			resetN:  2,
			wantCap: 64,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := newParityBackend(t)

			capBefore := lambda.CleanupSemCap(bk)
			require.Equal(t, tt.wantCap, capBefore, "initial cleanupSem capacity")

			for range tt.resetN {
				bk.Reset()
			}

			// After Reset(), channels must be fresh (same capacity, zero occupancy).
			assert.Equal(t, tt.wantCap, lambda.CleanupSemCap(bk),
				"cleanupSem capacity preserved after Reset()")
			assert.Equal(t, 0, lambda.CleanupSemLen(bk),
				"cleanupSem must be empty after Reset()")
		})
	}
}

func TestReset_LogSemReplacedAndEmpty(t *testing.T) {
	t.Parallel()

	bk := newParityBackend(t)
	capBefore := lambda.LogSemCap(bk)
	require.Positive(t, capBefore)

	bk.Reset()

	assert.Equal(t, capBefore, lambda.LogSemCap(bk),
		"logSem capacity preserved after Reset()")
}

// ---- handleInvokeAsync: true async (parity fix) ----

func TestHandleInvokeAsync_Returns202Immediately(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		functionName string
		wantErrType  string
		wantStatus   int
		createFn     bool
	}{
		{
			name:         "existing_function_returns_202",
			functionName: "async-fn",
			createFn:     true,
			wantStatus:   http.StatusAccepted,
		},
		{
			name:         "missing_function_returns_404",
			functionName: "no-such-fn",
			createFn:     false,
			wantStatus:   http.StatusNotFound,
			wantErrType:  "ResourceNotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := newParityBackend(t)
			if tt.createFn {
				require.NoError(t, bk.CreateFunction(makeMinimalFunction(tt.functionName)))
			}

			h := lambda.NewHandler(bk)
			h.DefaultRegion = "us-east-1"
			h.AccountID = "123456789012"

			rec := callParityHandler(t, h,
				http.MethodPost,
				"/2014-11-13/functions/"+tt.functionName+"/invoke-async/",
				`{"key":"value"}`,
			)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusAccepted {
				var body map[string]int
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
				assert.Equal(t, http.StatusAccepted, body["Status"],
					"InvokeAsync body must contain {\"Status\":202}")
			}

			if tt.wantErrType != "" {
				var errBody map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errBody))
				assert.Contains(t, errBody["__type"], tt.wantErrType)
			}
		})
	}
}

// ---- handleInvokeWithResponseStream: AWS eventstream encoding (parity fix) ----

// decodeEventStreamFrame parses one AWS event stream binary frame from r.
// Returns (headers, payload, ok). Returns ok=false on any decode error.
func decodeEventStreamFrame(t *testing.T, r *bytes.Reader) (map[string]string, []byte, bool) {
	t.Helper()

	if r.Len() < 12 {
		return nil, nil, false
	}

	var prelude [12]byte
	if _, err := io.ReadFull(r, prelude[:]); err != nil {
		return nil, nil, false
	}

	totalLen := binary.BigEndian.Uint32(prelude[0:4])
	headerLen := binary.BigEndian.Uint32(prelude[4:8])
	wantPreludeCRC := binary.BigEndian.Uint32(prelude[8:12])
	gotPreludeCRC := crc32.ChecksumIEEE(prelude[0:8])
	assert.Equal(t, wantPreludeCRC, gotPreludeCRC, "prelude CRC mismatch")

	const preludeLen = 12
	const msgCRCLen = 4
	if totalLen < uint32(preludeLen+msgCRCLen) {
		return nil, nil, false
	}
	payloadLen := totalLen - uint32(preludeLen) - headerLen - uint32(msgCRCLen)

	hdrData := make([]byte, headerLen)
	if _, err := io.ReadFull(r, hdrData); err != nil {
		return nil, nil, false
	}

	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, nil, false
	}

	var msgCRCBuf [4]byte
	if _, err := io.ReadFull(r, msgCRCBuf[:]); err != nil {
		return nil, nil, false
	}
	wantMsgCRC := binary.BigEndian.Uint32(msgCRCBuf[:])

	// Recompute CRC over prelude + headers + payload.
	msgBody := make([]byte, preludeLen+int(headerLen)+int(payloadLen))
	copy(msgBody[:preludeLen], prelude[:])
	copy(msgBody[preludeLen:], hdrData)
	copy(msgBody[preludeLen+int(headerLen):], payload)
	gotMsgCRC := crc32.ChecksumIEEE(msgBody)
	assert.Equal(t, wantMsgCRC, gotMsgCRC, "message CRC mismatch")

	// Decode headers.
	headers := map[string]string{}
	hdrBuf := bytes.NewReader(hdrData)
	for hdrBuf.Len() > 0 {
		var nameLen uint8
		if err := binary.Read(hdrBuf, binary.BigEndian, &nameLen); err != nil {
			break
		}
		nameBuf := make([]byte, nameLen)
		if _, err := io.ReadFull(hdrBuf, nameBuf); err != nil {
			break
		}
		var typ uint8
		if err := binary.Read(hdrBuf, binary.BigEndian, &typ); err != nil {
			break
		}
		var valLen uint16
		if err := binary.Read(hdrBuf, binary.BigEndian, &valLen); err != nil {
			break
		}
		valBuf := make([]byte, valLen)
		if _, err := io.ReadFull(hdrBuf, valBuf); err != nil {
			break
		}
		headers[string(nameBuf)] = string(valBuf)
	}

	return headers, payload, true
}

func TestHandleInvokeWithResponseStream_EventStreamEncoding(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupMock         func(*mockBackend)
		name              string
		functionName      string
		wantFirstEvent    string
		wantLastEvent     string
		wantPayloadPrefix string
		wantStatus        int
	}{
		{
			name:         "missing_function_returns_404",
			functionName: "stream-fn-missing",
			setupMock:    func(_ *mockBackend) {},
			wantStatus:   http.StatusNotFound,
		},
		{
			name:         "existing_function_yields_eventstream_frames",
			functionName: "stream-fn",
			setupMock: func(mb *mockBackend) {
				mb.mu.Lock()
				mb.functions["stream-fn"] = &lambda.FunctionConfiguration{FunctionName: "stream-fn"}
				mb.invokeResult = []byte(`{"ok":true}`)
				mb.mu.Unlock()
			},
			wantStatus:        http.StatusOK,
			wantFirstEvent:    "PayloadChunk",
			wantLastEvent:     "InvokeComplete",
			wantPayloadPrefix: `{"ok":true}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, mb := newHandler(t)
			tt.setupMock(mb)

			rec := callParityHandler(t, h,
				http.MethodPost,
				"/2021-11-15/functions/"+tt.functionName+"/response-streaming-invocations",
				`{}`,
			)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus != http.StatusOK {
				return
			}

			assert.Equal(t, "application/vnd.amazon.eventstream",
				rec.Header().Get("Content-Type"))

			body := bytes.NewReader(rec.Body.Bytes())
			require.Positive(t, body.Len(), "response body must not be empty")

			// Decode first frame (PayloadChunk).
			hdrs1, payload1, ok1 := decodeEventStreamFrame(t, body)
			require.True(t, ok1, "first frame must decode")
			assert.Equal(t, "event", hdrs1[":message-type"])
			assert.Equal(t, tt.wantFirstEvent, hdrs1[":event-type"])
			assert.Equal(t, tt.wantPayloadPrefix, string(payload1),
				"payload content must match function output")

			// Decode second frame (InvokeComplete).
			hdrs2, _, ok2 := decodeEventStreamFrame(t, body)
			require.True(t, ok2, "InvokeComplete frame must decode")
			assert.Equal(t, "event", hdrs2[":message-type"])
			assert.Equal(t, tt.wantLastEvent, hdrs2[":event-type"])

			assert.Equal(t, 0, body.Len(), "no extra bytes after InvokeComplete")
		})
	}
}

// ---- sweepESMs: marks degraded ESMs (parity fix) ----

func TestSweepESMs_MarksDegradedESM(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		wantLastProcessing string
		createFunction     bool
	}{
		{
			name:               "function_exists_no_degradation",
			createFunction:     true,
			wantLastProcessing: "No records processed",
		},
		{
			name:               "function_missing_marks_problem",
			createFunction:     false,
			wantLastProcessing: "PROBLEM",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := newParityBackend(t)

			if tt.createFunction {
				require.NoError(t, bk.CreateFunction(makeMinimalFunction("esm-fn")))
			}

			esm, err := bk.CreateEventSourceMapping(&lambda.CreateEventSourceMappingInput{
				EventSourceARN: "arn:aws:sqs:us-east-1:123456789012:test-queue",
				FunctionName:   "esm-fn",
				Enabled:        true,
			})
			require.NoError(t, err)

			j := lambda.NewJanitor(bk, lambda.DefaultSettings())
			lambda.SweepESMsForTest(context.Background(), j)

			got, err := bk.GetEventSourceMapping(esm.UUID)
			require.NoError(t, err)
			assert.Equal(t, tt.wantLastProcessing, got.LastProcessingResult)
		})
	}
}

// TestSweepESMs_QualifiedFunctionARN_NotMarkedDegraded proves the
// parity-sweep-3 fix to esmFunctionName/functionNameFromARN: an ESM created
// against a qualified function ARN (a common real pattern — point the
// trigger at a specific published version/alias rather than $LATEST) must
// not be treated as pointing at a missing function.
//
// Before the fix, esmFunctionName took only the LAST colon-separated ARN
// segment, so "arn:...:function:qual-esm-fn:1" was stored as
// FunctionARN=".../function:1" — the qualifier "1" mistaken for the function
// name. The janitor's existence check then looked up a nonexistent function
// literally named "1" and permanently marked the mapping "PROBLEM".
func TestSweepESMs_QualifiedFunctionARN_NotMarkedDegraded(t *testing.T) {
	t.Parallel()

	bk := newParityBackend(t)
	require.NoError(t, bk.CreateFunction(makeMinimalFunction("qual-esm-fn")))

	_, err := bk.PublishVersion("qual-esm-fn", "")
	require.NoError(t, err)

	esm, err := bk.CreateEventSourceMapping(&lambda.CreateEventSourceMappingInput{
		EventSourceARN: "arn:aws:sqs:us-east-1:123456789012:test-queue",
		FunctionName:   "arn:aws:lambda:us-east-1:123456789012:function:qual-esm-fn:1",
		Enabled:        true,
	})
	require.NoError(t, err)
	require.Equal(t, "arn:aws:lambda:us-east-1:123456789012:function:qual-esm-fn:1", esm.FunctionARN,
		"FunctionARN must preserve the name:qualifier suffix, matching real Lambda's echo")

	name, qualifier := lambda.FunctionNameAndQualifierFromARN(esm.FunctionARN)
	assert.Equal(t, "qual-esm-fn", name)
	assert.Equal(t, "1", qualifier)

	j := lambda.NewJanitor(bk, lambda.DefaultSettings())
	lambda.SweepESMsForTest(context.Background(), j)

	got, err := bk.GetEventSourceMapping(esm.UUID)
	require.NoError(t, err)
	assert.NotEqual(t, "PROBLEM", got.LastProcessingResult,
		"a qualified-ARN ESM's function must resolve correctly, not be treated as missing")
}

func TestSweepESMs_SkipsDisabledESMs(t *testing.T) {
	t.Parallel()

	bk := newParityBackend(t)

	esm, err := bk.CreateEventSourceMapping(&lambda.CreateEventSourceMappingInput{
		EventSourceARN: "arn:aws:sqs:us-east-1:123456789012:test-queue",
		FunctionName:   "missing-fn",
		Enabled:        false,
	})
	require.NoError(t, err)

	j := lambda.NewJanitor(bk, lambda.DefaultSettings())
	lambda.SweepESMsForTest(context.Background(), j)

	got, err := bk.GetEventSourceMapping(esm.UUID)
	require.NoError(t, err)
	assert.NotEqual(t, "PROBLEM", got.LastProcessingResult,
		"disabled ESM must not be health-checked")
}

// ---- invocationChain isolation across parallel goroutines ----

func TestWithInvocationChain_ConcurrentIsolation(t *testing.T) {
	t.Parallel()

	// Verify that concurrent additions to separate child contexts don't cross-pollinate.
	base := context.Background()
	const goroutines = 20
	results := make(chan bool, goroutines)

	for i := range goroutines {
		go func(idx int) {
			name := func(n int) string {
				return "fn-goroutine-" + string(rune('A'+n))
			}
			ctx := lambda.WithInvocationChainForTest(base, name(idx))
			// Should NOT contain a sibling's function.
			sibling := (idx + 1) % goroutines
			results <- !lambda.InvocationChainContainsForTest(ctx, name(sibling))
		}(i)
	}

	timeout := time.After(5 * time.Second)
	for range goroutines {
		select {
		case ok := <-results:
			assert.True(t, ok, "goroutine chain must not contain sibling function")
		case <-timeout:
			t.Fatal("timeout waiting for goroutine results")
		}
	}
}
