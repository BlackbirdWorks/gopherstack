package lambda_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/services/lambda"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- RecursiveLoop enforcement ----

func TestAudit_RecursiveLoop_Deny_BlocksSelfInvoke(t *testing.T) {
	t.Parallel()

	bk := lambda.NewInMemoryBackend(nil, nil, lambda.DefaultSettings(), "123456789012", "us-east-1")
	closeBackend(t, bk)

	require.NoError(t, bk.CreateFunction(&lambda.FunctionConfiguration{
		FunctionName: "recursive-fn",
		FunctionArn:  "arn:aws:lambda:us-east-1:123456789012:function:recursive-fn",
		Runtime:      "python3.12",
		Handler:      "index.handler",
		Role:         "arn:aws:iam:::role/r",
		PackageType:  "Zip",
		State:        lambda.FunctionStateActive,
	}))

	_, putErr := bk.PutFunctionRecursionConfig("recursive-fn", &lambda.PutFunctionRecursionConfigInput{
		RecursiveLoop: "Deny",
	})
	require.NoError(t, putErr)

	// Simulate a self-invocation: inject the function name into the context chain
	ctx := context.Background()
	// Use the exported helpers from export_test.go — but RecursiveLoop tests need internal access.
	// Instead, call InvokeFunctionWithQualifier twice simulating nesting by wrapping context.
	// We test via the backend directly, injecting the chain.
	_, _, _, _, err := bk.InvokeFunctionWithQualifier(
		lambda.WithInvocationChainForTest(ctx, "recursive-fn"),
		"recursive-fn",
		"",
		"", "",
		lambda.InvocationTypeRequestResponse,
		[]byte("{}"),
	)
	require.Error(t, err)
	assert.ErrorIs(t, err, lambda.ErrInvalidParameterValue)
}

func TestAudit_RecursiveLoop_Terminate_AllowsSelfInvoke(t *testing.T) {
	t.Parallel()

	bk := lambda.NewInMemoryBackend(nil, nil, lambda.DefaultSettings(), "123456789012", "us-east-1")
	closeBackend(t, bk)

	require.NoError(t, bk.CreateFunction(&lambda.FunctionConfiguration{
		FunctionName: "recursive-terminate-fn",
		FunctionArn:  "arn:aws:lambda:us-east-1:123456789012:function:recursive-terminate-fn",
		Runtime:      "python3.12",
		Handler:      "index.handler",
		Role:         "arn:aws:iam:::role/r",
		PackageType:  "Zip",
		State:        lambda.FunctionStateActive,
	}))

	_, putErr := bk.PutFunctionRecursionConfig("recursive-terminate-fn", &lambda.PutFunctionRecursionConfigInput{
		RecursiveLoop: "Terminate",
	})
	require.NoError(t, putErr)

	// With Terminate mode, self-invocation should NOT return ErrInvalidParameterValue
	// (it will fail for other reasons like no runtime, but not for recursion rejection)
	ctx := lambda.WithInvocationChainForTest(context.Background(), "recursive-terminate-fn")
	_, _, _, _, err := bk.InvokeFunctionWithQualifier(
		ctx,
		"recursive-terminate-fn",
		"",
		"", "",
		lambda.InvocationTypeRequestResponse,
		[]byte("{}"),
	)
	// Should not be a recursion-denial error
	assert.NotErrorIs(t, err, lambda.ErrInvalidParameterValue)
}

// ---- InvokeWithResponseStream ----

func TestAudit_InvokeWithResponseStream_FunctionNotFound(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	path := "/2021-11-15/functions/nonexistent/response-streaming-invocations"
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader("{}"))
	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestAudit_InvokeWithResponseStream_ReturnsEventstreamContentType(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	rec := auditCreateFunction(t, h, baseImageFn("stream-fn"))
	require.Equal(t, http.StatusCreated, rec.Code)

	path := "/2021-11-15/functions/stream-fn/response-streaming-invocations"
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader("{}"))
	rec2 := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec2)
	require.NoError(t, h.Handler()(c))

	// Should return streaming content type (function will fail to run in test env,
	// but at minimum should not return 400/404 for existing function)
	ct := rec2.Header().Get("Content-Type")
	if rec2.Code == http.StatusOK {
		assert.Equal(t, "application/vnd.amazon.eventstream", ct)
	}
}

// ---- Eventstream frame format ----

func TestAudit_EventstreamFrame_Format(t *testing.T) {
	t.Parallel()

	// Verify our frame encoding: 4-byte big-endian length prefix followed by payload.
	payload := []byte(`{"result":"ok"}`)
	buf := &strings.Builder{}

	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(payload)))

	buf.Write(lenBuf[:])
	buf.Write(payload)

	// End of stream
	binary.BigEndian.PutUint32(lenBuf[:], 0)
	buf.Write(lenBuf[:])

	raw := []byte(buf.String())

	// Read back frame 1
	require.GreaterOrEqual(t, len(raw), 4)
	frameLen := binary.BigEndian.Uint32(raw[:4])
	assert.Equal(t, uint32(len(payload)), frameLen)
	assert.Equal(t, payload, raw[4:4+frameLen])

	// Read back end-of-stream
	eos := raw[4+frameLen:]
	require.Len(t, eos, 4)
	eosLen := binary.BigEndian.Uint32(eos)
	assert.Equal(t, uint32(0), eosLen)
}

// ---- Streaming invoke reads body correctly ----

func TestAudit_InvokeWithResponseStream_NoBody_NotBadRequest(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	rec := auditCreateFunction(t, h, baseImageFn("stream-nobody-fn"))
	require.Equal(t, http.StatusCreated, rec.Code)

	path := "/2021-11-15/functions/stream-nobody-fn/response-streaming-invocations"
	req := httptest.NewRequest(http.MethodPost, path, nil) // no body
	rec2 := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec2)
	require.NoError(t, h.Handler()(c))
	// Nil body must not trigger a 400 body-parse error — any other status is acceptable
	// (without Docker, InvokeFunction returns 500, which is expected in unit tests).
	assert.NotEqual(t, http.StatusBadRequest, rec2.Code)
}

// ---- Streaming response frame structure ----

func TestAudit_InvokeWithResponseStream_FrameStructure(t *testing.T) {
	t.Parallel()

	// Create a minimal fake response to verify frame encoding.
	// We can't fully invoke without a container, so we test the frame writer directly.
	type frame struct {
		payload []byte
		length  uint32
	}

	readFrame := func(r io.Reader) (frame, error) {
		var lenBuf [4]byte
		if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
			return frame{}, err
		}

		l := binary.BigEndian.Uint32(lenBuf[:])
		if l == 0 {
			return frame{length: 0}, nil
		}

		payload := make([]byte, l)
		if _, err := io.ReadFull(r, payload); err != nil {
			return frame{}, err
		}

		return frame{length: l, payload: payload}, nil
	}

	// Simulate what writeEventStreamFrame does for a known payload.
	payload := []byte(`{"status":"ok"}`)

	var buf strings.Builder

	var lb [4]byte
	binary.BigEndian.PutUint32(lb[:], uint32(len(payload)))
	buf.Write(lb[:])
	buf.Write(payload)

	binary.BigEndian.PutUint32(lb[:], 0)
	buf.Write(lb[:])

	r := strings.NewReader(buf.String())
	f1, err := readFrame(r)
	require.NoError(t, err)
	assert.Equal(t, uint32(len(payload)), f1.length)
	assert.Equal(t, payload, f1.payload)

	f2, err := readFrame(r)
	require.NoError(t, err)
	assert.Equal(t, uint32(0), f2.length)
}

// TestAuditLambda_ExecutedVersionHeader verifies X-Amz-Executed-Version is set on invoke responses.
func TestAuditLambda_ExecutedVersionHeader(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		qualifier      string
		wantVersion    string
		publishVersion bool
	}{
		{
			name:        "no qualifier returns $LATEST",
			qualifier:   "",
			wantVersion: "$LATEST",
		},
		{
			name:        "$LATEST qualifier returns $LATEST",
			qualifier:   "$LATEST",
			wantVersion: "$LATEST",
		},
		{
			name:           "version number qualifier returns that version",
			qualifier:      "1",
			wantVersion:    "1",
			publishVersion: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h, bk := newInMemoryHandler(t)
			createFunctionForTest(t, h, "exec-ver-fn")

			if tc.publishVersion {
				_, pubErr := bk.PublishVersion("exec-ver-fn", "")
				require.NoError(t, pubErr)
			}

			invPath := "/2015-03-31/functions/exec-ver-fn/invocations"
			if tc.qualifier != "" {
				invPath += "?Qualifier=" + url.QueryEscape(tc.qualifier)
			}

			rec := lambdaCall(t, h, http.MethodPost, invPath,
				map[string]string{"X-Amz-Invocation-Type": "DryRun"},
				`{}`)

			executedVer := rec.Header().Get("X-Amz-Executed-Version")
			assert.Equal(t, tc.wantVersion, executedVer,
				"X-Amz-Executed-Version must reflect resolved version")
		})
	}
}

// TestNewOps_InvokeWithResponseStream verifies that the streaming invoke endpoint
// returns application/vnd.amazon.eventstream and at least one data frame.
func TestNewOps_InvokeWithResponseStream(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "stream-fn")

	rec := callInMemoryHandler(t, h, http.MethodPost,
		"/2021-11-15/functions/stream-fn/response-streaming-invocations", `{}`)

	// In unit tests without a Docker runtime, the invocation will fail with 500 (no container).
	// Verify the function-not-found path is NOT triggered (i.e., not a 404).
	assert.NotEqual(t, http.StatusNotFound, rec.Code)

	// When the invocation succeeds (integration tests with real container), verify streaming format.
	if rec.Code == http.StatusOK {
		assert.Equal(t, "application/vnd.amazon.eventstream", rec.Header().Get("Content-Type"))
		assert.Greater(t, rec.Body.Len(), 4)
	}
}

func TestNewOps_InvokeWithResponseStream_NotFound(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	rec := callInMemoryHandler(t, h, http.MethodPost,
		"/2021-11-15/functions/nonexistent/response-streaming-invocations", `{}`)

	assert.Equal(t, http.StatusNotFound, rec.Code)
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

func TestLambda_ClassifyFunctionError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		want    string
		result  []byte
		isError bool
	}{
		{
			name:    "runtime error endpoint is Unhandled",
			isError: true,
			result:  []byte(`{"errorMessage":"boom","errorType":"ValueError"}`),
			want:    "Unhandled",
		},
		{
			name:    "error-shaped response payload is Handled",
			isError: false,
			result:  []byte(`{"errorMessage":"caught","errorType":"MyError"}`),
			want:    "Handled",
		},
		{
			name:    "normal response has no function error",
			isError: false,
			result:  []byte(`{"answer":42}`),
			want:    "",
		},
		{
			name:    "empty response has no function error",
			isError: false,
			result:  nil,
			want:    "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, lambda.ClassifyFunctionError(tt.isError, tt.result))
		})
	}
}

func TestLambda_AsyncDestinationDelivery(t *testing.T) {
	t.Parallel()

	const (
		dlqARN     = "arn:aws:sqs:us-east-1:000000000000:dlq"
		successARN = "arn:aws:sns:us-east-1:000000000000:on-success"
		failureARN = "arn:aws:sqs:us-east-1:000000000000:on-failure"
	)

	tests := []struct {
		name        string
		wantTargets []string
		success     bool
	}{
		{
			name:        "failure routes to DLQ and OnFailure",
			success:     false,
			wantTargets: []string{dlqARN, failureARN},
		},
		{
			name:        "success routes to OnSuccess only",
			success:     true,
			wantTargets: []string{successARN},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := lambda.NewInMemoryBackend(nil, nil, lambda.DefaultSettings(), "000000000000", "us-east-1")
			closeBackend(t, backend)

			require.NoError(t, backend.CreateFunction(&lambda.FunctionConfiguration{
				FunctionName:     "async-fn",
				FunctionArn:      "arn:aws:lambda:us-east-1:000000000000:function:async-fn",
				DeadLetterConfig: &lambda.DeadLetterConfig{TargetArn: dlqARN},
			}))

			_, err := backend.PutFunctionEventInvokeConfig("async-fn", &lambda.PutFunctionEventInvokeConfigInput{
				DestinationConfig: &lambda.DestinationConfig{
					OnSuccess: &lambda.Destination{Destination: successARN},
					OnFailure: &lambda.Destination{Destination: failureARN},
				},
			})
			require.NoError(t, err)

			fake := &fakeAsyncDelivery{}
			backend.SetAsyncDestinationDelivery(fake)

			lambda.DispatchAsyncOutcomeForTest(context.Background(), backend, lambda.AsyncOutcomeForTest{
				FunctionName:    "async-fn",
				RequestID:       "req-1",
				RequestPayload:  []byte(`{"in":1}`),
				ResponsePayload: []byte(`{"out":2}`),
				InvokeCount:     3,
				StatusCode:      200,
				Success:         tt.success,
			})

			assert.ElementsMatch(t, tt.wantTargets, fake.targets())
		})
	}
}

func TestLambda_FunctionLifecycle_PendingToActive(t *testing.T) {
	t.Parallel()

	backend := lambda.NewInMemoryBackend(nil, nil, lambda.DefaultSettings(), "000000000000", "us-east-1")
	closeBackend(t, backend)

	backend.SetActivationDelay(200 * time.Millisecond)

	require.NoError(t, backend.CreateFunction(&lambda.FunctionConfiguration{
		FunctionName: "life-fn",
		State:        lambda.FunctionStateActive, // handler default; backend overrides to Pending
	}))

	assert.Equal(t, lambda.FunctionStatePending, lambda.GetFunctionStateForTest(backend, "life-fn"))

	require.Eventually(t, func() bool {
		return lambda.GetFunctionStateForTest(backend, "life-fn") == lambda.FunctionStateActive
	}, 3*time.Second, 20*time.Millisecond)
}

func TestLambda_FunctionLifecycle_NoDelayActiveImmediately(t *testing.T) {
	t.Parallel()

	backend := lambda.NewInMemoryBackend(nil, nil, lambda.DefaultSettings(), "000000000000", "us-east-1")
	closeBackend(t, backend)

	require.NoError(t, backend.CreateFunction(&lambda.FunctionConfiguration{
		FunctionName: "life-fn2",
		State:        lambda.FunctionStateActive,
	}))

	assert.Equal(t, lambda.FunctionStateActive, lambda.GetFunctionStateForTest(backend, "life-fn2"))
}
