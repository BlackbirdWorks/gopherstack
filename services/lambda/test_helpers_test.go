package lambda_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/lambda"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- DurableExecution real-state tests ---

const durableExecARN = "arn:aws:lambda:us-east-1:000000000000:durable:test-exec-1"

func durableExecURL(suffix string) string {
	return "/2025-12-01/durable-executions/" + url.PathEscape(durableExecARN) + suffix
}

// durableExecCallbackURL builds a SendDurableExecutionCallback{Success,Failure,Heartbeat}
// URL: /2025-12-01/durable-execution-callbacks/{callbackID}{suffix}.
func durableExecCallbackURL(callbackID, suffix string) string {
	return "/2025-12-01/durable-execution-callbacks/" + url.PathEscape(callbackID) + suffix
}

// durableExecListURL builds a ListDurableExecutionsByFunction URL:
// /2025-12-01/functions/{functionName}/durable-executions[?query].
func durableExecListURL(functionName, query string) string {
	u := "/2025-12-01/functions/" + url.PathEscape(functionName) + "/durable-executions"
	if query != "" {
		u += "?" + query
	}

	return u
}

// ---- helpers ----

func auditCreateFunction(t *testing.T, h *lambda.Handler, body string) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, "/2015-03-31/functions", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	return rec
}

func auditUpdateConfig(t *testing.T, h *lambda.Handler, name, body string) *httptest.ResponseRecorder {
	t.Helper()

	path := "/2015-03-31/functions/" + name + "/configuration"
	req := httptest.NewRequest(http.MethodPut, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	return rec
}

func auditGetConfig(t *testing.T, h *lambda.Handler, name string) *httptest.ResponseRecorder {
	t.Helper()

	path := "/2015-03-31/functions/" + name + "/configuration"
	req := httptest.NewRequest(http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	return rec
}

func auditGetFunction(t *testing.T, h *lambda.Handler, name string) *httptest.ResponseRecorder {
	t.Helper()

	path := "/2015-03-31/functions/" + name
	req := httptest.NewRequest(http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	return rec
}

func baseImageFn(name string) string {
	return fmt.Sprintf(
		`{"FunctionName":%q,"PackageType":"Image","Code":{"ImageUri":"ecr/x:latest"},"Role":"arn:aws:iam:::role/r"}`,
		name,
	)
}

func baseZipFn(name string) string {
	const tpl = `{"FunctionName":%q,"PackageType":"Zip","Runtime":"python3.12",` +
		`"Handler":"index.handler","Code":{"ZipFile":"UEsDBA=="},"Role":"arn:aws:iam:::role/r"}`

	return fmt.Sprintf(tpl, name)
}

// lambdaCall is a thin wrapper around callInMemoryHandler that also accepts headers.
func lambdaCall(
	t *testing.T,
	h *lambda.Handler,
	method, path string,
	headers map[string]string,
	body string,
) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	return rec
}

func lambdaParseBody(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	return out
}

// newInMemoryHandler creates a handler backed by a real InMemoryBackend for unit tests.
// It uses a nil port allocator so no real HTTP servers are started.
func newInMemoryHandler(t *testing.T) (*lambda.Handler, *lambda.InMemoryBackend) {
	t.Helper()

	bk := lambda.NewInMemoryBackend(
		nil,
		nil, // no real HTTP servers in unit tests
		lambda.DefaultSettings(),
		"000000000000",
		"us-east-1",
	)
	closeBackend(t, bk)
	h := lambda.NewHandler(bk)
	h.DefaultRegion = "us-east-1"
	h.AccountID = "000000000000"

	return h, bk
}

// callInMemoryHandler sends a request to the handler and returns the response recorder.
func callInMemoryHandler(
	t *testing.T,
	h *lambda.Handler,
	method, path, body string,
) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	return rec
}

// createFunctionForTest creates a Lambda function and returns its name.
func createFunctionForTest(t *testing.T, h *lambda.Handler, fnName string) {
	t.Helper()

	body := fmt.Sprintf(
		`{"FunctionName":%q,"PackageType":"Image","Code":{"ImageUri":"x"},"Role":"arn:aws:iam:::role/r"}`,
		fnName,
	)
	rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions", body)
	require.Equal(t, http.StatusCreated, rec.Code)
}

// parity_fixes_test.go tests the parity, performance, and leak fixes applied to the
// lambda service:
//
//   - Parity: handleInvokeAsync truly async (returns 202 without waiting for execution)
//   - Parity: handleInvokeWithResponseStream uses AWS event stream binary protocol
//   - Parity: sweepESMs marks enabled ESMs whose function is missing as "PROBLEM"
//   - Performance: withInvocationChain uses []string — verified via contain/len semantics
//   - Leak: activeConcurrencies entries deleted when count reaches zero
//   - Leak: cleanupSem and logSem replaced with fresh channels in Reset()

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

// fakeAsyncDelivery records delivery calls for assertion.
type fakeAsyncDelivery struct {
	calls []deliveryCall
	mu    sync.Mutex
}

type deliveryCall struct {
	attrs   map[string]string
	target  string
	payload []byte
}

func (f *fakeAsyncDelivery) DeliverToTarget(
	_ context.Context, target string, payload []byte, attrs map[string]string,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, deliveryCall{target: target, payload: payload, attrs: attrs})

	return nil
}

func (f *fakeAsyncDelivery) targets() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.calls))
	for i, c := range f.calls {
		out[i] = c.target
	}

	return out
}

// forcedError is a trivial error used to force invocation failures in tests.
type forcedError struct{}

func (forcedError) Error() string { return "forced failure" }
