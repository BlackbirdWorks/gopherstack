package lambda

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// An extension that waits for platform.runtimeDone before asking for the next event must not be starved by the
// runtime's next call waiting for extensions to go idle.
func TestRuntimeDoneBeforeExtensionsIdle(t *testing.T) {
	t.Parallel()

	h := newExtHarness(t)
	lis := newHTTPListener(t)
	h.srv.tel.ph.setIdentity(hubIdentity{functionName: "ext-fn", version: "$LATEST", memoryMB: 128})
	h.srv.tel.initStart(time.Now())
	h.srv.exts.setExternal([]string{"tel-ext"})

	id := h.register(t, "tel-ext", "INVOKE")
	port := lis.addr[strings.LastIndex(lis.addr, ":")+1:]
	status, out := sub(t, h, apiTelemetry, id, `{"schemaVersion":"2022-07-01","types":["platform"],`+
		`"buffering":{"timeoutMs":25},"destination":{"protocol":"HTTP","URI":"http://sandbox.localdomain:`+port+`"}}`)
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
	<-firstNext

	status, _, _ = h.do(t, http.MethodPost, "/2018-06-01/runtime/invocation/"+reqID+"/response", nil, `"ok"`)
	require.Equal(t, http.StatusAccepted, status)
	<-done

	go func() {
		_, _, _, _ = h.doRaw(ctx, http.MethodGet, "/2018-06-01/runtime/invocation/next", nil, "")
	}()

	require.Eventually(t, func() bool {
		_, ok := lis.find("platform.runtimeDone")

		return ok
	}, 10*time.Second, 10*time.Millisecond, "extension has not called next yet; got %v", lis.types())

	_, early := lis.find("platform.report")
	assert.False(t, early, "report waits for the extension")

	_ = h.nextAsync(t, id)

	require.Eventually(t, func() bool {
		_, ok := lis.find("platform.report")

		return ok
	}, 10*time.Second, 10*time.Millisecond, "got %v", lis.types())

	n := 0

	for _, ty := range lis.types() {
		if ty == "platform.runtimeDone" {
			n++
		}
	}

	assert.Equal(t, 1, n)
}
