package vpclattice

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func ruleCount(b *InMemoryBackend) int {
	b.mu.RLock("test.ruleCount")
	defer b.mu.RUnlock()

	return b.rules.Len()
}

func doWhiteboxRequest(t *testing.T, h *Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var buf *bytes.Reader
	if body != nil {
		data, err := json.Marshal(body)
		require.NoError(t, err)
		buf = bytes.NewReader(data)
	} else {
		buf = bytes.NewReader(nil)
	}

	req := httptest.NewRequest(method, path, buf)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
		req.ContentLength = int64(buf.Len())
	}

	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	return rec
}

func parseWhiteboxBody(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()

	var m map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &m))

	return m
}

// TestServiceDelete_CascadesDependents verifies that once a service has no
// service-network association, deleting it also removes its listeners,
// listener rules, resource policy, auth policy, and access log
// subscriptions -- the cascade real AWS documents on DeleteService -- and
// leaves no ghost rows behind in any of those tables.
func TestServiceDelete_CascadesDependents(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "us-east-1")
	h := NewHandler(backend)

	svcRec := doWhiteboxRequest(t, h, http.MethodPost, "/services", map[string]any{"name": "svc-cascade"})
	require.Equal(t, http.StatusCreated, svcRec.Code)
	svc := parseWhiteboxBody(t, svcRec)
	svcID, _ := svc["id"].(string)
	svcARN, _ := svc["arn"].(string)

	lRec := doWhiteboxRequest(t, h, http.MethodPost, "/services/"+svcID+"/listeners", map[string]any{
		"name": "l1", "protocol": "HTTP",
	})
	require.Equal(t, http.StatusCreated, lRec.Code)
	require.Equal(t, 1, ListenerCount(backend))
	require.Equal(t, 1, ruleCount(backend), "CreateListener implicitly creates a default rule")

	require.Equal(t, http.StatusOK,
		doWhiteboxRequest(t, h, http.MethodPut, "/authpolicy/"+svcARN, map[string]any{"policy": `{}`}).Code)
	require.Equal(t, http.StatusOK,
		doWhiteboxRequest(t, h, http.MethodPut, "/resourcepolicy/"+svcARN, map[string]any{"policy": `{}`}).Code)
	require.Equal(t, http.StatusCreated,
		doWhiteboxRequest(t, h, http.MethodPost, "/accesslogsubscriptions", map[string]any{
			"resourceIdentifier": svcARN, "destinationArn": "arn:aws:s3:::bucket",
		}).Code)
	require.Equal(
		t,
		http.StatusOK,
		doWhiteboxRequest(
			t,
			h,
			http.MethodPost,
			"/tags/"+svcARN,
			map[string]any{"tags": map[string]any{"k": "v"}},
		).Code,
	)

	rec := doWhiteboxRequest(t, h, http.MethodDelete, "/services/"+svcID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	assert.Equal(t, 0, ListenerCount(backend), "listeners must be cascade-deleted")
	assert.Equal(t, 0, ruleCount(backend), "rules must be cascade-deleted")

	assert.Equal(t, http.StatusNotFound,
		doWhiteboxRequest(t, h, http.MethodGet, "/authpolicy/"+svcARN, nil).Code, "auth policy must be cascade-deleted")
	assert.Equal(t, http.StatusNotFound,
		doWhiteboxRequest(t, h, http.MethodGet, "/resourcepolicy/"+svcARN, nil).Code,
		"resource policy must be cascade-deleted")

	alsRec := doWhiteboxRequest(t, h, http.MethodGet, "/accesslogsubscriptions?resourceIdentifier="+svcARN, nil)
	require.Equal(t, http.StatusOK, alsRec.Code)
	alsItems, _ := parseWhiteboxBody(t, alsRec)["items"].([]any)
	assert.Empty(t, alsItems, "access log subscriptions must be cascade-deleted")

	tagsRec := doWhiteboxRequest(t, h, http.MethodGet, "/tags/"+svcARN, nil)
	require.Equal(t, http.StatusOK, tagsRec.Code)
	tagsMap, _ := parseWhiteboxBody(t, tagsRec)["tags"].(map[string]any)
	assert.Empty(t, tagsMap, "tags must be cascade-deleted")
}
