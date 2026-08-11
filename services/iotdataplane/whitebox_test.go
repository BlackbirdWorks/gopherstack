package iotdataplane

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func doWhiteboxRequest(t *testing.T, h *Handler, method, path string, body []byte) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(method, path, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)

	require.NoError(t, h.Handler()(c))

	return rec
}

// buildNestedShadowStateJSON returns a JSON object literal nested to exactly
// depth levels (e.g. depth=1 -> `{"k":1}`, depth=2 -> `{"k":{"k":1}}`), for
// exercising the maxShadowStateDepth boundary.
func buildNestedShadowStateJSON(depth int) string {
	s := "1"
	for range depth {
		s = fmt.Sprintf(`{"k":%s}`, s)
	}

	return s
}

func Test_ShadowStateDepth_AtMaxAccepted(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend()

	doc := fmt.Appendf(nil, `{"state":{"desired":%s}}`, buildNestedShadowStateJSON(maxShadowStateDepth))

	_, err := b.UpdateThingShadow("thing1", "", doc)
	require.NoError(t, err, "state.desired at exactly the documented AWS max depth (%d) must be accepted",
		maxShadowStateDepth)
}

func Test_ShadowStateDepth_ExceedsMaxRejected(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend()

	tests := []struct {
		section string
	}{
		{section: "desired"},
		{section: "reported"},
	}

	for _, tt := range tests {
		t.Run(tt.section, func(t *testing.T) {
			t.Parallel()

			nested := buildNestedShadowStateJSON(maxShadowStateDepth + 1)
			doc := fmt.Appendf(nil, `{"state":{%q:%s}}`, tt.section, nested)

			_, err := b.UpdateThingShadow("thing1", tt.section, doc)
			require.ErrorIs(t, err, ErrValidation,
				"state.%s exceeding the documented AWS max depth (%d) must be InvalidRequestException",
				tt.section, maxShadowStateDepth)
		})
	}
}

func Test_ShadowStateDepth_ViaHTTP(t *testing.T) {
	t.Parallel()

	h := NewHandler(NewInMemoryBackend())

	nested := buildNestedShadowStateJSON(maxShadowStateDepth + 1)
	body := fmt.Appendf(nil, `{"state":{"desired":%s}}`, nested)

	rec := doWhiteboxRequest(t, h, http.MethodPost, "/things/thing1/shadow", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "InvalidRequestException", resp["error"])
}
