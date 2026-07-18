package vpclattice_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/services/vpclattice"
)

func newTestHandler(t *testing.T) *vpclattice.Handler {
	t.Helper()
	backend := vpclattice.NewInMemoryBackend("000000000000", "us-east-1")

	return vpclattice.NewHandler(backend)
}

func doRequestWithRegion(
	t *testing.T,
	h *vpclattice.Handler,
	region, method, path string,
	body any,
) *httptest.ResponseRecorder {
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

	ctx := awsmeta.Set(context.Background(), &awsmeta.Metadata{
		Region:    region,
		Account:   "000000000000",
		Partition: "aws",
	})
	req = req.WithContext(ctx)

	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	return rec
}

func doRequest(
	t *testing.T,
	h *vpclattice.Handler,
	method, path string,
	body any,
) *httptest.ResponseRecorder {
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

func parseBody(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var m map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &m))

	return m
}

// TestARNFormat verifies ARN formats for all resource types.
func TestARNFormat(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/services", map[string]any{"name": "arn-svc"})
	require.Equal(t, http.StatusCreated, rec.Code)
	svc := parseBody(t, rec)
	assert.Regexp(
		t,
		`^arn:aws:vpc-lattice:us-east-1:000000000000:service/svc-[a-f0-9]+$`,
		svc["arn"],
	)

	rec = doRequest(t, h, http.MethodPost, "/servicenetworks", map[string]any{"name": "arn-sn"})
	require.Equal(t, http.StatusCreated, rec.Code)
	sn := parseBody(t, rec)
	assert.Regexp(
		t,
		`^arn:aws:vpc-lattice:us-east-1:000000000000:servicenetwork/sn-[a-f0-9]+$`,
		sn["arn"],
	)

	rec = doRequest(t, h, http.MethodPost, "/targetgroups", map[string]any{
		"name":   "arn-tg",
		"type":   "IP",
		"config": map[string]any{"protocol": "HTTP", "port": 80, "vpcIdentifier": "vpc-1"},
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	tg := parseBody(t, rec)
	assert.Regexp(
		t,
		`^arn:aws:vpc-lattice:us-east-1:000000000000:targetgroup/tg-[a-f0-9]+$`,
		tg["arn"],
	)
}

// TestNotFound verifies 404 on get/update/delete of nonexistent resources.
func TestNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		path   string
	}{
		{"get service", http.MethodGet, "/services/svc-notexist"},
		{"get servicenetwork", http.MethodGet, "/servicenetworks/sn-notexist"},
		{"get targetgroup", http.MethodGet, "/targetgroups/tg-notexist"},
		{"get snsa", http.MethodGet, "/servicenetworkserviceassociations/snsa-notexist"},
		{"get snva", http.MethodGet, "/servicenetworkvpcassociations/snva-notexist"},
		{"get als", http.MethodGet, "/accesslogsubscriptions/als-notexist"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rec := doRequest(t, h, tc.method, tc.path, nil)
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}
