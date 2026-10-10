package apigateway

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
)

// StageRequest is an in-process execute-api call to a deployed REST API stage.
type StageRequest struct {
	Headers map[string]string
	Query   map[string]string
	APIID   string
	Stage   string
	Method  string
	Path    string
	Body    []byte
}

// StageResponse is the execute-api response to a StageRequest.
type StageResponse struct {
	Header http.Header
	Body   []byte
	Status int
}

// InvokeStage serves req through the same data-plane path as an HTTP stage invoke, so
// authorizers, validators, throttling, caching and integrations all run.
func (h *Handler) InvokeStage(ctx context.Context, req StageRequest) StageResponse {
	path := req.Path
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}

	q := url.Values{}
	for k, v := range req.Query {
		q.Set(k, v)
	}

	target := "/" + req.Stage + path
	if len(q) > 0 {
		target += "?" + q.Encode()
	}

	method := req.Method
	if method == "" {
		method = http.MethodPost
	}

	r, err := http.NewRequestWithContext(ctx, method, target, bytes.NewReader(req.Body))
	if err != nil {
		return StageResponse{Status: http.StatusBadRequest}
	}

	for k, v := range req.Headers {
		r.Header.Set(k, v)
	}

	owner := h.invokeOwner(req.APIID)
	rec := httptest.NewRecorder()
	owner.handleProxyRequest(req.APIID, req.Stage)(rec, owner.invokeRequest(r))

	return StageResponse{Status: rec.Code, Header: rec.Header().Clone(), Body: rec.Body.Bytes()}
}
