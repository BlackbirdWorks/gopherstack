package gcpfunctions

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"

	"cloud.google.com/go/functions/apiv2/functionspb"
)

const maxInvokeBody = 1 << 20

// API exposes the Functions server for in-process callers (dashboard proxy).
type API struct {
	Functions functionspb.FunctionServiceServer
	invoker   *InMemoryBackend
}

// API exposes the Cloud Functions server for in-process callers.
func (h *Handler) API() *API {
	return &API{Functions: &server{b: h.Backend}, invoker: h.Backend}
}

// InvokeRequest is an HTTP call made to a function through the ingress.
type InvokeRequest struct {
	Header http.Header
	Method string
	Path   string
	Body   []byte
}

// InvokeResult is the function's HTTP response; Body is capped at 1 MiB.
type InvokeResult struct {
	Header http.Header
	Body   []byte
	Status int
}

// Upload stores a source archive in the slot issued by GenerateUploadUrl, bypassing the network.
func (a *API) Upload(ctx context.Context, uploadURL string, data []byte) error {
	u, err := url.Parse(uploadURL)
	if err != nil || !strings.HasPrefix(u.Path, uploadPrefix) {
		return errInvalid("Invalid upload URL.")
	}

	req, err := localRequest(ctx, http.MethodPut, "localhost", u.Path, data)
	if err != nil {
		return err
	}

	rec := httptest.NewRecorder()
	a.invoker.IngressHandler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		return errPrecondition("Upload rejected: %s", strings.TrimSpace(rec.Body.String()))
	}

	return nil
}

// Invoke calls the function's HTTP endpoint in-process.
func (a *API) Invoke(ctx context.Context, name string, in InvokeRequest) (*InvokeResult, error) {
	n, err := parseName(name)
	if err != nil {
		return nil, err
	}

	if _, err = a.invoker.GetFunction(n.full()); err != nil {
		return nil, err
	}

	path := in.Path
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}

	req, err := localRequest(ctx, in.Method, hostLabel(n)+localhostSuffix, path, in.Body)
	if err != nil {
		return nil, err
	}

	for k, vs := range in.Header {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}

	rec := httptest.NewRecorder()
	a.invoker.IngressHandler().ServeHTTP(rec, req)

	body := rec.Body.Bytes()
	if len(body) > maxInvokeBody {
		body = body[:maxInvokeBody]
	}

	return &InvokeResult{Status: rec.Code, Header: rec.Header(), Body: body}, nil
}

func localRequest(ctx context.Context, method, host, path string, body []byte) (*http.Request, error) {
	t, err := url.ParseRequestURI(path)
	if err != nil {
		return nil, errInvalid("Invalid path %q.", path)
	}

	t.Scheme, t.Host = "http", host

	req, err := http.NewRequestWithContext(ctx, method, t.String(), bytes.NewReader(body))
	if err != nil {
		return nil, errInvalid("Invalid request: %v", err)
	}

	req.RemoteAddr = "127.0.0.1:0"

	return req, nil
}
