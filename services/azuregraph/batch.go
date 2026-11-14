package azuregraph

import (
	"bytes"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"

	"github.com/golang-jwt/jwt/v5"
	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const maxBatchRequests = 20

type batchRequest struct {
	Headers   map[string]string `json:"headers"`
	ID        string            `json:"id"`
	Method    string            `json:"method"`
	URL       string            `json:"url"`
	Body      jsontext.Value    `json:"body"`
	DependsOn []string          `json:"dependsOn"`
}

type batchResponse struct {
	Headers map[string]string `json:"headers,omitempty"`
	ID      string            `json:"id"`
	Body    jsontext.Value    `json:"body,omitempty"`
	Status  int               `json:"status"`
}

// recorder captures a sub-request's response.
type recorder struct {
	header http.Header
	body   bytes.Buffer
	status int
}

func (r *recorder) Header() http.Header { return r.header }

func (r *recorder) WriteHeader(code int) {
	if r.status == 0 {
		r.status = code
	}
}

func (r *recorder) Write(p []byte) (int, error) {
	r.WriteHeader(http.StatusOK)

	return r.body.Write(p)
}

func (h *Handler) handleBatch(c *echo.Context, segs []string, claims jwt.MapClaims) error {
	if len(segs) != 1 || c.Request().Method != http.MethodPost {
		return h.methodNotAllowed(c)
	}

	data, err := io.ReadAll(http.MaxBytesReader(c.Response(), c.Request().Body, maxBodyBytes))

	var in struct {
		Requests []batchRequest `json:"requests"`
	}

	if err != nil || json.Unmarshal(data, &in, jsonx.V1()) != nil {
		return h.writeErr(c, badRequest(
			"Unable to read JSON request payload. Please ensure Content-Type header is set and payload is of valid JSON format.",
		))
	}

	if verr := validateBatch(in.Requests); verr != nil {
		return h.writeErr(c, verr)
	}

	status := map[string]int{}
	out := make(map[string]batchResponse, len(in.Requests))

	for _, req := range orderBatch(in.Requests) {
		if failed := slices.ContainsFunc(
			req.DependsOn,
			func(d string) bool { return status[d] >= http.StatusBadRequest },
		); failed {
			out[req.ID] = failedDependency(req.ID)
		} else {
			out[req.ID] = h.runBatchItem(c, req, claims)
		}

		status[req.ID] = out[req.ID].Status
	}

	responses := make([]batchResponse, 0, len(in.Requests))
	for _, req := range in.Requests {
		responses = append(responses, out[req.ID])
	}

	return h.json(c, http.StatusOK, map[string]any{"responses": responses})
}

func failedDependency(id string) batchResponse {
	body, _ := json.Marshal(errorEnvelope{Error: errorBody{
		Code: "FailedDependency", Message: "Failed Dependency",
	}}, jsonx.V1())

	return batchResponse{
		ID: id, Status: http.StatusFailedDependency, Body: body,
		Headers: map[string]string{headerContentType: "application/json"},
	}
}

func validateBatch(reqs []batchRequest) error {
	if len(reqs) == 0 || len(reqs) > maxBatchRequests {
		return badRequest("A $batch request must contain between 1 and %d individual requests.", maxBatchRequests)
	}

	ids := map[string]bool{}

	for _, r := range reqs {
		if r.ID == "" || ids[r.ID] {
			return badRequest("Batch request ids must be present and unique; '%s' is not.", r.ID)
		}

		ids[r.ID] = true
	}

	for _, r := range reqs {
		if r.Method == "" || !strings.HasPrefix(r.URL, "/") {
			return badRequest("Batch request '%s' needs a method and a relative url starting with '/'.", r.ID)
		}

		for _, d := range r.DependsOn {
			if !ids[d] || d == r.ID {
				return badRequest(
					"Batch request '%s' depends on '%s', which is not another request in the batch.",
					r.ID,
					d,
				)
			}
		}
	}

	if len(orderBatch(reqs)) != len(reqs) {
		return badRequest("Batch requests contain a circular dependency.")
	}

	return nil
}

// orderBatch lists requests so each follows its dependencies, keeping request order otherwise. Requests in a
// cycle are left out.
func orderBatch(reqs []batchRequest) []batchRequest {
	done := map[string]bool{}
	out := make([]batchRequest, 0, len(reqs))

	for progress := true; progress; {
		progress = false

		for _, r := range reqs {
			if !done[r.ID] && !slices.ContainsFunc(r.DependsOn, func(d string) bool { return !done[d] }) {
				done[r.ID] = true
				out = append(out, r)
				progress = true
			}
		}
	}

	return out
}

func (h *Handler) runBatchItem(c *echo.Context, item batchRequest, claims jwt.MapClaims) batchResponse {
	outer := c.Request()
	version := "/v1.0"
	if strings.HasPrefix(outer.URL.Path, "/beta") {
		version = "/beta"
	}

	var body io.Reader
	if len(item.Body) > 0 && string(item.Body) != jsonNull {
		body = bytes.NewReader(item.Body)
	}

	req, err := http.NewRequestWithContext(outer.Context(), strings.ToUpper(item.Method),
		"https://"+outer.Host+version+item.URL, body)
	if err != nil {
		return errorItem(
			item.ID,
			http.StatusBadRequest,
			"Request_BadRequest",
			"The batched request url or method is invalid.",
		)
	}

	req.Host = outer.Host
	req.Header.Set(headerContentType, "application/json")

	for k, v := range item.Headers {
		req.Header.Set(k, v)
	}

	segs := graphSegments(req)
	if len(segs) == 0 || strings.HasPrefix(segs[0], "$batch") {
		return errorItem(
			item.ID,
			http.StatusBadRequest,
			codeBadSegment,
			"Resource not found for the segment '"+item.URL+"'.",
		)
	}

	rec := &recorder{header: http.Header{}}
	rec.header.Set("Request-Id", c.Response().Header().Get("Request-Id"))
	rec.header.Set("Client-Request-Id", c.Response().Header().Get("Client-Request-Id"))

	if derr := h.dispatch(h.sub.NewContext(req, rec), segs, claims); derr != nil {
		return errorItem(item.ID, http.StatusInternalServerError, "generalException", "An internal error occurred.")
	}

	resp := batchResponse{ID: item.ID, Status: max(rec.status, http.StatusOK)}

	if ct := rec.header.Get(headerContentType); ct != "" {
		resp.Headers = map[string]string{headerContentType: ct}
	}

	switch raw := bytes.TrimSpace(rec.body.Bytes()); {
	case len(raw) == 0:
	case jsontext.Value(raw).IsValid(jsonx.V1()) && strings.Contains(rec.header.Get(headerContentType), "json"):
		resp.Body = slices.Clone(raw)
	default:
		resp.Body = []byte(strconv.Quote(string(raw)))
	}

	return resp
}

func errorItem(id string, status int, code, msg string) batchResponse {
	body, _ := json.Marshal(errorEnvelope{Error: errorBody{Code: code, Message: msg}}, jsonx.V1())

	return batchResponse{
		ID:      id,
		Status:  status,
		Body:    body,
		Headers: map[string]string{headerContentType: "application/json"},
	}
}
