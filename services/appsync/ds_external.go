package appsync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

var (
	// ErrEventBridgeNotConfigured is returned when no EventBridge backend is wired.
	ErrEventBridgeNotConfigured = errors.New("eventbridge backend not configured")
	// ErrOpenSearchNotConfigured is returned when no OpenSearch requester is wired.
	ErrOpenSearchNotConfigured = errors.New("opensearch requester not configured")
	// ErrOpenSearchRequest is returned when the domain answers with an error status.
	ErrOpenSearchRequest = errors.New("opensearch request failed")
	// ErrRDSNotConfigured is returned when no RDS Data API backend is wired.
	ErrRDSNotConfigured = errors.New("rds data api backend not configured")
)

// EventBridgeEntry is one PutEvents entry.
type EventBridgeEntry struct {
	Time         *time.Time
	Source       string
	DetailType   string
	Detail       string
	EventBusName string
	Resources    []string
}

// EventBridgeResult is one PutEvents result entry.
type EventBridgeResult struct {
	EventID      string
	ErrorCode    string
	ErrorMessage string
}

// EventBridgePutter publishes events to the emulator's EventBridge.
type EventBridgePutter interface {
	PutEvents(ctx context.Context, entries []EventBridgeEntry) ([]EventBridgeResult, error)
}

// OpenSearchRequest is an HTTP request to an OpenSearch domain endpoint.
type OpenSearchRequest struct {
	Headers  map[string]string
	Endpoint string
	Method   string
	Path     string
	RawQuery string
	Body     []byte
}

// OpenSearchRequester serves requests against the emulator's OpenSearch domains.
type OpenSearchRequester interface {
	OpenSearchRequest(ctx context.Context, req OpenSearchRequest) (int, []byte, error)
}

// RDSStatement is one RDS Data API ExecuteStatement call.
type RDSStatement struct {
	Variables map[string]any
	Hints     map[string]string
	ClusterID string
	Database  string
	SQL       string
}

// RDSDataExecutor runs statements through the emulator's RDS Data API and
// returns the ExecuteStatement response as a JSON-shaped map.
type RDSDataExecutor interface {
	ExecuteRDSStatement(ctx context.Context, st RDSStatement) (map[string]any, error)
}

// SetEventBridge wires AMAZON_EVENTBRIDGE data sources.
func (b *InMemoryBackend) SetEventBridge(p EventBridgePutter) { b.ebPutter = p }

// SetOpenSearch wires AMAZON_OPENSEARCH_SERVICE and AMAZON_ELASTICSEARCH data sources.
func (b *InMemoryBackend) SetOpenSearch(r OpenSearchRequester) { b.osRequester = r }

// SetRDSData wires RELATIONAL_DATABASE data sources.
func (b *InMemoryBackend) SetRDSData(e RDSDataExecutor) { b.rdsExec = e }

func requestObject(mu mappingUnit, args map[string]any, prev any, what string) (map[string]any, error) {
	value, ok, err := evalRequestMapping(mu, args, prev)
	if err != nil {
		return nil, err
	}

	req, _ := value.(map[string]any)
	if !ok || req == nil {
		return nil, fmt.Errorf("%w: %s request mapping must produce a request object", ErrValidation, what)
	}

	return req, nil
}

// invokeEventBridgeDataSource sends the mapped PutEvents request to the data source's bus.
func invokeEventBridgeDataSource(
	ctx context.Context,
	backend *InMemoryBackend,
	mu mappingUnit,
	ds *DataSource,
	args map[string]any,
	prev any,
) (any, error) {
	if backend.ebPutter == nil {
		return nil, ErrEventBridgeNotConfigured
	}

	if ds.EventBridgeConfig == nil || ds.EventBridgeConfig.EventBusARN == "" {
		return nil, fmt.Errorf("%w: eventbridge data source missing eventBridgeConfig", ErrValidation)
	}

	req, err := requestObject(mu, args, prev, "EventBridge")
	if err != nil {
		return nil, err
	}

	if op := strVal(req["operation"]); op != "PutEvents" {
		return nil, fmt.Errorf("%w: unsupported EventBridge operation %q", ErrValidation, op)
	}

	entries, err := eventBridgeEntries(req, ds.EventBridgeConfig.EventBusARN)
	if err != nil {
		return nil, err
	}

	results, err := backend.ebPutter.PutEvents(withRegion(ctx, backend.region), entries)
	if err != nil {
		return nil, fmt.Errorf("eventbridge PutEvents failed: %w", err)
	}

	return evalResponseMapping(mu, args, putEventsResult(results))
}

func putEventsResult(results []EventBridgeResult) map[string]any {
	out := make([]any, len(results))
	failed := int64(0)

	for i, r := range results {
		e := map[string]any{}
		if r.EventID != "" {
			e["EventId"] = r.EventID
		}

		if r.ErrorCode != "" {
			e["ErrorCode"], e["ErrorMessage"] = r.ErrorCode, r.ErrorMessage
			failed++
		}

		out[i] = e
	}

	return map[string]any{"Entries": out, "FailedEntryCount": failed}
}

func eventBridgeEntries(req map[string]any, busARN string) ([]EventBridgeEntry, error) {
	events := listOf(req["events"])
	if len(events) == 0 {
		return nil, fmt.Errorf("%w: PutEvents requires a non-empty events list", ErrValidation)
	}

	out := make([]EventBridgeEntry, 0, len(events))

	for _, ev := range events {
		m, isMap := ev.(map[string]any)
		if !isMap {
			return nil, fmt.Errorf("%w: each event must be an object", ErrValidation)
		}

		entry, err := eventBridgeEntry(m, busARN)
		if err != nil {
			return nil, err
		}

		out = append(out, entry)
	}

	return out, nil
}

func eventBridgeEntry(m map[string]any, busARN string) (EventBridgeEntry, error) {
	e := EventBridgeEntry{
		Source:       strVal(m["source"]),
		DetailType:   strVal(m["detailType"]),
		EventBusName: busARN,
	}

	switch d := m["detail"].(type) {
	case string:
		e.Detail = d
	case nil:
	default:
		raw, err := json.Marshal(d)
		if err != nil {
			return e, fmt.Errorf("%w: event detail is not JSON-serializable", ErrValidation)
		}

		e.Detail = string(raw)
	}

	for _, r := range listOf(m["resources"]) {
		e.Resources = append(e.Resources, vrender(r))
	}

	if ts := strVal(m["time"]); ts != "" {
		t, err := time.Parse(time.RFC3339, ts)
		if err != nil {
			return e, fmt.Errorf("%w: event time must be ISO8601", ErrValidation)
		}

		e.Time = &t
	}

	return e, nil
}

// invokeOpenSearchDataSource sends the mapped request to the data source's domain endpoint.
func invokeOpenSearchDataSource(
	ctx context.Context,
	backend *InMemoryBackend,
	mu mappingUnit,
	ds *DataSource,
	args map[string]any,
	prev any,
) (any, error) {
	if backend.osRequester == nil {
		return nil, ErrOpenSearchNotConfigured
	}

	endpoint := searchEndpoint(ds)
	if endpoint == "" {
		return nil, fmt.Errorf("%w: search data source missing endpoint", ErrValidation)
	}

	reqMap, err := requestObject(mu, args, prev, "OpenSearch")
	if err != nil {
		return nil, err
	}

	req, err := openSearchRequest(endpoint, reqMap)
	if err != nil {
		return nil, err
	}

	status, body, err := backend.osRequester.OpenSearchRequest(withRegion(ctx, backend.region), req)
	if err != nil {
		return nil, fmt.Errorf("opensearch request failed: %w", err)
	}

	if status >= http.StatusBadRequest {
		return nil, fmt.Errorf("%w: status %d: %s", ErrOpenSearchRequest, status, strings.TrimSpace(string(body)))
	}

	return evalResponseMapping(mu, args, parseSearchBody(body))
}

func searchEndpoint(ds *DataSource) string {
	switch {
	case ds.OpenSearchConfig != nil:
		return ds.OpenSearchConfig.Endpoint
	case ds.ElasticsearchConfig != nil:
		return ds.ElasticsearchConfig.Endpoint
	}

	return ""
}

func parseSearchBody(body []byte) any {
	if len(body) == 0 {
		return nil
	}

	var v any
	if err := json.Unmarshal(body, &v); err != nil {
		return string(body)
	}

	return v
}

func openSearchRequest(endpoint string, m map[string]any) (OpenSearchRequest, error) {
	method := strings.ToUpper(strVal(m["operation"]))
	switch method {
	case http.MethodGet, http.MethodPut, http.MethodPost, http.MethodDelete, http.MethodHead:
	default:
		return OpenSearchRequest{}, fmt.Errorf("%w: unsupported OpenSearch operation %q", ErrValidation, method)
	}

	req := OpenSearchRequest{Endpoint: endpoint, Method: method, Path: strVal(m["path"]), Headers: map[string]string{}}
	params, _ := m["params"].(map[string]any)

	if hm, ok := params[keyHeaders].(map[string]any); ok {
		for k, v := range hm {
			req.Headers[k] = vrender(v)
		}
	}

	if qm, ok := params["queryString"].(map[string]any); ok {
		q := url.Values{}
		for k, v := range qm {
			q.Set(k, vrender(v))
		}

		req.RawQuery = q.Encode()
	}

	switch body := params["body"].(type) {
	case nil:
	case string:
		req.Body = []byte(body)
	default:
		raw, err := json.Marshal(body)
		if err != nil {
			return req, fmt.Errorf("%w: request body is not JSON-serializable", ErrValidation)
		}

		req.Body = raw
	}

	return req, nil
}
