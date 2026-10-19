package appsync

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
)

const (
	httpDSTimeout     = 10 * time.Second
	httpDSMaxResponse = 5 << 20
)

// invokeHTTPDataSource sends mu's request mapping ({method, resourcePath,
// params:{headers,query,body}}) to the data source endpoint and maps the
// {statusCode, headers, body} result.
func invokeHTTPDataSource(
	ctx context.Context,
	mu mappingUnit,
	ds *DataSource,
	args map[string]any,
	prevResult any,
) (any, error) {
	if ds.HTTPConfig == nil || ds.HTTPConfig.Endpoint == "" {
		return nil, fmt.Errorf("%w: http data source missing httpConfig endpoint", ErrValidation)
	}

	value, ok, err := evalRequestMapping(mu, args, prevResult)
	if err != nil {
		return nil, err
	}

	req, _ := value.(map[string]any)
	if !ok || req == nil {
		return nil, fmt.Errorf("%w: HTTP request mapping must produce a request object", ErrValidation)
	}

	httpReq, err := buildHTTPRequest(ctx, ds.HTTPConfig.Endpoint, req)
	if err != nil {
		return nil, err
	}

	client := httpclient.New(httpDSTimeout)
	defer client.CloseIdleConnections()

	resp, err := client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("http data source request failed: %w", err)
	}

	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, httpDSMaxResponse))
	if err != nil {
		return nil, fmt.Errorf("http data source response read failed: %w", err)
	}

	headers := make(map[string]any, len(resp.Header))
	for k, v := range resp.Header {
		headers[strings.ToLower(k)] = strings.Join(v, ",")
	}

	result := map[string]any{"statusCode": int64(resp.StatusCode), keyHeaders: headers, "body": string(body)}

	return evalResponseMapping(mu, args, result)
}

func buildHTTPRequest(ctx context.Context, endpoint string, req map[string]any) (*http.Request, error) {
	method := strings.ToUpper(firstNonEmpty(strVal(req["method"]), http.MethodGet))
	target := strings.TrimRight(endpoint, "/") + "/" + strings.TrimLeft(strVal(req["resourcePath"]), "/")

	u, err := url.Parse(target)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return nil, fmt.Errorf("%w: invalid http data source endpoint", ErrValidation)
	}

	params, _ := req["params"].(map[string]any)

	q := u.Query()

	if qm, ok := params["query"].(map[string]any); ok {
		for k, v := range qm {
			q.Set(k, vrender(v))
		}
	}

	u.RawQuery = q.Encode()

	var payload io.Reader

	switch b := params["body"].(type) {
	case nil:
	case string:
		payload = strings.NewReader(b)
	default:
		raw, mErr := json.Marshal(b)
		if mErr != nil {
			return nil, fmt.Errorf("%w: request body is not JSON-serializable", ErrValidation)
		}

		payload = bytes.NewReader(raw)
	}

	httpReq, err := http.NewRequestWithContext(ctx, method, u.String(), payload)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid HTTP request", ErrValidation)
	}

	if hm, ok := params[keyHeaders].(map[string]any); ok {
		for k, v := range hm {
			httpReq.Header.Set(k, vrender(v))
		}
	}

	if payload != nil && httpReq.Header.Get("Content-Type") == "" {
		httpReq.Header.Set("Content-Type", "application/json")
	}

	return httpReq, nil
}

func strVal(v any) string {
	s, _ := v.(string)

	return s
}
