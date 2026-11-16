package apigatewayv2

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/pkgs/vtl"
	"github.com/blackbirdworks/gopherstack/services/apigateway"
)

const (
	wsMaxBackendBody       = 10 << 20
	wsMaxMessageBytes      = 128 << 10
	maxHTTPStatus          = 599
	wsMillisPerSecond      = int32(1000)
	passthroughNever       = "NEVER"
	passthroughNoTemplates = "WHEN_NO_TEMPLATES"
	wsInternalError        = "Internal server error"
	IntegrationTypeAWS     = "AWS"
)

// ErrNoIntegrationResponse marks a non-proxy backend result matching no integration response.
var ErrNoIntegrationResponse = errors.New("no integration response matches the backend result")

var wsSelVarRE = regexp.MustCompile(`\$\{([^}]+)\}|\$((?:request|integration)\.[A-Za-z0-9_.\-]+)`)

type wsBackendResult struct {
	header       http.Header
	errorMessage string
	body         []byte
	status       int
	byStatus     bool
}

type wsCall struct {
	integration *Integration
	route       *Route
	event       map[string]any
	body        []byte
}

type connectRejectedError struct{ status int }

func (e *connectRejectedError) Error() string {
	return fmt.Sprintf("%v: upstream status %d", ErrConnectRejected, e.status)
}

func (e *connectRejectedError) Unwrap() error { return ErrConnectRejected }

// evalSelection evaluates a template selection expression; "" means no key resolved.
func evalSelection(expr string, resolve func(name string) (string, bool)) string {
	if expr == "" {
		return routeKeyDefault
	}

	const escaped = "\x00"

	expr = strings.ReplaceAll(expr, `\$`, escaped)
	failed := false

	out := wsSelVarRE.ReplaceAllStringFunc(expr, func(m string) string {
		name := strings.TrimSuffix(strings.TrimPrefix(m, "${"), "}")
		name = strings.TrimPrefix(name, "$")

		v, ok := resolve(name)
		if !ok {
			failed = true
		}

		return v
	})

	if failed || (strings.Contains(out, "$") && out != routeKeyDefault) {
		return ""
	}

	return strings.ReplaceAll(out, escaped, "$")
}

func requestBodyResolver(body []byte) func(string) (string, bool) {
	var doc any

	_ = json.Unmarshal(body, &doc, jsonx.V1())

	return func(name string) (string, bool) {
		path, ok := strings.CutPrefix(name, "request.body.")
		if !ok {
			return "", false
		}

		return jsonPathString(doc, path)
	}
}

func responseResolver(body []byte, res wsBackendResult) func(string) (string, bool) {
	reqBody := requestBodyResolver(body)

	return func(name string) (string, bool) {
		switch {
		case name == "integration.response.statuscode":
			return strconv.Itoa(res.status), true
		case strings.HasPrefix(name, "integration.response.header."):
			v := res.header.Get(strings.TrimPrefix(name, "integration.response.header."))

			return v, v != ""
		case strings.HasPrefix(name, "integration.response.multivalueheader."):
			vs := res.header.Values(strings.TrimPrefix(name, "integration.response.multivalueheader."))

			return strings.Join(vs, ","), len(vs) > 0
		default:
			return reqBody(name)
		}
	}
}

func (s *wsSession) vtlContext(call wsCall, body string) vtl.Context {
	reqID := uuid.NewString()
	route := call.route
	extra := map[string]any{
		jsonKeyConnectionID: s.connID,
		"routeKey":          route.RouteKey,
		jsonKeyEventType:    wsEventMessage,
		"connectedAt":       s.connectedAt.UnixMilli(),
	}

	rc, _ := call.event[evKeyRequestContext].(map[string]any)
	for _, k := range []string{jsonKeyEventType, "messageId", "disconnectStatusCode", "disconnectReason"} {
		if v, ok := rc[k]; ok {
			extra[k] = v
		}
	}

	return vtl.Context{
		Body:        body,
		RequestID:   reqID,
		Stage:       s.stage,
		APIID:       s.apiID,
		AccountID:   config.DefaultAccountID,
		DomainName:  s.domain,
		SourceIP:    s.sourceIP,
		UserAgent:   s.userAgent,
		Authorizer:  s.authorizer,
		Extra:       extra,
		Headers:     singleValues(s.header),
		QueryParams: singleValues(s.query),
	}
}

// mapRequest applies the integration's request template, or its passthrough behaviour.
func (s *wsSession) mapRequest(call wsCall) ([]byte, error) {
	integ := call.integration

	key := evalSelection(integ.TemplateSelectionExpression, requestBodyResolver(call.body))

	tpl, ok := integ.RequestTemplates[key]
	if !ok {
		tpl, ok = integ.RequestTemplates[routeKeyDefault]
	}

	if ok {
		out, err := vtl.RenderStrict(tpl, s.vtlContext(call, string(call.body)))
		if err != nil {
			return nil, fmt.Errorf("%w: request template: %w", ErrIntegrationInvoke, err)
		}

		return []byte(out), nil
	}

	switch integ.PassthroughBehavior {
	case passthroughNever:
		return nil, fmt.Errorf("%w: no request template matches and passthrough is NEVER", ErrIntegrationInvoke)
	case passthroughNoTemplates:
		if len(integ.RequestTemplates) > 0 {
			return nil, fmt.Errorf("%w: no request template matches", ErrIntegrationInvoke)
		}
	}

	return call.body, nil
}

func selectIntegrationResponse(irs []IntegrationResponse, subject string) (*IntegrationResponse, bool) {
	sorted := make([]IntegrationResponse, len(irs))
	copy(sorted, irs)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].IntegrationResponseKey < sorted[j].IntegrationResponseKey
	})

	var fallback *IntegrationResponse

	for i := range sorted {
		key := sorted[i].IntegrationResponseKey

		if key == routeKeyDefault {
			fallback = &sorted[i]

			continue
		}

		if pattern, isRE := strings.CutPrefix(key, "/"); isRE && strings.HasSuffix(pattern, "/") {
			re, err := regexp.Compile(strings.TrimSuffix(pattern, "/"))
			if err == nil && re.MatchString(subject) {
				return &sorted[i], true
			}

			continue
		}

		if key == subject {
			return &sorted[i], true
		}
	}

	return fallback, fallback != nil
}

// mapResponse selects the integration response for res and applies its response template.
func (s *wsSession) mapResponse(call wsCall, res wsBackendResult) ([]byte, error) {
	irs, err := s.h.Backend.GetIntegrationResponses(s.apiID, call.integration.IntegrationID)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrIntegrationInvoke, err)
	}

	subject := strconv.Itoa(res.status)
	if call.integration.IntegrationType == IntegrationTypeAWS && !res.byStatus {
		subject = res.errorMessage
	}

	ir, ok := selectIntegrationResponse(irs, subject)
	if !ok {
		return nil, fmt.Errorf("%w: %w: %q", ErrIntegrationInvoke, ErrNoIntegrationResponse, subject)
	}

	key := evalSelection(ir.TemplateSelectionExpression, responseResolver(call.body, res))

	tpl, found := ir.ResponseTemplates[key]
	if !found {
		tpl, found = ir.ResponseTemplates[routeKeyDefault]
	}

	if !found {
		return res.body, nil
	}

	out, renderErr := vtl.RenderStrict(tpl, s.vtlContext(call, string(res.body)))
	if renderErr != nil {
		return nil, fmt.Errorf("%w: response template: %w", ErrIntegrationInvoke, renderErr)
	}

	return []byte(out), nil
}

// integrationContext bounds a backend call by the integration's TimeoutInMillis.
func integrationContext(ctx context.Context, integ *Integration) (context.Context, context.CancelFunc) {
	if integ.TimeoutInMillis <= 0 {
		return context.WithCancel(ctx)
	}

	return context.WithTimeout(ctx, time.Duration(integ.TimeoutInMillis/wsMillisPerSecond)*time.Second+
		time.Duration(integ.TimeoutInMillis%wsMillisPerSecond)*time.Millisecond)
}

func (s *wsSession) callHTTP(ctx context.Context, integ *Integration, payload []byte) (wsBackendResult, error) {
	if integ.IntegrationURI == "" {
		return wsBackendResult{}, fmt.Errorf("%w: integration URI is empty", ErrIntegrationInvoke)
	}

	ctx, cancel := integrationContext(ctx, integ)
	defer cancel()

	method := http.MethodPost
	if integ.IntegrationMethod != "" && !strings.EqualFold(integ.IntegrationMethod, httpMethodAny) {
		method = strings.ToUpper(integ.IntegrationMethod)
	}

	req, err := http.NewRequestWithContext(ctx, method, integ.IntegrationURI, bytes.NewReader(payload))
	if err != nil {
		return wsBackendResult{}, fmt.Errorf("%w: %w", ErrIntegrationInvoke, err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, doErr := s.h.getHTTPClient().Do(req)
	if doErr != nil {
		return wsBackendResult{}, fmt.Errorf("%w: %w", ErrIntegrationInvoke, doErr)
	}

	defer resp.Body.Close()

	body, readErr := io.ReadAll(io.LimitReader(resp.Body, wsMaxBackendBody))
	if readErr != nil {
		return wsBackendResult{}, fmt.Errorf("%w: %w", ErrIntegrationInvoke, readErr)
	}

	return wsBackendResult{status: resp.StatusCode, header: resp.Header, body: body}, nil
}

func (s *wsSession) callLambda(ctx context.Context, integ *Integration, payload []byte) (wsBackendResult, error) {
	if !strings.Contains(integ.IntegrationURI, "function:") {
		return wsBackendResult{}, fmt.Errorf("%w: %s", ErrInvalidLambdaArn, integ.IntegrationURI)
	}

	if s.h.lambdaInvoker == nil {
		return wsBackendResult{}, fmt.Errorf("%w: Lambda invoker not configured", ErrIntegrationInvoke)
	}

	out, _, err := s.h.lambdaInvoker.InvokeFunction(
		ctx, extractHTTPAPILambdaARN(integ.IntegrationURI), "RequestResponse", payload)
	if err != nil {
		return wsBackendResult{}, fmt.Errorf("lambda invocation failed: %w: %w", ErrIntegrationInvoke, err)
	}

	res := wsBackendResult{status: http.StatusOK, body: out}

	var fnErr struct {
		ErrorMessage *string `json:"errorMessage"`
	}
	if json.Unmarshal(out, &fnErr, jsonx.V1()) == nil && fnErr.ErrorMessage != nil {
		res.errorMessage = *fnErr.ErrorMessage
	}

	return res, nil
}

func mockStatus(rendered []byte) int {
	var doc struct {
		StatusCode *int `json:"statusCode"`
	}
	if json.Unmarshal(rendered, &doc, jsonx.V1()) == nil && doc.StatusCode != nil {
		return *doc.StatusCode
	}

	return http.StatusOK
}

// invokeIntegration runs a non-Lambda-proxy integration of any supported type.
func (s *wsSession) invokeIntegration(ctx context.Context, call wsCall) ([]byte, error) {
	integ := call.integration

	if integ.IntegrationType == integrationTypeHTTPProxy {
		res, err := s.callHTTP(ctx, integ, call.body)
		if err == nil && rejectsConnect(call.route, res.status) {
			return nil, &connectRejectedError{status: res.status}
		}

		return res.body, err
	}

	payload, err := s.mapRequest(call)
	if err != nil {
		return nil, err
	}

	var res wsBackendResult

	switch integ.IntegrationType {
	case integrationTypeHTTPType:
		res, err = s.callHTTP(ctx, integ, payload)
	case IntegrationTypeAWS:
		res, err = s.callAWS(ctx, integ, payload)
	default:
		res = wsBackendResult{status: mockStatus(payload), body: payload}
	}

	if err != nil {
		return nil, err
	}

	if integ.IntegrationType != IntegrationTypeAWS && rejectsConnect(call.route, res.status) {
		return nil, &connectRejectedError{status: res.status}
	}

	out, err := s.mapResponse(call, res)
	if err == nil && integ.IntegrationType == IntegrationTypeAWS && len(out) > wsMaxMessageBytes {
		return nil, fmt.Errorf("%w: response of %d bytes exceeds the %d-byte message payload quota",
			ErrIntegrationInvoke, len(out), wsMaxMessageBytes)
	}

	return out, err
}

func (s *wsSession) sendIntegrationError() {
	b, _ := json.Marshal(map[string]string{
		jsonKeyMessage:      wsInternalError,
		jsonKeyConnectionID: s.connID,
		jsonKeyRequestID:    uuid.NewString(),
	}, jsonx.V1())
	s.send(b)
}

func rejectsConnect(route *Route, status int) bool {
	outside2xx := status < http.StatusOK || status >= http.StatusMultipleChoices

	return route != nil && route.RouteKey == routeKeyConnect && outside2xx
}

// callAWS sends a non-proxy AWS integration to Lambda, or to the in-process service for other service URIs.
func (s *wsSession) callAWS(ctx context.Context, integ *Integration, payload []byte) (wsBackendResult, error) {
	region, svc, kind, spec := apigateway.ParseAWSIntegrationURI(integ.IntegrationURI)
	if svc == "" || svc == "lambda" || !apigateway.AWSIntegrationKindsValid(kind) {
		return s.callLambda(ctx, integ, payload)
	}

	if s.h.awsInvoker == nil || !s.h.awsInvoker.Supports(svc) {
		return wsBackendResult{}, fmt.Errorf("%w: service %q is not reachable", ErrIntegrationInvoke, svc)
	}

	method := strings.ToUpper(integ.IntegrationMethod)
	if method == "" || method == httpMethodAny {
		method = http.MethodPost
	}

	ctx, cancel := integrationContext(ctx, integ)
	defer cancel()

	resp, err := s.h.awsInvoker.InvokeAWSService(ctx, apigateway.AWSServiceRequest{
		Region: region, Service: svc, Kind: kind, Spec: spec, Method: method, Body: payload,
	})
	if err != nil {
		return wsBackendResult{}, fmt.Errorf("%w: %w", ErrIntegrationInvoke, err)
	}

	return wsBackendResult{status: resp.Status, body: resp.Body, byStatus: true}, nil
}
