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
)

const (
	wsMaxBackendBody       = 10 << 20
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
}

type wsCall struct {
	integration *Integration
	route       *Route
	body        []byte
}

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

func (s *wsSession) vtlContext(route *Route, body string) vtl.Context {
	reqID := uuid.NewString()

	return vtl.Context{
		Body:       body,
		RequestID:  reqID,
		Stage:      s.stage,
		APIID:      s.apiID,
		AccountID:  config.DefaultAccountID,
		DomainName: s.domain,
		SourceIP:   s.sourceIP,
		UserAgent:  s.userAgent,
		Authorizer: s.authorizer,
		Extra: map[string]any{
			jsonKeyConnectionID: s.connID,
			"routeKey":          route.RouteKey,
			"eventType":         wsEventMessage,
			"connectedAt":       s.connectedAt.UnixMilli(),
		},
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
		out, err := vtl.RenderStrict(tpl, s.vtlContext(call.route, string(call.body)))
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
	if call.integration.IntegrationType == IntegrationTypeAWS {
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

	out, renderErr := vtl.RenderStrict(tpl, s.vtlContext(call.route, string(res.body)))
	if renderErr != nil {
		return nil, fmt.Errorf("%w: response template: %w", ErrIntegrationInvoke, renderErr)
	}

	return []byte(out), nil
}

func (s *wsSession) callHTTP(ctx context.Context, integ *Integration, payload []byte) (wsBackendResult, error) {
	if integ.IntegrationURI == "" {
		return wsBackendResult{}, fmt.Errorf("%w: integration URI is empty", ErrIntegrationInvoke)
	}

	if integ.TimeoutInMillis > 0 {
		var cancel context.CancelFunc

		ctx, cancel = context.WithTimeout(ctx, time.Duration(integ.TimeoutInMillis/wsMillisPerSecond)*time.Second+
			time.Duration(integ.TimeoutInMillis%wsMillisPerSecond)*time.Millisecond)
		defer cancel()
	}

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
		res, err = s.callLambda(ctx, integ, payload)
	default:
		res = wsBackendResult{status: mockStatus(payload), body: payload}
	}

	if err != nil {
		return nil, err
	}

	return s.mapResponse(call, res)
}

func (s *wsSession) sendIntegrationError() {
	b, _ := json.Marshal(map[string]string{
		jsonKeyMessage:      wsInternalError,
		jsonKeyConnectionID: s.connID,
		jsonKeyRequestID:    uuid.NewString(),
	}, jsonx.V1())
	s.send(b)
}
