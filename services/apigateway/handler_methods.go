package apigateway

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

type testInvokeMethodHandlerInput struct {
	TestInvokeMethodInput
}

type putMethodInput struct {
	RequestParameters  map[string]bool   `json:"requestParameters,omitempty"`
	RequestModels      map[string]string `json:"requestModels,omitempty"`
	RestAPIID          string            `json:"restApiId"`
	ResourceID         string            `json:"resourceId"`
	HTTPMethod         string            `json:"httpMethod"`
	AuthorizationType  string            `json:"authorizationType"`
	AuthorizerID       string            `json:"authorizerId"`
	RequestValidatorID string            `json:"requestValidatorId"`
	OperationName      string            `json:"operationName,omitempty"`
	APIKeyRequired     bool              `json:"apiKeyRequired"`
}

type getMethodInput struct {
	RestAPIID  string `json:"restApiId"`
	ResourceID string `json:"resourceId"`
	HTTPMethod string `json:"httpMethod"`
}

type deleteMethodInput struct {
	RestAPIID  string `json:"restApiId"`
	ResourceID string `json:"resourceId"`
	HTTPMethod string `json:"httpMethod"`
}

type putMethodResponseInput struct {
	PutMethodResponseInput

	RestAPIID  string `json:"restApiId"`
	ResourceID string `json:"resourceId"`
	HTTPMethod string `json:"httpMethod"`
	StatusCode string `json:"statusCode"`
}

type getMethodResponseInput struct {
	RestAPIID  string `json:"restApiId"`
	ResourceID string `json:"resourceId"`
	HTTPMethod string `json:"httpMethod"`
	StatusCode string `json:"statusCode"`
}

type deleteMethodResponseInput struct {
	RestAPIID  string `json:"restApiId"`
	ResourceID string `json:"resourceId"`
	HTTPMethod string `json:"httpMethod"`
	StatusCode string `json:"statusCode"`
}

// parseAPIGWMethodPath handles paths under /restapis/{id}/resources/{resId}/methods/{httpMethod}.
//
// parseAPIGWMethodPath handles paths under /restapis/{id}/resources/{resId}/methods/{httpMethod}.
func parseAPIGWMethodPath(method string, segs []string) (string, map[string]string, bool) {
	const (
		idxID         = 1
		idxResourceID = 3
		idxHTTPMethod = 5
		idxIntegSeg   = 6
		idxRespSeg    = 7
	)

	if len(segs) <= idxHTTPMethod {
		return apiGWUnknownOp, nil, false
	}

	apiID := segs[idxID]
	resID := segs[idxResourceID]
	httpMethod := segs[idxHTTPMethod]
	baseParams := map[string]string{
		keyRestAPIID:  apiID,
		keyResourceID: resID,
		keyHTTPMethod: httpMethod,
	}

	if len(segs) > idxIntegSeg {
		op, params, ok := parseAPIGWMethodSubPath(
			method, segs, idxIntegSeg, idxRespSeg, apiID, resID, httpMethod, baseParams,
		)
		if ok || op == apiGWUnknownOp {
			return op, params, ok
		}
	}

	return parseAPIGWMethodBasePath(method, baseParams)
}

// parseAPIGWMethodSubPath routes method sub-paths (integration and responses).
func parseAPIGWMethodSubPath(
	method string,
	segs []string,
	idxInteg, idxResp int,
	apiID, resID, httpMethod string,
	baseParams map[string]string,
) (string, map[string]string, bool) {
	seg := segs[idxInteg]

	switch seg {
	case apiGWSegInteg:
		return parseAPIGWIntegrationPath(method, segs, idxInteg, idxResp, apiID, resID, httpMethod, baseParams)
	case apiGWSegResponses:
		return parseAPIGWMethodResponsePath(method, segs, idxInteg, apiID, resID, httpMethod)
	}

	return "", nil, false
}

// parseAPIGWMethodResponsePath routes method response paths.
func parseAPIGWMethodResponsePath(
	method string,
	segs []string,
	idxInteg int,
	apiID, resID, httpMethod string,
) (string, map[string]string, bool) {
	if len(segs) <= idxInteg+1 {
		return apiGWUnknownOp, nil, false
	}

	params := map[string]string{
		keyRestAPIID:  apiID,
		keyResourceID: resID,
		keyHTTPMethod: httpMethod,
		keyStatusCode: segs[idxInteg+1],
	}

	switch method {
	case http.MethodPut:
		return opPutMethodResponse, params, true
	case http.MethodGet:
		return opGetMethodResponse, params, true
	case http.MethodDelete:
		return opDeleteMethodResponse, params, true
	case http.MethodPatch:
		return opUpdateMethodResponse, params, true
	}

	return apiGWUnknownOp, nil, false
}

// parseAPIGWMethodBasePath routes the base method path (no sub-segment).
func parseAPIGWMethodBasePath(method string, baseParams map[string]string) (string, map[string]string, bool) {
	switch method {
	case http.MethodPut:
		return opPutMethod, baseParams, true
	case http.MethodGet:
		return opGetMethod, baseParams, true
	case http.MethodDelete:
		return opDeleteMethod, baseParams, true
	case http.MethodPost:
		return opTestInvokeMethod, baseParams, true
	case http.MethodPatch:
		return opUpdateMethod, baseParams, true
	}

	return apiGWUnknownOp, nil, false
}

// testInvokeMethod executes a test invocation of a method, routing through the real
// integration when Lambda is configured. Falls back to mock/stub for unsupported types.
func (h *Handler) testInvokeMethod(input TestInvokeMethodInput) (*TestInvokeMethodOutput, error) {
	resource, err := h.Backend.GetResource(input.RestAPIID, input.ResourceID)
	if err != nil {
		return nil, fmt.Errorf("%w: resource %s", ErrResourceNotFound, input.ResourceID)
	}

	integration, err := h.Backend.GetIntegration(input.RestAPIID, input.ResourceID, input.HTTPMethod)
	if err != nil {
		integration, _ = h.Backend.GetIntegration(input.RestAPIID, input.ResourceID, "ANY")
	}

	if integration == nil {
		// No integration: return empty 200.
		return &TestInvokeMethodOutput{
			Status:  http.StatusOK,
			Body:    "{}",
			Latency: 1,
			Log:     "Test invocation: no integration configured",
			Headers: map[string]string{headerContentType: contentTypeJSON},
		}, nil
	}

	switch integration.Type {
	case IntegrationTypeMock:
		// For MOCK, select the integration response matching "200" and apply its template.
		body := `{"statusCode": 200}`
		ir, irErr := h.Backend.GetIntegrationResponse(
			input.RestAPIID, input.ResourceID, input.HTTPMethod, "200",
		)
		if irErr == nil {
			if t, ok := ir.ResponseTemplates["application/json"]; ok && t != "" {
				body = t
			}
		}

		return &TestInvokeMethodOutput{
			Status:  http.StatusOK,
			Body:    body,
			Latency: 1,
			Log:     "Test invocation: MOCK integration",
			Headers: map[string]string{headerContentType: contentTypeJSON},
		}, nil

	case IntegrationTypeAWSProxy, "AWS":
		return h.invokeLambdaTestMethod(input, resource, integration)

	default:
		return h.Backend.TestInvokeMethod(input)
	}
}

func (h *Handler) invokeLambdaTestMethod(
	input TestInvokeMethodInput,
	resource *Resource,
	integration *Integration,
) (*TestInvokeMethodOutput, error) {
	if h.lambda == nil {
		return h.Backend.TestInvokeMethod(input)
	}

	// Build a synthetic HTTP request from the TestInvokeMethodInput.
	rawPath := input.PathWithQueryString
	if rawPath == "" {
		rawPath = resource.Path
	}

	syntheticReq, reqErr := http.NewRequestWithContext(
		context.Background(),
		input.HTTPMethod,
		"http://test-invoke-endpoint"+rawPath,
		strings.NewReader(input.Body),
	)
	if reqErr != nil {
		return nil, fmt.Errorf("test invoke: failed to build request: %w", reqErr)
	}

	for k, v := range input.Headers {
		syntheticReq.Header.Set(k, v)
	}

	event, buildErr := BuildProxyEvent(syntheticReq, input.RestAPIID, "test-invoke", resource.Path, rawPath, nil)
	if buildErr != nil {
		return nil, fmt.Errorf("test invoke: failed to build proxy event: %w", buildErr)
	}

	payload, _ := json.Marshal(event)

	lambdaFn := ExtractLambdaFunctionName(integration.URI)
	respBytes, _, invokeErr := h.lambda.InvokeFunction(context.Background(), lambdaFn, "RequestResponse", payload)

	return lambdaTestOutput(respBytes, invokeErr), nil
}

// lambdaTestOutput converts a raw Lambda invocation result into a TestInvokeMethodOutput.
// Any invocation error is surfaced as a 502 response body rather than a Go error,
// because TestInvokeMethod always returns a (possibly error-body) output, never a Go error.
func lambdaTestOutput(respBytes []byte, invokeErr error) *TestInvokeMethodOutput {
	if invokeErr != nil {
		return &TestInvokeMethodOutput{
			Status:  http.StatusBadGateway,
			Body:    `{"message":"Lambda invocation failed"}`,
			Latency: 1,
			Log:     "Test invocation: Lambda error: " + invokeErr.Error(),
			Headers: map[string]string{headerContentType: contentTypeJSON},
		}
	}

	var lambdaResp LambdaProxyResponse
	if json.Unmarshal(respBytes, &lambdaResp) == nil {
		sc := lambdaResp.StatusCode
		if sc == 0 {
			sc = http.StatusOK
		}

		hdrs := lambdaResp.Headers
		if hdrs == nil {
			hdrs = map[string]string{headerContentType: contentTypeJSON}
		}

		return &TestInvokeMethodOutput{
			Status:  sc,
			Body:    lambdaResp.Body,
			Latency: 1,
			Log:     "Test invocation: AWS_PROXY Lambda integration",
			Headers: hdrs,
		}
	}

	return &TestInvokeMethodOutput{
		Status:  http.StatusOK,
		Body:    string(respBytes),
		Latency: 1,
		Log:     "Test invocation: Lambda raw response",
		Headers: map[string]string{headerContentType: contentTypeJSON},
	}
}

func (h *Handler) methodActions() map[string]actionFn {
	return map[string]actionFn{
		opPutMethod:        h.putMethodAction,
		opGetMethod:        h.getMethodAction,
		opDeleteMethod:     h.deleteMethodAction,
		opUpdateMethod:     h.updateMethodAction,
		opTestInvokeMethod: h.testInvokeMethodAction,
	}
}

func (h *Handler) putMethodAction(b []byte) (int, any, error) {
	var input putMethodInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	switch input.AuthorizationType {
	case AuthTypeNone, AuthTypeAWSIAM, AuthTypeCustom, AuthTypeCognitoUserPool:
	default:
		return 0, nil, fmt.Errorf(
			"%w: invalid authorizationType %q; must be NONE, AWS_IAM, CUSTOM, or COGNITO_USER_POOLS",
			ErrInvalidParameter, input.AuthorizationType,
		)
	}
	if (input.AuthorizationType == AuthTypeCustom || input.AuthorizationType == AuthTypeCognitoUserPool) &&
		input.AuthorizerID == "" {
		return 0, nil, fmt.Errorf(
			"%w: authorizerId is required when authorizationType is %s",
			ErrInvalidParameter, input.AuthorizationType,
		)
	}
	m, err := h.Backend.PutMethod(PutMethodInput(input))
	if err != nil {
		return 0, nil, err
	}

	return http.StatusCreated, m, nil
}

func (h *Handler) getMethodAction(b []byte) (int, any, error) {
	var input getMethodInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	m, err := h.Backend.GetMethod(input.RestAPIID, input.ResourceID, input.HTTPMethod)
	if err != nil {
		return 0, nil, err
	}

	return http.StatusOK, m, nil
}

func (h *Handler) deleteMethodAction(b []byte) (int, any, error) {
	var input deleteMethodInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	if err := h.Backend.DeleteMethod(input.RestAPIID, input.ResourceID, input.HTTPMethod); err != nil {
		return 0, nil, err
	}

	return http.StatusNoContent, map[string]any{}, nil
}

func (h *Handler) updateMethodAction(b []byte) (int, any, error) {
	var input UpdateMethodInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	out, err := h.Backend.UpdateMethod(input)
	if err != nil {
		return 0, nil, err
	}

	return http.StatusOK, out, nil
}

func (h *Handler) testInvokeMethodAction(b []byte) (int, any, error) {
	var input testInvokeMethodHandlerInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	out, err := h.testInvokeMethod(input.TestInvokeMethodInput)
	if err != nil {
		return 0, nil, err
	}

	return http.StatusOK, out, nil
}

//nolint:dupl // methodResponseActions and integrationResponseActions have similar structure by design
func (h *Handler) methodResponseActions() map[string]actionFn {
	return map[string]actionFn{
		opPutMethodResponse: func(b []byte) (int, any, error) {
			var input putMethodResponseInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			mr, err := h.Backend.PutMethodResponse(
				input.RestAPIID,
				input.ResourceID,
				input.HTTPMethod,
				input.StatusCode,
				input.PutMethodResponseInput,
			)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusCreated, mr, nil
		},
		opGetMethodResponse: func(b []byte) (int, any, error) {
			var input getMethodResponseInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			mr, err := h.Backend.GetMethodResponse(
				input.RestAPIID,
				input.ResourceID,
				input.HTTPMethod,
				input.StatusCode,
			)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, mr, nil
		},
		opDeleteMethodResponse: func(b []byte) (int, any, error) {
			var input deleteMethodResponseInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			if err := h.Backend.DeleteMethodResponse(
				input.RestAPIID,
				input.ResourceID,
				input.HTTPMethod,
				input.StatusCode,
			); err != nil {
				return 0, nil, err
			}

			return http.StatusNoContent, map[string]any{}, nil
		},
		opUpdateMethodResponse: func(b []byte) (int, any, error) {
			var input UpdateMethodResponseInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			out, err := h.Backend.UpdateMethodResponse(input)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, out, nil
		},
	}
}
