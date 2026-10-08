package apigateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
)

const testInvokeStage = "test-invoke-stage"

var errTestInvokeNoLambda = errors.New("lambda integration not configured")

// testInvokeRequest builds the synthetic request a test invocation simulates.
func testInvokeRequest(
	ctx context.Context, method, pathWithQuery, body string, headers map[string]string, multi map[string][]string,
) (*http.Request, error) {
	if pathWithQuery == "" {
		pathWithQuery = "/"
	}

	r, err := http.NewRequestWithContext(
		ctx,
		method,
		"http://test-invoke-endpoint"+pathWithQuery,
		strings.NewReader(body),
	)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid pathWithQueryString: %w", ErrInvalidParameter, err)
	}

	for k, v := range headers {
		r.Header.Set(k, v)
	}

	for k, vs := range multi {
		r.Header.Del(k)

		for _, v := range vs {
			r.Header.Add(k, v)
		}
	}

	return r, nil
}

// testInvokeAuthorizer runs the authorizer against the simulated request, as the data plane would.
func (h *Handler) testInvokeAuthorizer(
	ctx context.Context, auth *Authorizer, apiID string, input TestInvokeAuthorizerInput,
) (*TestInvokeAuthorizerOutput, error) {
	start := time.Now()

	r, err := testInvokeRequest(ctx, http.MethodGet, input.PathWithQueryString, input.Body,
		input.Headers, input.MultiValueHeaders)
	if err != nil {
		return nil, err
	}

	var out *TestInvokeAuthorizerOutput

	if auth.Type == AuthTypeCognitoUserPool {
		out = h.testInvokeCognito(r, auth)
	} else {
		out, err = h.testInvokeLambdaAuthorizer(ctx, r, auth, apiID, input)
		if err != nil {
			return nil, err
		}
	}

	out.Latency = time.Since(start).Milliseconds()

	return out, nil
}

func (h *Handler) testInvokeCognito(r *http.Request, auth *Authorizer) *TestInvokeAuthorizerOutput {
	claims, err := h.verifyCognitoToken(r, auth)
	if err != nil {
		return &TestInvokeAuthorizerOutput{
			ClientStatus: http.StatusUnauthorized,
			Log:          "Unauthorized: " + err.Error(),
		}
	}

	flat := make(map[string]string, len(claims))
	for k, v := range claims {
		flat[k] = claimString(v)
	}

	return &TestInvokeAuthorizerOutput{Claims: flat, Log: "Successfully verified Cognito user pool token"}
}

func claimString(v any) string {
	if s, ok := v.(string); ok {
		return s
	}

	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprint(v)
	}

	return string(b)
}

func (h *Handler) testInvokeLambdaAuthorizer(
	ctx context.Context, r *http.Request, auth *Authorizer, apiID string, input TestInvokeAuthorizerInput,
) (*TestInvokeAuthorizerOutput, error) {
	if auth.Type == authorizerTypeToken {
		if token := extractTokenFromIdentitySource(r, auth.IdentitySource); token == "" ||
			(auth.IdentityValidationExpression != "" && !h.tokenMatches(auth.IdentityValidationExpression, token)) {
			return &TestInvokeAuthorizerOutput{
				ClientStatus: http.StatusUnauthorized,
				Log:          "Unauthorized: identity source missing or not matching the validation expression",
			}, nil
		}
	}

	if h.lambda == nil {
		return nil, fmt.Errorf("test invoke authorizer: %w", errTestInvokeNoLambda)
	}

	r = r.WithContext(ctx)
	event := h.buildAuthorizerEvent(ctx, r, auth, apiID, testInvokeStage)
	event.StageVariables = input.StageVariables
	methodArn := authorizerMethodArn(ctx, r, apiID, testInvokeStage, r.URL.Path)
	event.MethodArn = methodArn

	payload, _ := json.Marshal(event)
	funcName := ExtractLambdaFunctionName(auth.AuthorizerURI)

	resp, failure := h.invokeAuthorizerFunction(ctx, funcName, payload)
	if failure != "" {
		return &TestInvokeAuthorizerOutput{ClientStatus: http.StatusUnauthorized, Log: failure}, nil
	}

	policy, _ := json.Marshal(resp.PolicyDocument)
	out := &TestInvokeAuthorizerOutput{
		PrincipalID:    resp.PrincipalID,
		PolicyDocument: string(policy),
		Authorization:  authorizationContext(resp.Context),
		Log:            "Authorizer function " + funcName + " returned principal " + resp.PrincipalID,
	}

	if allowed, _ := evaluateAuthorizerPolicy(resp.PolicyDocument, methodArn); !allowed {
		out.ClientStatus = http.StatusForbidden
	}

	return out, nil
}

func (h *Handler) tokenMatches(expression, token string) bool {
	re := h.cachedRegexp(expression)

	return re == nil || re.MatchString(token)
}

// authorizationContext renders an authorizer's returned context as the response's authorization map.
func authorizationContext(ctxMap map[string]any) map[string][]string {
	if len(ctxMap) == 0 {
		return nil
	}

	keys := make([]string, 0, len(ctxMap))
	for k := range ctxMap {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	out := make(map[string][]string, len(keys))
	for _, k := range keys {
		out[k] = []string{claimString(ctxMap[k])}
	}

	return out
}

// pathParametersFor extracts a resource's path variables from a simulated request path.
func pathParametersFor(resource *Resource, path string) map[string]string {
	trie := newResourcePathTrie()
	trie.insert(*resource)

	if matched, params := trie.match(path); matched != nil {
		return params
	}

	return nil
}

// dataPlaneContext carries this handler's region into simulated invocations.
func (h *Handler) dataPlaneContext() context.Context {
	if h.region == "" {
		return context.Background()
	}

	return awsmeta.Set(context.Background(), &awsmeta.Metadata{Region: h.region, Account: awsmeta.DefaultAccount})
}

// invokeAuthorizerFunction runs the authorizer's Lambda; failure describes why it produced no usable response.
func (h *Handler) invokeAuthorizerFunction(
	ctx context.Context, funcName string, payload []byte,
) (AuthorizerResponse, string) {
	var resp AuthorizerResponse

	respBytes, _, invokeErr := h.lambda.InvokeFunction(ctx, funcName, "RequestResponse", payload)
	if invokeErr != nil {
		return resp, "Authorizer function invocation failed: " + invokeErr.Error()
	}

	if err := json.Unmarshal(respBytes, &resp); err != nil {
		return resp, "Authorizer function returned an unparseable response: " + err.Error()
	}

	return resp, ""
}
