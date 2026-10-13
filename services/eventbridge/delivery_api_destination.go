package eventbridge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/httptarget"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const apiDestTimeout = 5 * time.Second

// APIResponse is the HTTP response of an API destination or API Gateway invocation.
type APIResponse struct {
	Body   []byte
	Status int
}

var errNoAPIDestinationResolver = errors.New("eventbridge: no API-destination resolver configured")

var errAPIDestinationNotFound = errors.New("eventbridge: API destination not found")

// deliverToAPIDestination invokes an API destination for a rule target and reports whether
// delivery failed (a transport error or a >=400 response), which drives retry/DLQ upstream.
func deliverToAPIDestination(
	ctx context.Context,
	resolver APIDestinationResolver,
	target *Target,
	payload string,
) bool {
	resp, err := invokeAPIDestination(ctx, resolver, target.Arn, payload, target.HTTPParameters)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "EventBridge: API-destination delivery failed",
			"arn", target.Arn, "error", err)

		return true
	}

	return resp.Status >= http.StatusBadRequest
}

// InvokeAPIDestination performs the HTTP call of an API destination: it applies the
// destination's body, header and query parameters, fills endpoint "*" wildcards from hp,
// signs the request per the connection's authorization type and honours the destination's rate limit.
func (b *InMemoryBackend) InvokeAPIDestination(
	ctx context.Context, destARN string, payload []byte, hp *HTTPParameters,
) (APIResponse, error) {
	return invokeAPIDestination(ctx, b, destARN, string(payload), hp)
}

func invokeAPIDestination(
	ctx context.Context,
	resolver APIDestinationResolver,
	destARN, payload string,
	hp *HTTPParameters,
) (APIResponse, error) {
	if resolver == nil {
		return APIResponse{}, errNoAPIDestinationResolver
	}

	dest, ok := resolver.ResolveAPIDestination(destARN)
	if !ok {
		return APIResponse{}, fmt.Errorf("%w: %s", errAPIDestinationNotFound, destARN)
	}

	resolver.WaitAPIDestinationRateLimit(ctx, destARN, dest.RateLimitPerSecond)

	method := dest.HTTPMethod
	if method == "" {
		method = http.MethodPost
	}

	endpoint := dest.Endpoint
	if hp != nil {
		endpoint = httptarget.FillWildcards(endpoint, hp.PathParameterValues)
	}

	req, err := http.NewRequestWithContext(
		ctx, method, endpoint, strings.NewReader(mergeBodyParameters(payload, dest.BodyParameters)),
	)
	if err != nil {
		return APIResponse{}, err
	}

	req.Header.Set("Content-Type", "application/json")

	applyTargetHTTPParameters(req, hp)
	applyConnectionHTTPParameters(req, dest.HeaderParameters, dest.QueryStringParameters)

	if authErr := applyAPIDestinationAuth(ctx, req, dest); authErr != nil {
		return APIResponse{}, authErr
	}

	client := &http.Client{Timeout: apiDestTimeout}

	resp, err := client.Do(req)
	if err != nil {
		return APIResponse{}, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxOAuthResponseBytes))
	if err != nil {
		return APIResponse{}, err
	}

	return APIResponse{Status: resp.StatusCode, Body: body}, nil
}

// mergeBodyParameters merges a connection's invocation body parameters into the
// event payload. When the payload is a JSON object the parameters are added as
// top-level keys (matching AWS, which merges connection body parameters into the
// request body); otherwise the payload is returned unchanged.
func mergeBodyParameters(payload string, params []ConnectionBodyParameter) string {
	if len(params) == 0 {
		return payload
	}

	var obj map[string]any
	if err := json.Unmarshal([]byte(payload), &obj); err != nil || obj == nil {
		return payload
	}

	for _, p := range params {
		obj[p.Key] = p.Value
	}

	merged, err := json.Marshal(obj)
	if err != nil {
		return payload
	}

	return string(merged)
}

// applyTargetHTTPParameters adds a PutTargets Target's own HttpParameters
// (as opposed to the connection's InvocationHttpParameters) to the outbound
// request. Applied before the connection's parameters so a conflicting key
// is overwritten by the connection's value, matching AWS's documented
// precedence for Target.HttpParameters.
func applyTargetHTTPParameters(req *http.Request, hp *HTTPParameters) {
	if hp == nil {
		return
	}

	for k, v := range hp.HeaderParameters {
		req.Header.Set(k, v)
	}

	if len(hp.QueryStringParameters) > 0 {
		q := req.URL.Query()
		for k, v := range hp.QueryStringParameters {
			q.Set(k, v)
		}
		req.URL.RawQuery = q.Encode()
	}
}

// applyConnectionHTTPParameters adds a connection's invocation header and
// query-string parameters to the outbound request.
func applyConnectionHTTPParameters(
	req *http.Request,
	headers []ConnectionHeaderParameter,
	queries []ConnectionQueryStringParameter,
) {
	for _, h := range headers {
		req.Header.Set(h.Key, h.Value)
	}

	if len(queries) > 0 {
		q := req.URL.Query()
		for _, qp := range queries {
			q.Set(qp.Key, qp.Value)
		}
		req.URL.RawQuery = q.Encode()
	}
}

// applyAPIDestinationAuth signs the outbound request according to the resolved
// connection's authorization type.
func applyAPIDestinationAuth(ctx context.Context, req *http.Request, dest *ResolvedAPIDestination) error {
	switch dest.AuthType {
	case connectionAuthAPIKey:
		if dest.APIKeyName != "" {
			req.Header.Set(dest.APIKeyName, dest.APIKeyValue)
		}
	case connectionAuthBasic:
		req.SetBasicAuth(dest.BasicUsername, dest.BasicPassword)
	case connectionAuthOAuth:
		if dest.OAuth == nil {
			return nil
		}
		token, err := fetchOAuthToken(ctx, dest.OAuth)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+token)
	}

	return nil
}

// fetchOAuthToken performs an OAuth 2.0 client-credentials grant against the
// connection's authorization endpoint and returns the access token. Client
// credentials are sent via HTTP Basic auth and the grant_type in the form body,
// mirroring the common client-credentials flow AWS uses for OAuth connections.
func fetchOAuthToken(ctx context.Context, oauth *ResolvedOAuth) (string, error) {
	form := url.Values{}
	form.Set("grant_type", "client_credentials")
	for _, bp := range oauth.BodyParameters {
		form.Set(bp.Key, bp.Value)
	}

	method := oauth.HTTPMethod
	if method == "" {
		method = http.MethodPost
	}

	req, err := http.NewRequestWithContext(
		ctx, method, oauth.AuthorizationEndpoint, strings.NewReader(form.Encode()),
	)
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.SetBasicAuth(oauth.ClientID, oauth.ClientSecret)

	applyConnectionHTTPParameters(req, oauth.HeaderParameters, oauth.QueryStringParameters)

	client := &http.Client{Timeout: apiDestTimeout}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(io.LimitReader(resp.Body, maxOAuthResponseBytes))
	if err != nil {
		return "", err
	}
	if resp.StatusCode >= http.StatusBadRequest {
		return "", fmt.Errorf("%w: OAuth token endpoint returned %d", ErrInvalidParameter, resp.StatusCode)
	}

	var tokenResp struct {
		AccessToken string `json:"access_token"`
	}
	if err = json.Unmarshal(data, &tokenResp); err != nil {
		return "", err
	}
	if tokenResp.AccessToken == "" {
		return "", fmt.Errorf("%w: OAuth token endpoint returned no access_token", ErrInvalidParameter)
	}

	return tokenResp.AccessToken, nil
}

const maxOAuthResponseBytes = 1 << 20
