package eventbridge

import (
	"context"
	"maps"
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/httptarget"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// APIGatewayRequest is an execute-api call made on behalf of a rule target.
type APIGatewayRequest struct {
	Headers map[string]string
	Query   map[string]string
	APIID   string
	Stage   string
	Method  string
	Path    string
	Body    []byte
}

// APIGatewayInvoker calls a deployed API Gateway REST API stage in-process.
type APIGatewayInvoker interface {
	InvokeAPIGateway(ctx context.Context, req APIGatewayRequest) (APIResponse, error)
}

func isAPIGatewayARN(arn string) bool {
	return strings.HasPrefix(arn, "arn:aws:execute-api:")
}

// BuildAPIGatewayRequest turns an execute-api target ARN, its HttpParameters and the payload into a request.
func BuildAPIGatewayRequest(arn string, hp *HTTPParameters, payload []byte) (APIGatewayRequest, bool) {
	route, ok := httptarget.ParseExecuteAPIARN(arn)
	if !ok {
		return APIGatewayRequest{}, false
	}

	req := APIGatewayRequest{
		APIID:   route.APIID,
		Stage:   route.Stage,
		Method:  route.Method,
		Body:    payload,
		Headers: map[string]string{"Content-Type": "application/json"},
	}

	if hp == nil {
		req.Path = route.Path

		return req, true
	}

	req.Path = httptarget.FillWildcards(route.Path, hp.PathParameterValues)
	req.Query = hp.QueryStringParameters

	maps.Copy(req.Headers, hp.HeaderParameters)

	return req, true
}

func deliverToAPIGateway(ctx context.Context, svc APIGatewayInvoker, target *Target, payload string) bool {
	log := logger.Load(ctx)
	if svc == nil {
		log.WarnContext(ctx, "EventBridge: no API Gateway invoker configured", "arn", target.Arn)

		return true
	}

	req, ok := BuildAPIGatewayRequest(target.Arn, target.HTTPParameters, []byte(payload))
	if !ok {
		log.WarnContext(ctx, "EventBridge: malformed execute-api target ARN", "arn", target.Arn)

		return true
	}

	resp, err := svc.InvokeAPIGateway(ctx, req)
	if err != nil {
		log.WarnContext(ctx, "EventBridge: API Gateway invocation failed", "arn", target.Arn, "error", err)

		return true
	}

	return resp.Status >= http.StatusBadRequest
}
