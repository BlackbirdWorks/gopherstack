package pipes

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
)

var errHTTPTargetStatus = errors.New("pipes: HTTP target returned an error status")

// HTTPResponse is the response of an API Gateway or API destination call.
type HTTPResponse struct {
	Body   []byte
	Status int
}

// PipeAPIGatewayInvoker calls the execute-api stage named by the target ARN, with the ARN's "*" path
// wildcards filled from hp.
type PipeAPIGatewayInvoker interface {
	InvokeAPIGateway(ctx context.Context, apiARN string, hp *TargetHTTPParameters, payload []byte) (HTTPResponse, error)
}

// PipeAPIDestinationInvoker calls the EventBridge API destination named by destARN.
type PipeAPIDestinationInvoker interface {
	InvokeAPIDestination(
		ctx context.Context, destARN string, hp *TargetHTTPParameters, payload []byte,
	) (HTTPResponse, error)
}

// HTTPTargets holds the API Gateway and API destination invokers, used for targets and enrichment.
type HTTPTargets struct {
	APIGateway     PipeAPIGatewayInvoker
	APIDestination PipeAPIDestinationInvoker
}

// SetHTTPTargets installs the API Gateway and API destination invokers.
func (r *Runner) SetHTTPTargets(h HTTPTargets) { r.http = h }

func isAPIGatewayARN(arn string) bool { return strings.HasPrefix(arn, "arn:aws:execute-api:") }

func isAPIDestinationARN(arn string) bool {
	return strings.HasPrefix(arn, "arn:aws:events:") && strings.Contains(arn, ":api-destination/")
}

// callHTTP invokes an API Gateway or API destination ARN; handled is false for any other ARN.
func (r *Runner) callHTTP(
	ctx context.Context, arn string, hp *TargetHTTPParameters, payload []byte,
) (HTTPResponse, bool, error) {
	var (
		resp HTTPResponse
		err  error
	)

	switch {
	case isAPIGatewayARN(arn):
		if r.http.APIGateway == nil {
			return HTTPResponse{}, true, fmt.Errorf("%w: api gateway %q", ErrTargetInvokerUnwired, arn)
		}

		resp, err = r.http.APIGateway.InvokeAPIGateway(ctx, arn, hp, payload)
	case isAPIDestinationARN(arn):
		if r.http.APIDestination == nil {
			return HTTPResponse{}, true, fmt.Errorf("%w: api destination %q", ErrTargetInvokerUnwired, arn)
		}

		resp, err = r.http.APIDestination.InvokeAPIDestination(ctx, arn, hp, payload)
	default:
		return HTTPResponse{}, false, nil
	}

	if err != nil {
		return HTTPResponse{}, true, err
	}

	if resp.Status >= http.StatusBadRequest {
		return resp, true, fmt.Errorf("%w: %d from %q", errHTTPTargetStatus, resp.Status, arn)
	}

	return resp, true, nil
}

func (r *Runner) invokeHTTPTarget(ctx context.Context, p *Pipe, payload []byte) (bool, error) {
	var hp *TargetHTTPParameters
	if p.TargetParameters != nil {
		hp = p.TargetParameters.HTTPParameters
	}

	_, handled, err := r.callHTTP(ctx, p.Target, hp, applyInputTemplate(p, payload))

	return handled, err
}

func (r *Runner) invokeHTTPEnrichment(ctx context.Context, p *Pipe, payload []byte) ([]byte, bool, error) {
	var hp *TargetHTTPParameters

	if ep := p.EnrichmentParameters; ep != nil {
		if ep.InputTemplate != "" {
			payload = []byte(ep.InputTemplate)
		}

		if h := ep.HTTPParameters; h != nil {
			hp = &TargetHTTPParameters{
				HeaderParameters:      h.HeaderParameters,
				QueryStringParameters: h.QueryStringParameters,
				PathParameterValues:   h.PathParameterValues,
			}
		}
	}

	resp, handled, err := r.callHTTP(ctx, p.Enrichment, hp, payload)
	if err != nil || !handled {
		return nil, handled, err
	}

	if len(resp.Body) == 0 {
		return nil, true, nil
	}

	return resp.Body, true, nil
}
