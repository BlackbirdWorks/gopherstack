package app

import (
	"context"
	"net/http"
	"net/url"
	"strings"

	apigwbackend "github.com/blackbirdworks/gopherstack/services/apigateway"
	transferbackend "github.com/blackbirdworks/gopherstack/services/transfer"
)

// transferAPIGatewayClient serves Transfer API_GATEWAY identity provider calls that target the emulator's own
// API Gateway in-process and sends every other URL outbound.
type transferAPIGatewayClient struct {
	gw       *apigwbackend.Handler
	outbound transferbackend.APIGatewayClient
}

func (c *transferAPIGatewayClient) Get(
	ctx context.Context, rawURL string, headers map[string]string,
) (int, []byte, error) {
	if c.gw != nil {
		if req, ok := stageRequestFor(rawURL, headers); ok {
			resp := c.gw.InvokeStage(ctx, req)

			return resp.Status, resp.Body, nil
		}
	}

	return c.outbound.Get(ctx, rawURL, headers)
}

// stageRequestFor maps https://{apiId}.execute-api.{region}.amazonaws.com/{stage}/... and
// /restapis/{apiId}/{stage}/_user_request_/... URLs onto a stage invocation.
func stageRequestFor(rawURL string, headers map[string]string) (apigwbackend.StageRequest, bool) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return apigwbackend.StageRequest{}, false
	}

	var apiID, rest string

	if host, _, ok := strings.Cut(u.Hostname(), ".execute-api."); ok && host != "" {
		apiID, rest = host, strings.TrimPrefix(u.Path, "/")
	} else if after, found := strings.CutPrefix(u.Path, "/restapis/"); found {
		apiID, rest, _ = strings.Cut(after, "/")
		rest = strings.Replace(rest, "/_user_request_", "", 1)
	}

	stage, path, _ := strings.Cut(rest, "/")
	if apiID == "" || stage == "" {
		return apigwbackend.StageRequest{}, false
	}

	query := map[string]string{}
	for k, v := range u.Query() {
		query[k] = v[0]
	}

	return apigwbackend.StageRequest{
		APIID: apiID, Stage: stage, Method: http.MethodGet, Path: "/" + path, Query: query, Headers: headers,
	}, true
}
