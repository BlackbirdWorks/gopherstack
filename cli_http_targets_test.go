package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/apigateway"
	apigwtypes "github.com/aws/aws-sdk-go-v2/service/apigateway/types"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	ebtypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	"github.com/aws/aws-sdk-go-v2/service/pipes"
	ptypes "github.com/aws/aws-sdk-go-v2/service/pipes/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/require"
)

type recordedRequest struct {
	Header http.Header
	Method string
	Path   string
	Query  string
	Body   string
}

type httpRecorder struct {
	reqs []recordedRequest
	mu   sync.Mutex
}

func newHTTPRecorder(t *testing.T, status int, respBody string) (*httptest.Server, *httpRecorder) {
	t.Helper()

	rec := &httpRecorder{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)

		rec.mu.Lock()
		rec.reqs = append(rec.reqs, recordedRequest{
			Method: r.Method, Path: r.URL.Path, Query: r.URL.RawQuery, Header: r.Header.Clone(), Body: string(body),
		})
		rec.mu.Unlock()

		w.WriteHeader(status)
		_, _ = w.Write([]byte(respBody))
	}))
	t.Cleanup(srv.Close)

	return srv, rec
}

func (r *httpRecorder) first() (recordedRequest, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.reqs) == 0 {
		return recordedRequest{}, false
	}

	return r.reqs[0], true
}

type apiRoute struct {
	path   string
	method string
	uri    string
}

// deployHTTPProxyAPI creates a REST API whose routes proxy to the given URIs and deploys it to stage prod.
func deployHTTPProxyAPI(t *testing.T, fx *sfnFixture, routes []apiRoute) string {
	t.Helper()

	ag := apigateway.NewFromConfig(fx.cfg)

	api, err := ag.CreateRestApi(t.Context(), &apigateway.CreateRestApiInput{Name: aws.String("target-api")})
	require.NoError(t, err)

	res, err := ag.GetResources(t.Context(), &apigateway.GetResourcesInput{RestApiId: api.Id})
	require.NoError(t, err)

	ids := map[string]string{"": aws.ToString(res.Items[0].Id)}

	for _, rt := range routes {
		parent := ""

		for part := range strings.SplitSeq(strings.Trim(rt.path, "/"), "/") {
			full := parent + "/" + part
			if _, ok := ids[full]; !ok {
				created, cerr := ag.CreateResource(t.Context(), &apigateway.CreateResourceInput{
					RestApiId: api.Id, ParentId: aws.String(ids[parent]), PathPart: aws.String(part),
				})
				require.NoError(t, cerr)

				ids[full] = aws.ToString(created.Id)
			}

			parent = full
		}

		_, err = ag.PutMethod(t.Context(), &apigateway.PutMethodInput{
			RestApiId: api.Id, ResourceId: aws.String(ids[parent]), HttpMethod: aws.String(rt.method),
			AuthorizationType: aws.String("NONE"),
		})
		require.NoError(t, err)

		_, err = ag.PutIntegration(t.Context(), &apigateway.PutIntegrationInput{
			RestApiId: api.Id, ResourceId: aws.String(ids[parent]), HttpMethod: aws.String(rt.method),
			Type: apigwtypes.IntegrationTypeHttpProxy, IntegrationHttpMethod: aws.String(rt.method),
			Uri: aws.String(rt.uri),
		})
		require.NoError(t, err)
	}

	_, err = ag.CreateDeployment(t.Context(), &apigateway.CreateDeploymentInput{
		RestApiId: api.Id, StageName: aws.String("prod"),
	})
	require.NoError(t, err)

	return aws.ToString(api.Id)
}

func executeAPIARN(apiID, method, path string) string {
	return "arn:aws:execute-api:us-east-1:000000000000:" + apiID + "/prod/" + method + "/" + path
}

func TestEventBridgeAPIGatewayTarget(t *testing.T) {
	t.Parallel()

	tests := []struct {
		params     *ebtypes.HttpParameters
		transform  *ebtypes.InputTransformer
		name       string
		method     string
		arnPath    string
		wantPath   string
		wantQuery  string
		wantHeader string
		wantBody   string
	}{
		{
			name: "default_event", method: http.MethodPost, arnPath: "pets/dog", wantPath: "/dog-hit",
			wantBody: `"source":"apigw.target"`,
		},
		{
			name: "http_parameters", method: http.MethodPost, arnPath: "pets/*", wantPath: "/cat-hit",
			params: &ebtypes.HttpParameters{
				PathParameterValues:   []string{"cat"},
				HeaderParameters:      map[string]string{"X-Probe": "1"},
				QueryStringParameters: map[string]string{"k": "v"},
			},
			transform: &ebtypes.InputTransformer{
				InputPathsMap: map[string]string{"s": "$.source"},
				InputTemplate: aws.String(`{"src": <s>}`),
			},
			wantQuery: "k=v", wantHeader: "1", wantBody: `{"src": "apigw.target"}`,
		},
		{
			name: "get_method", method: http.MethodGet, arnPath: "pets/dog", wantPath: "/dog-hit",
			wantBody: `"source":"apigw.target"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fx := newSFNFixture(t)
			authzStartWorkers(t, fx, "EventBridge")

			srv, rec := newHTTPRecorder(t, http.StatusOK, `{}`)
			apiID := deployHTTPProxyAPI(t, fx, []apiRoute{
				{path: "/pets/dog", method: tt.method, uri: srv.URL + "/dog-hit"},
				{path: "/pets/cat", method: tt.method, uri: srv.URL + "/cat-hit"},
			})

			ebc := eventbridge.NewFromConfig(fx.cfg)
			_, err := ebc.PutRule(t.Context(), &eventbridge.PutRuleInput{
				Name: aws.String("r"), EventPattern: aws.String(`{"source":["apigw.target"]}`),
			})
			require.NoError(t, err)

			_, err = ebc.PutTargets(t.Context(), &eventbridge.PutTargetsInput{
				Rule: aws.String("r"),
				Targets: []ebtypes.Target{{
					Id:               aws.String("t"),
					Arn:              aws.String(executeAPIARN(apiID, tt.method, tt.arnPath)),
					HttpParameters:   tt.params,
					InputTransformer: tt.transform,
				}},
			})
			require.NoError(t, err)

			_, err = ebc.PutEvents(t.Context(), &eventbridge.PutEventsInput{Entries: []ebtypes.PutEventsRequestEntry{{
				Source: aws.String("apigw.target"), DetailType: aws.String("t"), Detail: aws.String(`{}`),
			}}})
			require.NoError(t, err)

			var got recordedRequest

			require.Eventually(t, func() bool {
				var ok bool
				got, ok = rec.first()

				return ok
			}, authzDeadline, authzTick)

			require.Equal(t, tt.method, got.Method)
			require.Equal(t, tt.wantPath, got.Path)
			require.Equal(t, tt.wantQuery, got.Query)
			require.Equal(t, tt.wantHeader, got.Header.Get("X-Probe"))
			require.Contains(t, got.Body, tt.wantBody)
		})
	}
}

// createAPIDestination creates an API-key connection and an API destination, returning the destination ARN.
func createAPIDestination(t *testing.T, fx *sfnFixture, name, endpoint string) string {
	t.Helper()

	ebc := eventbridge.NewFromConfig(fx.cfg)

	conn, err := ebc.CreateConnection(t.Context(), &eventbridge.CreateConnectionInput{
		Name:              aws.String(name + "-conn"),
		AuthorizationType: ebtypes.ConnectionAuthorizationTypeApiKey,
		AuthParameters: &ebtypes.CreateConnectionAuthRequestParameters{
			ApiKeyAuthParameters: &ebtypes.CreateConnectionApiKeyAuthRequestParameters{
				ApiKeyName: aws.String("X-Api-Key"), ApiKeyValue: aws.String("secret"),
			},
		},
	})
	require.NoError(t, err)

	dest, err := ebc.CreateApiDestination(t.Context(), &eventbridge.CreateApiDestinationInput{
		Name:               aws.String(name),
		ConnectionArn:      conn.ConnectionArn,
		InvocationEndpoint: aws.String(endpoint),
		HttpMethod:         ebtypes.ApiDestinationHttpMethodPost,
	})
	require.NoError(t, err)

	return aws.ToString(dest.ApiDestinationArn)
}

// pipeFromQueue creates a pipe reading a fresh queue and sends it a message; it returns nothing to await on
// besides the effects of the target.
func pipeFromQueue(t *testing.T, fx *sfnFixture, in *pipes.CreatePipeInput, message string) {
	t.Helper()

	srcURL, srcARN := authzQueue(t, fx, "pipe-src")

	in.Name = aws.String("p")
	in.RoleArn = aws.String(authzAccount + "pipe")
	in.Source = aws.String(srcARN)

	_, err := pipes.NewFromConfig(fx.cfg).CreatePipe(t.Context(), in)
	require.NoError(t, err)

	_, err = sqs.NewFromConfig(fx.cfg).SendMessage(t.Context(), &sqs.SendMessageInput{
		QueueUrl: aws.String(srcURL), MessageBody: aws.String(message),
	})
	require.NoError(t, err)
}

func awaitRequest(t *testing.T, rec *httpRecorder) recordedRequest {
	t.Helper()

	var got recordedRequest

	require.Eventually(t, func() bool {
		var ok bool
		got, ok = rec.first()

		return ok
	}, authzDeadline, authzTick)

	return got
}

func TestPipesHTTPTargets(t *testing.T) {
	t.Parallel()

	hp := &ptypes.PipeTargetHttpParameters{
		PathParameterValues:   []string{"abc"},
		HeaderParameters:      map[string]string{"X-Probe": "1"},
		QueryStringParameters: map[string]string{"k": "v"},
	}

	tests := []struct {
		setup      func(t *testing.T, fx *sfnFixture, srvURL string) (string, *ptypes.PipeTargetParameters)
		name       string
		wantPath   string
		wantAPIKey string
	}{
		{
			name:     "api_gateway",
			wantPath: "/pets-hit",
			setup: func(t *testing.T, fx *sfnFixture, srvURL string) (string, *ptypes.PipeTargetParameters) {
				t.Helper()

				id := deployHTTPProxyAPI(
					t,
					fx,
					[]apiRoute{{path: "/pets/abc", method: "POST", uri: srvURL + "/pets-hit"}},
				)

				return "arn:aws:execute-api:us-east-1:000000000000:" + id + "/prod/POST/pets/*",
					&ptypes.PipeTargetParameters{HttpParameters: hp, InputTemplate: aws.String(`{"t":1}`)}
			},
		},
		{
			name:       "api_destination",
			wantPath:   "/dest/abc",
			wantAPIKey: "secret",
			setup: func(t *testing.T, fx *sfnFixture, srvURL string) (string, *ptypes.PipeTargetParameters) {
				t.Helper()

				return createAPIDestination(t, fx, "pipe-dest", srvURL+"/dest/*"),
					&ptypes.PipeTargetParameters{HttpParameters: hp, InputTemplate: aws.String(`{"t":1}`)}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fx := newSFNFixture(t)
			authzStartWorkers(t, fx, "Pipes")

			srv, rec := newHTTPRecorder(t, http.StatusOK, `{}`)
			target, params := tt.setup(t, fx, srv.URL)

			pipeFromQueue(t, fx, &pipes.CreatePipeInput{Target: aws.String(target), TargetParameters: params}, "{}")

			got := awaitRequest(t, rec)
			require.Equal(t, http.MethodPost, got.Method)
			require.Equal(t, tt.wantPath, got.Path)
			require.Equal(t, "k=v", got.Query)
			require.Equal(t, "1", got.Header.Get("X-Probe"))
			require.Equal(t, tt.wantAPIKey, got.Header.Get("X-Api-Key"))
			require.JSONEq(t, `{"t":1}`, got.Body)
		})
	}
}

func TestPipesHTTPEnrichment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(t *testing.T, fx *sfnFixture, srvURL string) string
		name  string
	}{
		{
			name: "api_gateway",
			setup: func(t *testing.T, fx *sfnFixture, srvURL string) string {
				t.Helper()

				id := deployHTTPProxyAPI(
					t,
					fx,
					[]apiRoute{{path: "/enrich", method: "POST", uri: srvURL + "/enrich-hit"}},
				)

				return "arn:aws:execute-api:us-east-1:000000000000:" + id + "/prod/POST/enrich"
			},
		},
		{
			name: "api_destination",
			setup: func(t *testing.T, fx *sfnFixture, srvURL string) string {
				t.Helper()

				return createAPIDestination(t, fx, "enrich-dest", srvURL+"/enrich-hit")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fx := newSFNFixture(t)
			authzStartWorkers(t, fx, "Pipes")

			srv, rec := newHTTPRecorder(t, http.StatusOK, `{"enriched":true}`)
			enrichment := tt.setup(t, fx, srv.URL)
			dstURL, dstARN := authzQueue(t, fx, "pipe-dst")

			pipeFromQueue(t, fx, &pipes.CreatePipeInput{
				Target: aws.String(dstARN), Enrichment: aws.String(enrichment),
				EnrichmentParameters: &ptypes.PipeEnrichmentParameters{
					HttpParameters: &ptypes.PipeEnrichmentHttpParameters{
						HeaderParameters: map[string]string{"X-Probe": "1"},
					},
				},
			}, `{"raw":true}`)

			got := awaitRequest(t, rec)
			require.Equal(t, "1", got.Header.Get("X-Probe"))
			require.Contains(t, got.Body, `{\"raw\":true}`)

			sqsc := sqs.NewFromConfig(fx.cfg)

			var body string

			require.Eventually(t, func() bool {
				out, err := sqsc.ReceiveMessage(t.Context(), &sqs.ReceiveMessageInput{QueueUrl: aws.String(dstURL)})
				if err != nil || len(out.Messages) == 0 {
					return false
				}

				body = aws.ToString(out.Messages[0].Body)

				return true
			}, authzDeadline, authzTick)

			require.JSONEq(t, `{"enriched":true}`, body)
		})
	}
}
