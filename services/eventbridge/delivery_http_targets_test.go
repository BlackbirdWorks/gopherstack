package eventbridge_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
)

func TestBuildAPIGatewayRequest(t *testing.T) {
	t.Parallel()

	const arn = "arn:aws:execute-api:us-east-1:000000000000:api1/prod/PUT/pets/*/toys/*"

	tests := []struct {
		hp     *eventbridge.HTTPParameters
		name   string
		arn    string
		want   eventbridge.APIGatewayRequest
		wantOK bool
	}{
		{
			name: "no_parameters", arn: arn, wantOK: true,
			want: eventbridge.APIGatewayRequest{
				APIID: "api1", Stage: "prod", Method: http.MethodPut, Path: "/pets/*/toys/*", Body: []byte("b"),
				Headers: map[string]string{"Content-Type": "application/json"},
			},
		},
		{
			name: "parameters", arn: arn, wantOK: true,
			hp: &eventbridge.HTTPParameters{
				PathParameterValues:   []string{"cat", "ball"},
				HeaderParameters:      map[string]string{"X-A": "1"},
				QueryStringParameters: map[string]string{"q": "v"},
			},
			want: eventbridge.APIGatewayRequest{
				APIID: "api1", Stage: "prod", Method: http.MethodPut, Path: "/pets/cat/toys/ball", Body: []byte("b"),
				Headers: map[string]string{"Content-Type": "application/json", "X-A": "1"},
				Query:   map[string]string{"q": "v"},
			},
		},
		{name: "not_execute_api", arn: "arn:aws:sqs:us-east-1:000000000000:q"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, ok := eventbridge.BuildAPIGatewayRequest(tt.arn, tt.hp, []byte("b"))
			require.Equal(t, tt.wantOK, ok)

			if tt.wantOK {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestInvokeAPIDestination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		hp         *eventbridge.HTTPParameters
		name       string
		endpoint   string
		wantPath   string
		wantStatus int
	}{
		{name: "plain", endpoint: "/hook", wantPath: "/hook", wantStatus: http.StatusOK},
		{
			name: "path_wildcards", endpoint: "/a/*/b/*", wantPath: "/a/x/b/y", wantStatus: http.StatusOK,
			hp: &eventbridge.HTTPParameters{PathParameterValues: []string{"x", "y"}},
		},
		{name: "error_status", endpoint: "/fail", wantPath: "/fail", wantStatus: http.StatusBadGateway},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rs := newRecordingServer(t)
			rs.status = tt.wantStatus

			backend := eventbridge.NewInMemoryBackend()
			conn, err := backend.CreateConnection(t.Context(), eventbridge.CreateConnectionInput{
				Name:              "c",
				AuthorizationType: "API_KEY",
				AuthParameters: &eventbridge.ConnectionAuthParameters{
					APIKeyAuthParameters: &eventbridge.ConnectionAPIKeyAuthParameters{
						APIKeyName: "x-k", APIKeyValue: "v",
					},
				},
			})
			require.NoError(t, err)

			dest, err := backend.CreateAPIDestination(t.Context(), eventbridge.CreateAPIDestinationInput{
				Name: "d", ConnectionArn: conn.ConnectionArn,
				InvocationEndpoint: rs.server.URL + tt.endpoint, HTTPMethod: http.MethodPost,
			})
			require.NoError(t, err)

			resp, err := backend.InvokeAPIDestination(t.Context(), dest.APIDestinationArn, []byte(`{"a":1}`), tt.hp)
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, resp.Status)

			reqs := rs.Requests()
			require.Len(t, reqs, 1)
			assert.Equal(t, tt.wantPath, reqs[0].path)
			assert.Equal(t, "v", reqs[0].header.Get("X-K"))
			assert.JSONEq(t, `{"a":1}`, reqs[0].body)
		})
	}
}

func TestInvokeAPIDestinationUnknown(t *testing.T) {
	t.Parallel()

	_, err := eventbridge.NewInMemoryBackend().InvokeAPIDestination(
		t.Context(), "arn:aws:events:us-east-1:000000000000:api-destination/nope/x", nil, nil,
	)
	require.Error(t, err)
}
