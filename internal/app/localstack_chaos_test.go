package app

import (
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/ses"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
)

func TestLocalStackChaosAPIInjectsFaultsIntoSDKCalls(t *testing.T) {
	t.Parallel()

	srv, cfg := devEndpointsServer(t)

	do := func(method, body string) {
		t.Helper()

		req, err := http.NewRequestWithContext(
			t.Context(), method, srv.URL+"/_localstack/chaos/faults", strings.NewReader(body),
		)
		require.NoError(t, err)

		resp, err := testclient.New(t).Do(req)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		require.Equal(t, http.StatusOK, resp.StatusCode)
	}

	tests := []struct {
		call    func() error
		name    string
		service string
	}{
		{
			name: "json_sqs", service: "sqs",
			call: func() error {
				_, err := sqs.NewFromConfig(cfg).ListQueues(t.Context(), &sqs.ListQueuesInput{})

				return err
			},
		},
		{
			name: "query_ses", service: "ses",
			call: func() error {
				_, err := ses.NewFromConfig(cfg).ListIdentities(t.Context(), &ses.ListIdentitiesInput{})

				return err
			},
		},
	}

	for _, tt := range tests {
		do(http.MethodPost, `[{"service":"`+tt.service+`","error":{"statusCode":400,"code":"BoomFault"}}]`)

		var apiErr smithy.APIError

		require.ErrorAs(t, tt.call(), &apiErr, tt.name)
		assert.Equal(t, "BoomFault", apiErr.ErrorCode(), tt.name)

		do(http.MethodDelete, `[{"service":"`+tt.service+`","error":{"statusCode":400,"code":"BoomFault"}}]`)
		require.NoError(t, tt.call(), tt.name)
	}
}
