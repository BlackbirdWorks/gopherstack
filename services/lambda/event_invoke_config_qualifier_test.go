package lambda_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lambda"
)

func TestAsyncInvoke_QualifiedEventInvokeConfig(t *testing.T) {
	t.Parallel()

	const (
		baseARN = "arn:aws:sqs:us-east-1:000000000000:base"
		qualARN = "arn:aws:sqs:us-east-1:000000000000:qualified"
	)

	tests := []struct {
		name        string
		qualifier   string
		wantTarget  string
		wantRetries int
	}{
		{name: "qualifier_with_own_config", qualifier: "$LATEST", wantTarget: qualARN, wantRetries: 0},
		{name: "unqualified", qualifier: "", wantTarget: baseARN, wantRetries: 1},
		{name: "qualifier_without_config_falls_back", qualifier: "3", wantTarget: baseARN, wantRetries: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := lambda.NewInMemoryBackend(nil, nil, lambda.DefaultSettings(), "000000000000", "us-east-1")
			closeBackend(t, backend)

			require.NoError(t, backend.CreateFunction(&lambda.FunctionConfiguration{
				FunctionName: "q-fn",
				FunctionArn:  "arn:aws:lambda:us-east-1:000000000000:function:q-fn",
			}))

			one, zero := 1, 0
			_, err := backend.PutFunctionEventInvokeConfig("q-fn", &lambda.PutFunctionEventInvokeConfigInput{
				MaximumRetryAttempts: &one,
				DestinationConfig:    &lambda.DestinationConfig{OnSuccess: &lambda.Destination{Destination: baseARN}},
			})
			require.NoError(t, err)

			_, err = backend.PutFunctionEventInvokeConfigQualified(
				"q-fn",
				"$LATEST",
				&lambda.PutFunctionEventInvokeConfigInput{
					MaximumRetryAttempts: &zero,
					DestinationConfig: &lambda.DestinationConfig{
						OnSuccess: &lambda.Destination{Destination: qualARN},
					},
				},
			)
			require.NoError(t, err)

			fake := &fakeAsyncDelivery{}
			backend.SetAsyncDestinationDelivery(fake)

			lambda.DispatchAsyncOutcomeForTest(context.Background(), backend, lambda.AsyncOutcomeForTest{
				FunctionName: "q-fn", Qualifier: tt.qualifier, RequestID: "r1", Success: true, InvokeCount: 1,
			})

			assert.Equal(t, []string{tt.wantTarget}, fake.targets())
			assert.Equal(t, tt.wantRetries, lambda.ReadAsyncRetryConfigForTest(backend, "q-fn", tt.qualifier))
		})
	}
}
