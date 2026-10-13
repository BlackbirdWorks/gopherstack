package lambda_test

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	lambdasdk "github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newImageFunctionWithVersions(t *testing.T, client *lambdasdk.Client, name string, versions int) {
	t.Helper()

	_, err := client.CreateFunction(t.Context(), &lambdasdk.CreateFunctionInput{
		FunctionName: aws.String(name),
		PackageType:  types.PackageTypeImage,
		Code:         &types.FunctionCode{ImageUri: aws.String("ecr/myapp:latest")},
		Role:         aws.String("arn:aws:iam:::role/r"),
	})
	require.NoError(t, err)

	for range versions {
		_, err = client.PublishVersion(t.Context(), &lambdasdk.PublishVersionInput{FunctionName: aws.String(name)})
		require.NoError(t, err)
	}
}

func Test_SDKRoundTrip_Lambda_AliasRoutingValidation(t *testing.T) {
	t.Parallel()

	const (
		notFound = "ResourceNotFoundException"
		invalid  = "InvalidParameterValueException"
	)

	tests := []struct {
		weights map[string]float64
		name    string
		primary string
		wantErr string
	}{
		{name: "valid", primary: "1", weights: map[string]float64{"2": 0.3}},
		{name: "unknown additional", primary: "1", weights: map[string]float64{"9": 0.3}, wantErr: notFound},
		{name: "same as primary", primary: "1", weights: map[string]float64{"1": 0.3}, wantErr: invalid},
		{name: "weight above one", primary: "1", weights: map[string]float64{"2": 1.5}, wantErr: invalid},
		{name: "latest primary", primary: "$LATEST", weights: map[string]float64{"2": 0.3}, wantErr: invalid},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newInMemoryHandler(t)
			client := newTestLambdaClient(t, h)
			newImageFunctionWithVersions(t, client, "rt-fn", 2)

			_, err := client.CreateAlias(t.Context(), &lambdasdk.CreateAliasInput{
				FunctionName:    aws.String("rt-fn"),
				Name:            aws.String("live"),
				FunctionVersion: aws.String(tt.primary),
				RoutingConfig:   &types.AliasRoutingConfiguration{AdditionalVersionWeights: tt.weights},
			})

			if tt.wantErr == "" {
				require.NoError(t, err)

				return
			}

			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func Test_SDKRoundTrip_Lambda_ReservedConcurrencyAccountFloor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		reserved int32
		wantErr  bool
	}{
		{name: "within limit", reserved: 900},
		{name: "leaves under floor", reserved: 901, wantErr: true},
		{name: "above account", reserved: 5000, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newInMemoryHandler(t)
			client := newTestLambdaClient(t, h)
			newImageFunctionWithVersions(t, client, "cc-fn", 0)

			_, err := client.PutFunctionConcurrency(t.Context(), &lambdasdk.PutFunctionConcurrencyInput{
				FunctionName:                 aws.String("cc-fn"),
				ReservedConcurrentExecutions: aws.Int32(tt.reserved),
			})

			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "InvalidParameterValueException")

				return
			}

			require.NoError(t, err)
		})
	}
}

func Test_SDKRoundTrip_Lambda_InvokeRejectsNonJSONPayload(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		payload string
	}{
		{name: "garbage", payload: "notjson"},
		{name: "truncated", payload: `{"a":`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newInMemoryHandler(t)
			client := newTestLambdaClient(t, h)
			newImageFunctionWithVersions(t, client, "inv-fn", 0)

			_, err := client.Invoke(t.Context(), &lambdasdk.InvokeInput{
				FunctionName: aws.String("inv-fn"),
				Payload:      []byte(tt.payload),
			})

			var ire *types.InvalidRequestContentException

			require.ErrorAs(t, err, &ire)
		})
	}
}

func Test_SDKRoundTrip_Lambda_EventSourceMappingBatchRules(t *testing.T) {
	t.Parallel()

	const sqsARN = "arn:aws:sqs:us-east-1:000000000000:q"

	tests := []struct {
		batch     *int32
		window    *int32
		name      string
		source    string
		wantBatch int32
		wantErr   bool
	}{
		{name: "sqs default", source: sqsARN, wantBatch: 10},
		{name: "sqs large needs window", source: sqsARN, batch: aws.Int32(500), wantErr: true},
		{name: "sqs large with window", source: sqsARN, batch: aws.Int32(500), window: aws.Int32(1), wantBatch: 500},
		{name: "sqs over max", source: sqsARN, batch: aws.Int32(10001), window: aws.Int32(1), wantErr: true},
		{name: "fifo over ten", source: sqsARN + ".fifo", batch: aws.Int32(11), window: aws.Int32(1), wantErr: true},
		{name: "kinesis default", source: "arn:aws:kinesis:us-east-1:000000000000:stream/s", wantBatch: 100},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newInMemoryHandler(t)
			client := newTestLambdaClient(t, h)
			newImageFunctionWithVersions(t, client, "esm-fn", 0)

			in := &lambdasdk.CreateEventSourceMappingInput{
				FunctionName:                   aws.String("esm-fn"),
				EventSourceArn:                 aws.String(tt.source),
				BatchSize:                      tt.batch,
				MaximumBatchingWindowInSeconds: tt.window,
			}
			if !strings.Contains(tt.source, ":sqs:") {
				in.StartingPosition = types.EventSourcePositionLatest
			}

			out, err := client.CreateEventSourceMapping(t.Context(), in)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "InvalidParameterValueException")

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantBatch, aws.ToInt32(out.BatchSize))
			assert.Equal(t, "USER_INITIATED", aws.ToString(out.StateTransitionReason))
		})
	}
}

func Test_SDKRoundTrip_Lambda_EventSourceMappingDuplicate(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	client := newTestLambdaClient(t, h)
	newImageFunctionWithVersions(t, client, "dup-fn", 0)

	in := &lambdasdk.CreateEventSourceMappingInput{
		FunctionName:   aws.String("dup-fn"),
		EventSourceArn: aws.String("arn:aws:sqs:us-east-1:000000000000:q"),
	}

	_, err := client.CreateEventSourceMapping(t.Context(), in)
	require.NoError(t, err)

	_, err = client.CreateEventSourceMapping(t.Context(), in)

	var conflict *types.ResourceConflictException

	require.ErrorAs(t, err, &conflict)
}
