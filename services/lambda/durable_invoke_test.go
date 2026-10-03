package lambda_test

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	lambdasdk "github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRealClient_DurableInvoke drives Invoke's durable-execution wiring
// (PARITY.md durable_execution items_still_open, closed this pass): the
// X-Amz-Durable-Execution-Name request header / X-Amz-Durable-Execution-Arn
// response header, and the DurableExecution.FunctionARN/Version assignment
// that ListDurableExecutionsByFunction's FunctionName/Qualifier filters
// depend on. This backend has no Docker runtime configured
// (newInMemoryHandler), so a real (non-DryRun) invocation always fails with
// ServiceException -- exactly like every other lambda unit test that
// exercises Invoke without a mocked container -- but a durable execution is
// recorded before that failure, since real AWS starts the execution first
// and only then invokes the function body.
func TestRealClient_DurableInvoke(t *testing.T) {
	t.Parallel()

	createDurableFn := func(t *testing.T, client *lambdasdk.Client, name string) {
		t.Helper()

		_, err := client.CreateFunction(t.Context(), &lambdasdk.CreateFunctionInput{
			FunctionName:  aws.String(name),
			PackageType:   types.PackageTypeImage,
			Code:          &types.FunctionCode{ImageUri: aws.String("ecr/myapp:latest")},
			Role:          aws.String("arn:aws:iam:::role/r"),
			DurableConfig: &types.DurableConfig{ExecutionTimeout: aws.Int32(3600)},
		})
		require.NoError(t, err)
	}

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "invoke assigns function arn and version",
			run: func(t *testing.T) {
				t.Helper()

				h, _ := newInMemoryHandler(t)
				client := newTestLambdaClient(t, h)
				createDurableFn(t, client, "durinv-basic-fn")

				// DurableExecutionName pins retries to the SAME execution: without
				// it, each of the real SDK client's automatic retries of the
				// underlying ServiceException would start its own execution (real
				// AWS behavior too -- "no name" always starts a new execution, per
				// the idempotency table), making the assertion below flaky.
				_, err := client.Invoke(t.Context(), &lambdasdk.InvokeInput{
					FunctionName:         aws.String("durinv-basic-fn"),
					Qualifier:            aws.String("$LATEST"),
					DurableExecutionName: aws.String("basic-exec"),
					Payload:              []byte(`{"x":1}`),
				})
				require.Error(t, err) // no Docker runtime configured

				listOut, err := client.ListDurableExecutionsByFunction(
					t.Context(),
					&lambdasdk.ListDurableExecutionsByFunctionInput{FunctionName: aws.String("durinv-basic-fn")},
				)
				require.NoError(t, err)
				require.Len(t, listOut.DurableExecutions, 1)

				ex := listOut.DurableExecutions[0]
				assert.Equal(t,
					"arn:aws:lambda:us-east-1:000000000000:function:durinv-basic-fn:$LATEST",
					aws.ToString(ex.FunctionArn),
				)
				assert.Contains(t, aws.ToString(ex.DurableExecutionArn), "/durable-execution/")
				assert.Equal(t, types.ExecutionStatusRunning, ex.Status)
			},
		},
		{
			name: "idempotent replay with identical payload does not re-invoke",
			run: func(t *testing.T) {
				t.Helper()

				h, _ := newInMemoryHandler(t)
				client := newTestLambdaClient(t, h)
				createDurableFn(t, client, "durinv-idem-fn")

				payload := []byte(`{"orderId":"123"}`)

				_, err := client.Invoke(t.Context(), &lambdasdk.InvokeInput{
					FunctionName:         aws.String("durinv-idem-fn"),
					Qualifier:            aws.String("$LATEST"),
					DurableExecutionName: aws.String("idem-exec"),
					Payload:              payload,
				})
				require.Error(t, err)

				listOut, err := client.ListDurableExecutionsByFunction(
					t.Context(),
					&lambdasdk.ListDurableExecutionsByFunctionInput{FunctionName: aws.String("durinv-idem-fn")},
				)
				require.NoError(t, err)
				require.Len(t, listOut.DurableExecutions, 1)
				arn := aws.ToString(listOut.DurableExecutions[0].DurableExecutionArn)

				// Close the execution out-of-band (this backend has no Docker
				// runtime to complete it for real) so the replay below hits a
				// CLOSED execution, per the documented idempotency table.
				_, err = client.StopDurableExecution(t.Context(), &lambdasdk.StopDurableExecutionInput{
					DurableExecutionArn: aws.String(arn),
				})
				require.NoError(t, err)

				// Same name + identical payload against a CLOSED execution: real
				// AWS returns the closed execution's result instead of starting a
				// duplicate. This succeeds even with no Docker runtime configured,
				// proving the function was NOT invoked again.
				out, err := client.Invoke(t.Context(), &lambdasdk.InvokeInput{
					FunctionName:         aws.String("durinv-idem-fn"),
					Qualifier:            aws.String("$LATEST"),
					DurableExecutionName: aws.String("idem-exec"),
					Payload:              payload,
				})
				require.NoError(t, err)
				assert.Equal(t, "Unhandled", aws.ToString(out.FunctionError))
				assert.Equal(t, arn, aws.ToString(out.DurableExecutionArn))

				listOut2, err := client.ListDurableExecutionsByFunction(
					t.Context(),
					&lambdasdk.ListDurableExecutionsByFunctionInput{FunctionName: aws.String("durinv-idem-fn")},
				)
				require.NoError(t, err)
				assert.Len(t, listOut2.DurableExecutions, 1, "reuse must not create a duplicate execution")
			},
		},
		{
			name: "differing payload with same name conflicts",
			run: func(t *testing.T) {
				t.Helper()

				h, _ := newInMemoryHandler(t)
				client := newTestLambdaClient(t, h)
				createDurableFn(t, client, "durinv-conflict-fn")

				_, err := client.Invoke(t.Context(), &lambdasdk.InvokeInput{
					FunctionName:         aws.String("durinv-conflict-fn"),
					Qualifier:            aws.String("$LATEST"),
					DurableExecutionName: aws.String("conflict-exec"),
					Payload:              []byte(`{"a":1}`),
				})
				require.Error(t, err) // no Docker runtime configured

				_, err = client.Invoke(t.Context(), &lambdasdk.InvokeInput{
					FunctionName:         aws.String("durinv-conflict-fn"),
					Qualifier:            aws.String("$LATEST"),
					DurableExecutionName: aws.String("conflict-exec"),
					Payload:              []byte(`{"a":2}`),
				})
				require.Error(t, err)

				var apiErr *types.DurableExecutionAlreadyStartedException
				require.ErrorAs(t, err, &apiErr, "expected DurableExecutionAlreadyStartedException, got %v", err)
			},
		},
		{
			name: "dry run does not start an execution",
			run: func(t *testing.T) {
				t.Helper()

				h, _ := newInMemoryHandler(t)
				client := newTestLambdaClient(t, h)
				createDurableFn(t, client, "durinv-dryrun-fn")

				_, err := client.Invoke(t.Context(), &lambdasdk.InvokeInput{
					FunctionName:   aws.String("durinv-dryrun-fn"),
					Qualifier:      aws.String("$LATEST"),
					InvocationType: types.InvocationTypeDryRun,
					Payload:        []byte(`{}`),
				})
				require.NoError(t, err)

				listOut, err := client.ListDurableExecutionsByFunction(
					t.Context(),
					&lambdasdk.ListDurableExecutionsByFunctionInput{FunctionName: aws.String("durinv-dryrun-fn")},
				)
				require.NoError(t, err)
				assert.Empty(t, listOut.DurableExecutions)
			},
		},
		{
			name: "qualifier filters by resolved version",
			run: func(t *testing.T) {
				t.Helper()

				h, _ := newInMemoryHandler(t)
				client := newTestLambdaClient(t, h)
				createDurableFn(t, client, "durinv-qual-fn")

				// DurableExecutionName pins each real-client retry to the same
				// execution (see the "invoke assigns function arn and version"
				// case's comment) so the counts asserted below are stable.
				_, err := client.Invoke(t.Context(), &lambdasdk.InvokeInput{
					FunctionName:         aws.String("durinv-qual-fn"),
					Qualifier:            aws.String("$LATEST"),
					DurableExecutionName: aws.String("qual-latest-exec"),
					Payload:              []byte(`{}`),
				})
				require.Error(t, err)

				pubOut, err := client.PublishVersion(t.Context(), &lambdasdk.PublishVersionInput{
					FunctionName: aws.String("durinv-qual-fn"),
				})
				require.NoError(t, err)

				_, err = client.Invoke(t.Context(), &lambdasdk.InvokeInput{
					FunctionName:         aws.String("durinv-qual-fn"),
					Qualifier:            pubOut.Version,
					DurableExecutionName: aws.String("qual-v1-exec"),
					Payload:              []byte(`{}`),
				})
				require.Error(t, err)

				allOut, err := client.ListDurableExecutionsByFunction(
					t.Context(),
					&lambdasdk.ListDurableExecutionsByFunctionInput{FunctionName: aws.String("durinv-qual-fn")},
				)
				require.NoError(t, err)
				require.Len(t, allOut.DurableExecutions, 2, "no Qualifier: executions across every version")

				latestOut, err := client.ListDurableExecutionsByFunction(
					t.Context(),
					&lambdasdk.ListDurableExecutionsByFunctionInput{
						FunctionName: aws.String("durinv-qual-fn"),
						Qualifier:    aws.String("$LATEST"),
					},
				)
				require.NoError(t, err)
				require.Len(t, latestOut.DurableExecutions, 1)
				assert.True(t, strings.HasSuffix(aws.ToString(latestOut.DurableExecutions[0].FunctionArn), ":$LATEST"))

				versionOut, err := client.ListDurableExecutionsByFunction(
					t.Context(),
					&lambdasdk.ListDurableExecutionsByFunctionInput{
						FunctionName: aws.String("durinv-qual-fn"),
						Qualifier:    pubOut.Version,
					},
				)
				require.NoError(t, err)
				require.Len(t, versionOut.DurableExecutions, 1)
				assert.True(t, strings.HasSuffix(
					aws.ToString(versionOut.DurableExecutions[0].FunctionArn), ":"+aws.ToString(pubOut.Version),
				))
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

// TestInvoke_DurableFunctionRequiresQualifier guards the qualified-ARN requirement.
// docs.aws.amazon.com/lambda/latest/dg/durable-invoking.html#durable-invoking-qualified-arns.
func TestInvoke_DurableFunctionRequiresQualifier(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		qualifier    string
		durable      bool
		wantRejected bool
	}{
		{name: "durable function, no qualifier is rejected", durable: true, wantRejected: true},
		{name: "durable function, explicit $LATEST is accepted", durable: true, qualifier: "$LATEST"},
		{name: "standard function, no qualifier is accepted"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newInMemoryHandler(t)
			client := newTestLambdaClient(t, h)

			fnName := "qual-req-" + strings.ReplaceAll(tt.name, " ", "-")

			createInput := &lambdasdk.CreateFunctionInput{
				FunctionName: aws.String(fnName),
				PackageType:  types.PackageTypeImage,
				Code:         &types.FunctionCode{ImageUri: aws.String("ecr/myapp:latest")},
				Role:         aws.String("arn:aws:iam:::role/r"),
			}
			if tt.durable {
				createInput.DurableConfig = &types.DurableConfig{ExecutionTimeout: aws.Int32(3600)}
			}

			_, err := client.CreateFunction(t.Context(), createInput)
			require.NoError(t, err)

			invokeInput := &lambdasdk.InvokeInput{FunctionName: aws.String(fnName), Payload: []byte(`{}`)}
			if tt.qualifier != "" {
				invokeInput.Qualifier = aws.String(tt.qualifier)
			}

			_, err = client.Invoke(t.Context(), invokeInput)
			require.Error(t, err) // no Docker runtime configured either way

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)

			if tt.wantRejected {
				assert.Equal(t, "InvalidParameterValueException", apiErr.ErrorCode())
			} else {
				assert.NotEqual(t, "InvalidParameterValueException", apiErr.ErrorCode())
			}
		})
	}
}
