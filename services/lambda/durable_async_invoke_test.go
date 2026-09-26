package lambda_test

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	lambdasdk "github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	"github.com/blackbirdworks/gopherstack/services/lambda"
)

// TestAsyncDurableInvoke_RecordsCompletion checks an Event invocation of a durable
// function leaves RUNNING once it finishes (docs: lambda/latest/dg/durable-invoking.html).
func TestAsyncDurableInvoke_RecordsCompletion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		respond    func(t *testing.T, port int, requestID string)
		wantStatus types.ExecutionStatus
		wantEvent  types.EventType
		name       string
		fnName     string
		portBase   int
		maxRetries int32
	}{
		{
			name:       "success",
			fnName:     "durasync-ok",
			portBase:   21200,
			maxRetries: 0,
			respond: func(t *testing.T, port int, requestID string) {
				t.Helper()
				simulateContainerResponse(t, port, requestID, `{"ok":true}`)
			},
			wantStatus: types.ExecutionStatusSucceeded,
			wantEvent:  types.EventTypeExecutionSucceeded,
		},
		{
			name:       "failure_after_retries_exhausted",
			fnName:     "durasync-fail",
			portBase:   21300,
			maxRetries: 0, // no retries: the first (and only) failure is terminal
			respond: func(t *testing.T, port int, requestID string) {
				t.Helper()
				simulateContainerError(t, port, requestID, `{"errorMessage":"boom"}`)
			},
			wantStatus: types.ExecutionStatusFailed,
			wantEvent:  types.EventTypeExecutionFailed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			portRange := [2]int{tt.portBase, tt.portBase + 50}
			pa, err := portalloc.New(portRange[0], portRange[1])
			require.NoError(t, err)

			bk := lambda.NewInMemoryBackend(
				newMockDockerClient(), pa, lambda.DefaultSettings(), "000000000000", "us-east-1",
			)
			closeBackend(t, bk)

			h := lambda.NewHandler(bk)
			h.DefaultRegion = "us-east-1"
			h.AccountID = "000000000000"

			client := newTestLambdaClient(t, h)

			fnName := tt.fnName

			_, err = client.CreateFunction(t.Context(), &lambdasdk.CreateFunctionInput{
				FunctionName:  aws.String(fnName),
				PackageType:   types.PackageTypeImage,
				Code:          &types.FunctionCode{ImageUri: aws.String("myimage:latest")},
				Role:          aws.String("arn:aws:iam:::role/r"),
				DurableConfig: &types.DurableConfig{ExecutionTimeout: aws.Int32(3600)},
			})
			require.NoError(t, err)

			_, err = client.PutFunctionEventInvokeConfig(t.Context(), &lambdasdk.PutFunctionEventInvokeConfigInput{
				FunctionName:         aws.String(fnName),
				MaximumRetryAttempts: aws.Int32(tt.maxRetries),
			})
			require.NoError(t, err)

			invokeErrCh := make(chan error, 1)
			var durableARN string

			go func() {
				out, invokeErr := client.Invoke(t.Context(), &lambdasdk.InvokeInput{
					FunctionName:         aws.String(fnName),
					InvocationType:       types.InvocationTypeEvent,
					DurableExecutionName: aws.String("exec-" + tt.name),
					Payload:              []byte(`{}`),
				})
				if out != nil {
					durableARN = aws.ToString(out.DurableExecutionArn)
				}
				invokeErrCh <- invokeErr
			}()

			require.NoError(t, <-invokeErrCh)
			require.NotEmpty(t, durableARN)

			httpClient := newHTTPClient(t, 200*time.Millisecond)

			var (
				runtimePort int
				requestID   string
			)

			require.Eventually(t, func() bool {
				for p := portRange[0]; p < portRange[1]; p++ {
					req, reqErr := http.NewRequestWithContext(
						t.Context(), http.MethodGet,
						fmt.Sprintf("http://127.0.0.1:%d/2018-06-01/runtime/invocation/next", p), nil,
					)
					if reqErr != nil {
						continue
					}

					resp, doErr := httpClient.Do(req)
					if doErr != nil || resp == nil {
						continue
					}

					id := resp.Header.Get("Lambda-Runtime-Aws-Request-Id")
					resp.Body.Close()

					if id != "" {
						runtimePort, requestID = p, id

						return true
					}
				}

				return false
			}, 4*time.Second, 50*time.Millisecond, "async invocation was never queued to a runtime")

			tt.respond(t, runtimePort, requestID)

			require.Eventually(t, func() bool {
				out, getErr := client.GetDurableExecution(t.Context(), &lambdasdk.GetDurableExecutionInput{
					DurableExecutionArn: aws.String(durableARN),
				})

				return getErr == nil && out.Status == tt.wantStatus
			}, 4*time.Second, 50*time.Millisecond, "durable execution never left RUNNING")

			histOut, err := client.GetDurableExecutionHistory(t.Context(), &lambdasdk.GetDurableExecutionHistoryInput{
				DurableExecutionArn: aws.String(durableARN),
			})
			require.NoError(t, err)

			var sawEvent bool
			for _, ev := range histOut.Events {
				if ev.EventType == tt.wantEvent {
					sawEvent = true
				}
			}
			assert.True(t, sawEvent, "expected a %s history event", tt.wantEvent)
		})
	}
}
