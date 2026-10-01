package stepfunctions_test

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sfnsdk "github.com/aws/aws-sdk-go-v2/service/sfn"
	sfntypes "github.com/aws/aws-sdk-go-v2/service/sfn/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/stepfunctions"
	"github.com/blackbirdworks/gopherstack/services/stepfunctions/asl"
)

const (
	mockFailFn = "arn:aws:lambda:us-east-1:000000000000:function:fn-fail"
	mockEchoFn = "arn:aws:lambda:us-east-1:000000000000:function:fn-echo"
)

const mockCfgJSON = `{
  "StateMachines": {
    "mocksm": {"TestCases": {
      "Happy": {"Call": "Ok"},
      "Sad": {"Call": "Boom"},
      "Hybrid": {"Call": "Ok"},
      "Fan": {"Call": "Ok"},
      "Flaky": {"Call": "Flaky"}
    }}
  },
  "MockedResponses": {
    "Ok": {"0-9": {"Return": {"v": 7}}},
    "Boom": {"0": {"Throw": {"Error": "Custom.Boom", "Cause": "mocked failure"}}},
    "Flaky": {
      "0-1": {"Throw": {"Error": "Custom.Flaky", "Cause": "try again"}},
      "2": {"Return": {"v": 99}}
    }
  }
}`

func newMockBackend(t *testing.T) *stepfunctions.InMemoryBackend {
	t.Helper()

	cfg, err := asl.ParseMockConfig([]byte(mockCfgJSON))
	require.NoError(t, err)

	b := stepfunctions.NewInMemoryBackend()
	b.SetLambdaInvoker(jsonataLambda{})
	b.SetMockConfig(cfg)

	return b
}

func TestMockedIntegrations_SDK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		definition string
		testCase   string
		wantStatus string
		wantOutput string
		wantError  string
	}{
		{
			name: "return_with_result_selector",
			definition: `{"StartAt":"Call","States":{"Call":{"Type":"Task","Resource":"` + mockFailFn + `",` +
				`"ResultSelector":{"got.$":"$.v"},"ResultPath":"$.r","End":true}}}`,
			testCase:   "Happy",
			wantStatus: "SUCCEEDED",
			wantOutput: `{"in":1,"r":{"got":7}}`,
		},
		{
			name: "throw_caught",
			definition: `{"StartAt":"Call","States":{"Call":{"Type":"Task","Resource":"` + mockFailFn + `",` +
				`"Catch":[{"ErrorEquals":["Custom.Boom"],"ResultPath":"$.err","Next":"Done"}],"End":true},` +
				`"Done":{"Type":"Succeed"}}}`,
			testCase:   "Sad",
			wantStatus: "SUCCEEDED",
			wantOutput: `{"in":1,"err":{"Error":"Custom.Boom","Cause":"mocked failure"}}`,
		},
		{
			name: "throw_uncaught",
			definition: `{"StartAt":"Call","States":{"Call":{"Type":"Task","Resource":"` + mockFailFn +
				`","End":true}}}`,
			testCase:   "Sad",
			wantStatus: "FAILED",
			wantError:  "TaskFailed",
		},
		{
			name: "jsonata_return",
			definition: `{"QueryLanguage":"JSONata","StartAt":"Call","States":{"Call":{"Type":"Task",` +
				`"Resource":"` + mockFailFn + `","Output":"{% $states.result.v * 2 %}","End":true}}}`,
			testCase:   "Happy",
			wantStatus: "SUCCEEDED",
			wantOutput: `14`,
		},
		{
			name: "map_iterations",
			definition: `{"StartAt":"M","States":{"M":{"Type":"Map","ItemsPath":"$.items","MaxConcurrency":1,` +
				`"ItemProcessor":{"StartAt":"Call","States":{"Call":{"Type":"Task","Resource":"` + mockFailFn +
				`","End":true}}},"End":true}}}`,
			testCase:   "Fan",
			wantStatus: "SUCCEEDED",
			wantOutput: `[{"v":7},{"v":7},{"v":7}]`,
		},
		{
			name: "parallel_branches",
			definition: `{"StartAt":"P","States":{"P":{"Type":"Parallel","End":true,"Branches":[` +
				`{"StartAt":"Call","States":{"Call":{"Type":"Task","Resource":"` + mockFailFn + `","End":true}}},` +
				`{"StartAt":"Pass","States":{"Pass":{"Type":"Pass","Result":"x","End":true}}}]}}}`,
			testCase:   "Happy",
			wantStatus: "SUCCEEDED",
			wantOutput: `[{"v":7},"x"]`,
		},
		{
			name: "unmocked_state_calls_real_integration",
			definition: `{"StartAt":"Call","States":{"Call":{"Type":"Task","Resource":"` + mockFailFn +
				`","ResultPath":"$.mocked","Next":"Real"},` +
				`"Real":{"Type":"Task","Resource":"` + mockEchoFn + `","End":true}}}`,
			testCase:   "Hybrid",
			wantStatus: "SUCCEEDED",
			wantOutput: `{"in":1,"mocked":{"v":7}}`,
		},
		{
			name: "unknown_test_case",
			definition: `{"StartAt":"Call","States":{"Call":{"Type":"Task","Resource":"` + mockFailFn +
				`","End":true}}}`,
			testCase:  "Nope",
			wantError: "InvalidArn",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newJSONataClient(t, stepfunctions.NewHandler(newMockBackend(t)))
			created, err := client.CreateStateMachine(t.Context(), &sfnsdk.CreateStateMachineInput{
				Name:       aws.String("mocksm"),
				Definition: aws.String(tt.definition),
				RoleArn:    aws.String(testRoleArn),
				Type:       sfntypes.StateMachineTypeExpress,
			})
			require.NoError(t, err)

			input := `{"in":1}`
			if tt.name == "map_iterations" {
				input = `{"items":[1,2,3]}`
			}

			out, err := client.StartSyncExecution(t.Context(), &sfnsdk.StartSyncExecutionInput{
				StateMachineArn: aws.String(aws.ToString(created.StateMachineArn) + "#" + tt.testCase),
				Input:           aws.String(input),
			})

			if tt.wantStatus == "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantError)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, string(out.Status))

			if tt.wantOutput != "" {
				assert.JSONEq(t, tt.wantOutput, aws.ToString(out.Output))
			}

			if tt.wantError != "" {
				assert.Equal(t, tt.wantError, aws.ToString(out.Error))
			}
		})
	}
}

func TestMockedIntegrations_AsyncStartExecution(t *testing.T) {
	t.Parallel()

	client := newJSONataClient(t, stepfunctions.NewHandler(newMockBackend(t)))
	def := `{"StartAt":"Call","States":{"Call":{"Type":"Task","Resource":"` + mockFailFn + `","End":true}}}`

	created, err := client.CreateStateMachine(t.Context(), &sfnsdk.CreateStateMachineInput{
		Name: aws.String("mocksm"), Definition: aws.String(def), RoleArn: aws.String(testRoleArn),
	})
	require.NoError(t, err)

	started, err := client.StartExecution(t.Context(), &sfnsdk.StartExecutionInput{
		StateMachineArn: aws.String(aws.ToString(created.StateMachineArn) + "#Happy"),
		Input:           aws.String(`{}`),
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		d, derr := client.DescribeExecution(t.Context(), &sfnsdk.DescribeExecutionInput{
			ExecutionArn: started.ExecutionArn,
		})

		return derr == nil && d.Status == sfntypes.ExecutionStatusSucceeded &&
			assert.JSONEq(t, `{"v":7}`, aws.ToString(d.Output))
	}, 5*time.Second, 20*time.Millisecond)
}

func TestMockedIntegrations_RangedRetries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		maxAttempt string
		wantStatus string
		wantOutput string
	}{
		{name: "retries_then_returns", maxAttempt: "3", wantStatus: "SUCCEEDED", wantOutput: `{"v":99}`},
		{name: "retries_exhausted", maxAttempt: "1", wantStatus: "FAILED"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := newMockBackend(t)
				def := `{"StartAt":"Call","States":{"Call":{"Type":"Task","Resource":"` + mockFailFn + `",` +
					`"Retry":[{"ErrorEquals":["Custom.Flaky"],"IntervalSeconds":1,"MaxAttempts":` +
					tt.maxAttempt + `,"BackoffRate":2}],"End":true}}}`

				sm, err := b.CreateStateMachine(t.Context(), "mocksm", def, testRoleArn, "EXPRESS")
				require.NoError(t, err)

				res, err := b.StartSyncExecution(sm.StateMachineArn+"#Flaky", "", `{}`)
				require.NoError(t, err)
				assert.Equal(t, tt.wantStatus, res.Status)

				if tt.wantOutput != "" {
					assert.JSONEq(t, tt.wantOutput, res.Output)
				}
			})
		})
	}
}

func TestMockConfig_Invalid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  string
	}{
		{name: "not_json", cfg: `{`},
		{
			name: "undefined_response",
			cfg:  `{"StateMachines":{"a":{"TestCases":{"t":{"S":"Missing"}}}},"MockedResponses":{}}`,
		},
		{name: "bad_key", cfg: `{"MockedResponses":{"r":{"x":{"Return":1}}}}`},
		{name: "reversed_range", cfg: `{"MockedResponses":{"r":{"3-1":{"Return":1}}}}`},
		{name: "overlap", cfg: `{"MockedResponses":{"r":{"0-2":{"Return":1},"2":{"Return":2}}}}`},
		{name: "both_return_and_throw", cfg: `{"MockedResponses":{"r":{"0":{"Return":1,"Throw":{"Error":"E"}}}}}`},
		{name: "neither", cfg: `{"MockedResponses":{"r":{"0":{}}}}`},
		{name: "throw_without_error", cfg: `{"MockedResponses":{"r":{"0":{"Throw":{"Cause":"c"}}}}}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := asl.ParseMockConfig([]byte(tt.cfg))
			require.ErrorIs(t, err, asl.ErrMockConfigInvalid)
		})
	}

	_, err := asl.LoadMockConfig("/nonexistent/mock.json")
	require.ErrorIs(t, err, asl.ErrMockConfigInvalid)
}
