package stepfunctions_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sfnsdk "github.com/aws/aws-sdk-go-v2/service/sfn"
	sfntypes "github.com/aws/aws-sdk-go-v2/service/sfn/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/stepfunctions"
)

func TestTestState_MockAndOutcomeStatus(t *testing.T) {
	t.Parallel()

	const (
		task = `{"Type":"Task","Resource":"arn:aws:states:::lambda:invoke","Next":"After"`
		boom = "Custom.Boom"
	)

	errMock := &sfntypes.MockInput{ErrorOutput: &sfntypes.MockErrorOutput{
		Error: aws.String(boom), Cause: aws.String("it broke"),
	}}

	tests := []struct {
		mock       *sfntypes.MockInput
		config     *sfntypes.TestStateConfiguration
		name       string
		definition string
		wantStatus sfntypes.TestExecutionStatus
		wantError  string
		wantNext   string
		wantOutput string
	}{
		{
			name:       "mock result succeeds",
			definition: task + `}`,
			mock:       &sfntypes.MockInput{Result: aws.String(`{"ok":true}`)},
			wantStatus: sfntypes.TestExecutionStatusSucceeded,
			wantNext:   "After",
			wantOutput: `{"ok":true}`,
		},
		{
			name:       "mock error with matching retry is retriable",
			definition: task + `,"Retry":[{"ErrorEquals":["Custom.Boom"],"MaxAttempts":2}]}`,
			mock:       errMock,
			wantStatus: sfntypes.TestExecutionStatusRetriable,
			wantError:  boom,
		},
		{
			name: "exhausted retry falls to catch",
			definition: task + `,"Retry":[{"ErrorEquals":["Custom.Boom"],"MaxAttempts":2}],` +
				`"Catch":[{"ErrorEquals":["States.ALL"],"Next":"Recover","ResultPath":"$.err"}]}`,
			mock:       errMock,
			config:     &sfntypes.TestStateConfiguration{RetrierRetryCount: aws.Int32(2)},
			wantStatus: sfntypes.TestExecutionStatusCaughtError,
			wantError:  boom,
			wantNext:   "Recover",
			wantOutput: `{"err":{"Error":"Custom.Boom","Cause":"it broke"}}`,
		},
		{
			name:       "catch only",
			definition: task + `,"Catch":[{"ErrorEquals":["Custom.Boom"],"Next":"Recover"}]}`,
			mock:       errMock,
			wantStatus: sfntypes.TestExecutionStatusCaughtError,
			wantError:  boom,
			wantNext:   "Recover",
		},
		{
			name:       "unmatched error fails",
			definition: task + `,"Catch":[{"ErrorEquals":["Other"],"Next":"Recover"}]}`,
			mock:       errMock,
			wantStatus: sfntypes.TestExecutionStatusFailed,
			wantError:  boom,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newJSONataClient(t, stepfunctions.NewHandler(stepfunctions.NewInMemoryBackend()))

			out, err := client.TestState(t.Context(), &sfnsdk.TestStateInput{
				Definition:         aws.String(tt.definition),
				Input:              aws.String(`{}`),
				RoleArn:            aws.String(testRoleArn),
				Mock:               tt.mock,
				StateConfiguration: tt.config,
			})
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, out.Status)
			assert.Equal(t, tt.wantError, aws.ToString(out.Error))
			assert.Equal(t, tt.wantNext, aws.ToString(out.NextState))

			if tt.wantOutput != "" {
				assert.JSONEq(t, tt.wantOutput, aws.ToString(out.Output))
			}
		})
	}
}

func TestTestState_MockAndContextValidation(t *testing.T) {
	t.Parallel()

	const taskDef = `{"Type":"Task","Resource":"arn:aws:states:::lambda:invoke","End":true,` +
		`"Parameters":{"who.$":"$$.Custom.Who"}}`

	tests := []struct {
		mock       *sfntypes.MockInput
		context    *string
		name       string
		definition string
		wantErr    bool
	}{
		{name: "context without mock", definition: taskDef, context: aws.String(`{}`), wantErr: true},
		{
			name: "mock on pass", definition: `{"Type":"Pass","End":true}`,
			mock: &sfntypes.MockInput{Result: aws.String(`1`)}, wantErr: true,
		},
		{name: "mock with both", definition: taskDef, wantErr: true, mock: &sfntypes.MockInput{
			Result: aws.String(`1`), ErrorOutput: &sfntypes.MockErrorOutput{Error: aws.String("E")},
		}},
		{
			name:       "mock result not json",
			definition: taskDef,
			mock:       &sfntypes.MockInput{Result: aws.String(`{`)},
			wantErr:    true,
		},
		{
			name: "context is used", definition: taskDef,
			mock:    &sfntypes.MockInput{Result: aws.String(`1`)},
			context: aws.String(`{"Custom":{"Who":"ada"}}`),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newJSONataClient(t, stepfunctions.NewHandler(stepfunctions.NewInMemoryBackend()))

			out, err := client.TestState(t.Context(), &sfnsdk.TestStateInput{
				Definition:      aws.String(tt.definition),
				Input:           aws.String(`{}`),
				RoleArn:         aws.String(testRoleArn),
				Mock:            tt.mock,
				Context:         tt.context,
				InspectionLevel: sfntypes.InspectionLevelDebug,
			})
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, sfntypes.TestExecutionStatusSucceeded, out.Status)
			require.NotNil(t, out.InspectionData)
			assert.JSONEq(t, `{"who":"ada"}`, aws.ToString(out.InspectionData.AfterParameters))
		})
	}
}
