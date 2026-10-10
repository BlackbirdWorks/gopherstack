package stepfunctions_test

import (
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sfnsdk "github.com/aws/aws-sdk-go-v2/service/sfn"
	sfntypes "github.com/aws/aws-sdk-go-v2/service/sfn/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/stepfunctions"
)

func TestSDKCreateStateMachineDefinitionValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		def  string
		want string
	}{
		{
			name: "missing_next_target",
			def:  `{"StartAt":"A","States":{"A":{"Type":"Pass","Next":"B"}}}`,
			want: "missing state",
		},
		{
			name: "missing_catch_target",
			def: `{"StartAt":"A","States":{"A":{"Type":"Task","Resource":"arn:aws:states:::lambda:invoke",` +
				`"Catch":[{"ErrorEquals":["States.ALL"],"Next":"Nope"}],"End":true}}}`,
			want: "missing state",
		},
		{
			name: "missing_choice_default",
			def:  `{"StartAt":"C","States":{"C":{"Type":"Choice","Choices":[],"Default":"Nope"}}}`,
			want: "missing state",
		},
		{
			name: "no_next_or_end",
			def:  `{"StartAt":"A","States":{"A":{"Type":"Pass"}}}`,
			want: "either Next or End",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newSFNSDKClient(t, stepfunctions.NewHandler(stepfunctions.NewInMemoryBackend()))

			_, err := client.CreateStateMachine(t.Context(), &sfnsdk.CreateStateMachineInput{
				Name:       aws.String("sm"),
				Definition: aws.String(tt.def),
				RoleArn:    aws.String("arn:aws:iam::000000000000:role/r"),
			})

			var invalid *sfntypes.InvalidDefinition
			require.ErrorAs(t, err, &invalid)
			assert.Contains(t, aws.ToString(invalid.Message), tt.want)
			assert.False(t, strings.HasPrefix(aws.ToString(invalid.Message), "InvalidDefinition"))
		})
	}
}

func TestSDKStartExecutionValidation(t *testing.T) {
	t.Parallel()

	const def = `{"StartAt":"A","States":{"A":{"Type":"Pass","End":true}}}`

	tests := []struct {
		name  string
		exec  string
		input string
		code  string
	}{
		{name: "bad_json_input", input: "notjson", code: "InvalidExecutionInput"},
		{name: "space_in_name", exec: "a b", code: "InvalidName"},
		{name: "slash_in_name", exec: "a/b", code: "InvalidName"},
		{name: "bracket_in_name", exec: "a[1]", code: "InvalidName"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newSFNSDKClient(t, stepfunctions.NewHandler(stepfunctions.NewInMemoryBackend()))
			sm, err := client.CreateStateMachine(t.Context(), &sfnsdk.CreateStateMachineInput{
				Name:       aws.String("sm"),
				Definition: aws.String(def),
				RoleArn:    aws.String("arn:aws:iam::000000000000:role/r"),
			})
			require.NoError(t, err)

			in := &sfnsdk.StartExecutionInput{StateMachineArn: sm.StateMachineArn}
			if tt.exec != "" {
				in.Name = aws.String(tt.exec)
			}

			if tt.input != "" {
				in.Input = aws.String(tt.input)
			}

			_, err = client.StartExecution(t.Context(), in)

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, tt.code, apiErr.ErrorCode())
			assert.False(t, strings.HasPrefix(apiErr.ErrorMessage(), tt.code+":"))
		})
	}
}

func TestSDKListTokensAndStatusFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		call func(c *sfnsdk.Client, smArn *string) error
		code string
	}{
		{
			name: "list_state_machines_bad_token",
			call: func(c *sfnsdk.Client, _ *string) error {
				_, err := c.ListStateMachines(
					t.Context(),
					&sfnsdk.ListStateMachinesInput{NextToken: aws.String("garbage")},
				)

				return err
			},
			code: "InvalidToken",
		},
		{
			name: "list_executions_bad_token",
			call: func(c *sfnsdk.Client, smArn *string) error {
				_, err := c.ListExecutions(t.Context(), &sfnsdk.ListExecutionsInput{
					StateMachineArn: smArn, NextToken: aws.String("zzz"),
				})

				return err
			},
			code: "InvalidToken",
		},
		{
			name: "list_executions_bad_status",
			call: func(c *sfnsdk.Client, smArn *string) error {
				_, err := c.ListExecutions(t.Context(), &sfnsdk.ListExecutionsInput{
					StateMachineArn: smArn, StatusFilter: sfntypes.ExecutionStatus("BOGUS"),
				})

				return err
			},
			code: "ValidationException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newSFNSDKClient(t, stepfunctions.NewHandler(stepfunctions.NewInMemoryBackend()))
			sm, err := client.CreateStateMachine(t.Context(), &sfnsdk.CreateStateMachineInput{
				Name:       aws.String("sm"),
				Definition: aws.String(`{"StartAt":"A","States":{"A":{"Type":"Pass","End":true}}}`),
				RoleArn:    aws.String("arn:aws:iam::000000000000:role/r"),
			})
			require.NoError(t, err)

			var apiErr smithy.APIError
			require.ErrorAs(t, tt.call(client, sm.StateMachineArn), &apiErr)
			assert.Equal(t, tt.code, apiErr.ErrorCode())
		})
	}
}

func TestSDKStopDateNotBeforeStartDate(t *testing.T) {
	t.Parallel()

	client := newSFNSDKClient(t, stepfunctions.NewHandler(stepfunctions.NewInMemoryBackend()))
	sm, err := client.CreateStateMachine(t.Context(), &sfnsdk.CreateStateMachineInput{
		Name:       aws.String("sm"),
		Definition: aws.String(`{"StartAt":"A","States":{"A":{"Type":"Pass","End":true}}}`),
		RoleArn:    aws.String("arn:aws:iam::000000000000:role/r"),
	})
	require.NoError(t, err)

	started, err := client.StartExecution(t.Context(), &sfnsdk.StartExecutionInput{StateMachineArn: sm.StateMachineArn})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		d, derr := client.DescribeExecution(
			t.Context(),
			&sfnsdk.DescribeExecutionInput{ExecutionArn: started.ExecutionArn},
		)

		return derr == nil && d.StopDate != nil && !d.StopDate.Before(*d.StartDate)
	}, 5*time.Second, 5*time.Millisecond)
}
