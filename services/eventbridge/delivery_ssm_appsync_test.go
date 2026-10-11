package eventbridge_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
)

func TestBuildSSMRequests(t *testing.T) {
	t.Parallel()

	rc := &eventbridge.RunCommandParameters{RunCommandTargets: []eventbridge.RunCommandTarget{
		{Key: "InstanceIds", Values: []string{"i-1"}},
	}}

	tests := []struct {
		name    string
		target  eventbridge.Target
		payload string
		wantCmd eventbridge.SSMCommandRequest
		wantErr bool
	}{
		{
			name:    "document_from_arn",
			target:  eventbridge.Target{Arn: "arn:aws:ssm:eu-west-1:1:document/Doc", RunCommandParameters: rc},
			payload: `{"Parameters":{"commands":["a"],"one":"b"}}`,
			wantCmd: eventbridge.SSMCommandRequest{
				DocumentName: "Doc", Region: "eu-west-1", Targets: rc.RunCommandTargets,
				Parameters: map[string][]string{"commands": {"a"}, "one": {"b"}},
			},
		},
		{
			name: "document_from_input",
			target: eventbridge.Target{
				Arn:                  "arn:aws:ssm:us-east-1:1:document/AWS-RunShellScript",
				RunCommandParameters: rc,
			},
			payload: `{"DocumentName":"Mine","DocumentVersion":"2"}`,
			wantCmd: eventbridge.SSMCommandRequest{
				DocumentName: "Mine", DocumentVersion: "2", Region: "us-east-1", Targets: rc.RunCommandTargets,
				Parameters: map[string][]string{},
			},
		},
		{
			name:    "no_run_command_parameters",
			target:  eventbridge.Target{Arn: "arn:aws:ssm:us-east-1:1:document/Doc"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := eventbridge.BuildSSMCommandRequest(&tt.target, tt.payload)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantCmd, got)
		})
	}
}

func TestBuildSSMAutomationRequest(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		arn     string
		payload string
		want    eventbridge.SSMAutomationRequest
	}{
		{
			name:    "payload_is_parameters",
			arn:     "arn:aws:ssm:us-east-1:1:automation-definition/Run:3",
			payload: `{"InstanceId":["i-1"],"Dry":"x","skip":{"a":1}}`,
			want: eventbridge.SSMAutomationRequest{
				DocumentName: "Run", DocumentVersion: "3", Region: "us-east-1",
				Parameters: map[string][]string{"InstanceId": {"i-1"}, "Dry": {"x"}},
			},
		},
		{
			name:    "parameters_key",
			arn:     "arn:aws:ssm:us-east-1:1:automation-definition/Run:$DEFAULT",
			payload: `{"Parameters":{"A":"b"}}`,
			want: eventbridge.SSMAutomationRequest{
				DocumentName: "Run", DocumentVersion: "$DEFAULT", Region: "us-east-1",
				Parameters: map[string][]string{"A": {"b"}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(
				t,
				tt.want,
				eventbridge.BuildSSMAutomationRequest(&eventbridge.Target{Arn: tt.arn}, tt.payload),
			)
		})
	}
}
