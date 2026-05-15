package stepfunctions_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/stepfunctions"
)

const validRoleARN = "arn:aws:iam::000000000000:role/sfn-test"

const minimalDefinition = `{"StartAt":"S","States":{"S":{"Type":"Pass","End":true}}}`

func TestSetStateMachineConfigurations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(*stepfunctions.InMemoryBackend) string
		tracing     *stepfunctions.TracingConfiguration
		logging     *stepfunctions.LoggingConfiguration
		name        string
		wantTracing bool
		wantLogging bool
		wantErr     bool
	}{
		{
			name: "set_both_configs",
			setup: func(b *stepfunctions.InMemoryBackend) string {
				sm, err := b.CreateStateMachine("sm-both", minimalDefinition, validRoleARN, "STANDARD")
				require.NoError(t, err)

				return sm.StateMachineArn
			},
			tracing:     &stepfunctions.TracingConfiguration{Enabled: true},
			logging:     &stepfunctions.LoggingConfiguration{Level: "ALL", IncludeExecutionData: true},
			wantTracing: true,
			wantLogging: true,
		},
		{
			name: "set_only_tracing",
			setup: func(b *stepfunctions.InMemoryBackend) string {
				sm, err := b.CreateStateMachine("sm-tracing", minimalDefinition, validRoleARN, "STANDARD")
				require.NoError(t, err)

				return sm.StateMachineArn
			},
			tracing:     &stepfunctions.TracingConfiguration{Enabled: true},
			wantTracing: true,
		},
		{
			name: "missing_state_machine_returns_error",
			setup: func(_ *stepfunctions.InMemoryBackend) string {
				return "arn:aws:states:us-east-1:000000000000:stateMachine:none"
			},
			tracing: &stepfunctions.TracingConfiguration{Enabled: true},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := stepfunctions.NewInMemoryBackend()
			arn := tt.setup(backend)
			err := backend.SetStateMachineConfigurations(arn, tt.tracing, tt.logging, nil)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			sm, err := backend.DescribeStateMachine(arn)
			require.NoError(t, err)

			if tt.wantTracing {
				require.NotNil(t, sm.TracingConfiguration)
				assert.True(t, sm.TracingConfiguration.Enabled)
			}

			if tt.wantLogging {
				require.NotNil(t, sm.LoggingConfiguration)
				assert.Equal(t, "ALL", sm.LoggingConfiguration.Level)
				assert.True(t, sm.LoggingConfiguration.IncludeExecutionData)
			}
		})
	}
}
