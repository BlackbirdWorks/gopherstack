package ecs

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRegisterTaskDefinition_RestartPolicyAndLogModeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		policy  *ContainerRestartPolicy
		log     *LogConfiguration
		wantErr bool
	}{
		{name: "defaults", policy: &ContainerRestartPolicy{Enabled: true}},
		{name: "period min", policy: &ContainerRestartPolicy{Enabled: true, RestartAttemptPeriod: 60}},
		{name: "period max", policy: &ContainerRestartPolicy{Enabled: true, RestartAttemptPeriod: 1800}},
		{name: "period below min", policy: &ContainerRestartPolicy{Enabled: true, RestartAttemptPeriod: 59}, wantErr: true},
		{name: "period above max", policy: &ContainerRestartPolicy{Enabled: true, RestartAttemptPeriod: 1801}, wantErr: true},
		{
			name:    "too many ignored codes",
			policy:  &ContainerRestartPolicy{Enabled: true, IgnoredExitCodes: make([]int, 51)},
			wantErr: true,
		},
		{
			name:   "fifty ignored codes",
			policy: &ContainerRestartPolicy{Enabled: true, IgnoredExitCodes: make([]int, 50)},
		},
		{name: "log mode ok", log: &LogConfiguration{LogDriver: "json-file", Options: map[string]string{"mode": "blocking"}}},
		{name: "log mode bad", log: &LogConfiguration{LogDriver: "json-file", Options: map[string]string{"mode": "x"}}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := NewInMemoryBackend("000000000000", "us-east-1", NewNoopRunner())
			_, err := b.RegisterTaskDefinition(RegisterTaskDefinitionInput{
				Family: "f",
				ContainerDefinitions: []ContainerDefinition{{
					Name: "a", Image: "i", Essential: true, RestartPolicy: tt.policy, LogConfiguration: tt.log,
				}},
			})

			if tt.wantErr {
				require.ErrorIs(t, err, ErrClient)

				return
			}

			require.NoError(t, err)
		})
	}
}
