package ecs

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildLogConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		lc          *LogConfiguration
		secrets     map[string]string
		wantOpts    map[string]string
		name        string
		wantType    string
		accountMode string
		wantErr     bool
	}{
		{name: "none configured", lc: nil, wantType: ""},
		{
			name: "awslogs uses json-file and drops cloudwatch options",
			lc: &LogConfiguration{LogDriver: "awslogs", Options: map[string]string{
				"awslogs-group": "g", "awslogs-region": "us-east-1", "awslogs-stream-prefix": "p",
			}},
			wantType: "json-file",
			wantOpts: map[string]string{"mode": "non-blocking", "max-buffer-size": "10m"},
		},
		{
			name: "awslogs blocking mode",
			lc: &LogConfiguration{LogDriver: "awslogs", Options: map[string]string{
				"awslogs-group": "g", "mode": "blocking",
			}},
			wantType: "json-file",
			wantOpts: map[string]string{"mode": "blocking"},
		},
		{
			name: "awslogs buffer size",
			lc: &LogConfiguration{LogDriver: "awslogs", Options: map[string]string{
				"mode": "non-blocking", "max-buffer-size": "25m",
			}},
			wantType: "json-file",
			wantOpts: map[string]string{"mode": "non-blocking", "max-buffer-size": "25m"},
		},
		{
			name: "json-file passthrough",
			lc: &LogConfiguration{LogDriver: "json-file", Options: map[string]string{
				"max-size": "1m", "max-file": "2",
			}},
			wantType: "json-file",
			wantOpts: map[string]string{
				"max-size": "1m", "max-file": "2", "mode": "non-blocking", "max-buffer-size": "10m",
			},
		},
		{
			name:     "splunk with secret token",
			lc:       &LogConfiguration{LogDriver: "splunk", Options: map[string]string{"splunk-url": "https://s"}},
			secrets:  map[string]string{"splunk-token": "tok"},
			wantType: "splunk",
			wantOpts: map[string]string{
				"splunk-url": "https://s", "splunk-token": "tok", "mode": "non-blocking", "max-buffer-size": "10m",
			},
		},
		{
			name: "syslog explicit blocking",
			lc: &LogConfiguration{LogDriver: "syslog", Options: map[string]string{
				"syslog-address": "udp://h:514", "mode": "blocking",
			}},
			wantType: "syslog",
			wantOpts: map[string]string{"syslog-address": "udp://h:514", "mode": "blocking"},
		},
		{
			name:        "account blocking default",
			lc:          &LogConfiguration{LogDriver: "awslogs", Options: map[string]string{"awslogs-group": "g"}},
			accountMode: "blocking",
			wantType:    "json-file",
			wantOpts:    map[string]string{"mode": "blocking"},
		},
		{
			name: "explicit mode beats account default",
			lc: &LogConfiguration{LogDriver: "awslogs", Options: map[string]string{
				"awslogs-group": "g", "mode": "non-blocking",
			}},
			accountMode: "blocking",
			wantType:    "json-file",
			wantOpts:    map[string]string{"mode": "non-blocking", "max-buffer-size": "10m"},
		},
		{name: "none driver", lc: &LogConfiguration{LogDriver: "none"}, wantType: "none"},
		{
			name:    "bad mode",
			lc:      &LogConfiguration{LogDriver: "fluentd", Options: map[string]string{"mode": "fast"}},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := buildLogConfig(tt.lc, tt.secrets, tt.accountMode)
			if tt.wantErr {
				require.ErrorIs(t, err, errInvalidLogOption)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantType, got.Type)

			if tt.wantOpts != nil {
				assert.Equal(t, tt.wantOpts, got.Config)
			}
		})
	}
}

func TestRunTask_LogDriver(t *testing.T) {
	t.Parallel()

	fake := &fakeDockerClient{}
	r := newLaunchRunner(t, fake)
	r.SetSecretResolver(func(_ context.Context, v string) (string, error) { return "val:" + v, nil })

	td := &TaskDefinition{ContainerDefinitions: []ContainerDefinition{{
		Name: "app", Image: "busybox", Essential: true,
		LogConfiguration: &LogConfiguration{
			LogDriver:     "splunk",
			Options:       map[string]string{"splunk-url": "https://s"},
			SecretOptions: []SecretOption{{Name: "splunk-token", ValueFrom: "arn:tok"}},
		},
	}}}

	require.NoError(t, r.RunTask(launchTask(), td))

	lc := fake.createAt(0).host.LogConfig
	assert.Equal(t, "splunk", lc.Type)
	assert.Equal(t, "val:arn:tok", lc.Config["splunk-token"])
}

func TestDefaultLogDriverMode(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend("000000000000", "us-east-1", nil)
	assert.Empty(t, b.defaultLogDriverMode())

	_, err := b.PutAccountSettingDefault("defaultLogDriverMode", "blocking")
	require.NoError(t, err)
	assert.Equal(t, "blocking", b.defaultLogDriverMode())
}
