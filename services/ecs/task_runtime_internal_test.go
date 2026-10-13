package ecs

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errIssuerDown = errors.New("issuer down")

type stubIssuer struct {
	err   error
	roles []string
}

func (s *stubIssuer) IssueTaskCredentials(roleArn, sessionName string) (RoleCredentials, error) {
	s.roles = append(s.roles, roleArn+"#"+sessionName)

	return RoleCredentials{AccessKeyID: "ASIA", SecretAccessKey: "sec", SessionToken: "tok"}, s.err
}

func envMap(env []string) map[string]string {
	m := map[string]string{}

	for _, kv := range env {
		for i := range len(kv) {
			if kv[i] == '=' {
				m[kv[:i]] = kv[i+1:]

				break
			}
		}
	}

	return m
}

func TestContainerRuntimeEnv(t *testing.T) {
	t.Parallel()

	const taskArn = "arn:aws:ecs:us-west-2:123456789012:task/c/0123abcd-0123-0123-0123-0123456789ab"

	tests := []struct {
		wantKeys   map[string]string
		name       string
		endpoint   string
		role       string
		launchType string
		wantAbsent []string
		issuer     bool
	}{
		{
			name: "role_and_endpoint", endpoint: "http://172.17.0.1:8123/", role: "arn:aws:iam::1:role/r",
			launchType: "FARGATE", issuer: true,
			wantKeys: map[string]string{
				"AWS_REGION":         "us-west-2",
				"AWS_DEFAULT_REGION": "us-west-2",
				"AWS_EXECUTION_ENV":  "AWS_ECS_FARGATE",
				"AWS_ENDPOINT_URL":   "http://172.17.0.1:8123",
				"AWS_CONTAINER_CREDENTIALS_FULL_URI": "http://172.17.0.1:8123/_gopherstack/ecs/credentials/" +
					"0123abcd-0123-0123-0123-0123456789ab",
				"ECS_CONTAINER_METADATA_URI_V4": "http://172.17.0.1:8123/_gopherstack/ecs/metadata/" +
					"0123abcd-0123-0123-0123-0123456789ab/app",
			},
		},
		{
			name: "no_role", endpoint: "http://h:1", launchType: "EC2", issuer: true,
			wantKeys:   map[string]string{"AWS_EXECUTION_ENV": "AWS_ECS_EC2", "AWS_ENDPOINT_URL": "http://h:1"},
			wantAbsent: []string{"AWS_CONTAINER_CREDENTIALS_FULL_URI"},
		},
		{
			name:       "no_issuer",
			endpoint:   "http://h:1",
			role:       "arn:aws:iam::1:role/r",
			launchType: "EC2",
			wantKeys: map[string]string{
				"ECS_CONTAINER_METADATA_URI_V4": "http://h:1/_gopherstack/ecs/metadata/0123abcd-0123-0123-0123-0123456789ab/app",
			},
			wantAbsent: []string{"AWS_CONTAINER_CREDENTIALS_FULL_URI"},
		},
		{
			name:       "no_endpoint",
			role:       "arn:aws:iam::1:role/r",
			launchType: "FARGATE",
			issuer:     true,
			wantKeys:   map[string]string{"AWS_REGION": "us-west-2"},
			wantAbsent: []string{
				"AWS_ENDPOINT_URL",
				"ECS_CONTAINER_METADATA_URI_V4",
				"AWS_CONTAINER_CREDENTIALS_FULL_URI",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := NewInMemoryBackend("123456789012", "us-west-2", NewNoopRunner())
			b.SetEndpointURL(tt.endpoint)

			if tt.issuer {
				b.SetRoleCredentialIssuer(&stubIssuer{})
			}

			env := envMap(b.containerRuntimeEnv(
				&Task{TaskArn: taskArn, LaunchType: tt.launchType, TaskRoleArn: tt.role}, "app"))

			for k, v := range tt.wantKeys {
				assert.Equal(t, v, env[k], k)
			}

			for _, k := range tt.wantAbsent {
				assert.NotContains(t, env, k)
			}
		})
	}
}

func TestDockerRunnerInjectsRuntimeEnv(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend("123456789012", "us-east-1", NewNoopRunner())
	b.SetEndpointURL("http://172.17.0.1:8123")
	b.SetRoleCredentialIssuer(&stubIssuer{})

	fake := &fakeDockerClient{}
	runner := newDockerRunnerWithClient(context.Background(), fake)
	runner.SetTaskEnvProvider(b.containerRuntimeEnv)

	task := &Task{
		TaskArn: "arn:aws:ecs:us-east-1:123456789012:task/c/abc", ClusterArn: "arn:c",
		LaunchType: "FARGATE", TaskRoleArn: "arn:aws:iam::123456789012:role/r",
	}
	td := &TaskDefinition{ContainerDefinitions: []ContainerDefinition{{
		Name: "app", Image: "nginx", Environment: []KeyValuePair{{Name: "MINE", Value: "1"}},
	}}}

	require.NoError(t, runner.RunTask(task, td))
	require.Len(t, fake.envs, 1)

	env := envMap(fake.envs[0])
	assert.Equal(
		t,
		"http://172.17.0.1:8123/_gopherstack/ecs/credentials/abc",
		env["AWS_CONTAINER_CREDENTIALS_FULL_URI"],
	)
	assert.Equal(t, "http://172.17.0.1:8123/_gopherstack/ecs/metadata/abc/app", env["ECS_CONTAINER_METADATA_URI_V4"])
	assert.Equal(t, "http://172.17.0.1:8123", env["AWS_ENDPOINT_URL"])
	assert.Equal(t, "1", env["MINE"])
}

func TestIssueTaskCredentials(t *testing.T) {
	t.Parallel()

	tests := []struct {
		issuer  RoleCredentialIssuer
		wantErr error
		name    string
		role    string
	}{
		{name: "ok", issuer: &stubIssuer{}, role: "arn:aws:iam::1:role/r"},
		{
			name:    "issuer_error",
			issuer:  &stubIssuer{err: errIssuerDown},
			role:    "arn:aws:iam::1:role/r",
			wantErr: errIssuerDown,
		},
		{name: "no_role", issuer: &stubIssuer{}, wantErr: ErrTaskCredentialsUnavailable},
		{name: "no_issuer", role: "arn:aws:iam::1:role/r", wantErr: ErrTaskCredentialsUnavailable},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := NewInMemoryBackend("123456789012", "us-east-1", NewNoopRunner())
			if tt.issuer != nil {
				b.SetRoleCredentialIssuer(tt.issuer)
			}

			creds, err := b.issueTaskCredentials(
				Task{TaskArn: "arn:aws:ecs:us-east-1:1:task/c/tid", TaskRoleArn: tt.role},
			)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, "tok", creds.SessionToken)
			assert.Equal(t, []string{tt.role + "#tid"}, tt.issuer.(*stubIssuer).roles)
		})
	}
}
