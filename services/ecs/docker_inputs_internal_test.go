package ecs

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeEnvStore struct {
	objects map[string]string
}

func (f fakeEnvStore) ObjectBytes(_ context.Context, bucket, key string) ([]byte, error) {
	if v, ok := f.objects[bucket+"/"+key]; ok {
		return []byte(v), nil
	}

	return nil, errTestMissing
}

const testSecretARN = "arn:aws:secretsmanager:us-east-1:000000000000:secret:regcreds-AbCdEf"

func runInputsTask(
	t *testing.T, fake *fakeDockerClient, secrets SecretsResolver, store EnvFileStore, cd ContainerDefinition,
) Task {
	t.Helper()

	runner := newLaunchRunner(t, fake)
	b := NewInMemoryBackend("000000000000", "us-east-1", runner)
	b.wireRunnerHooks(runner)

	if secrets != nil {
		b.SetSecretsResolver(secrets)
	}

	if store != nil {
		b.SetEnvFileStore(store)
	}

	_, err := b.CreateCluster(CreateClusterInput{ClusterName: "c"})
	require.NoError(t, err)

	_, err = b.RegisterTaskDefinition(RegisterTaskDefinitionInput{
		Family: "job", ContainerDefinitions: []ContainerDefinition{cd},
	})
	require.NoError(t, err)

	tasks, _, err := b.RunTask(RunTaskInput{Cluster: "c", TaskDefinition: "job"})
	require.NoError(t, err)

	tasks = settleTasks(t, b, tasks)

	return describeOne(t, b, tasks[0].TaskArn)
}

func TestRepositoryCredentials(t *testing.T) {
	t.Parallel()

	tests := []struct {
		secrets    *fakeSecrets
		name       string
		credParam  string
		wantReason string
		wantUser   string
		wantPulled bool
	}{
		{
			name: "auth_from_secret", credParam: testSecretARN, wantPulled: true, wantUser: "bob",
			secrets: &fakeSecrets{smValue: `{"username":"bob","password":"hunter2"}`},
		},
		{
			name: "secret_name_in_task_region", credParam: "regcreds", wantPulled: true, wantUser: "bob",
			secrets: &fakeSecrets{smValue: `{"username":"bob","password":"hunter2"}`},
		},
		{
			name: "missing_secret", credParam: testSecretARN, secrets: &fakeSecrets{smErr: errTestSecretNotFound},
			wantReason: "ResourceInitializationError: unable to pull secrets or registry auth",
		},
		{
			name: "invalid_json", credParam: testSecretARN, secrets: &fakeSecrets{smValue: `not json`},
			wantReason: "ResourceInitializationError: unable to pull secrets or registry auth",
		},
		{
			name: "missing_password", credParam: testSecretARN, secrets: &fakeSecrets{smValue: `{"username":"bob"}`},
			wantReason: "ResourceInitializationError: unable to pull secrets or registry auth",
		},
		{
			name: "ssm_arn_rejected", credParam: "arn:aws:ssm:us-east-1:000000000000:parameter/x",
			secrets:    &fakeSecrets{},
			wantReason: "ResourceInitializationError: unable to pull secrets or registry auth",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fake := &fakeDockerClient{}
			got := runInputsTask(t, fake, tt.secrets, nil, ContainerDefinition{
				Name: "app", Image: "registry.example.com/team/app:1", Essential: true,
				RepositoryCredentials: &RepositoryCredentials{CredentialsParameter: tt.credParam},
			})

			if !tt.wantPulled {
				assert.Equal(t, statusStopped, got.LastStatus)
				assert.Equal(t, stopCodeTaskFailedToStart, got.StopCode)
				assert.Contains(t, got.StoppedReason, tt.wantReason)
				assert.NotContains(t, got.StoppedReason, "hunter2")
				assert.Empty(t, fake.pulls)

				return
			}

			require.Len(t, fake.pulls, 1)

			raw, err := base64.URLEncoding.DecodeString(fake.pulls[0].auth)
			require.NoError(t, err)

			var ac map[string]string

			require.NoError(t, json.Unmarshal(raw, &ac))
			assert.Equal(t, tt.wantUser, ac["username"])
			assert.Equal(t, "hunter2", ac["password"])
			assert.Equal(t, "registry.example.com", ac["serveraddress"])
		})
	}
}

func TestEnvironmentFiles(t *testing.T) {
	t.Parallel()

	files := func(vals ...string) []EnvironmentFile {
		out := make([]EnvironmentFile, 0, len(vals))
		for _, v := range vals {
			out = append(out, EnvironmentFile{Type: "s3", Value: v})
		}

		return out
	}

	store := fakeEnvStore{objects: map[string]string{
		"bkt/a.env": "# comment\r\nA=from-a\r\nSHARED=first\r\n\r\nQUOTED=\"x y\"\r\nnoequals\r\n",
		"bkt/b.env": "B=from-b\nSHARED=second\nOVERRIDDEN=file\n",
	}}

	tests := []struct {
		name       string
		wantReason string
		files      []EnvironmentFile
		wantEnv    []string
	}{
		{
			name:  "ordered_first_wins_environment_last",
			files: files("arn:aws:s3:::bkt/a.env", "arn:aws:s3:::bkt/b.env"),
			wantEnv: []string{
				"A=from-a", "SHARED=first", `QUOTED="x y"`, "B=from-b", "OVERRIDDEN=file", "OVERRIDDEN=env",
			},
		},
		{
			name:       "missing_object",
			files:      files("arn:aws:s3:::bkt/none.env"),
			wantReason: "ResourceInitializationError: failed to download env files",
		},
		{
			name:       "bad_arn",
			files:      files("bkt/a.env"),
			wantReason: "ResourceInitializationError: failed to download env files",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fake := &fakeDockerClient{}
			got := runInputsTask(t, fake, nil, store, ContainerDefinition{
				Name: "app", Image: "busybox", Essential: true, EnvironmentFiles: tt.files,
				Environment: []KeyValuePair{{Name: "OVERRIDDEN", Value: "env"}},
			})

			if tt.wantReason != "" {
				assert.Equal(t, statusStopped, got.LastStatus)
				assert.Equal(t, stopCodeTaskFailedToStart, got.StopCode)
				assert.Contains(t, got.StoppedReason, tt.wantReason)
				assert.Empty(t, fake.createdImages())

				return
			}

			env := fake.createAt(0).cfg.Env
			assert.Subset(t, env, tt.wantEnv)
			assert.Less(t, indexOf(env, "OVERRIDDEN=file"), indexOf(env, "OVERRIDDEN=env"))
			assert.NotContains(t, env, "SHARED=second")
			assert.NotContains(t, env, "noequals")
		})
	}
}

func indexOf(s []string, v string) int {
	for i, x := range s {
		if x == v {
			return i
		}
	}

	return -1
}

func TestEnvironmentFiles_NoStore(t *testing.T) {
	t.Parallel()

	fake := &fakeDockerClient{}
	got := runInputsTask(t, fake, nil, nil, ContainerDefinition{
		Name: "app", Image: "busybox", Essential: true,
		EnvironmentFiles: []EnvironmentFile{{Type: "s3", Value: "arn:aws:s3:::bkt/a.env"}},
	})

	assert.Equal(t, statusStopped, got.LastStatus)
	assert.Contains(t, got.StoppedReason, "failed to download env files")
}

func TestFirelensFailsDocumented(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cd   ContainerDefinition
	}{
		{
			name: "router_container",
			cd: ContainerDefinition{
				Name: "log_router", Image: "fluent/fluent-bit", Essential: true,
				FirelensConfiguration: &FirelensConfiguration{Type: "fluentbit"},
			},
		},
		{
			name: "awsfirelens_driver",
			cd: ContainerDefinition{
				Name: "app", Image: "busybox", Essential: true,
				LogConfiguration: &LogConfiguration{LogDriver: "awsfirelens"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fake := &fakeDockerClient{}
			got := runInputsTask(t, fake, nil, nil, tt.cd)

			assert.Equal(t, statusStopped, got.LastStatus)
			assert.Equal(t, stopCodeTaskFailedToStart, got.StopCode)
			assert.Contains(t, got.StoppedReason, "firelens")
			assert.Empty(t, fake.createdImages())
		})
	}
}
