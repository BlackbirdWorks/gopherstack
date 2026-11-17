package ecs

import (
	"context"
	"slices"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type objectCall struct{ bucket, key, body string }

type fakeTranscriptStore struct {
	objects []objectCall
	mu      sync.Mutex
}

func (s *fakeTranscriptStore) PutObject(_ context.Context, bucket, key string, body []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.objects = append(s.objects, objectCall{bucket, key, string(body)})

	return nil
}

func TestExecTranscriptDelivery(t *testing.T) {
	t.Parallel()

	awslogs := &LogConfiguration{LogDriver: logDriverAwslogs, Options: map[string]string{"awslogs-group": "/ecs/app"}}
	override := func(lc *ExecuteCommandLogConfiguration, kms string) *ClusterConfiguration {
		return &ClusterConfiguration{ExecuteCommandConfiguration: &ExecuteCommandConfiguration{
			Logging: "OVERRIDE", LogConfiguration: lc, KmsKeyID: kms,
		}}
	}

	tests := []struct {
		cfg        *ClusterConfiguration
		logCfg     *LogConfiguration
		name       string
		wantGroup  string
		wantKey    string
		wantBucket string
		wantLines  []string
		noOnEnd    bool
	}{
		{
			name: "default uses the awslogs group", logCfg: awslogs,
			wantGroup: "/ecs/app", wantLines: []string{"hello", "world"},
		},
		{name: "default without awslogs logs nothing", noOnEnd: true},
		{
			name: "none logs nothing", logCfg: awslogs, noOnEnd: true,
			cfg: &ClusterConfiguration{ExecuteCommandConfiguration: &ExecuteCommandConfiguration{Logging: "NONE"}},
		},
		{
			name: "override cloudwatch", logCfg: awslogs,
			cfg:       override(&ExecuteCommandLogConfiguration{CloudWatchLogGroupName: "/exec"}, ""),
			wantGroup: "/exec", wantLines: []string{"hello", "world"},
		},
		{
			name: "override s3 with prefix and kms",
			cfg: override(&ExecuteCommandLogConfiguration{
				S3BucketName: "audit", S3KeyPrefix: "/exec/", S3EncryptionEnabled: true,
			}, "key-1"),
			wantBucket: "audit", wantKey: "exec/ecs-execute-command-sess-1.log",
		},
		{name: "override without log configuration logs nothing", cfg: override(nil, ""), noOnEnd: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			starter := &recordingStarter{}
			cwl := &mockECSCWLogsBackend{}
			store := &fakeTranscriptStore{}

			b := NewInMemoryBackend("123456789012", "us-east-1", idRunner{})
			b.SetExecSessionStarter(starter)
			b.SetCWLogsBackend(cwl)
			b.SetExecTranscriptStore(store)

			_, err := b.CreateCluster(CreateClusterInput{ClusterName: "x", Configuration: tt.cfg})
			require.NoError(t, err)

			td, err := b.RegisterTaskDefinition(RegisterTaskDefinitionInput{
				Family:               "x",
				ContainerDefinitions: []ContainerDefinition{{Name: "web", Image: "nginx", LogConfiguration: tt.logCfg}},
			})
			require.NoError(t, err)

			tasks, _, err := b.RunTask(RunTaskInput{
				Cluster: "x", TaskDefinition: td.TaskDefinitionArn, EnableExecuteCommand: true,
			})
			require.NoError(t, err)

			tasks = settleTasks(t, b, tasks)

			_, err = b.ExecuteCommandContext(
				WithStreamBase(t.Context(), "ws://emu"),
				"x",
				tasks[0].TaskArn,
				"web",
				"ls",
				true,
			)
			require.NoError(t, err)
			require.Len(t, starter.got, 1)

			onEnd := starter.got[0].OnEnd
			if tt.noOnEnd {
				assert.Nil(t, onEnd)

				return
			}

			require.NotNil(t, onEnd)
			onEnd("sess-1", []byte("hello\r\nworld\n"))

			if tt.wantGroup != "" {
				puts := cwl.putCalls()
				require.Len(t, puts, 1)
				assert.Equal(t, tt.wantGroup, puts[0].group)
				assert.Equal(t, "ecs-execute-command-sess-1", puts[0].stream)
				assert.Equal(t, tt.wantLines, puts[0].messages)
			}

			if tt.wantBucket != "" {
				require.Len(t, store.objects, 1)
				assert.Equal(t, objectCall{tt.wantBucket, tt.wantKey, "hello\r\nworld\n"}, store.objects[0])
			}
		})
	}
}

func TestTranscriptLinesBounded(t *testing.T) {
	t.Parallel()

	long := make([]byte, 3*maxTranscriptLineLen)
	for i := range long {
		long[i] = 'a'
	}

	lines := transcriptLines(slices.Concat(long, []byte("\nb\n\n")))
	require.Len(t, lines, 2)
	assert.Len(t, lines[0], maxTranscriptLineLen)
	assert.Equal(t, "b", lines[1])
}
