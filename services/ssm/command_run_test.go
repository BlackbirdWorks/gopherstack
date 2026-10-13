package ssm_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

var errDaemonGone = errors.New("daemon gone")

type fakeInstances struct{ list []ssm.ManagedContainer }

func (f fakeInstances) ContainerInstances(_ string, ids []string) []ssm.ManagedContainer {
	if len(ids) == 0 {
		return f.list
	}

	var out []ssm.ManagedContainer

	for _, c := range f.list {
		for _, id := range ids {
			if c.InstanceID == id {
				out = append(out, c)
			}
		}
	}

	return out
}

type runFn func(ctx context.Context, containerID string, req ssm.ScriptRequest) (ssm.ScriptResult, error)

type fakeExec struct{ fn runFn }

func (f fakeExec) RunScript(ctx context.Context, id string, req ssm.ScriptRequest) (ssm.ScriptResult, error) {
	return f.fn(ctx, id, req)
}

type fakeSink struct {
	objects map[string]string
	logs    map[string][]string
	mu      sync.Mutex
}

func newFakeSink() *fakeSink {
	return &fakeSink{objects: map[string]string{}, logs: map[string][]string{}}
}

func (s *fakeSink) PutObject(bucket, key string, body []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.objects[bucket+"/"+key] = string(body)

	return nil
}

func (s *fakeSink) PutLogLines(_, group, stream string, lines []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.logs[group+"|"+stream] = append(s.logs[group+"|"+stream], lines...)

	return nil
}

func containers(ids ...string) fakeInstances {
	var f fakeInstances

	for _, id := range ids {
		f.list = append(f.list, ssm.ManagedContainer{
			InstanceID: id, ContainerID: "c-" + id, Tags: map[string]string{"Env": "prod"},
			RegisteredAt: time.Unix(1700000000, 0),
		})
	}

	return f
}

func runnerBackend(inst fakeInstances, fn runFn, sink ssm.CommandOutputSink) *ssm.InMemoryBackend {
	b := ssm.NewInMemoryBackend()
	b.SetCommandRunner(&ssm.CommandRunner{Instances: inst, Executor: fakeExec{fn: fn}, Sink: sink})

	return b
}

func shellInput(ids []string, cmds ...string) *ssm.SendCommandInput {
	return &ssm.SendCommandInput{
		DocumentName: "AWS-RunShellScript",
		InstanceIDs:  ids,
		Parameters:   map[string][]string{"commands": cmds},
	}
}

func invocation(t *testing.T, b *ssm.InMemoryBackend, cmdID, inst string) *ssm.GetCommandInvocationOutput {
	t.Helper()

	out, err := b.GetCommandInvocation(t.Context(), &ssm.GetCommandInvocationInput{CommandID: cmdID, InstanceID: inst})
	require.NoError(t, err)

	return out
}

func commandStatus(t *testing.T, b *ssm.InMemoryBackend, cmdID string) ssm.Command {
	t.Helper()

	out, err := b.ListCommands(t.Context(), &ssm.ListCommandsInput{CommandID: cmdID})
	require.NoError(t, err)
	require.Len(t, out.Commands, 1)

	return out.Commands[0]
}

func TestRunCommandStatusMapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		script     string
		wantStatus string
		wantStdout string
		wantStderr string
		wantCode   int32
	}{
		{name: "success", script: "echo ok", wantStatus: "Success", wantStdout: "ok\n"},
		{
			name:       "failed exit 3",
			script:     "echo hi; exit 3",
			wantStatus: "Failed",
			wantStdout: "hi\n",
			wantStderr: "boom\n",
			wantCode:   3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				var gotReq ssm.ScriptRequest

				b := runnerBackend(
					containers("i-1"),
					func(_ context.Context, id string, req ssm.ScriptRequest) (ssm.ScriptResult, error) {
						gotReq = req
						assert.Equal(t, "c-i-1", id)

						if strings.Contains(req.Script, "exit 3") {
							return ssm.ScriptResult{Stdout: []byte("hi\n"), Stderr: []byte("boom\n"), ExitCode: 3}, nil
						}

						return ssm.ScriptResult{Stdout: []byte("ok\n")}, nil
					},
					nil,
				)

				in := shellInput([]string{"i-1"}, tt.script)
				in.Parameters["workingDirectory"] = []string{"/srv"}

				out, err := b.SendCommand(t.Context(), in)
				require.NoError(t, err)
				synctest.Wait()

				inv := invocation(t, b, out.Command.CommandID, "i-1")
				assert.Equal(t, tt.wantStatus, inv.Status)
				assert.Equal(t, tt.wantStdout, inv.StandardOutputContent)
				assert.Equal(t, tt.wantStderr, inv.StandardErrorContent)
				assert.Equal(t, tt.wantCode, inv.ResponseCode)
				assert.NotEmpty(t, inv.ExecutionEndDateTime)
				assert.Equal(t, tt.wantStatus, commandStatus(t, b, out.Command.CommandID).Status)
				assert.Equal(t, "/srv", gotReq.WorkDir)
				assert.Equal(t, tt.script+"\n", gotReq.Script)
				assert.Equal(t, []string{"sh", ssm.ScriptFilePlaceholder}, gotReq.Interpreter)
			})
		})
	}
}

func TestRunCommandStaysInProgressUntilDone(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		b := runnerBackend(
			containers("i-1"),
			func(context.Context, string, ssm.ScriptRequest) (ssm.ScriptResult, error) {
				<-release

				return ssm.ScriptResult{Stdout: []byte("late")}, nil
			},
			nil,
		)

		out, err := b.SendCommand(t.Context(), shellInput([]string{"i-1"}, "sleep 1"))
		require.NoError(t, err)
		synctest.Wait()

		inv := invocation(t, b, out.Command.CommandID, "i-1")
		assert.Equal(t, "InProgress", inv.Status)
		assert.Empty(t, inv.StandardOutputContent)
		assert.Equal(t, "InProgress", commandStatus(t, b, out.Command.CommandID).Status)

		close(release)
		synctest.Wait()

		assert.Equal(t, "Success", invocation(t, b, out.Command.CommandID, "i-1").Status)
	})
}

func TestRunCommandOutputTruncation(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		sink := newFakeSink()
		b := runnerBackend(
			containers("i-1"),
			func(context.Context, string, ssm.ScriptRequest) (ssm.ScriptResult, error) {
				return ssm.ScriptResult{
					Stdout: []byte(strings.Repeat("a", 30000)), Stderr: []byte(strings.Repeat("é", 9000)),
				}, nil
			},
			sink,
		)

		in := shellInput([]string{"i-1"}, "gen")
		in.OutputS3BucketName = "out"
		out, err := b.SendCommand(t.Context(), in)
		require.NoError(t, err)
		synctest.Wait()

		inv := invocation(t, b, out.Command.CommandID, "i-1")
		assert.Len(t, inv.StandardOutputContent, 24000)
		assert.Len(t, []rune(inv.StandardErrorContent), 8000)
		assert.NotEmpty(t, inv.StandardOutputURL)
		assert.Len(
			t,
			sink.objects["out/"+out.Command.CommandID+"/i-1/awsrunShellScript/0.awsrunShellScript/stdout"],
			30000,
		)
	})
}

func TestRunCommandS3AndCloudWatchOutput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		prefix     string
		cw         *ssm.CloudWatchOutputConfig
		wantPrefix string
		wantGroup  string
	}{
		{
			name:       "prefix and default group",
			prefix:     "p/q",
			cw:         &ssm.CloudWatchOutputConfig{CloudWatchOutputEnabled: true},
			wantPrefix: "p/q/",
			wantGroup:  "/aws/ssm/AWS-RunShellScript",
		},
		{
			name:      "no prefix custom group",
			cw:        &ssm.CloudWatchOutputConfig{CloudWatchOutputEnabled: true, CloudWatchLogGroupName: "/my/grp"},
			wantGroup: "/my/grp",
		},
		{name: "cloudwatch disabled", cw: &ssm.CloudWatchOutputConfig{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				sink := newFakeSink()
				b := runnerBackend(
					containers("i-1"),
					func(context.Context, string, ssm.ScriptRequest) (ssm.ScriptResult, error) {
						return ssm.ScriptResult{Stdout: []byte("l1\nl2\n"), Stderr: []byte("e1\n")}, nil
					},
					sink,
				)

				in := shellInput([]string{"i-1"}, "x")
				in.OutputS3BucketName, in.OutputS3KeyPrefix, in.CloudWatchOutputConfig = "bkt", tt.prefix, tt.cw
				out, err := b.SendCommand(t.Context(), in)
				require.NoError(t, err)
				synctest.Wait()

				id := out.Command.CommandID
				base := "bkt/" + tt.wantPrefix + id + "/i-1/awsrunShellScript/0.awsrunShellScript/"
				assert.Equal(t, "l1\nl2\n", sink.objects[base+"stdout"])
				assert.Equal(t, "e1\n", sink.objects[base+"stderr"])

				inv := invocation(t, b, id, "i-1")
				assert.Equal(
					t,
					"https://s3.amazonaws.com/bkt/"+tt.wantPrefix+id+"/i-1/awsrunShellScript/0.awsrunShellScript/stdout",
					inv.StandardOutputURL,
				)
				assert.Equal(t, tt.cw, inv.CloudWatchOutputConfig)

				stream := id + "/i-1/aws-runShellScript/"
				if tt.wantGroup == "" {
					assert.Empty(t, sink.logs)

					return
				}

				assert.Equal(t, []string{"l1", "l2"}, sink.logs[tt.wantGroup+"|"+stream+"stdout"])
				assert.Equal(t, []string{"e1"}, sink.logs[tt.wantGroup+"|"+stream+"stderr"])
			})
		})
	}
}

func TestRunCommandExecutionTimeout(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b := runnerBackend(
			containers("i-1"),
			func(ctx context.Context, _ string, _ ssm.ScriptRequest) (ssm.ScriptResult, error) {
				<-ctx.Done()

				return ssm.ScriptResult{}, ctx.Err()
			},
			nil,
		)

		in := shellInput([]string{"i-1"}, "sleep 100")
		in.Parameters["executionTimeout"] = []string{"5"}
		out, err := b.SendCommand(t.Context(), in)
		require.NoError(t, err)
		synctest.Wait()
		assert.Equal(t, "InProgress", invocation(t, b, out.Command.CommandID, "i-1").Status)

		time.Sleep(6 * time.Second)
		synctest.Wait()

		inv := invocation(t, b, out.Command.CommandID, "i-1")
		assert.Equal(t, "TimedOut", inv.Status)
		assert.Equal(t, "ExecutionTimedOut", inv.StatusDetails)
		assert.Equal(t, "TimedOut", commandStatus(t, b, out.Command.CommandID).Status)
	})
}

func TestRunCommandCancel(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		var stopped atomic.Int32

		b := runnerBackend(
			containers("i-1", "i-2"),
			func(ctx context.Context, _ string, _ ssm.ScriptRequest) (ssm.ScriptResult, error) {
				<-ctx.Done()
				stopped.Add(1)

				return ssm.ScriptResult{}, ctx.Err()
			},
			nil,
		)

		out, err := b.SendCommand(t.Context(), shellInput([]string{"i-1", "i-2"}, "sleep 100"))
		require.NoError(t, err)
		synctest.Wait()

		_, err = b.CancelCommand(
			t.Context(),
			&ssm.CancelCommandInput{CommandID: out.Command.CommandID, InstanceIDs: []string{"i-1"}},
		)
		require.NoError(t, err)
		synctest.Wait()

		assert.Equal(t, int32(1), stopped.Load())
		assert.Equal(t, "Cancelled", invocation(t, b, out.Command.CommandID, "i-1").Status)
		assert.Equal(t, "InProgress", invocation(t, b, out.Command.CommandID, "i-2").Status)

		_, err = b.CancelCommand(t.Context(), &ssm.CancelCommandInput{CommandID: out.Command.CommandID})
		require.NoError(t, err)
		synctest.Wait()

		assert.Equal(t, int32(2), stopped.Load())
		assert.Equal(t, "Cancelled", invocation(t, b, out.Command.CommandID, "i-2").Status)
		assert.Equal(t, "Cancelled", commandStatus(t, b, out.Command.CommandID).Status)
	})
}

func TestRunCommandMaxConcurrency(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		conc string
		want int32
	}{
		{name: "absolute", conc: "2", want: 2},
		{name: "percent", conc: "50%", want: 2},
		{name: "one", conc: "1", want: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				var running, peak atomic.Int32

				release := make(chan struct{})
				b := runnerBackend(
					containers("i-1", "i-2", "i-3", "i-4"),
					func(context.Context, string, ssm.ScriptRequest) (ssm.ScriptResult, error) {
						n := running.Add(1)
						for {
							p := peak.Load()
							if n <= p || peak.CompareAndSwap(p, n) {
								break
							}
						}

						<-release
						running.Add(-1)

						return ssm.ScriptResult{}, nil
					},
					nil,
				)

				in := shellInput([]string{"i-1", "i-2", "i-3", "i-4"}, "x")
				in.MaxConcurrency = tt.conc
				out, err := b.SendCommand(t.Context(), in)
				require.NoError(t, err)
				synctest.Wait()
				assert.Equal(t, tt.want, running.Load())

				close(release)
				synctest.Wait()

				assert.Equal(t, tt.want, peak.Load())
				assert.Equal(t, "Success", commandStatus(t, b, out.Command.CommandID).Status)
			})
		})
	}
}

func TestRunCommandMaxErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		maxErrors   string
		wantStatus  string
		wantDetails string
		wantRan     int32
	}{
		{
			name:        "default stops after first error",
			maxErrors:   "",
			wantRan:     1,
			wantStatus:  "Failed",
			wantDetails: "Failed",
		},
		{name: "one allowed", maxErrors: "1", wantRan: 2, wantStatus: "Failed", wantDetails: "Failed"},
		{name: "all allowed", maxErrors: "4", wantRan: 4, wantStatus: "Failed", wantDetails: "Incomplete"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				var ran atomic.Int32

				ids := []string{"i-1", "i-2", "i-3", "i-4"}
				b := runnerBackend(
					containers(ids...),
					func(context.Context, string, ssm.ScriptRequest) (ssm.ScriptResult, error) {
						ran.Add(1)

						return ssm.ScriptResult{ExitCode: 1}, nil
					},
					nil,
				)

				in := shellInput(ids, "exit 1")
				in.MaxConcurrency, in.MaxErrors = "1", tt.maxErrors
				out, err := b.SendCommand(t.Context(), in)
				require.NoError(t, err)
				synctest.Wait()

				assert.Equal(t, tt.wantRan, ran.Load())

				cmd := commandStatus(t, b, out.Command.CommandID)
				assert.Equal(t, tt.wantStatus, cmd.Status)
				assert.Equal(t, tt.wantDetails, cmd.StatusDetails)

				for i, id := range ids {
					inv := invocation(t, b, out.Command.CommandID, id)
					if int32(i) >= tt.wantRan {
						assert.Equal(t, "Cancelled", inv.Status)
						assert.Equal(t, "Terminated", inv.StatusDetails)
					} else {
						assert.Equal(t, "Failed", inv.Status)
					}
				}
			})
		})
	}
}

func TestRunCommandTargetsAndFallback(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int32

		b := runnerBackend(
			containers("i-1", "i-2"),
			func(context.Context, string, ssm.ScriptRequest) (ssm.ScriptResult, error) {
				calls.Add(1)

				return ssm.ScriptResult{Stdout: []byte("real\n")}, nil
			},
			nil,
		)

		in := shellInput([]string{"i-sim"}, "echo simulated")
		in.Targets = []ssm.CommandTarget{{Key: "tag:Env", Values: []string{"prod"}}}
		out, err := b.SendCommand(t.Context(), in)
		require.NoError(t, err)
		synctest.Wait()

		id := out.Command.CommandID
		assert.Equal(t, "real\n", invocation(t, b, id, "i-1").StandardOutputContent)
		assert.Equal(t, "real\n", invocation(t, b, id, "i-2").StandardOutputContent)
		assert.Equal(t, "simulated\n", invocation(t, b, id, "i-sim").StandardOutputContent)
		assert.Equal(t, int32(2), calls.Load())
		assert.Equal(t, "Success", commandStatus(t, b, id).Status)
	})
}

func TestRunCommandCustomDocument(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		var scripts []string

		b := runnerBackend(
			containers("i-1"),
			func(_ context.Context, _ string, req ssm.ScriptRequest) (ssm.ScriptResult, error) {
				scripts = append(scripts, req.Script+"@"+req.WorkDir)

				return ssm.ScriptResult{Stdout: []byte(req.Script)}, nil
			},
			nil,
		)

		_, err := b.CreateDocument(t.Context(), &ssm.CreateDocumentInput{
			Name: "Custom", DocumentType: "Command", DocumentFormat: "JSON",
			Content: `{"schemaVersion":"2.2","parameters":{"who":{"type":"String","default":"world"}},"mainSteps":[` +
				`{"action":"aws:runShellScript","name":"one","inputs":{"workingDirectory":"/w","runCommand":["echo {{ who }}"]}},` +
				`{"action":"aws:runShellScript","name":"two","inputs":{"runCommand":["echo second"]}}]}`,
		})
		require.NoError(t, err)

		out, err := b.SendCommand(
			t.Context(),
			&ssm.SendCommandInput{DocumentName: "Custom", InstanceIDs: []string{"i-1"}},
		)
		require.NoError(t, err)
		synctest.Wait()

		assert.Equal(t, []string{"echo world\n@/w", "echo second\n@"}, scripts)

		id := out.Command.CommandID
		inv := invocation(t, b, id, "i-1")
		assert.Equal(t, "echo world\necho second\n", inv.StandardOutputContent)

		pl, err := b.GetCommandInvocation(
			t.Context(),
			&ssm.GetCommandInvocationInput{CommandID: id, InstanceID: "i-1", PluginName: "two"},
		)
		require.NoError(t, err)
		assert.Equal(t, "echo second\n", pl.StandardOutputContent)

		_, err = b.GetCommandInvocation(
			t.Context(),
			&ssm.GetCommandInvocationInput{CommandID: id, InstanceID: "i-1", PluginName: "nope"},
		)
		require.ErrorIs(t, err, ssm.ErrInvalidPluginName)

		list, err := b.ListCommandInvocations(
			t.Context(),
			&ssm.ListCommandInvocationsInput{CommandID: id, Details: true},
		)
		require.NoError(t, err)
		require.Len(t, list.CommandInvocations, 1)
		assert.Len(t, list.CommandInvocations[0].CommandPlugins, 2)

		plain, err := b.ListCommandInvocations(t.Context(), &ssm.ListCommandInvocationsInput{CommandID: id})
		require.NoError(t, err)
		assert.Empty(t, plain.CommandInvocations[0].CommandPlugins)
	})
}

func TestRunCommandInstanceInformation(t *testing.T) {
	t.Parallel()

	b := runnerBackend(containers("i-b", "i-a"), nil, nil)

	out, err := b.DescribeInstanceInformation(t.Context(), &ssm.DescribeInstanceInformationInput{})
	require.NoError(t, err)
	require.Len(t, out.InstanceInformationList, 2)
	assert.Equal(t, "i-a", out.InstanceInformationList[0].InstanceID)
	assert.Equal(t, "Online", out.InstanceInformationList[0].PingStatus)
	assert.Equal(t, "EC2Instance", out.InstanceInformationList[0].ResourceType)
	assert.Equal(t, "Linux", out.InstanceInformationList[0].PlatformType)

	filtered, err := b.DescribeInstanceInformation(t.Context(), &ssm.DescribeInstanceInformationInput{
		Filters: []ssm.InstanceInformationStringFilterEntry{{Key: "InstanceIds", Values: []string{"i-b"}}},
	})
	require.NoError(t, err)
	require.Len(t, filtered.InstanceInformationList, 1)
	assert.Equal(t, "i-b", filtered.InstanceInformationList[0].InstanceID)
}

func TestRunCommandInfrastructureError(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b := runnerBackend(
			containers("i-1"),
			func(context.Context, string, ssm.ScriptRequest) (ssm.ScriptResult, error) {
				return ssm.ScriptResult{}, errDaemonGone
			},
			nil,
		)

		out, err := b.SendCommand(t.Context(), shellInput([]string{"i-1"}, "x"))
		require.NoError(t, err)
		synctest.Wait()

		inv := invocation(t, b, out.Command.CommandID, "i-1")
		assert.Equal(t, "Failed", inv.Status)
		assert.Equal(t, int32(-1), inv.ResponseCode)
		assert.Contains(t, inv.StandardErrorContent, "daemon gone")
	})
}

func TestSimulatedCommandResponseCode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cmd  string
		want int32
	}{
		{name: "exit 3", cmd: "exit 3", want: 3},
		{name: "success", cmd: "echo ok", want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ssm.NewInMemoryBackend()
			out, err := b.SendCommand(t.Context(), shellInput([]string{"i-x"}, tt.cmd))
			require.NoError(t, err)
			assert.Equal(t, tt.want, invocation(t, b, out.Command.CommandID, "i-x").ResponseCode)
		})
	}
}
