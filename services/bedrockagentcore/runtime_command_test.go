package bedrockagentcore_test

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/bedrockagentcore"
	"github.com/aws/aws-sdk-go-v2/service/bedrockagentcore/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/services/bedrockagentcore"
)

type fakeContainers struct {
	exec func(ctx context.Context, cmd []string) (container.ExecResult, error)
}

func (fakeContainers) CreateAndStart(context.Context, container.Spec) (string, error) {
	return "c1", nil
}

func (fakeContainers) StopAndRemove(context.Context, string) error { return nil }

func (f fakeContainers) Exec(ctx context.Context, _ string, cmd []string) (container.ExecResult, error) {
	return f.exec(ctx, cmd)
}

type pingOK struct{}

func (pingOK) Do(*http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("")), Header: http.Header{},
	}, nil
}

func commandFixture(t *testing.T, rt fakeContainers) *fixture {
	t.Helper()

	ctl := defaultControl()
	ctl.runtimes[testRuntime+"|DEFAULT"] = bedrockagentcore.RuntimeInfo{
		ID: testRuntime, ImageURI: "img:latest", Protocol: "HTTP",
	}

	f := newFixture(t, ctl)
	f.backend.EnableEngine(bedrockagentcore.EngineConfig{Runtime: rt, HTTP: pingOK{}})

	return f
}

type commandOutput struct {
	stop   *types.ContentStopEvent
	stdout string
	stderr string
}

func collectCommand(t *testing.T, f *fixture, in *sdk.InvokeAgentRuntimeCommandInput) (commandOutput, error) {
	t.Helper()

	in.AgentRuntimeArn = aws.String(runtimeARN)
	in.RuntimeSessionId = aws.String("session-cmd-" + strings.Repeat("c", 30))

	out, err := f.client.InvokeAgentRuntimeCommand(t.Context(), in)
	if err != nil {
		return commandOutput{}, err
	}

	stream := out.GetStream()
	defer stream.Close()

	var res commandOutput

	started := false

	for ev := range stream.Events() {
		chunk, ok := ev.(*types.InvokeAgentRuntimeCommandStreamOutputMemberChunk)
		require.True(t, ok)

		switch {
		case chunk.Value.ContentStart != nil:
			started = true
		case chunk.Value.ContentDelta != nil:
			res.stdout += aws.ToString(chunk.Value.ContentDelta.Stdout)
			res.stderr += aws.ToString(chunk.Value.ContentDelta.Stderr)
		case chunk.Value.ContentStop != nil:
			res.stop = chunk.Value.ContentStop
		}
	}

	if err = stream.Err(); err != nil {
		return commandOutput{}, err
	}

	require.True(t, started, "contentStart is the first chunk")

	return res, nil
}

func TestInvokeAgentRuntimeCommand(t *testing.T) {
	t.Parallel()

	tests := []struct {
		exec       func(ctx context.Context, cmd []string) (container.ExecResult, error)
		body       *types.InvokeAgentRuntimeCommandRequestBody
		name       string
		wantOut    string
		wantErr    string
		wantStatus types.CommandExecutionStatus
		wantExit   int32
	}{
		{
			name: "output",
			body: &types.InvokeAgentRuntimeCommandRequestBody{Command: aws.String("echo hi")},
			exec: func(_ context.Context, cmd []string) (container.ExecResult, error) {
				assert.Equal(t, []string{"sh", "-c", "echo hi"}, cmd)

				return container.ExecResult{Stdout: "hi\n", Stderr: "warn\n", ExitCode: 3}, nil
			},
			wantOut: "hi\n", wantStatus: types.CommandExecutionStatusCompleted, wantExit: 3,
		},
		{
			name: "timed out",
			body: &types.InvokeAgentRuntimeCommandRequestBody{Command: aws.String("sleep 9"), Timeout: aws.Int32(1)},
			exec: func(context.Context, []string) (container.ExecResult, error) {
				return container.ExecResult{}, context.DeadlineExceeded
			},
			wantStatus: types.CommandExecutionStatusTimedOut, wantExit: -1,
		},
		{
			name:    "timeout range",
			body:    &types.InvokeAgentRuntimeCommandRequestBody{Command: aws.String("x"), Timeout: aws.Int32(3601)},
			exec:    nil,
			wantErr: "ValidationException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := commandFixture(t, fakeContainers{exec: tt.exec})
			res, err := collectCommand(t, f, &sdk.InvokeAgentRuntimeCommandInput{Body: tt.body})

			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Equal(t, tt.wantErr, apiCode(err))

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantOut, res.stdout)
			assert.Equal(t, tt.wantStatus, res.stop.Status)
			assert.Equal(t, tt.wantExit, aws.ToInt32(res.stop.ExitCode))
		})
	}
}

func TestRuntimeProtectedResourceMetadata(t *testing.T) {
	t.Parallel()

	tests := []struct {
		meta       func(*http.Request) map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "jwt runtime",
			meta: func(*http.Request) map[string]any {
				return map[string]any{
					"resource": "https://rt/invocations", "authorization_servers": []string{"https://idp"},
				}
			},
			wantStatus: http.StatusOK,
		},
		{name: "no authorizer", wantStatus: http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctl := defaultControl()
			ctl.runtimes[testRuntime+"|DEFAULT"] = bedrockagentcore.RuntimeInfo{
				ID: testRuntime, ImageURI: "img:latest", ProtectedResource: tt.meta,
			}

			f := newFixture(t, ctl)
			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet,
				f.url+"/runtimes/"+url.PathEscape(runtimeARN)+"/invocations/.well-known/oauth-protected-resource", nil)
			require.NoError(t, err)

			resp, err := f.client.Options().HTTPClient.Do(req)
			require.NoError(t, err)

			defer resp.Body.Close()

			body, _ := io.ReadAll(resp.Body)
			assert.Equal(t, tt.wantStatus, resp.StatusCode)

			if tt.wantStatus == http.StatusOK {
				want := `{"resource":"https://rt/invocations","authorization_servers":["https://idp"]}`
				assert.JSONEq(t, want, string(body))
			}
		})
	}
}
