package bedrockagentcore_test

import (
	"testing"
	"time"

	sdk "github.com/aws/aws-sdk-go-v2/service/bedrockagentcore"
	"github.com/aws/aws-sdk-go-v2/service/bedrockagentcore/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	ciID      = "aws.codeinterpreter.v1"
	browserID = "aws.browser.v1"
)

func TestBrowserSessions_Lifecycle(t *testing.T) {
	t.Parallel()

	f := newFixture(t, defaultControl())

	start, err := f.client.StartBrowserSession(t.Context(), &sdk.StartBrowserSessionInput{
		BrowserIdentifier: new(browserID), Name: new("b1"), SessionTimeoutSeconds: new(int32(60)),
		ViewPort: &types.ViewPort{Width: new(int32(800)), Height: new(int32(600))},
	})
	require.NoError(t, err)

	got, err := f.client.GetBrowserSession(t.Context(), &sdk.GetBrowserSessionInput{
		BrowserIdentifier: new(browserID), SessionId: start.SessionId,
	})
	require.NoError(t, err)
	assert.Equal(t, types.BrowserSessionStatusReady, got.Status)
	assert.Equal(t, "b1", *got.Name)
	assert.EqualValues(t, 800, *got.ViewPort.Width)
	assert.EqualValues(t, 60, *got.SessionTimeoutSeconds)

	list, err := f.client.ListBrowserSessions(t.Context(), &sdk.ListBrowserSessionsInput{
		BrowserIdentifier: new(browserID), Status: types.BrowserSessionStatusReady,
	})
	require.NoError(t, err)
	require.Len(t, list.Items, 1)

	f.clock.advance(2 * time.Minute)

	got, err = f.client.GetBrowserSession(t.Context(), &sdk.GetBrowserSessionInput{
		BrowserIdentifier: new(browserID), SessionId: start.SessionId,
	})
	require.NoError(t, err)
	assert.Equal(t, types.BrowserSessionStatusTerminated, got.Status, "idle timeout elapsed on the injected clock")

	stop, err := f.client.StopBrowserSession(t.Context(), &sdk.StopBrowserSessionInput{
		BrowserIdentifier: new(browserID), SessionId: start.SessionId,
	})
	require.NoError(t, err)
	assert.Equal(t, *start.SessionId, *stop.SessionId)

	_, err = f.client.StartBrowserSession(t.Context(), &sdk.StartBrowserSessionInput{BrowserIdentifier: new("nope")})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	_, err = f.client.GetBrowserSession(t.Context(), &sdk.GetBrowserSessionInput{
		BrowserIdentifier: new(browserID), SessionId: new("missing"),
	})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}

func startCI(t *testing.T, f *fixture) string {
	t.Helper()

	out, err := f.client.StartCodeInterpreterSession(t.Context(), &sdk.StartCodeInterpreterSessionInput{
		CodeInterpreterIdentifier: new(ciID),
	})
	require.NoError(t, err)

	return *out.SessionId
}

func invoke(
	t *testing.T, f *fixture, session string, name types.ToolName, args *types.ToolArguments,
) (*types.CodeInterpreterResult, error) {
	t.Helper()

	out, err := f.client.InvokeCodeInterpreter(t.Context(), &sdk.InvokeCodeInterpreterInput{
		CodeInterpreterIdentifier: new(ciID), SessionId: &session, Name: name, Arguments: args,
	})
	if err != nil {
		return nil, err
	}

	stream := out.GetStream()
	defer stream.Close()

	for ev := range stream.Events() {
		if r, ok := ev.(*types.CodeInterpreterStreamOutputMemberResult); ok {
			return &r.Value, stream.Err()
		}
	}

	return nil, stream.Err()
}

func TestCodeInterpreter_Files(t *testing.T) {
	t.Parallel()

	f := newFixture(t, defaultControl())
	session := startCI(t, f)

	_, err := invoke(t, f, session, types.ToolNameWriteFiles, &types.ToolArguments{Content: []types.InputContentBlock{
		{Path: new("data/a.txt"), Text: new("alpha")},
		{Path: new("b.bin"), Blob: []byte{0xff, 0xfe, 0x00}},
	}})
	require.NoError(t, err)

	res, err := invoke(
		t,
		f,
		session,
		types.ToolNameReadFiles,
		&types.ToolArguments{Paths: []string{"data/a.txt", "b.bin"}},
	)
	require.NoError(t, err)
	require.Len(t, res.Content, 2)
	assert.Equal(t, "alpha", *res.Content[0].Resource.Text)
	assert.Equal(t, []byte{0xff, 0xfe, 0x00}, res.Content[1].Resource.Blob)

	res, err = invoke(t, f, session, types.ToolNameListFiles, &types.ToolArguments{DirectoryPath: new("data")})
	require.NoError(t, err)
	require.Len(t, res.Content, 1)
	assert.Equal(t, "data/a.txt", *res.Content[0].Name)

	res, err = invoke(t, f, session, types.ToolNameRemoveFiles, &types.ToolArguments{Paths: []string{"data/a.txt"}})
	require.NoError(t, err)
	assert.False(t, *res.IsError)

	res, err = invoke(t, f, session, types.ToolNameReadFiles, &types.ToolArguments{Paths: []string{"data/a.txt"}})
	require.NoError(t, err)
	assert.True(t, *res.IsError, "a missing file is a tool error")
}

func TestCodeInterpreter_Errors(t *testing.T) {
	t.Parallel()

	f := newFixture(t, defaultControl())
	session := startCI(t, f)

	tests := []struct {
		name    string
		session string
		tool    types.ToolName
		args    *types.ToolArguments
		code    string
	}{
		{name: "no engine", session: session, tool: types.ToolNameExecuteCode,
			args: &types.ToolArguments{Code: new("print(1)"), Language: types.ProgrammingLanguagePython},
			code: "InternalServerException"},
		{name: "unsupported language", session: session, tool: types.ToolNameExecuteCode,
			args: &types.ToolArguments{Code: new("1"), Language: types.ProgrammingLanguageJavascript},
			code: "ValidationException"},
		{name: "unknown tool", session: session, tool: types.ToolNameStopTask, args: &types.ToolArguments{},
			code: "ValidationException"},
		{name: "unknown session", session: "missing", tool: types.ToolNameListFiles, args: &types.ToolArguments{},
			code: "ResourceNotFoundException"},
		{name: "path escape is contained", session: session, tool: types.ToolNameWriteFiles,
			args: &types.ToolArguments{Content: []types.InputContentBlock{{Path: new("../../x"), Text: new("x")}}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := invoke(t, f, tt.session, tt.tool, tt.args)
			assert.Equal(t, tt.code, apiCode(err))
		})
	}
}

func TestCodeInterpreter_StopReleasesSession(t *testing.T) {
	t.Parallel()

	f := newFixture(t, defaultControl())
	session := startCI(t, f)

	_, err := f.client.StopCodeInterpreterSession(t.Context(), &sdk.StopCodeInterpreterSessionInput{
		CodeInterpreterIdentifier: new(ciID), SessionId: &session,
	})
	require.NoError(t, err)

	_, err = invoke(t, f, session, types.ToolNameListFiles, &types.ToolArguments{})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err), "stopped sessions cannot be used")

	got, err := f.client.GetCodeInterpreterSession(t.Context(), &sdk.GetCodeInterpreterSessionInput{
		CodeInterpreterIdentifier: new(ciID), SessionId: &session,
	})
	require.NoError(t, err)
	assert.Equal(t, types.CodeInterpreterSessionStatusTerminated, got.Status)
}
