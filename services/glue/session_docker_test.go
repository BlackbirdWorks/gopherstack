package glue_test

import (
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/services/glue"
)

const dockerSessionImage = "python:3.12-slim"

func TestDockerSessionEngine_PersistentREPL(t *testing.T) {
	t.Parallel()

	if os.Getenv("GOPHERSTACK_TEST_DOCKER") != "1" {
		t.Skip("set GOPHERSTACK_TEST_DOCKER=1 to run docker-backed session engine tests")
	}

	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI not available")
	}

	rt, err := container.NewRuntime(container.Config{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = rt.Close() })

	b := glue.NewInMemoryBackend(testAccountID, testRegion)
	b.SetSessionEngine(glue.NewDockerSessionEngine(glue.DockerSessionConfig{
		Runtime: glue.NewContainerSessionRuntime(rt), Image: dockerSessionImage, StartTimeout: 3 * time.Minute,
	}))
	t.Cleanup(b.StopReconciler)

	_, err = b.CreateSession(
		"dk", "arn:aws:iam::123456789012:role/r", glue.SessionCommand{Name: "python"}, glue.Session{},
	)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		s, gErr := b.GetSession("dk")
		require.NoError(t, gErr)
		require.NotEqual(t, "FAILED", s.Status, s.ErrorMessage)

		return s.Status == "READY"
	}, 4*time.Minute, 200*time.Millisecond)

	run := func(code, state string) *glue.StatementOutput {
		st, rErr := b.RunStatement("dk", code)
		require.NoError(t, rErr)

		return outputOf(t, waitStatementFor(t, b, "dk", st.Id, state, 60*time.Second))
	}

	run("x = 21", "AVAILABLE")

	out := run("x * 2", "AVAILABLE")
	require.NotNil(t, out.Data)
	assert.Equal(t, "42", out.Data.TextPlain)
	assert.Equal(t, int32(2), out.ExecutionCount)

	out = run("print('hi'); 1/0", "ERROR")
	assert.Equal(t, "ZeroDivisionError", out.ErrorName)
	assert.NotEmpty(t, out.Traceback)

	out = run("x + 1", "AVAILABLE")
	assert.Equal(t, "22", out.Data.TextPlain)

	slow, err := b.RunStatement("dk", "import time\ntime.sleep(120)")
	require.NoError(t, err)
	waitStatementFor(t, b, "dk", slow.Id, "RUNNING", 30*time.Second)
	require.NoError(t, b.CancelStatement("dk", slow.Id))
	waitStatementFor(t, b, "dk", slow.Id, "CANCELLED", 30*time.Second)

	out = run("x", "AVAILABLE")
	assert.Equal(t, "21", out.Data.TextPlain)

	require.NoError(t, b.StopSession("dk"))
	require.Eventually(t, func() bool {
		s, gErr := b.GetSession("dk")
		require.NoError(t, gErr)

		return s.Status == "STOPPED"
	}, 30*time.Second, 50*time.Millisecond)
}

func waitStatementFor(
	t *testing.T, b *glue.InMemoryBackend, sid string, id int32, want string, d time.Duration,
) *glue.Statement {
	t.Helper()

	var got *glue.Statement

	require.Eventually(t, func() bool {
		st, err := b.GetStatement(sid, id)
		require.NoError(t, err)
		got = st

		return st.State == want
	}, d, 50*time.Millisecond, "statement %d never reached %s", id, want)

	return got
}
