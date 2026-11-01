package lambdaweb_test

import (
	"archive/zip"
	"bytes"
	"context"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	lwsdk "github.com/aws/aws-sdk-go-v2/service/lambdaweb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/services/lambdaweb"
)

// fakeRuntime starts an in-process listener on the published host port, standing in for the container's app.
type fakeRuntime struct {
	listeners map[string]net.Listener
	logLines  string
	specs     []container.Spec
	removed   []string
	delay     time.Duration
	mu        sync.Mutex
}

func (r *fakeRuntime) CreateAndStart(_ context.Context, spec container.Spec) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.specs = append(r.specs, spec)

	if r.delay < 0 {
		return "ctr-1", nil
	}

	hostPort := strings.Split(spec.Ports[0], ":")
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", net.JoinHostPort(hostPort[0], hostPort[1]))
	if err != nil {
		return "", err
	}

	if r.listeners == nil {
		r.listeners = map[string]net.Listener{}
	}

	r.listeners["ctr-1"] = ln

	go func() {
		for {
			c, aerr := ln.Accept()
			if aerr != nil {
				return
			}

			_ = c // hold the connection open: the app is listening and quiet
		}
	}()

	return "ctr-1", nil
}

func (r *fakeRuntime) StopAndRemove(_ context.Context, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.removed = append(r.removed, id)

	if ln := r.listeners[id]; ln != nil {
		_ = ln.Close()
	}

	return nil
}

func (r *fakeRuntime) StreamLogs(_ context.Context, _ string, stdout, _ io.Writer) error {
	_, err := io.WriteString(stdout, r.logLines)

	return err
}

type fakeLogs struct {
	lines map[string][]string
	mu    sync.Mutex
}

func (l *fakeLogs) EnsureLogGroupAndStream(context.Context, string, string, string) error { return nil }

func (l *fakeLogs) PutLogLines(_ context.Context, _, group, _ string, lines []string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.lines == nil {
		l.lines = map[string][]string{}
	}

	l.lines[group] = append(l.lines[group], lines...)

	return nil
}

func (l *fakeLogs) snapshot(group string) []string {
	l.mu.Lock()
	defer l.mu.Unlock()

	return append([]string(nil), l.lines[group]...)
}

type fakeCreds struct{}

func (fakeCreds) IssueCredentials(roleARN, _ string) (lambdaweb.Credentials, error) {
	return lambdaweb.Credentials{AccessKeyID: "AKIAROLE", SecretAccessKey: "secret", SessionToken: roleARN}, nil
}

func buildZip(t *testing.T, files map[string]string) []byte {
	t.Helper()

	var buf bytes.Buffer

	zw := zip.NewWriter(&buf)
	for name, content := range files {
		w, err := zw.Create(name)
		require.NoError(t, err)
		_, err = io.WriteString(w, content)
		require.NoError(t, err)
	}

	require.NoError(t, zw.Close())

	return buf.Bytes()
}

func launchSpec(t *testing.T, env *testEnv, vars map[string]string) lambdaweb.LaunchSpec {
	t.Helper()

	rc := revisionConfig()
	rc.ServiceConfig.EnvironmentVariables = vars

	_, err := env.client.CreateWebFunction(
		t.Context(),
		&lwsdk.CreateWebFunctionInput{FunctionName: aws.String("app"), RevisionConfig: rc},
	)
	require.NoError(t, err)

	revs, err := env.client.ListWebFunctionRevisions(
		t.Context(),
		&lwsdk.ListWebFunctionRevisionsInput{FunctionName: aws.String("app")},
	)
	require.NoError(t, err)

	got, err := env.backend.GetWebFunctionRevision(testRegion, "app", aws.ToString(revs.Revisions[0].RevisionId))
	require.NoError(t, err)

	return lambdaweb.LaunchSpec{Revision: got}
}

func newLauncherEnv(t *testing.T, files map[string]string) (*testEnv, *fakeRuntime, *fakeLogs, lambdaweb.Launcher) {
	t.Helper()

	env := newTestEnv(t)
	codes := newFakeCodes()
	codes.objects["code/app.zip@v1"] = buildZip(t, files)
	env.backend.SetCodeStore(codes)

	rt := &fakeRuntime{}
	logs := &fakeLogs{}
	env.backend.SetLogSink(logs)
	env.backend.SetCredentialIssuer(fakeCreds{})
	env.backend.SetEndpointURL("http://host.docker.internal:4566")

	return env, rt, logs, lambdaweb.NewDockerLauncher(
		env.backend,
		lambdaweb.DockerConfig{Runtime: rt, ListenTimeout: 3 * time.Second},
	)
}

func TestDockerLauncher_Launch(t *testing.T) {
	t.Parallel()

	env, rt, logs, l := newLauncherEnv(t, map[string]string{"index.js": "console.log('hi')", "lib/util.js": "x"})
	rt.logLines = "line one\nline two\n"
	spec := launchSpec(t, env, map[string]string{"AWS_LAMBDA_NODEJS_ENTRYPOINT": "server.js", "FOO": "bar"})

	e, err := l.Launch(t.Context(), spec)
	require.NoError(t, err)

	host, port := e.Addr()
	assert.Equal(t, "127.0.0.1", host)
	assert.NotZero(t, port)
	assert.Regexp(t, `^\d{4}/\d{2}/\d{2}/\[rev-[0-9a-f]{8}\][0-9a-f]{32}$`, e.LogStream())

	require.Len(t, rt.specs, 1)
	cs := rt.specs[0]
	assert.Equal(t, "docker.io/library/node:24-slim", cs.Image)
	assert.Equal(t, []string{"node", "server.js"}, cs.Cmd)
	assert.Equal(t, "/var/task", cs.WorkingDir)
	assert.True(t, strings.HasSuffix(cs.Mounts[0], ":/var/task:ro"))
	assert.ElementsMatch(t, []string{
		"AWS_LAMBDA_HTTP_ENDPOINT=0.0.0.0:8080",
		"AWS_REGION=us-east-1",
		"AWS_DEFAULT_REGION=us-east-1",
		"TZ=:UTC",
		"AWS_ENDPOINT_URL=http://host.docker.internal:4566",
		"AWS_ACCESS_KEY_ID=AKIAROLE",
		"AWS_SECRET_ACCESS_KEY=secret",
		"AWS_SESSION_TOKEN=" + testRoleARN,
		"AWS_LAMBDA_NODEJS_ENTRYPOINT=server.js",
		"FOO=bar",
	}, cs.Env)

	dir, _, _ := strings.Cut(cs.Mounts[0], ":")
	b, err := os.ReadFile(dir + "/lib/util.js")
	require.NoError(t, err)
	assert.Equal(t, "x", string(b), "the package is extracted into the mounted directory")

	group := "/aws/lambda/web/app"
	assert.Eventually(t, func() bool { return len(logs.snapshot(group)) == 2 }, 3*time.Second, 5*time.Millisecond)
	assert.Equal(t, []string{"line one", "line two"}, logs.snapshot(group))
	assert.Equal(
		t,
		[]string{"line one", "line two"},
		logs.snapshot(group+".local"),
		"logs go to the .local companion too",
	)

	e.Close(t.Context())
	assert.Equal(t, []string{"ctr-1"}, rt.removed)

	_, statErr := os.Stat(dir)
	assert.True(t, os.IsNotExist(statErr), "the extracted package is removed on close")
}

func TestDockerLauncher_DefaultEntrypoint(t *testing.T) {
	t.Parallel()

	env, rt, _, l := newLauncherEnv(t, map[string]string{"index.js": "x"})

	e, err := l.Launch(t.Context(), launchSpec(t, env, nil))
	require.NoError(t, err)
	t.Cleanup(func() { e.Close(context.Background()) })

	assert.Equal(t, []string{"node", "index.js"}, rt.specs[0].Cmd)
}

func TestDockerLauncher_NeverListens(t *testing.T) {
	t.Parallel()

	env, rt, _, _ := newLauncherEnv(t, map[string]string{"index.js": "x"})
	rt.delay = -1

	l := lambdaweb.NewDockerLauncher(
		env.backend,
		lambdaweb.DockerConfig{Runtime: rt, ListenTimeout: 400 * time.Millisecond},
	)

	_, err := l.Launch(t.Context(), launchSpec(t, env, nil))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "did not start listening")
	assert.Equal(t, []string{"ctr-1"}, rt.removed, "the container is removed when the app never listens")
}

func TestDockerLauncher_CodeProblems(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		setup func(env *testEnv)
		files map[string]string
		raw   []byte
	}{
		{name: "zip slip", files: nil, raw: buildSlipZip(t)},
		{name: "not a zip", files: nil, raw: []byte("plain text")},
		{
			name:  "no code store",
			files: map[string]string{"index.js": "x"},
			setup: func(env *testEnv) { env.backend.SetCodeStore(nil) },
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			env, _, _, l := newLauncherEnv(t, map[string]string{"index.js": "x"})
			spec := launchSpec(t, env, nil)

			if tc.raw != nil {
				codes := newFakeCodes()
				codes.objects["code/app.zip@v1"] = tc.raw
				env.backend.SetCodeStore(codes)
			}

			if tc.setup != nil {
				tc.setup(env)
			}

			_, err := l.Launch(t.Context(), spec)
			require.Error(t, err)
			assert.ErrorIs(t, err, lambdaweb.ErrCodeUnavailable, "got %v", err)
		})
	}
}

func buildSlipZip(t *testing.T) []byte {
	t.Helper()

	var buf bytes.Buffer

	zw := zip.NewWriter(&buf)
	w, err := zw.Create("../../escape.txt")
	require.NoError(t, err)
	_, err = io.WriteString(w, "x")
	require.NoError(t, err)
	require.NoError(t, zw.Close())

	return buf.Bytes()
}
