package lambdaweb_test

import (
	"bufio"
	"encoding/json/v2"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/services/lambdaweb"
)

const shimServer = `
const http = require('node:http');
console.info('booting');
http.createServer(async (req, res) => {
  const who = new URL(req.url, 'http://x').searchParams.get('who');
  console.log(who + ' start');
  await new Promise((r) => setTimeout(r, who === 'a' ? 60 : 10));
  console.warn({ who, step: 'after wait' });
  res.end('ok');
}).listen(Number(process.env.PORT), '127.0.0.1', () => console.log('listening'));
`

type shimLine struct {
	Message      any    `json:"message"`
	Level        string `json:"level"`
	RequestID    string `json:"requestId"`
	EndpointName string `json:"endpointName"`
}

func TestRuntimeShim_TagsConsoleOutputWithTheRequest(t *testing.T) {
	t.Parallel()

	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node is not installed")
	}

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "preload.cjs"), lambdaweb.PreloadScript(), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "server.js"), []byte(shimServer), 0o600))

	port := freeLocalPort(t)

	cmd := exec.CommandContext(t.Context(), node, "--require", filepath.Join(dir, "preload.cjs"),
		filepath.Join(dir, "server.js"))
	cmd.Env = append(os.Environ(), "PORT="+port)

	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)

	stderr, err := cmd.StderrPipe()
	require.NoError(t, err)
	require.NoError(t, cmd.Start())
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })

	lines := make(chan shimLine, 16)
	pump := func(sc *bufio.Scanner) {
		for sc.Scan() {
			var l shimLine
			if json.Unmarshal(sc.Bytes(), &l, jsonx.V1()) == nil {
				lines <- l
			}
		}
	}

	go pump(bufio.NewScanner(stdout))
	go pump(bufio.NewScanner(stderr))

	boot := <-lines
	assert.Equal(t, "INFO", boot.Level)
	assert.Equal(t, "booting", boot.Message)
	assert.Empty(t, boot.RequestID)
	assert.Equal(t, "listening", (<-lines).Message)

	done := make(chan struct{}, 2)
	client := httpclient.New(10 * time.Second)

	for _, who := range []string{"a", "b"} {
		go func() {
			target := "http://127.0.0.1:" + port + "/?who=" + who
			req, rerr := http.NewRequestWithContext(t.Context(), http.MethodGet, target, nil)
			if rerr == nil {
				req.Header.Set("Lambda-Web-Request-Id", "req-"+who)
				req.Header.Set("Lambda-Web-Endpoint-Name", "ep-"+who)

				if resp, derr := client.Do(req); derr == nil {
					_ = resp.Body.Close()
				}
			}

			done <- struct{}{}
		}()
	}

	<-done
	<-done

	byReq := map[string][]shimLine{}

	for tagged := 0; tagged < 4; {
		l := <-lines
		if l.RequestID == "" {
			continue
		}

		byReq[l.RequestID] = append(byReq[l.RequestID], l)
		tagged++
	}

	for _, who := range []string{"a", "b"} {
		got := byReq["req-"+who]
		require.Len(t, got, 2, who)
		assert.Equal(t, "ep-"+who, got[0].EndpointName)
		assert.Equal(t, who+" start", got[0].Message)
		assert.Equal(t, "INFO", got[0].Level)
		assert.Equal(t, "WARN", got[1].Level)
		assert.Equal(t, map[string]any{"who": who, "step": "after wait"}, got[1].Message,
			"an object argument stays structured and the request survives the await")
	}
}

func freeLocalPort(t *testing.T) string {
	t.Helper()

	var lc net.ListenConfig

	l, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	defer l.Close()

	addr, ok := l.Addr().(*net.TCPAddr)
	require.True(t, ok)

	return strconv.Itoa(addr.Port)
}

func TestRuntimeShim_HeaderLineLimit(t *testing.T) {
	t.Parallel()

	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node is not installed")
	}

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "preload.cjs"), lambdaweb.PreloadScript(), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "server.js"), []byte(`
const http = require('node:http');
const srv = http.createServer((req, res) => res.end('ok'));
srv.listen(0, '127.0.0.1', () => process.stdout.write(srv.address().port + '\n'));
`), 0o600))

	cmd := exec.CommandContext(t.Context(), node, "--max-http-header-size=131072", "--require",
		filepath.Join(dir, "preload.cjs"), filepath.Join(dir, "server.js"))
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, cmd.Start())
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })

	port, err := bufio.NewReader(stdout).ReadString('\n')
	require.NoError(t, err)

	port = strings.TrimSpace(port)

	client := httpclient.New(10 * time.Second)

	tests := []struct {
		name    string
		headers int
		want    int
	}{
		{name: "under the limit", headers: 900, want: http.StatusOK},
		{name: "over the limit", headers: 1100, want: http.StatusRequestHeaderFieldsTooLarge},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			req, rerr := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://127.0.0.1:"+port+"/", nil)
			require.NoError(t, rerr)

			for i := range tc.headers {
				req.Header.Set("X-H"+strconv.Itoa(i), "v")
			}

			resp, derr := client.Do(req)
			require.NoError(t, derr)

			defer resp.Body.Close()

			assert.Equal(t, tc.want, resp.StatusCode)
		})
	}
}
