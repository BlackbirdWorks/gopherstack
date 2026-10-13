package ssm

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/blackbirdworks/gopherstack/internal/dockercompat/client"
)

var errWriteScript = errors.New("write script failed")

const (
	scriptChunkChars = 60000
	killTimeout      = 10 * time.Second
	exitCodeKilled   = 137
)

// ContainerExecAPI runs one command in a container and returns its output and exit code.
type ContainerExecAPI interface {
	ContainerExec(
		ctx context.Context,
		containerID string,
		cmd []string,
	) (stdout, stderr []byte, exitCode int, err error)
}

// dockerScriptExecutor runs scripts through docker exec: the script is written to a temp file
// in the container and started with a recorded pid so a cancelled run can be killed.
type dockerScriptExecutor struct {
	api ContainerExecAPI
	seq atomic.Uint64
}

// NewDockerScriptExecutor returns a ScriptExecutor backed by the Docker daemon selected by DOCKER_HOST.
func NewDockerScriptExecutor() (ScriptExecutor, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv(), client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("create docker client: %w", err)
	}

	return NewContainerScriptExecutor(cli), nil
}

// NewContainerScriptExecutor returns a ScriptExecutor that drives api.
func NewContainerScriptExecutor(api ContainerExecAPI) ScriptExecutor {
	return &dockerScriptExecutor{api: api}
}

func (d *dockerScriptExecutor) RunScript(
	ctx context.Context,
	containerID string,
	req ScriptRequest,
) (ScriptResult, error) {
	base := fmt.Sprintf("/tmp/ssm-run-%d-%d", time.Now().UnixNano(), d.seq.Add(1))
	scriptPath, pidPath := base+".sh", base+".pid"

	defer d.cleanup(containerID, scriptPath, pidPath)

	if err := d.writeScript(ctx, containerID, scriptPath, req.Script); err != nil {
		return ScriptResult{}, err
	}

	interp := make([]string, len(req.Interpreter))
	for i, a := range req.Interpreter {
		interp[i] = strings.ReplaceAll(a, ScriptFilePlaceholder, scriptPath)
	}

	cmd := append([]string{
		"sh", "-c", `echo $$ > "$1"; if [ -n "$2" ]; then cd "$2" || exit 1; fi; shift 2; exec "$@"`,
		"sh", pidPath, req.WorkDir,
	}, interp...)

	type outcome struct {
		err    error
		stdout []byte
		stderr []byte
		code   int
	}

	done := make(chan outcome, 1)

	go func() {
		o, e, code, err := d.api.ContainerExec(context.WithoutCancel(ctx), containerID, cmd)
		done <- outcome{stdout: o, stderr: e, code: code, err: err}
	}()

	select {
	case r := <-done:
		return ScriptResult{Stdout: r.stdout, Stderr: r.stderr, ExitCode: r.code}, r.err
	case <-ctx.Done():
		d.kill(containerID, pidPath)

		select {
		case <-done:
		case <-time.After(killTimeout):
		}

		return ScriptResult{ExitCode: exitCodeKilled}, ctx.Err()
	}
}

func (d *dockerScriptExecutor) writeScript(ctx context.Context, containerID, path, script string) error {
	enc := base64.StdEncoding.EncodeToString([]byte(script))

	for i := 0; i == 0 || i < len(enc); i += scriptChunkChars {
		chunk := enc[i:min(i+scriptChunkChars, len(enc))]
		redir := ">>"

		if i == 0 {
			redir = ">"
		}

		cmd := []string{"sh", "-c", `printf %s "$1" | base64 -d ` + redir + ` "$2"`, "sh", chunk, path}

		_, stderr, code, err := d.api.ContainerExec(ctx, containerID, cmd)
		if err != nil {
			return fmt.Errorf("write script: %w", err)
		}

		if code != 0 {
			return fmt.Errorf("%w: exit %d: %s", errWriteScript, code, stderr)
		}
	}

	return nil
}

func (d *dockerScriptExecutor) kill(containerID, pidPath string) {
	ctx, cancel := context.WithTimeout(context.Background(), killTimeout)
	defer cancel()

	_, _, _, _ = d.api.ContainerExec(ctx, containerID, []string{
		"sh", "-c", `n=0; while [ ! -s "$1" ] && [ $n -lt 5 ]; do n=$((n+1)); sleep 1; done; ` +
			`p=$(cat "$1" 2>/dev/null) && [ -n "$p" ] && { pkill -KILL -P "$p" 2>/dev/null; kill -KILL "$p"; }`,
		"sh", pidPath,
	})
}

func (d *dockerScriptExecutor) cleanup(containerID string, paths ...string) {
	ctx, cancel := context.WithTimeout(context.Background(), killTimeout)
	defer cancel()

	_, _, _, _ = d.api.ContainerExec(ctx, containerID, append([]string{"rm", "-f"}, paths...))
}
