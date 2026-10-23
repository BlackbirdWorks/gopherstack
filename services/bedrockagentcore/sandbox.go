package bedrockagentcore

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

// idleProgram keeps the sandbox container alive and exits promptly on SIGTERM (a PID 1 ignores it by default).
const idleProgram = "import signal, sys, time\n" +
	"signal.signal(signal.SIGTERM, lambda *a: sys.exit(0))\n" +
	"while True: time.sleep(3600)"

const (
	workspaceMount = "/workspace"
	scratchMount   = "/scratch"
	dirMode        = 0o755
	fileMode       = 0o644
	maxFileBytes   = 8 << 20
	maxPathLen     = 4096
)

// sandbox is the per-session workspace: a host directory mounted into a lazily started container.
type sandbox struct {
	boot        chan struct{}
	dir         string
	scratch     string
	containerID string
	execSeq     int
}

func newSandbox() (*sandbox, error) {
	dir, err := os.MkdirTemp("", "agentcore-sandbox-*")
	if err != nil {
		return nil, fmt.Errorf("create sandbox dir: %w", err)
	}

	scratch, err := os.MkdirTemp("", "agentcore-scratch-*")
	if err != nil {
		removeAll(dir)

		return nil, fmt.Errorf("create scratch dir: %w", err)
	}

	sb := &sandbox{dir: dir, scratch: scratch, boot: make(chan struct{}, 1)}
	sb.boot <- struct{}{}

	return sb, nil
}

// sandbox returns the session's sandbox, creating its workspace on first use.
func (e *engine) sandbox(key string) (*sandbox, error) {
	e.mu.Lock("sandbox")
	defer e.mu.Unlock()

	if sb, ok := e.sandboxes[key]; ok {
		return sb, nil
	}

	sb, err := newSandbox()
	if err != nil {
		return nil, internal("preparing the sandbox failed")
	}

	e.sandboxes[key] = sb

	return sb, nil
}

func (e *engine) releaseSandbox(key string) {
	if e == nil {
		return
	}

	e.mu.Lock("releaseSandbox")
	sb, ok := e.sandboxes[key]
	delete(e.sandboxes, key)
	e.mu.Unlock()

	if ok {
		e.teardownSandbox(sb)
	}
}

func (e *engine) teardownSandbox(sb *sandbox) {
	<-sb.boot
	e.stopContainer(sb.containerID)
	removeAll(sb.dir)
	removeAll(sb.scratch)
}

// ensureContainer starts the sandbox container on first command execution.
func (e *engine) ensureContainer(ctx context.Context, sb *sandbox) (string, error) {
	select {
	case <-sb.boot:
	case <-ctx.Done():
		return "", ctx.Err()
	}

	defer func() { sb.boot <- struct{}{} }()

	if sb.containerID != "" {
		return sb.containerID, nil
	}

	id, err := e.cfg.Runtime.CreateAndStart(ctx, e.sandboxSpec(sb))
	if err != nil {
		return "", internal("starting the code interpreter container failed: %v", err)
	}

	sb.containerID = id

	return id, nil
}

func (e *engine) sandboxSpec(sb *sandbox) container.Spec {
	env := []string{"HOME=/tmp", "PYTHONDONTWRITEBYTECODE=1", "PYTHONUNBUFFERED=1"}
	if e.cfg.EndpointURL != "" {
		env = append(env, "AWS_ENDPOINT_URL="+e.cfg.EndpointURL)
	}

	return container.Spec{
		Image:      e.cfg.PythonImage,
		Cmd:        []string{"python3", "-c", idleProgram},
		Env:        env,
		WorkingDir: workspaceMount,
		User:       strconv.Itoa(os.Getuid()) + ":" + strconv.Itoa(os.Getgid()),
		Mounts:     []string{sb.dir + ":" + workspaceMount, sb.scratch + ":" + scratchMount + ":ro"},
	}
}

// run executes cmd in the sandbox container.
func (e *engine) run(ctx context.Context, sb *sandbox, cmd []string) (container.ExecResult, error) {
	if e.cfg.Runtime == nil {
		return container.ExecResult{}, errNoRuntime
	}

	id, err := e.ensureContainer(ctx, sb)
	if err != nil {
		return container.ExecResult{}, err
	}

	res, err := e.cfg.Runtime.Exec(ctx, id, cmd)
	if err != nil {
		return res, internal("executing in the sandbox failed: %v", err)
	}

	return res, nil
}

// stageScript writes code to the read-only scratch mount and returns its in-container path.
func (sb *sandbox) stageScript(code, ext string) (string, error) {
	sb.execSeq++
	name := "exec-" + strconv.Itoa(sb.execSeq) + ext

	if err := os.WriteFile(filepath.Join(sb.scratch, name), []byte(code), fileMode); err != nil {
		return "", internal("staging the code failed")
	}

	return path.Join(scratchMount, name), nil
}

// workspacePath validates a caller path and returns it relative to the workspace root.
func workspacePath(p string) (string, error) {
	if p == "" || len(p) > maxPathLen || strings.ContainsRune(p, 0) {
		return "", invalid("path must be a non-empty relative path of at most %d characters", maxPathLen)
	}

	rel := strings.TrimPrefix(path.Clean("/"+strings.TrimPrefix(p, "file://")), "/")
	if rel == "" {
		rel = "."
	}

	return rel, nil
}

func (sb *sandbox) root() (*os.Root, error) {
	r, err := os.OpenRoot(sb.dir)
	if err != nil {
		return nil, internal("opening the sandbox failed")
	}

	return r, nil
}

func (sb *sandbox) writeFile(p string, data []byte) error {
	rel, err := workspacePath(p)
	if err != nil {
		return err
	}

	if len(data) > maxFileBytes {
		return invalid("file %q exceeds the %d byte limit", p, maxFileBytes)
	}

	r, err := sb.root()
	if err != nil {
		return err
	}

	defer r.Close()

	if dir := path.Dir(rel); dir != "." {
		if err = r.MkdirAll(dir, dirMode); err != nil {
			return invalid("cannot create directory for %q", p)
		}
	}

	if err = r.WriteFile(rel, data, fileMode); err != nil {
		return invalid("cannot write %q", p)
	}

	return nil
}

func (sb *sandbox) readFile(p string) ([]byte, error) {
	rel, err := workspacePath(p)
	if err != nil {
		return nil, err
	}

	r, err := sb.root()
	if err != nil {
		return nil, err
	}

	defer r.Close()

	st, err := r.Stat(rel)
	if err != nil || st.IsDir() {
		return nil, notFound("file %q not found", p)
	}

	if st.Size() > maxFileBytes {
		return nil, invalid("file %q exceeds the %d byte limit", p, maxFileBytes)
	}

	data, err := r.ReadFile(rel)
	if err != nil {
		return nil, notFound("file %q not found", p)
	}

	return data, nil
}

func (sb *sandbox) removeFile(p string) error {
	rel, err := workspacePath(p)
	if err != nil {
		return err
	}

	if rel == "." {
		return invalid("the workspace root cannot be removed")
	}

	r, err := sb.root()
	if err != nil {
		return err
	}

	defer r.Close()

	if _, err = r.Lstat(rel); err != nil {
		return notFound("file %q not found", p)
	}

	if err = r.RemoveAll(rel); err != nil {
		return invalid("cannot remove %q", p)
	}

	return nil
}

type fileEntry struct {
	name  string
	isDir bool
}

func (sb *sandbox) listDir(p string) ([]fileEntry, error) {
	rel := "."
	if p != "" {
		var err error
		if rel, err = workspacePath(p); err != nil {
			return nil, err
		}
	}

	r, err := sb.root()
	if err != nil {
		return nil, err
	}

	defer r.Close()

	ents, err := fs.ReadDir(r.FS(), rel)
	if err != nil {
		return nil, notFound("directory %q not found", p)
	}

	out := make([]fileEntry, 0, len(ents))
	for _, en := range ents {
		out = append(out, fileEntry{name: path.Join(rel, en.Name()), isDir: en.IsDir()})
	}

	return out, nil
}
