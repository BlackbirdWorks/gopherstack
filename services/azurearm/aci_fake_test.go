package azurearm_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"strings"
	"sync"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/services/azurearm"
)

var (
	errFakePull    = errors.New("pull access denied")
	errFakeBox     = errors.New("no such container")
	errFakeNoShare = errors.New("share not found")
)

type fakeBox struct {
	exit    chan int
	gone    chan struct{}
	spec    container.Spec
	removed bool
}

// fakeACIRuntime runs "containers" that exit immediately with scripted codes per image, or run until removed.
type fakeACIRuntime struct {
	boxes   map[string]*fakeBox
	scripts map[string][]int
	onStart func(spec container.Spec)
	follow  func(ctx context.Context, out io.Writer) error
	execFn  func(spec container.Spec, cmd []string) int
	logs    map[string]string
	creds   map[string]*azurearm.ACICredential
	specs   []container.Spec
	pulls   []string
	mu      sync.Mutex
	seq     int
}

func newFakeACIRuntime() *fakeACIRuntime {
	return &fakeACIRuntime{
		boxes:   map[string]*fakeBox{},
		scripts: map[string][]int{},
		logs:    map[string]string{},
		creds:   map[string]*azurearm.ACICredential{},
	}
}

func (f *fakeACIRuntime) Pull(_ context.Context, ref string, cred *azurearm.ACICredential) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.pulls = append(f.pulls, ref)
	f.creds[ref] = cred

	if strings.Contains(ref, "missing") {
		return false, fmt.Errorf("%w: %s", errFakePull, ref)
	}

	return cred != nil, nil
}

func (f *fakeACIRuntime) Start(_ context.Context, spec container.Spec) (string, error) {
	f.mu.Lock()
	f.seq++
	id := fmt.Sprintf("fake-%d", f.seq)
	b := &fakeBox{exit: make(chan int, 1), gone: make(chan struct{}), spec: spec}

	if codes := f.scripts[spec.Image]; len(codes) > 0 {
		b.exit <- codes[0]
		f.scripts[spec.Image] = codes[1:]
	}

	f.boxes[id] = b
	f.specs = append(f.specs, spec)
	hook := f.onStart
	f.mu.Unlock()

	if hook != nil {
		hook(spec)
	}

	return id, nil
}

func (f *fakeACIRuntime) Wait(ctx context.Context, id string) (int, error) {
	f.mu.Lock()
	b := f.boxes[id]
	f.mu.Unlock()

	if b == nil {
		return 0, errFakeBox
	}

	select {
	case code := <-b.exit:
		return code, nil
	case <-b.gone:
		return 137, nil
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func (f *fakeACIRuntime) Logs(_ context.Context, id, tail string) (string, error) {
	f.mu.Lock()
	b := f.boxes[id]
	f.mu.Unlock()

	if b == nil || b.removed {
		return "", errFakeBox
	}

	f.mu.Lock()
	override, ok := f.logs[b.spec.Image]
	f.mu.Unlock()

	if ok {
		return override, nil
	}

	lines := []string{"starting " + b.spec.Image, "line two", "line three"}
	if tail == "1" {
		lines = lines[2:]
	}

	return strings.Join(lines, "\n") + "\n", nil
}

func (f *fakeACIRuntime) Exec(_ context.Context, id string, cmd []string) (int, error) {
	f.mu.Lock()
	b := f.boxes[id]
	fn := f.execFn
	f.mu.Unlock()

	if b == nil || b.removed {
		return 0, errFakeBox
	}

	if fn == nil {
		return 0, nil
	}

	return fn(b.spec, cmd), nil
}

func (f *fakeACIRuntime) Remove(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if b := f.boxes[id]; b != nil && !b.removed {
		b.removed = true
		close(b.gone)
	}

	return nil
}

func (f *fakeACIRuntime) started() []container.Spec {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]container.Spec(nil), f.specs...)
}

func (f *fakeACIRuntime) live() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	n := 0

	for _, b := range f.boxes {
		if !b.removed {
			n++
		}
	}

	return n
}

func (f *fakeACIRuntime) credFor(ref string) *azurearm.ACICredential {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.creds[ref]
}

var (
	_ azurearm.ACIRuntime     = (*fakeACIRuntime)(nil)
	_ azurearm.ACIExecRuntime = (*fakeACIRuntime)(nil)
)

type fakeShares struct {
	trees map[string]azurearm.ACIShareTree
	mu    sync.Mutex
}

func (f *fakeShares) ExportShare(account, share string) (azurearm.ACIShareTree, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	t, ok := f.trees[account+"/"+share]
	if !ok {
		return azurearm.ACIShareTree{}, errFakeNoShare
	}

	return t, nil
}

func (f *fakeShares) ApplyShare(account, share string, tree azurearm.ACIShareTree) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.trees[account+"/"+share] = tree

	return nil
}

func (f *fakeShares) tree(account, share string) azurearm.ACIShareTree {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.trees[account+"/"+share]
}

func (f *fakeACIRuntime) ExecStream(_ context.Context, id string, cmd []string, out io.Writer) (int, error) {
	f.mu.Lock()
	b := f.boxes[id]
	f.mu.Unlock()

	if b == nil || b.removed {
		return 0, errFakeBox
	}

	_, err := io.WriteString(out, "ran:"+strings.Join(cmd, " "))

	return 0, err
}

func (f *fakeACIRuntime) ExecStdin(
	ctx context.Context, id string, cmd []string, stdin io.Reader, out io.Writer, term azurearm.ACITerminalSize,
) (int, error) {
	if _, err := f.ExecStream(ctx, id, cmd, out); err != nil {
		return 0, err
	}

	if term.Rows > 0 {
		fmt.Fprintf(out, " tty=%dx%d", term.Rows, term.Cols)
	}

	if !strings.HasSuffix(strings.Join(cmd, " "), "cat") {
		return 0, nil
	}

	buf := make([]byte, 64)

	var err error

	for err == nil {
		var n int

		n, err = stdin.Read(buf)
		if n > 0 {
			_, _ = io.WriteString(out, "stdin:"+string(buf[:n]))
		}
	}

	return 0, nil
}

func (f *fakeACIRuntime) FollowLogs(ctx context.Context, id string, out io.Writer) error {
	f.mu.Lock()
	fn := f.follow
	f.mu.Unlock()

	if fn != nil {
		return fn(ctx, out)
	}

	logs, err := f.Logs(ctx, id, "")
	if err != nil {
		return err
	}

	_, err = io.WriteString(out, logs)

	return err
}

func (f *fakeShares) set(key, name string, data []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()

	t := f.trees[key]
	files := maps.Clone(t.Files)
	if files == nil {
		files = map[string][]byte{}
	}

	files[name] = data
	f.trees[key] = azurearm.ACIShareTree{Files: files, Dirs: t.Dirs}
}

func (f *fakeShares) remove(key, name string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	t := f.trees[key]
	files := map[string][]byte{}

	for k, v := range t.Files {
		if k != name {
			files[k] = v
		}
	}

	f.trees[key] = azurearm.ACIShareTree{Files: files, Dirs: t.Dirs}
}

func (f *fakeShares) contents(key string) map[string]string {
	f.mu.Lock()
	defer f.mu.Unlock()

	out := map[string]string{}
	for k, v := range f.trees[key].Files {
		out[k] = string(v)
	}

	return out
}
