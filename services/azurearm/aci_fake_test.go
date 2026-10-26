package azurearm_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/services/azurearm"
)

var errFakePull = errors.New("pull access denied")

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
	creds   map[string]*azurearm.ACICredential
	specs   []container.Spec
	pulls   []string
	mu      sync.Mutex
	seq     int
}

func newFakeACIRuntime() *fakeACIRuntime {
	return &fakeACIRuntime{
		boxes: map[string]*fakeBox{}, scripts: map[string][]int{}, creds: map[string]*azurearm.ACICredential{},
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
		return 0, errors.New("no such container")
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
		return "", errors.New("no such container")
	}

	lines := []string{"starting " + b.spec.Image, "line two", "line three"}
	if tail == "1" {
		lines = lines[2:]
	}

	return strings.Join(lines, "\n") + "\n", nil
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

var _ azurearm.ACIRuntime = (*fakeACIRuntime)(nil)

type fakeShares struct {
	trees map[string]azurearm.ACIShareTree
	mu    sync.Mutex
}

func (f *fakeShares) ExportShare(account, share string) (azurearm.ACIShareTree, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	t, ok := f.trees[account+"/"+share]
	if !ok {
		return azurearm.ACIShareTree{}, errors.New("share not found")
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

func mustNoErr(t *testing.T, err error) {
	t.Helper()
	require.NoError(t, err)
}
