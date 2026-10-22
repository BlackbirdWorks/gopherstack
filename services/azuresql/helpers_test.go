package azuresql_test

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azuresql"
)

var errBoom = errors.New("boom")

type fakeEngine struct {
	logins map[string]string
	dbs    map[string]string
	mu     sync.Mutex
	port   int
	closed bool
}

func (e *fakeEngine) Endpoint() (string, int) { return "127.0.0.1", e.port }

func (e *fakeEngine) SetLogin(_ context.Context, login, password string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.logins[login] = password

	return nil
}

func (e *fakeEngine) CreateDatabase(_ context.Context, name, collation string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if strings.HasPrefix(name, "fail") {
		return errBoom
	}

	e.dbs[name] = collation

	return nil
}

func (e *fakeEngine) DropDatabase(_ context.Context, name string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	delete(e.dbs, name)

	return nil
}

func (e *fakeEngine) ListDatabases(context.Context) ([]string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	names := make([]string, 0, len(e.dbs))
	for n := range e.dbs {
		names = append(names, n)
	}

	sort.Strings(names)

	return names, nil
}

func (e *fakeEngine) Close(context.Context) {
	e.mu.Lock()
	e.closed = true
	e.mu.Unlock()
}

type fakeLauncher struct {
	err     error
	engines []*fakeEngine
	mu      sync.Mutex
}

func (l *fakeLauncher) Launch(_ context.Context, spec azuresql.LaunchSpec) (azuresql.Engine, error) {
	if l.err != nil {
		return nil, l.err
	}

	e := &fakeEngine{logins: map[string]string{}, dbs: map[string]string{}}
	if spec.Login != "" {
		e.logins[spec.Login] = spec.Password
	}

	l.mu.Lock()
	e.port = 14330 + len(l.engines)
	l.engines = append(l.engines, e)
	l.mu.Unlock()

	return e, nil
}

func newBackend(t *testing.T, l azuresql.Launcher) *azuresql.InMemoryBackend {
	t.Helper()

	b := azuresql.NewInMemoryBackend()
	if l != nil {
		b.SetLauncher(l, azuresql.DefaultStartupTimeout)
	}

	t.Cleanup(b.Close)

	return b
}

func mustEnsure(t *testing.T, b *azuresql.InMemoryBackend, id string, spec azuresql.Spec) azuresql.Endpoint {
	t.Helper()

	ep, err := b.Ensure(id, spec)
	require.NoError(t, err)

	return ep
}
