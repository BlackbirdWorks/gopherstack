package bolt_test

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/bolt"
)

var errBoom = errors.New("boom")

type fakeBackend struct {
	rows     [][]any
	log      []string
	mu       sync.Mutex
	authFail bool
}

func (f *fakeBackend) record(s string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.log = append(f.log, s)
}

func (f *fakeBackend) events() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]string(nil), f.log...)
}

func (f *fakeBackend) NewSession() bolt.Session { return &fakeSession{f: f} }

func (f *fakeBackend) Authenticate(_ context.Context, auth map[string]any) error {
	if f.authFail {
		return &bolt.Error{Code: bolt.CodeUnauthorized, Message: "denied " + auth["scheme"].(string)}
	}

	return nil
}

type fakeSession struct{ f *fakeBackend }

func (s *fakeSession) Run(_ context.Context, q string, _ map[string]any) (*bolt.Result, error) {
	s.f.record("run:" + q)

	switch q {
	case "FAIL":
		return nil, &bolt.Error{Code: bolt.CodeSyntax, Message: "bad query"}
	case "PLAIN":
		return nil, errBoom
	}

	return &bolt.Result{Fields: []string{"n"}, Rows: s.f.rows, Write: strings.HasPrefix(q, "CREATE")}, nil
}

func (s *fakeSession) Begin(context.Context) error {
	s.f.record("begin")

	return nil
}

func (s *fakeSession) Commit(context.Context) error {
	s.f.record("commit")

	return nil
}

func (s *fakeSession) Rollback(context.Context) error {
	s.f.record("rollback")

	return nil
}

func (s *fakeSession) Close() {}

func rowsOf(n int) [][]any {
	rows := make([][]any, n)
	for i := range rows {
		rows[i] = []any{int64(i)}
	}

	return rows
}

func startServer(t *testing.T, b *fakeBackend, vs ...bolt.Version) *bolt.Client {
	t.Helper()

	cli, srv := net.Pipe()

	t.Cleanup(func() { _ = cli.Close() })

	go bolt.Serve(t.Context(), srv, bolt.Config{Backend: b, Agent: "Neptune/test", Database: "neo4j", Addr: "h:1"})

	c, err := bolt.Dial(cli, vs...)
	require.NoError(t, err)

	return c
}

func login(t *testing.T, c *bolt.Client) {
	t.Helper()

	auth := map[string]any{"scheme": "basic", "principal": "u", "credentials": "p"}

	if c.Version.Major == 5 && c.Version.Minor >= 1 {
		r, err := c.Call(bolt.TagHello, map[string]any{"user_agent": "t"})
		require.NoError(t, err)
		require.Equal(t, bolt.TagSuccess, r.Tag)

		r, err = c.Call(bolt.TagLogon, auth)
		require.NoError(t, err)
		require.Equal(t, bolt.TagSuccess, r.Tag)

		return
	}

	hello := map[string]any{"user_agent": "t", "scheme": "basic", "principal": "u", "credentials": "p"}
	r, err := c.Call(bolt.TagHello, hello)
	require.NoError(t, err)
	require.Equal(t, bolt.TagSuccess, r.Tag)
	assert.Equal(t, "Neptune/test", r.Meta()["server"])
}

func TestServeQuery(t *testing.T) {
	t.Parallel()

	versions := []bolt.Version{{Major: 4, Minor: 4}, {Major: 5, Minor: 0}, {Major: 5, Minor: 1}, {Major: 5, Minor: 4}}

	for _, v := range versions {
		t.Run(string(rune('0'+v.Major))+"."+string(rune('0'+v.Minor)), func(t *testing.T) {
			t.Parallel()

			c := startServer(t, &fakeBackend{rows: rowsOf(5)}, v)
			assert.Equal(t, v, c.Version)
			login(t, c)

			rows, sum, err := c.Query("MATCH (n) RETURN n", nil)
			require.NoError(t, err)
			assert.Len(t, rows, 5)
			assert.Equal(t, int64(4), rows[4][0])
			assert.Equal(t, "r", sum.Meta()["type"])
			assert.NotEmpty(t, sum.Meta()["bookmark"])
		})
	}
}

func TestPullBatching(t *testing.T) {
	t.Parallel()

	c := startServer(t, &fakeBackend{rows: rowsOf(5)}, bolt.Version{Major: 5, Minor: 4})
	login(t, c)

	run, err := c.Call(bolt.TagRun, "CREATE", map[string]any{}, map[string]any{})
	require.NoError(t, err)
	assert.Equal(t, []any{"n"}, run.Meta()["fields"])

	rows, sum, err := c.PullAll(2)
	require.NoError(t, err)
	assert.Len(t, rows, 2)
	assert.Equal(t, true, sum.Meta()["has_more"])

	rows, sum, err = c.PullAll(2)
	require.NoError(t, err)
	assert.Len(t, rows, 2)
	assert.Equal(t, true, sum.Meta()["has_more"])

	rows, sum, err = c.PullAll(2)
	require.NoError(t, err)
	assert.Len(t, rows, 1)
	assert.Nil(t, sum.Meta()["has_more"])
	assert.Equal(t, "w", sum.Meta()["type"])

	// DISCARD drops the remainder.
	_, err = c.Call(bolt.TagRun, "MATCH", map[string]any{}, map[string]any{})
	require.NoError(t, err)

	r, err := c.Call(bolt.TagDiscard, map[string]any{"n": int64(-1)})
	require.NoError(t, err)
	assert.Equal(t, bolt.TagSuccess, r.Tag)
	assert.Nil(t, r.Meta()["has_more"])
}

func TestTransactions(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		want []string
		end  byte
	}{
		{"commit", []string{"begin", "run:CREATE", "commit"}, bolt.TagCommit},
		{"rollback", []string{"begin", "run:CREATE", "rollback"}, bolt.TagRollback},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := &fakeBackend{rows: rowsOf(1)}
			c := startServer(t, b, bolt.Version{Major: 4, Minor: 4})
			login(t, c)

			r, err := c.Call(bolt.TagBegin, map[string]any{})
			require.NoError(t, err)
			require.Equal(t, bolt.TagSuccess, r.Tag)

			r, err = c.Call(bolt.TagRun, "CREATE", map[string]any{}, map[string]any{})
			require.NoError(t, err)
			assert.Equal(t, int64(0), r.Meta()["qid"])

			rows, sum, err := c.PullAll(-1)
			require.NoError(t, err)
			assert.Len(t, rows, 1)
			assert.Nil(t, sum.Meta()["bookmark"])

			r, err = c.Call(tc.end)
			require.NoError(t, err)
			assert.Equal(t, bolt.TagSuccess, r.Tag)
			assert.Equal(t, tc.want, b.events())
		})
	}
}

func TestFailureAndReset(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name, query, code string
	}{
		{"typed", "FAIL", bolt.CodeSyntax},
		{"untyped", "PLAIN", bolt.CodeDatabaseError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := startServer(t, &fakeBackend{rows: rowsOf(1)}, bolt.Version{Major: 5, Minor: 0})
			login(t, c)

			r, err := c.Call(bolt.TagRun, tc.query, map[string]any{}, map[string]any{})
			require.NoError(t, err)
			require.Equal(t, bolt.TagFailure, r.Tag)
			assert.Equal(t, tc.code, r.Meta()["code"])

			r, err = c.Call(bolt.TagRun, "MATCH", map[string]any{}, map[string]any{})
			require.NoError(t, err)
			assert.Equal(t, bolt.TagIgnore, r.Tag)

			r, err = c.Call(bolt.TagReset)
			require.NoError(t, err)
			assert.Equal(t, bolt.TagSuccess, r.Tag)

			rows, _, err := c.Query("MATCH", nil)
			require.NoError(t, err)
			assert.Len(t, rows, 1)
		})
	}
}

func TestAuthFailure(t *testing.T) {
	t.Parallel()

	c := startServer(t, &fakeBackend{authFail: true}, bolt.Version{Major: 5, Minor: 4})

	_, err := c.Call(bolt.TagHello, map[string]any{})
	require.NoError(t, err)

	r, err := c.Call(bolt.TagLogon, map[string]any{"scheme": "basic"})
	require.NoError(t, err)
	assert.Equal(t, bolt.TagFailure, r.Tag)
	assert.Equal(t, bolt.CodeUnauthorized, r.Meta()["code"])
}

func TestRoute(t *testing.T) {
	t.Parallel()

	c := startServer(t, &fakeBackend{}, bolt.Version{Major: 4, Minor: 4})
	login(t, c)

	r, err := c.Call(bolt.TagRoute, map[string]any{}, []any{}, map[string]any{"db": "g"})
	require.NoError(t, err)
	require.Equal(t, bolt.TagSuccess, r.Tag)

	rt, ok := r.Meta()["rt"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "g", rt["db"])

	servers, ok := rt["servers"].([]any)
	require.True(t, ok)
	require.Len(t, servers, 3)
	assert.Equal(t, []any{"h:1"}, servers[0].(map[string]any)["addresses"])
}

func TestProtocolViolationCloses(t *testing.T) {
	t.Parallel()

	c := startServer(t, &fakeBackend{}, bolt.Version{Major: 5, Minor: 0})

	r, err := c.Call(bolt.TagRun, "x", map[string]any{}, map[string]any{})
	require.NoError(t, err)
	assert.Equal(t, bolt.TagFailure, r.Tag)

	_, err = c.Recv()
	assert.Error(t, err)
}

func TestHandshake(t *testing.T) {
	t.Parallel()

	slots := func(s ...[4]byte) [16]byte {
		var out [16]byte
		for i, x := range s {
			copy(out[i*4:], x[:])
		}

		return out
	}

	for _, tc := range []struct {
		name  string
		slots [16]byte
		want  bolt.Version
		ok    bool
	}{
		{"exact54", slots([4]byte{0, 0, 4, 5}), bolt.Version{Major: 5, Minor: 4}, true},
		{"range", slots([4]byte{0, 4, 7, 5}), bolt.Version{Major: 5, Minor: 4}, true},
		{"v44range", slots([4]byte{0, 2, 4, 4}), bolt.Version{Major: 4, Minor: 4}, true},
		{"fallback", slots([4]byte{0, 0, 0, 3}, [4]byte{0, 0, 0, 5}), bolt.Version{Major: 5, Minor: 0}, true},
		{"none", slots([4]byte{0, 0, 9, 9}), bolt.Version{}, false},
		{"v3", slots([4]byte{0, 0, 0, 3}), bolt.Version{}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cli, srv := net.Pipe()
			defer func() { _ = cli.Close() }()

			go bolt.Serve(t.Context(), srv, bolt.Config{Backend: &fakeBackend{}})

			c, err := bolt.DialRaw(cli, tc.slots)
			if !tc.ok {
				require.ErrorIs(t, err, bolt.ErrNoVersion)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.want, c.Version)
		})
	}
}

func TestBadMagicClosed(t *testing.T) {
	t.Parallel()

	cli, srv := net.Pipe()
	defer func() { _ = cli.Close() }()

	done := make(chan struct{})

	go func() {
		bolt.Serve(t.Context(), srv, bolt.Config{Backend: &fakeBackend{}})
		close(done)
	}()

	_, err := cli.Write(make([]byte, 20))
	require.NoError(t, err)
	<-done
}

func TestSplit(t *testing.T) {
	t.Parallel()

	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	served := make(chan struct{}, 1)
	sl := bolt.Split(ln, func(c net.Conn) {
		bolt.Serve(t.Context(), c, bolt.Config{Backend: &fakeBackend{rows: rowsOf(1)}, Agent: "Neptune/test"})
		served <- struct{}{}
	})

	t.Cleanup(func() { _ = sl.Close() })

	rest := make(chan string, 1)

	go func() {
		c, aerr := sl.Accept()
		if aerr != nil {
			return
		}

		buf := make([]byte, 4)
		_, _ = c.Read(buf)
		rest <- string(buf)
		_ = c.Close()
	}()

	hc, err := (&net.Dialer{}).DialContext(t.Context(), "tcp", ln.Addr().String())
	require.NoError(t, err)

	_, err = hc.Write([]byte("GET /"))
	require.NoError(t, err)
	assert.Equal(t, "GET ", <-rest)
	_ = hc.Close()

	bc, err := (&net.Dialer{}).DialContext(t.Context(), "tcp", ln.Addr().String())
	require.NoError(t, err)

	c, err := bolt.Dial(bc, bolt.Version{Major: 5, Minor: 0})
	require.NoError(t, err)
	login(t, c)

	rows, _, err := c.Query("MATCH", nil)
	require.NoError(t, err)
	assert.Len(t, rows, 1)

	_, err = c.Call(bolt.TagGoodbye)
	assert.Error(t, err)
	<-served
}
