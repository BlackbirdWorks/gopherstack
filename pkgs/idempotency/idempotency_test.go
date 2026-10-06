package idempotency_test

import (
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/idempotency"
)

var errGone = errors.New("gone")

type res struct{ id string }

func newCreator(counter *int) func() (*res, error) {
	return func() (*res, error) {
		*counter++

		return &res{id: "r" + strconv.Itoa(*counter)}, nil
	}
}

func TestCreate(t *testing.T) {
	t.Parallel()

	idOf := func(r *res) string { return r.id }
	getOK := func(id string) (*res, error) { return &res{id: id}, nil }
	getGone := func(string) (*res, error) { return nil, errGone }

	type call struct{ token, print string }

	same := []call{{"t", "a"}, {"t", "a"}}

	tests := []struct {
		name      string
		get       func(string) (*res, error)
		calls     []call
		wantIDs   []string
		wantErrAt int
	}{
		{"no_token_always_creates", getOK, []call{{"", "a"}, {"", "a"}}, []string{"r1", "r2"}, -1},
		{"same_token_replays", getOK, same, []string{"r1", "r1"}, -1},
		{"different_tokens_create", getOK, []call{{"t1", "a"}, {"t2", "a"}}, []string{"r1", "r2"}, -1},
		{"mismatch_conflicts", getOK, []call{{"t", "a"}, {"t", "b"}}, []string{"r1"}, 1},
		{"deleted_resource_recreates", getGone, same, []string{"r1", "r2"}, -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m := idempotency.New("test")
			counter := 0
			create := newCreator(&counter)

			var got []string

			for i, cl := range tt.calls {
				r, err := idempotency.Create(m, "Op", cl.token, cl.print, idOf, tt.get, create)
				if i == tt.wantErrAt {
					require.ErrorIs(t, err, idempotency.ErrParamsMismatch)

					continue
				}

				require.NoError(t, err)
				got = append(got, r.id)
			}

			assert.Equal(t, tt.wantIDs, got)
		})
	}
}

func TestCreateBoundsAndExpiry(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_700_000_000, 0)
	m := idempotency.New(
		"bounds",
		idempotency.WithClock(func() time.Time { return now }),
		idempotency.WithLimits(time.Minute, 3),
	)
	counter := 0
	create := newCreator(&counter)
	idOf := func(r *res) string { return r.id }
	get := func(id string) (*res, error) { return &res{id: id}, nil }

	for i := range 10 {
		_, err := idempotency.Create(m, "Op", "tok-"+strconv.Itoa(i), "p", idOf, get, create)
		require.NoError(t, err)
		assert.LessOrEqual(t, m.Len(), 3)
	}

	first, err := idempotency.Create(m, "Op", "expiring", "p", idOf, get, create)
	require.NoError(t, err)

	now = now.Add(2 * time.Minute)

	again, err := idempotency.Create(m, "Op", "expiring", "p", idOf, get, create)
	require.NoError(t, err)
	assert.NotEqual(t, first.id, again.id)
}

func TestReplay(t *testing.T) {
	t.Parallel()

	type call struct{ token, print string }

	tests := []struct {
		name      string
		calls     []call
		wantRuns  int
		wantErrAt int
	}{
		{"no_token_runs_each", []call{{"", "a"}, {"", "a"}}, 2, -1},
		{"same_token_replays", []call{{"t", "a"}, {"t", "a"}}, 1, -1},
		{"different_tokens_run", []call{{"t1", "a"}, {"t2", "a"}}, 2, -1},
		{"mismatch_conflicts", []call{{"t", "a"}, {"t", "b"}}, 1, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m := idempotency.New("test")
			runs := 0
			do := func() (*res, error) {
				runs++

				return &res{id: "r" + strconv.Itoa(runs)}, nil
			}

			var first *res

			for i, cl := range tt.calls {
				r, err := idempotency.Replay(m, "Op", cl.token, cl.print, do)
				if i == tt.wantErrAt {
					require.ErrorIs(t, err, idempotency.ErrParamsMismatch)

					continue
				}

				require.NoError(t, err)

				if first == nil {
					first = r
				} else if tt.wantRuns == 1 {
					assert.Same(t, first, r)
				}
			}

			assert.Equal(t, tt.wantRuns, runs)
		})
	}
}

func TestReplayErrorNotRecordedAndClear(t *testing.T) {
	t.Parallel()

	m := idempotency.New("test")
	runs := 0
	fail := true
	do := func() (*res, error) {
		runs++

		if fail {
			return nil, errGone
		}

		return &res{id: "ok"}, nil
	}

	_, err := idempotency.Replay(m, "Op", "t", "p", do)
	require.ErrorIs(t, err, errGone)
	assert.Equal(t, 0, m.Len())

	fail = false

	_, err = idempotency.Replay(m, "Op", "t", "p", do)
	require.NoError(t, err)
	assert.Equal(t, 1, m.Len())

	m.Clear()
	assert.Equal(t, 0, m.Len())
}

func TestLookupRecord(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		token     string
		print     string
		wantID    string
		recorded  bool
		wantHit   bool
		wantError bool
	}{
		{"hit", "t", "a", "id1", true, true, false},
		{"empty_token_misses", "", "a", "", true, false, false},
		{"unrecorded_misses", "t", "a", "", false, false, false},
		{"mismatch_errors", "t", "b", "", true, false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m := idempotency.New("test")
			if tt.recorded {
				m.Record("Op", "t", "a", "id1")
			}

			id, hit, err := m.Lookup("Op", tt.token, tt.print)
			if tt.wantError {
				require.ErrorIs(t, err, idempotency.ErrParamsMismatch)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantHit, hit)
			assert.Equal(t, tt.wantID, id)
		})
	}
}
