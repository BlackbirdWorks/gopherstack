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
