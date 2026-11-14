package streamlog_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/neptune/streamlog"
)

func change(op string) streamlog.Change {
	return streamlog.Change{Op: op, Data: streamlog.Data{Stmt: op}}
}

func TestLogRetentionAndRead(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_000_000, 0)
	l := streamlog.New()
	l.SetClock(func() time.Time { return now })
	l.SetPolicy(streamlog.Policy{Enabled: true, Retention: 48 * time.Hour})

	l.Commit([]streamlog.Change{change("ADD"), change("REMOVE")})

	now = now.Add(36 * time.Hour)
	l.Commit([]streamlog.Change{change("ADD")})

	at := func(c, o int64) streamlog.Query {
		return streamlog.Query{Iterator: streamlog.AtSequenceNumber, CommitNum: c, OpNum: o, Limit: 10}
	}
	horizon := streamlog.Query{Iterator: streamlog.TrimHorizon, Limit: 10}

	tests := []struct {
		wantErr error
		name    string
		q       streamlog.Query
		advance time.Duration
		want    int
	}{
		{name: "all", q: horizon, want: 3},
		{name: "expired", advance: 20 * time.Hour, q: at(1, 1), wantErr: streamlog.ErrExpired},
		{name: "survivor", advance: 20 * time.Hour, q: at(2, 1), want: 1},
		{name: "horizon trimmed", advance: 20 * time.Hour, q: horizon, want: 1},
		{name: "future", q: at(3, 1), wantErr: streamlog.ErrNotFound},
		{name: "op past commit", q: at(1, 5), wantErr: streamlog.ErrNotFound},
		{name: "zero op", q: at(1, 0), wantErr: streamlog.ErrInvalid},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := streamlog.New()
			c.SetClock(func() time.Time { return now.Add(tt.advance) })
			c.SetPolicy(streamlog.Policy{Enabled: true, Retention: 48 * time.Hour})

			raw, err := l.Export()
			require.NoError(t, err)
			require.NoError(t, c.Import(raw))

			page, err := c.Read(tt.q)
			require.ErrorIs(t, err, tt.wantErr)
			assert.Len(t, page.Records, tt.want)
		})
	}
}

func TestLogDisabledRecordsNothing(t *testing.T) {
	t.Parallel()

	l := streamlog.New()
	l.Commit([]streamlog.Change{change("ADD")})

	page, err := l.Read(streamlog.Query{Iterator: streamlog.TrimHorizon, Limit: 10})
	require.NoError(t, err)
	assert.Empty(t, page.Records)
}
