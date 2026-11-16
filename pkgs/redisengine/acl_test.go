package redisengine_test

import (
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/redisengine"
)

func TestUserAccessEnforcement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		access   string
		cmd      []any
		wantPerm bool
	}{
		{name: "read_ok", access: "on ~* +@read", cmd: []any{"GET", "k"}},
		{name: "write_denied", access: "on ~* +@read", cmd: []any{"SET", "k", "v"}, wantPerm: true},
		{name: "all_ok", access: "on ~* &* +@all", cmd: []any{"SET", "k", "v"}},
		{name: "key_denied", access: "on ~app:* +@all", cmd: []any{"SET", "other", "v"}, wantPerm: true},
		{name: "key_ok", access: "on ~app:* +@all", cmd: []any{"SET", "app:1", "v"}},
		{name: "readonly_pattern_write", access: "on %R~* +@all", cmd: []any{"SET", "k", "v"}, wantPerm: true},
		{name: "cmd_removed", access: "on ~* +@all -flushall", cmd: []any{"FLUSHALL"}, wantPerm: true},
		{name: "subcommand_allowed", access: "on ~* -@all +config|get", cmd: []any{"CONFIG", "GET", "maxmemory"}},
		{
			name: "channel_denied", access: "on ~* +@all resetchannels &news", cmd: []any{"PUBLISH", "sports", "x"},
			wantPerm: true,
		},
		{name: "channel_ok", access: "on ~* +@all resetchannels &news", cmd: []any{"PUBLISH", "news", "x"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := startEngine(t)
			e.Mini.RequireUserAuth("alice", "pw-alice-pw")
			require.NoError(t, e.SetUserAccess([]redisengine.UserAccess{
				{Name: "alice", Password: "pw-alice-pw", AccessString: tt.access},
			}))

			c := redis.NewClient(
				&redis.Options{Addr: e.Addr(), Username: "alice", Password: "pw-alice-pw", Protocol: 2},
			)
			t.Cleanup(func() { _ = c.Close() })

			err := c.Do(t.Context(), tt.cmd...).Err()
			if tt.wantPerm {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "NOPERM")

				return
			}

			assert.NotContains(t, errString(err), "NOPERM")
		})
	}
}

func TestUserAccessDisabledUser(t *testing.T) {
	t.Parallel()

	e := startEngine(t)
	e.Mini.RequireUserAuth("bob", "pw-bob-passwd")
	require.NoError(t, e.SetUserAccess([]redisengine.UserAccess{
		{Name: "bob", Password: "pw-bob-passwd", AccessString: "off ~* +@all"},
	}))

	c := redis.NewClient(&redis.Options{Addr: e.Addr(), Username: "bob", Password: "pw-bob-passwd", Protocol: 2})
	t.Cleanup(func() { _ = c.Close() })

	err := c.Ping(t.Context()).Err()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "WRONGPASS")
}

func TestUserAccessInvalidString(t *testing.T) {
	t.Parallel()

	e := startEngine(t)
	err := e.SetUserAccess([]redisengine.UserAccess{{Name: "x", AccessString: "bogus"}})
	require.ErrorIs(t, err, redisengine.ErrInvalidSettings)
}

func TestGlobMatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		pattern string
		key     string
		want    bool
	}{
		{pattern: "*", key: "anything", want: true},
		{pattern: "app:*", key: "app:1", want: true},
		{pattern: "app:*", key: "other", want: false},
		{pattern: "a?c", key: "abc", want: true},
		{pattern: "a?c", key: "ac", want: false},
		{pattern: "h[ae]llo", key: "hallo", want: true},
		{pattern: "h[^e]llo", key: "hello", want: false},
		{pattern: "h[a-b]llo", key: "hbllo", want: true},
		{pattern: `a\*b`, key: "a*b", want: true},
		{pattern: `a\*b`, key: "axb", want: false},
		{pattern: "*:x:*", key: "a:x:b", want: true},
		{pattern: "*abc", key: "xabxabc", want: true},
	}

	for _, tt := range tests {
		t.Run(tt.pattern+"_"+tt.key, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, redisengine.GlobMatch(tt.pattern, tt.key))
		})
	}
}

func errString(err error) string {
	if err == nil {
		return ""
	}

	return err.Error()
}

func TestUserAccessOffDefaultRequiresAuth(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		defaults string
		wantAuth bool
	}{
		{name: "off", defaults: "off ~* +@all", wantAuth: true},
		{name: "on", defaults: "on ~* +@all"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := startEngine(t)
			require.NoError(t, e.SetUserAccess([]redisengine.UserAccess{
				{Name: "default", AccessString: tt.defaults},
			}))

			anon := redis.NewClient(&redis.Options{Addr: e.Addr(), Protocol: 2, DisableIdentity: true})
			t.Cleanup(func() { _ = anon.Close() })

			err := anon.Set(t.Context(), "k", "v", 0).Err()
			if tt.wantAuth {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "NOAUTH")
				assert.Contains(t, errString(anon.Do(t.Context(), "HELLO", "3").Err()), "NOAUTH")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestUserAccessRejectsUnmodelledCategories(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		access  string
		wantErr bool
	}{
		{name: "plus_fast", access: "on ~* +@fast", wantErr: true},
		{name: "minus_slow", access: "on ~* +@all -@slow", wantErr: true},
		{name: "unknown", access: "on ~* +@bogus", wantErr: true},
		{name: "known", access: "on ~* +@read +@hash -@dangerous"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := startEngine(t)
			err := e.SetUserAccess([]redisengine.UserAccess{
				{Name: "x", Password: "pw-x-passwd", AccessString: tt.access},
			})

			if tt.wantErr {
				require.ErrorIs(t, err, redisengine.ErrInvalidSettings)
				require.ErrorIs(t, redisengine.ValidateAccessString(tt.access), redisengine.ErrInvalidSettings)

				return
			}

			require.NoError(t, err)
			require.NoError(t, redisengine.ValidateAccessString(tt.access))
		})
	}
}

func TestUserAccessSortPatterns(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		access string
		cmd    []any
		denied bool
	}{
		{
			name: "by_pattern_restricted", access: "on ~app:* +@all", denied: true,
			cmd: []any{"SORT", "app:l", "BY", "other_*"},
		},
		{
			name: "get_pattern_restricted", access: "on ~app:* +@all", denied: true,
			cmd: []any{"SORT_RO", "app:l", "GET", "other_*"},
		},
		{
			name: "get_hash_restricted", access: "on ~app:* +@all", denied: true,
			cmd: []any{"SORT", "app:l", "LIMIT", "0", "1", "GET", "#"},
		},
		{name: "by_constant_restricted", access: "on ~app:* +@all", cmd: []any{"SORT", "app:l", "BY", "nosort"}},
		{name: "plain_restricted", access: "on ~app:* +@all", cmd: []any{"SORT", "app:l", "ALPHA"}},
		{name: "by_pattern_allkeys", access: "on ~* +@all", cmd: []any{"SORT", "app:l", "BY", "w_*"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := startEngine(t)
			e.Mini.RequireUserAuth("alice", "pw-alice-pw")
			require.NoError(t, e.SetUserAccess([]redisengine.UserAccess{
				{Name: "alice", Password: "pw-alice-pw", AccessString: tt.access},
			}))

			c := redis.NewClient(
				&redis.Options{Addr: e.Addr(), Username: "alice", Password: "pw-alice-pw", Protocol: 2},
			)
			t.Cleanup(func() { _ = c.Close() })

			err := c.Do(t.Context(), tt.cmd...).Err()
			if tt.denied {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "denied due to insufficient ACL permissions")

				return
			}

			assert.NotContains(t, errString(err), "insufficient ACL permissions")
		})
	}
}
