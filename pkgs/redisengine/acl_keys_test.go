package redisengine_test

import (
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/redisengine"
)

func TestUserAccessKeyExtraction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		access string
		cmd    []any
		denied bool
	}{
		{name: "eval_foreign_key", access: "on ~app:* +@all", cmd: []any{"EVAL", "return 1", "1", "other"},
			denied: true},
		{name: "evalsha_foreign_key", access: "on ~app:* +@all", cmd: []any{"EVALSHA", "abc", "1", "other"},
			denied: true},
		{name: "zunionstore_foreign_src", access: "on ~app:* +@all", cmd: []any{"ZUNIONSTORE", "app:d", "1", "other"},
			denied: true},
		{name: "zunionstore_foreign_dst", access: "on ~app:* +@all", cmd: []any{"ZUNIONSTORE", "other", "1", "app:a"},
			denied: true},
		{name: "zunionstore_ok", access: "on ~app:* +@all", cmd: []any{"ZUNIONSTORE", "app:d", "1", "app:a"}},
		{name: "lmpop_foreign", access: "on ~app:* +@all", cmd: []any{"LMPOP", "1", "other", "LEFT"}, denied: true},
		{name: "blmpop_foreign", access: "on ~app:* +@all", cmd: []any{"BLMPOP", "1", "1", "other", "LEFT"},
			denied: true},
		{name: "sintercard_foreign", access: "on ~app:* +@all", cmd: []any{"SINTERCARD", "1", "other"}, denied: true},
		{name: "sort_store_foreign", access: "on ~app:* +@all", cmd: []any{"SORT", "app:a", "STORE", "other"},
			denied: true},
		{name: "sort_store_ok", access: "on ~app:* +@all", cmd: []any{"SORT", "app:a", "STORE", "app:b"}},
		{name: "georadius_store_foreign", access: "on ~app:* +@all",
			cmd: []any{"GEORADIUS", "app:g", "0", "0", "1", "km", "STORE", "other"}, denied: true},
		{name: "zrangestore_foreign", access: "on ~app:* +@all", cmd: []any{"ZRANGESTORE", "other", "app:s", "0", "1"},
			denied: true},
		{name: "touch_foreign", access: "on ~app:* +@all", cmd: []any{"TOUCH", "app:a", "other"}, denied: true},
		{name: "memory_usage_foreign", access: "on ~app:* +@all", cmd: []any{"MEMORY", "USAGE", "other"}, denied: true},
		{name: "lcs_foreign", access: "on ~app:* +@all", cmd: []any{"LCS", "app:a", "other"}, denied: true},
		{name: "readonly_zunionstore", access: "on %R~* +@all", cmd: []any{"ZUNIONSTORE", "d", "1", "a"}, denied: true},
		{name: "eval_restricted_keys", access: "on ~app:* +@all",
			cmd: []any{"EVAL", "return redis.call('GET','other')", "0"}, denied: true},
		{name: "eval_unrestricted", access: "on ~* &* +@all", cmd: []any{"EVAL", "return 1", "0"}},
		{name: "eval_denied_commands", access: "on ~* &* +@all -flushall", cmd: []any{"EVAL", "return 1", "0"},
			denied: true},
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
				assert.Contains(t, err.Error(), "NOPERM")

				return
			}

			assert.NotContains(t, errString(err), "NOPERM")
		})
	}
}

func TestTuneAfterUserAccess(t *testing.T) {
	t.Parallel()

	e := startEngine(t)
	require.NoError(t, e.SetUserAccess([]redisengine.UserAccess{
		{Name: "alice", Password: "pw-alice-pw", AccessString: "on ~* +@all"},
	}))

	err := e.Tune(redisengine.DefaultSettings())
	require.ErrorIs(t, err, redisengine.ErrInvalidSettings)
}
