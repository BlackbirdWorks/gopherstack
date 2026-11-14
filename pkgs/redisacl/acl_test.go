package redisacl_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/redisacl"
)

func TestParsePermissions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		perms   string
		cmd     string
		args    []string
		allowed bool
		invalid bool
	}{
		{name: "all", perms: "+@all allkeys", cmd: "SET", args: []string{"k", "v"}, allowed: true},
		{name: "read category", perms: "+@read ~*", cmd: "GET", args: []string{"k"}, allowed: true},
		{name: "read denies write", perms: "+@read ~*", cmd: "SET", args: []string{"k", "v"}},
		{name: "prefix key", perms: "+@read +set ~Az*", cmd: "SET", args: []string{"Azk", "v"}, allowed: true},
		{name: "prefix key miss", perms: "+@read +set ~Az*", cmd: "SET", args: []string{"other", "v"}},
		{name: "no keys", perms: "+@all", cmd: "GET", args: []string{"k"}},
		{name: "no keys needed", perms: "+@all", cmd: "PING", allowed: true},
		{name: "dangerous removed", perms: "+@all -@dangerous allkeys", cmd: "FLUSHALL"},
		{name: "dangerous removed keeps set", perms: "+@all -@dangerous allkeys", cmd: "SET", args: []string{"a", "b"},
			allowed: true},
		{name: "command deny", perms: "+@all -get allkeys", cmd: "GET", args: []string{"k"}},
		{name: "mset second key", perms: "+@all ~a*", cmd: "MSET", args: []string{"a1", "1", "b1", "2"}},
		{name: "mget all match", perms: "+@all ~a*", cmd: "MGET", args: []string{"a1", "a2"}, allowed: true},
		{name: "eval keys", perms: "+@all ~a*", cmd: "EVAL", args: []string{"x", "1", "b"}},
		{name: "subcommand allowed", perms: "-@all +client|list", cmd: "CLIENT", args: []string{"LIST"}, allowed: true},
		{name: "client list dangerous", perms: "+@all -@dangerous", cmd: "CLIENT", args: []string{"LIST"}},
		{
			name:    "client setname kept",
			perms:   "+@all -@dangerous",
			cmd:     "CLIENT",
			args:    []string{"SETNAME", "x"},
			allowed: true,
		},
		{name: "channels", perms: "+@all allkeys resetchannels &news", cmd: "PUBLISH", args: []string{"other", "m"}},
		{name: "channel hit", perms: "+@all allkeys resetchannels &news*", cmd: "PUBLISH", args: []string{"news1", "m"},
			allowed: true},
		{name: "unknown token", perms: "bogus", invalid: true},
		{name: "unknown command", perms: "+nosuchcommand", invalid: true},
		{name: "unknown category", perms: "+@nosuchcategory", invalid: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			acl, err := redisacl.ParsePermissions(tt.perms)
			if tt.invalid {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			msg, ok := acl.Check(tt.cmd, tt.args)
			assert.Equal(t, tt.allowed, ok, msg)

			if !ok {
				assert.True(t, strings.HasPrefix(msg, "NOPERM "), msg)
			}
		})
	}
}
