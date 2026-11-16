package redisengine

import (
	"strings"
	"sync"

	"github.com/alicebob/miniredis/v2"
)

// cmdInfo is the key layout and flags of one command from the server's COMMAND table.
type cmdInfo struct {
	first, last, step int
	write             bool
	denyOOM           bool
}

// extraCommands lists commands newer than the server's COMMAND table.
func extraCommands() map[string]cmdInfo {
	one := cmdInfo{first: 1, last: 1, step: 1, write: true}
	pair := cmdInfo{first: 1, last: twoKeys, step: 1, write: true, denyOOM: true}

	return map[string]cmdInfo{
		"hexpire": one, "hpexpire": one, "hexpireat": one, "hpexpireat": one, "hpersist": one,
		"hsetex": {first: 1, last: 1, step: 1, write: true, denyOOM: true},
		"getex":  one, "getdel": one, "xautoclaim": one,
		"lmove": pair, "blmove": pair, "copy": pair,
	}
}

// commandTable reads the COMMAND table of a throwaway server once per process.
var commandTable = sync.OnceValue(loadCommandTable) //nolint:gochecknoglobals // immutable catalog

func loadCommandTable() map[string]cmdInfo {
	out := extraCommands()

	mr := miniredis.NewMiniRedis()
	if mr.Start() != nil {
		return out
	}

	defer mr.Close()

	cl, err := newLocalClient(mr, "")
	if err != nil {
		return out
	}

	raw, err := cl.do("COMMAND")
	if err != nil {
		return out
	}

	entries, _ := raw.([]any)

	for _, en := range entries {
		f, _ := en.([]any)
		if len(f) < commandEntryFields {
			continue
		}

		info := cmdInfo{first: replyInt(f[3]), last: replyInt(f[4]), step: replyInt(f[5])}

		for _, flag := range replyStrings(f[2]) {
			switch flag {
			case categoryWrite:
				info.write = true
			case "denyoom":
				info.denyOOM = true
			}
		}

		name := strings.ToLower(replyString(f[0]))
		if _, ok := out[name]; !ok {
			out[name] = info
		}
	}

	return out
}

const (
	commandEntryFields = 6
	twoKeys            = 2
	cmdXReadGroup      = "XREADGROUP"
	cmdXRead           = "XREAD"
	cmdInfoName        = "INFO"
)

// keysOf returns the key arguments of a command.
func keysOf(info cmdInfo, cmd string, args []string) []string {
	if cmd == cmdXReadGroup || cmd == cmdXRead {
		return streamKeys(args)
	}

	if info.first <= 0 || info.step <= 0 {
		return nil
	}

	last := info.last
	if last < 0 {
		last = len(args) + 1 + last
	}

	var keys []string

	for i := info.first; i <= last && i-1 < len(args); i += info.step {
		keys = append(keys, args[i-1])
	}

	return keys
}

func streamKeys(args []string) []string {
	for i, a := range args {
		if strings.EqualFold(a, "STREAMS") {
			rest := args[i+1:]

			return rest[:len(rest)/idParts]
		}
	}

	return nil
}
