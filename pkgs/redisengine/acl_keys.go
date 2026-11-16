package redisengine

import (
	"strconv"
	"strings"
)

const (
	cmdSort      = "SORT"
	cmdSortRO    = "SORT_RO"
	migrateFixed = 5
	keysFrom     = 2
)

func isScripting(cmd string) bool {
	switch cmd {
	case "EVAL", "EVAL_RO", "EVALSHA", "EVALSHA_RO", "FCALL", "FCALL_RO":
		return true
	}

	return false
}

// aclKeys returns the keys a command touches and whether any is written. It extends the COMMAND table, which
// lacks the numkeys and STORE layouts.
func aclKeys(info cmdInfo, cmd string, args []string) ([]string, bool) {
	switch cmd {
	case "EVAL", "EVALSHA", "FCALL", "EVAL_RO", "EVALSHA_RO", "FCALL_RO":
		return numKeys(args, 1, keysFrom), !strings.HasSuffix(cmd, "_RO")
	case "ZUNIONSTORE", "ZINTERSTORE", "ZDIFFSTORE":
		return append(numKeys(args, 1, keysFrom), first(args, 1)...), true
	case "ZUNION", "ZINTER", "ZDIFF", "ZINTERCARD", "SINTERCARD":
		return numKeys(args, 0, 1), false
	case "LMPOP", "ZMPOP":
		return numKeys(args, 0, 1), true
	case "BLMPOP", "BZMPOP":
		return numKeys(args, 1, keysFrom), true
	case "ZRANGESTORE", "GEOSEARCHSTORE":
		return first(args, twoKeys), true
	case "LCS":
		return first(args, twoKeys), false
	case "TOUCH":
		return args, false
	case cmdSort, cmdSortRO, "GEORADIUS", "GEORADIUSBYMEMBER":
		return storeKeys(cmd, args), cmd != "SORT_RO"
	case "MEMORY":
		if len(args) > 1 && strings.EqualFold(args[0], "USAGE") {
			return args[1:2], false
		}
	case "MIGRATE":
		return migrateKeys(args), true
	}

	return keysOf(info, cmd, args), info.write
}

func first(args []string, n int) []string {
	return args[:min(n, len(args))]
}

// numKeys reads the key count at args[countAt] and returns the keys from args[from]. A bad count makes every
// remaining argument a key.
func numKeys(args []string, countAt, from int) []string {
	if len(args) <= countAt || from > len(args) {
		return nil
	}

	n, err := strconv.Atoi(args[countAt])
	if err != nil || n < 0 || n > len(args)-from {
		return args[from:]
	}

	return args[from : from+n]
}

func storeKeys(cmd string, args []string) []string {
	keys := first(args, 1)

	for i := 1; i+1 < len(args); i++ {
		if strings.EqualFold(args[i], "STORE") || (cmd != cmdSort && strings.EqualFold(args[i], "STOREDIST")) {
			keys = append(keys, args[i+1])
		}
	}

	return keys
}

func migrateKeys(args []string) []string {
	if len(args) < migrateFixed {
		return args
	}

	if args[2] != "" {
		return args[2:3]
	}

	for i := migrateFixed; i < len(args); i++ {
		if strings.EqualFold(args[i], "KEYS") {
			return args[i+1:]
		}
	}

	return nil
}
