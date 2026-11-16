package redisengine

import (
	"slices"
	"strings"
	"sync"
)

const (
	categoryAll   = "all"
	categoryWrite = "write"
)

// categories lists the commands of each ACL category that cannot be derived from the COMMAND table.
var categories = sync.OnceValue(func() map[string][]string { //nolint:gochecknoglobals // immutable catalog
	return map[string][]string{
		"admin": strings.Fields("acl bgrewriteaof bgsave config debug failover latency module monitor replicaof " +
			"save shutdown slaveof slowlog sync psync"),
		"dangerous": strings.Fields("acl bgrewriteaof bgsave config debug failover flushall flushdb keys latency " +
			"migrate module monitor replicaof restore save shutdown slaveof slowlog swapdb sync psync client cluster"),
		"pubsub": strings.Fields("psubscribe publish pubsub punsubscribe spublish ssubscribe subscribe sunsubscribe " +
			"unsubscribe"),
		"connection":  strings.Fields("auth client echo hello ping quit reset select"),
		"transaction": strings.Fields("discard exec multi unwatch watch"),
		"scripting":   strings.Fields("eval eval_ro evalsha evalsha_ro fcall fcall_ro function script"),
		"blocking":    strings.Fields("blmove blmpop blpop brpop brpoplpush bzmpop bzpopmax bzpopmin xread xreadgroup"),
		"keyspace": strings.Fields("copy del dump exists expire expireat expiretime keys move object persist pexpire " +
			"pexpireat pexpiretime pttl randomkey rename renamenx restore scan sort sort_ro touch ttl type unlink " +
			"wait waitaof"),
		"string": strings.Fields("append decr decrby get getdel getex getrange getset incr incrby incrbyfloat lcs " +
			"mget mset msetnx psetex set setex setnx setrange strlen substr"),
		"list": strings.Fields("blmove blmpop blpop brpop brpoplpush lindex linsert llen lmove lmpop lpop lpos lpush " +
			"lpushx lrange lrem lset ltrim rpop rpoplpush rpush rpushx"),
		"set": strings.Fields("sadd scard sdiff sdiffstore sinter sintercard sinterstore sismember smembers " +
			"smismember smove spop srandmember srem sscan sunion sunionstore"),
		"bitmap": strings.Fields("bitcount bitfield bitfield_ro bitop bitpos getbit setbit"),
	}
})

var prefixCategories = map[string]string{ //nolint:gochecknoglobals // immutable catalog
	"hash": "h", "sortedset": "z", "stream": "x", "hyperloglog": "pf", "geo": "geo",
}

// knownCategory reports whether cat is an ACL category inCategory can evaluate; @fast and @slow need the
// command catalog's per-command categories, which the COMMAND table lacks, so they are rejected.
func knownCategory(cat string) bool {
	if cat == categoryAll || cat == categoryWrite || cat == "read" {
		return true
	}

	if _, ok := prefixCategories[cat]; ok {
		return true
	}

	_, ok := categories()[cat]

	return ok
}

// inCategory reports whether cmd (lower-case) belongs to the ACL category cat. @read and @write follow the
// command's key access flags.
func inCategory(cat, cmd string, info cmdInfo) bool {
	switch cat {
	case categoryAll:
		return true
	case categoryWrite:
		return info.write
	case "read":
		return !info.write && info.first > 0
	}

	if pre, ok := prefixCategories[cat]; ok {
		return strings.HasPrefix(cmd, pre) && cmd != "hello"
	}

	return slices.Contains(categories()[cat], cmd)
}
