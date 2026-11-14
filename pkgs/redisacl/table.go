package redisacl

import "strings"

// aclCommands lists each supported command with its ACL categories and key positions:
// name;categories;keys. keys is first,last,step (last < 0 counts from the end), nk<N> for a numkeys count at
// argument N, nkd<N> for a destination key at 1 followed by numkeys at N, or "streams" for XREAD style calls.
// Categories follow https://redis.io/docs/latest/operate/oss_and_stack/management/security/acl/.
const aclCommands = `
AUTH;connection fast;
ECHO;connection fast;
HELLO;connection fast;
PING;connection fast;
QUIT;connection fast;
SELECT;connection fast;
CLIENT;connection slow;
CLIENT|KILL;admin dangerous connection slow;
CLIENT|LIST;admin dangerous connection slow;
CLIENT|PAUSE;admin dangerous connection slow;
CLIENT|UNPAUSE;admin dangerous connection slow;
CLIENT|NO-EVICT;admin dangerous connection slow;
CLIENT|NO-TOUCH;admin dangerous connection slow;
CLIENT|REPLY;admin dangerous connection slow;
CLIENT|UNBLOCK;admin dangerous connection slow;
CLUSTER;slow;
COMMAND;connection slow;
WAIT;connection slow;
SWAPDB;keyspace write dangerous fast;
DEL;keyspace write slow;1,-1,1
UNLINK;keyspace write fast;1,-1,1
EXISTS;keyspace read fast;1,-1,1
TOUCH;keyspace read fast;1,-1,1
TYPE;keyspace read fast;1,1,1
TTL;keyspace read fast;1,1,1
PTTL;keyspace read fast;1,1,1
EXPIRETIME;keyspace read fast;1,1,1
PEXPIRETIME;keyspace read fast;1,1,1
EXPIRE;keyspace write fast;1,1,1
EXPIREAT;keyspace write fast;1,1,1
PEXPIRE;keyspace write fast;1,1,1
PEXPIREAT;keyspace write fast;1,1,1
PERSIST;keyspace write fast;1,1,1
RENAME;keyspace write slow;1,2,1
RENAMENX;keyspace write fast;1,2,1
MOVE;keyspace write fast;1,1,1
COPY;keyspace write slow;1,2,1
DUMP;keyspace read slow;1,1,1
RESTORE;keyspace write dangerous slow;1,1,1
KEYS;keyspace read dangerous slow;
SCAN;keyspace read slow;
RANDOMKEY;keyspace read slow;
DBSIZE;keyspace read fast;
FLUSHALL;keyspace write dangerous slow;
FLUSHDB;keyspace write dangerous slow;
OBJECT;keyspace read slow;2,2,1
INFO;dangerous slow;
TIME;fast;
MEMORY;read slow;
GET;string read fast;1,1,1
SET;string write slow;1,1,1
SETNX;string write fast;1,1,1
SETEX;string write slow;1,1,1
PSETEX;string write slow;1,1,1
GETSET;string write slow;1,1,1
GETDEL;string write fast;1,1,1
GETEX;string write fast;1,1,1
DELEX;string write fast;1,1,1
GETRANGE;string read slow;1,1,1
SETRANGE;string write slow;1,1,1
STRLEN;string read fast;1,1,1
APPEND;string write fast;1,1,1
INCR;string write fast;1,1,1
DECR;string write fast;1,1,1
INCRBY;string write fast;1,1,1
DECRBY;string write fast;1,1,1
INCRBYFLOAT;string write fast;1,1,1
MGET;string read fast;1,-1,1
MSET;string write slow;1,-1,2
MSETNX;string write slow;1,-1,2
SETBIT;bitmap write slow;1,1,1
GETBIT;bitmap read fast;1,1,1
BITCOUNT;bitmap read slow;1,1,1
BITPOS;bitmap read slow;1,1,1
BITOP;bitmap write slow;2,-1,1
HGET;hash read fast;1,1,1
HGETALL;hash read slow;1,1,1
HDEL;hash write fast;1,1,1
HEXISTS;hash read fast;1,1,1
HINCRBY;hash write fast;1,1,1
HINCRBYFLOAT;hash write fast;1,1,1
HKEYS;hash read slow;1,1,1
HLEN;hash read fast;1,1,1
HMGET;hash read fast;1,1,1
HMSET;hash write fast;1,1,1
HSET;hash write fast;1,1,1
HSETNX;hash write fast;1,1,1
HSTRLEN;hash read fast;1,1,1
HVALS;hash read slow;1,1,1
HSCAN;hash read slow;1,1,1
HRANDFIELD;hash read slow;1,1,1
HEXPIRE;hash write fast;1,1,1
HPERSIST;hash write fast;1,1,1
HTTL;hash read fast;1,1,1
HPTTL;hash read fast;1,1,1
HSETEX;hash write fast;1,1,1
PFADD;hyperloglog write fast;1,1,1
PFCOUNT;hyperloglog read slow;1,-1,1
PFMERGE;hyperloglog write slow;1,-1,1
GEOADD;geo write slow;1,1,1
GEODIST;geo read slow;1,1,1
GEOPOS;geo read slow;1,1,1
GEORADIUS;geo write slow;1,1,1
GEORADIUSBYMEMBER;geo write slow;1,1,1
GEORADIUS_RO;geo read slow;1,1,1
GEORADIUSBYMEMBER_RO;geo read slow;1,1,1
GEOSEARCH;geo read slow;1,1,1
EVAL;slow scripting;nk2
EVAL_RO;slow scripting;nk2
EVALSHA;slow scripting;nk2
EVALSHA_RO;slow scripting;nk2
SCRIPT;slow scripting;
SUBSCRIBE;pubsub slow;
UNSUBSCRIBE;pubsub slow;
PSUBSCRIBE;pubsub slow;
PUNSUBSCRIBE;pubsub slow;
PUBLISH;pubsub fast;
PUBSUB;pubsub slow;
LINDEX;list read slow;1,1,1
LPOS;list read slow;1,1,1
LINSERT;list write slow;1,1,1
LLEN;list read fast;1,1,1
LMPOP;list write slow;nk1
LPOP;list write fast;1,1,1
RPOP;list write fast;1,1,1
LPUSH;list write fast;1,1,1
RPUSH;list write fast;1,1,1
LPUSHX;list write fast;1,1,1
RPUSHX;list write fast;1,1,1
LRANGE;list read slow;1,1,1
LREM;list write slow;1,1,1
LSET;list write slow;1,1,1
LTRIM;list write slow;1,1,1
RPOPLPUSH;list write slow;1,2,1
LMOVE;list write slow;1,2,1
BLPOP;list write slow blocking;1,-2,1
BRPOP;list write slow blocking;1,-2,1
BRPOPLPUSH;list write slow blocking;1,2,1
BLMOVE;list write slow blocking;1,2,1
SADD;set write fast;1,1,1
SCARD;set read fast;1,1,1
SDIFF;set read slow;1,-1,1
SDIFFSTORE;set write slow;1,-1,1
SINTER;set read slow;1,-1,1
SINTERSTORE;set write slow;1,-1,1
SINTERCARD;set read slow;nk1
SISMEMBER;set read fast;1,1,1
SMEMBERS;set read slow;1,1,1
SMISMEMBER;set read fast;1,1,1
SMOVE;set write fast;1,2,1
SPOP;set write fast;1,1,1
SRANDMEMBER;set read slow;1,1,1
SREM;set write fast;1,1,1
SUNION;set read slow;1,-1,1
SUNIONSTORE;set write slow;1,-1,1
SSCAN;set read slow;1,1,1
ZADD;sortedset write fast;1,1,1
ZCARD;sortedset read fast;1,1,1
ZCOUNT;sortedset read fast;1,1,1
ZINCRBY;sortedset write fast;1,1,1
ZINTER;sortedset read slow;nk1
ZINTERSTORE;sortedset write slow;nkd2
ZLEXCOUNT;sortedset read fast;1,1,1
ZRANGE;sortedset read slow;1,1,1
ZRANGEBYLEX;sortedset read slow;1,1,1
ZRANGEBYSCORE;sortedset read slow;1,1,1
ZRANK;sortedset read fast;1,1,1
ZREM;sortedset write fast;1,1,1
ZREMRANGEBYLEX;sortedset write slow;1,1,1
ZREMRANGEBYRANK;sortedset write slow;1,1,1
ZREMRANGEBYSCORE;sortedset write slow;1,1,1
ZREVRANGE;sortedset read slow;1,1,1
ZREVRANGEBYLEX;sortedset read slow;1,1,1
ZREVRANGEBYSCORE;sortedset read slow;1,1,1
ZREVRANK;sortedset read fast;1,1,1
ZSCORE;sortedset read fast;1,1,1
ZMSCORE;sortedset read fast;1,1,1
ZMPOP;sortedset write slow;nk1
ZUNION;sortedset read slow;nk1
ZUNIONSTORE;sortedset write slow;nkd2
ZSCAN;sortedset read slow;1,1,1
ZPOPMAX;sortedset write fast;1,1,1
ZPOPMIN;sortedset write fast;1,1,1
ZRANDMEMBER;sortedset read slow;1,1,1
BZPOPMAX;sortedset write slow blocking;1,-2,1
BZPOPMIN;sortedset write slow blocking;1,-2,1
BZMPOP;sortedset write slow blocking;nk2
XADD;stream write fast;1,1,1
XLEN;stream read fast;1,1,1
XREAD;stream read slow blocking;streams
XRANGE;stream read slow;1,1,1
XREVRANGE;stream read slow;1,1,1
XGROUP;stream write slow;2,2,1
XINFO;stream read slow;2,2,1
XREADGROUP;stream write slow blocking;streams
XACK;stream write fast;1,1,1
XDEL;stream write fast;1,1,1
XPENDING;stream read slow;1,1,1
XTRIM;stream write slow;1,1,1
XAUTOCLAIM;stream write fast;1,1,1
XCLAIM;stream write fast;1,1,1
MULTI;transaction fast;
EXEC;transaction slow;
DISCARD;transaction fast;
WATCH;transaction fast;1,-1,1
UNWATCH;transaction fast;
`

type cmdInfo struct {
	cats      map[string]bool
	first     int
	last      int
	step      int
	numKeysAt int
	streams   bool
	destKey   bool
}

func parseCommandTable() map[string]cmdInfo {
	out := map[string]cmdInfo{}

	for line := range strings.SplitSeq(strings.TrimSpace(aclCommands), "\n") {
		parts := strings.Split(line, ";")
		info := cmdInfo{cats: map[string]bool{catAll: true}}

		for c := range strings.FieldsSeq(parts[1]) {
			info.cats[c] = true
		}

		info.parseKeys(parts[2])
		out[strings.ToLower(parts[0])] = info
	}

	return out
}

func (c *cmdInfo) parseKeys(spec string) {
	switch {
	case spec == "":
	case spec == "streams":
		c.streams = true
	case strings.HasPrefix(spec, "nkd"):
		c.destKey, c.numKeysAt = true, atoiDefault(spec[3:])
	case strings.HasPrefix(spec, "nk"):
		c.numKeysAt = atoiDefault(spec[2:])
	default:
		f := strings.Split(spec, ",")
		c.first, c.last, c.step = atoiDefault(f[0]), atoiDefault(f[1]), atoiDefault(f[2])
	}
}
