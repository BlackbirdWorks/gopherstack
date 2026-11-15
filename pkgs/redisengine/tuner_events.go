package redisengine

import (
	"fmt"
	"hash/fnv"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type eventClass uint16

const (
	eventGeneric eventClass = 1 << iota
	eventString
	eventList
	eventSet
	eventHash
	eventZSet
	eventExpired
	eventEvicted
	eventStream
	eventKeyMiss
	eventModule

	eventAll = eventGeneric | eventString | eventList | eventSet | eventHash | eventZSet |
		eventExpired | eventEvicted | eventStream
	eventFlagChars = "KEg$lshzxetdmnA"
)

type eventFlags struct {
	classes  eventClass
	keyspace bool
	keyevent bool
}

// classFlagChars lists the class flags in the bit order of the eventClass constants.
const classFlagChars = "g$lshzxetmd"

func parseEventFlags(s string) eventFlags {
	var f eventFlags

	for _, c := range s {
		switch c {
		case 'K':
			f.keyspace = true
		case 'E':
			f.keyevent = true
		case 'A':
			f.classes |= eventAll
		default:
			if i := strings.IndexRune(classFlagChars, c); i >= 0 {
				f.classes |= eventClass(1) << i
			}
		}
	}

	return f
}

func (f eventFlags) enabled() bool { return f.classes != 0 && (f.keyspace || f.keyevent) }

// emitLocked publishes one keyspace notification when the configuration asks for its class.
func (t *tuner) emitLocked(db int, key, event string, class eventClass, cfg *Settings) {
	f := parseEventFlags(cfg.NotifyKeyspaceEvents)
	if f.classes&class == 0 {
		return
	}

	if f.keyspace {
		t.mr.Publish(fmt.Sprintf("__keyspace@%d__:%s", db, key), event)
	}

	if f.keyevent {
		t.mr.Publish(fmt.Sprintf("__keyevent@%d__:%s", db, event), key)
	}
}

// keyFP is what the tuner compares before and after a command to decide whether a key changed.
type keyFP struct {
	typ    string
	digest uint64
	exists bool
	hasTTL bool
}

func (t *tuner) fingerprint(db int, key string) keyFP {
	d := t.mr.DB(db)

	kd, ok := dumpKey(d, db, key)
	if !ok {
		return keyFP{}
	}

	return keyFP{exists: true, typ: kd.Type, digest: digestOf(&kd), hasTTL: kd.HasExpiry}
}

func digestOf(kd *keyDump) uint64 {
	h := fnv.New64a()
	w := func(s string) {
		_, _ = h.Write([]byte(strconv.Itoa(len(s)) + ":" + s))
	}

	w(kd.Str)

	for _, l := range [][]string{kd.List, sorted(kd.Set)} {
		for _, s := range l {
			w(s)
		}
	}

	for _, m := range kd.ZSet {
		w(m.Member + "=" + strconv.FormatFloat(m.Score, 'g', -1, 64))
	}

	hashKeys := make([]string, 0, len(kd.Hash))
	for f := range kd.Hash {
		hashKeys = append(hashKeys, f)
	}

	sort.Strings(hashKeys)

	for _, f := range hashKeys {
		w(f + "=" + kd.Hash[f] + "@" + kd.HashTTL[f].String())
	}

	for _, e := range kd.Stream {
		w(e.ID)
	}

	w(strconv.Itoa(len(kd.Stream)))

	return h.Sum64()
}

func sorted(in []string) []string {
	out := slices.Clone(in)
	sort.Strings(out)

	return out
}

type observedCmd struct {
	cmd  string
	args []string
	keys []string
	info cmdInfo
}

type observation struct {
	before   map[dbKey]keyFP
	cmds     []observedCmd
	touched  []string
	events   bool
	dirtyAll bool
}

// observe notes what the commands will touch, and for keyspace notifications how those keys look beforehand.
func (t *tuner) observe(db int, cmds []queuedCmd, cfg *Settings) *observation {
	obs := &observation{events: parseEventFlags(cfg.NotifyKeyspaceEvents).enabled()}

	seen := map[string]struct{}{}

	for _, c := range cmds {
		info := t.cmds[strings.ToLower(c.cmd)]
		oc := observedCmd{cmd: c.cmd, args: c.args, info: info, keys: keysOf(info, c.cmd, c.args)}

		if info.write && info.first == 0 {
			obs.dirtyAll = true
		}

		for _, k := range oc.keys {
			if _, ok := seen[k]; !ok {
				seen[k] = struct{}{}
				obs.touched = append(obs.touched, k)
			}
		}

		obs.cmds = append(obs.cmds, oc)
	}

	if obs.events {
		obs.before = map[dbKey]keyFP{}

		for _, k := range obs.touched {
			obs.before[dbKey{db: db, key: k}] = t.fingerprint(db, k)
		}
	}

	return obs
}

// after updates memory bookkeeping and publishes the notifications the finished commands earned.
func (t *tuner) after(db int, obs *observation, cfg *Settings) {
	if obs.dirtyAll {
		t.invalidate()
	}

	if cfg.MaxMemory > 0 {
		t.refresh(db, obs.touched, true, cfg)
	}

	if t.needSweep(cfg) {
		t.learnDeadlines(db, obs.touched)
	}

	if !obs.events {
		return
	}

	after := map[dbKey]keyFP{}
	for _, k := range obs.touched {
		after[dbKey{db: db, key: k}] = t.fingerprint(db, k)
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	for _, oc := range obs.cmds {
		for _, ev := range deriveEvents(oc, db, obs.before, after) {
			t.emitLocked(db, ev.key, ev.event, ev.class, cfg)
		}
	}
}

type keyEvent struct {
	key   string
	event string
	class eventClass
}

type evSpec struct {
	event  string
	key    int
	class  eventClass
	forced bool
	all    bool
	noDel  bool
	delOnl bool
}

func deriveEvents(oc observedCmd, db int, before, after map[dbKey]keyFP) []keyEvent {
	var out []keyEvent

	for _, sp := range specsFor(oc) {
		keys := oc.keys
		if !sp.all {
			if sp.key >= len(keys) {
				continue
			}

			keys = keys[sp.key : sp.key+1]
		}

		for _, k := range keys {
			out = append(out, eventsForKey(sp, k, before[dbKey{db: db, key: k}], after[dbKey{db: db, key: k}])...)
		}
	}

	return out
}

func eventsForKey(sp evSpec, key string, b, a keyFP) []keyEvent {
	changed := b != a
	vanished := b.exists && !a.exists

	if !changed && (!sp.forced || !a.exists) {
		return nil
	}

	if sp.forced && !vanished && !classMatches(sp.class, a.typ) && sp.class != eventGeneric {
		return nil
	}

	var out []keyEvent

	if !vanished || !sp.delOnl {
		out = append(out, keyEvent{key: key, event: sp.event, class: sp.class})
	}

	if vanished && !sp.noDel && sp.event != evDel {
		out = append(out, keyEvent{key: key, event: evDel, class: eventGeneric})
	}

	return out
}

func classMatches(c eventClass, typ string) bool {
	switch c {
	case eventString:
		return typ == typeString
	case eventList:
		return typ == typeList
	case eventSet:
		return typ == typeSet
	case eventHash:
		return typ == typeHash
	case eventZSet:
		return typ == typeZSet
	case eventStream:
		return typ == typeStream
	default:
		return true
	}
}

const (
	evDel    = "del"
	evExpire = "expire"
)

var eventSpecs = sync.OnceValue(buildEventSpecs) //nolint:gochecknoglobals // immutable catalog

type specBuilder map[string][]evSpec

func (m specBuilder) add(class eventClass, event string, cmds ...string) {
	for _, c := range cmds {
		m[c] = append(m[c], evSpec{event: event, class: class})
	}
}

func (m specBuilder) addForced(class eventClass, event string, cmds ...string) {
	for _, c := range cmds {
		m[c] = append(m[c], evSpec{event: event, class: class, forced: true})
	}
}

func buildEventSpecs() map[string][]evSpec {
	m := specBuilder{}

	m.stringSpecs()
	m.genericSpecs()
	m.listSpecs()
	m.setSpecs()
	m.hashSpecs()
	m.zsetSpecs()
	m.streamSpecs()

	return m
}

func (m specBuilder) stringSpecs() {
	m.addForced(eventString, "set", "SET", "SETEX", "PSETEX", "GETSET", "MSET")
	m.add(eventString, "set", "SETNX", "MSETNX")

	for _, c := range []string{"SETEX", "PSETEX"} {
		m[c] = append(m[c], evSpec{event: evExpire, class: eventGeneric, forced: true})
	}

	for _, c := range []string{"MSET", "MSETNX"} {
		m[c][0].all = true
	}

	m.add(eventString, "append", "APPEND")
	m.add(eventString, "setrange", "SETRANGE")
	m.add(eventString, "incrby", "INCR", "DECR", "INCRBY", "DECRBY")
	m.add(eventString, "incrbyfloat", "INCRBYFLOAT")
	m.add(eventString, "setbit", "SETBIT")
}

func (m specBuilder) genericSpecs() {
	m["DEL"] = []evSpec{{event: evDel, class: eventGeneric, all: true}}
	m["UNLINK"] = m["DEL"]
	m["GETDEL"] = []evSpec{{event: evDel, class: eventGeneric}}

	for _, c := range []string{"EXPIRE", "PEXPIRE", "EXPIREAT", "PEXPIREAT"} {
		m[c] = []evSpec{{event: evExpire, class: eventGeneric, forced: true, delOnl: true}}
	}

	m.add(eventGeneric, "persist", "PERSIST")

	for _, c := range []string{"RENAME", "RENAMENX"} {
		m[c] = []evSpec{
			{event: "rename_from", class: eventGeneric, key: 0, noDel: true},
			{event: "rename_to", class: eventGeneric, key: 1},
		}
	}

	m["COPY"] = []evSpec{{event: "copy_to", class: eventGeneric, key: 1}}
}

func (m specBuilder) listSpecs() {
	m.add(eventList, "lpush", "LPUSH", "LPUSHX")
	m.add(eventList, "rpush", "RPUSH", "RPUSHX")
	m.add(eventList, "lpop", "LPOP", "BLPOP")
	m.add(eventList, "rpop", "RPOP", "BRPOP")
	m.add(eventList, "linsert", "LINSERT")
	m.add(eventList, "lset", "LSET")
	m.add(eventList, "lrem", "LREM")
	m.add(eventList, "ltrim", "LTRIM")

	m["RPOPLPUSH"] = []evSpec{{event: "rpop", class: eventList, key: 0}, {event: "lpush", class: eventList, key: 1}}
	m["BRPOPLPUSH"] = m["RPOPLPUSH"]
}

func (m specBuilder) setSpecs() {
	m.add(eventSet, "sadd", "SADD")
	m.add(eventSet, "srem", "SREM")
	m.add(eventSet, "spop", "SPOP")

	m["SMOVE"] = []evSpec{{event: "srem", class: eventSet, key: 0}, {event: "sadd", class: eventSet, key: 1}}

	for _, c := range []string{"SUNIONSTORE", "SINTERSTORE", "SDIFFSTORE"} {
		m[c] = []evSpec{{event: strings.ToLower(c), class: eventSet, key: 0}}
	}
}

func (m specBuilder) hashSpecs() {
	m.addForced(eventHash, "hset", "HSET", "HMSET")
	m.add(eventHash, "hset", "HSETNX")
	m.add(eventHash, "hdel", "HDEL")
	m.add(eventHash, "hincrby", "HINCRBY")
	m.add(eventHash, "hincrbyfloat", "HINCRBYFLOAT")
	m.add(eventHash, "hexpire", "HEXPIRE", "HPEXPIRE", "HEXPIREAT", "HPEXPIREAT")
	m.add(eventHash, "hpersist", "HPERSIST")
}

func (m specBuilder) zsetSpecs() {
	m.add(eventZSet, "zadd", "ZADD")
	m.add(eventZSet, "zincr", "ZINCRBY")
	m.add(eventZSet, "zrem", "ZREM")
	m.add(eventZSet, "zpopmin", "ZPOPMIN")
	m.add(eventZSet, "zpopmax", "ZPOPMAX")
	m.add(eventZSet, "zremrangebyrank", "ZREMRANGEBYRANK")
	m.add(eventZSet, "zremrangebyscore", "ZREMRANGEBYSCORE")
	m.add(eventZSet, "zremrangebylex", "ZREMRANGEBYLEX")

	for _, c := range []string{"ZUNIONSTORE", "ZINTERSTORE", "ZDIFFSTORE"} {
		m[c] = []evSpec{{event: strings.ToLower(c), class: eventZSet, key: 0}}
	}
}

func (m specBuilder) streamSpecs() {
	m.add(eventStream, "xadd", "XADD")
	m.add(eventStream, "xdel", "XDEL")
	m.add(eventStream, "xtrim", "XTRIM")
}

func specsFor(oc observedCmd) []evSpec {
	if oc.cmd == "SET" {
		return setCmdSpecs(oc.args)
	}

	if oc.cmd == "XGROUP" && len(oc.args) >= idParts {
		return []evSpec{{event: "xgroup-" + strings.ToLower(oc.args[0]), class: eventStream, key: 0}}
	}

	return eventSpecs()[oc.cmd]
}

// setCmdSpecs is SET's notifications: a conditional SET only notifies when it changed something, and an expiry
// option adds the expire event.
func setCmdSpecs(args []string) []evSpec {
	sp := evSpec{event: "set", class: eventString, forced: true}
	out := []evSpec{sp}

	for _, a := range args[min(len(args), 1):] {
		switch strings.ToUpper(a) {
		case "NX", "XX":
			out[0].forced = false
		case "EX", "PX", "EXAT", "PXAT":
			out = append(out, evSpec{event: evExpire, class: eventGeneric, forced: true})
		}
	}

	return out
}
