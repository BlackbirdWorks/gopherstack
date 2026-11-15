package redisengine

import (
	"bufio"
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/alicebob/miniredis/v2/server"
)

const (
	slowMaxArgs     = 32
	slowMaxArgBytes = 128
	kib             = 1024
)

type slowEntry struct {
	at   time.Time
	name string
	args []string
	dur  time.Duration
	id   int64
}

// slowLog is the SLOWLOG ring, newest entry first.
type slowLog struct {
	entries []slowEntry
	mu      sync.Mutex
	next    int64
	max     int
}

func (s *slowLog) resize(maxLen int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.max = maxLen
	s.trimLocked()
}

func (s *slowLog) trimLocked() {
	if len(s.entries) > s.max {
		s.entries = s.entries[:max(s.max, 0)]
	}
}

func (s *slowLog) record(cfg *Settings, c queuedCmd, dur time.Duration, blocking bool) {
	if blocking || cfg.SlowlogLogSlowerThan < 0 || dur < cfg.SlowlogLogSlowerThan {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.max == 0 {
		s.max = cfg.SlowlogMaxLen
	}

	s.entries = append([]slowEntry{{id: s.next, at: time.Now(), dur: dur, args: slowArgs(c)}}, s.entries...)
	s.next++
	s.trimLocked()
}

func slowArgs(c queuedCmd) []string {
	all := append([]string{c.cmd}, c.args...)
	if c.cmd == "AUTH" {
		for i := 1; i < len(all); i++ {
			all[i] = "(redacted)"
		}
	}

	out := make([]string, 0, min(len(all), slowMaxArgs))

	for i, a := range all {
		if i == slowMaxArgs-1 && len(all) > slowMaxArgs {
			out = append(out, fmt.Sprintf("... (%d more arguments)", len(all)-i))

			break
		}

		if len(a) > slowMaxArgBytes {
			a = a[:slowMaxArgBytes] + fmt.Sprintf("... (%d more bytes)", len(a)-slowMaxArgBytes)
		}

		out = append(out, a)
	}

	return out
}

func (s *slowLog) get(count int) []slowEntry {
	s.mu.Lock()
	defer s.mu.Unlock()

	if count < 0 || count > len(s.entries) {
		count = len(s.entries)
	}

	return append([]slowEntry(nil), s.entries[:count]...)
}

func (s *slowLog) reset() {
	s.mu.Lock()
	s.entries = nil
	s.mu.Unlock()
}

// intercept answers the commands the tuner owns; it reports whether it did.
func (t *tuner) intercept(p *server.Peer, ps *peerState, cmd string, args []string, cfg *Settings) bool {
	switch cmd {
	case "SELECT":
		return t.selectDB(p, ps, args, cfg)
	case "SLOWLOG", cmdInfoName:
		if err := t.authorized(p); err != nil {
			p.WriteError(err.Error())

			return true
		}

		if cmd == cmdInfoName {
			return t.info(p, args, cfg)
		}

		t.slowlogCmd(p, args)

		return true
	}

	return false
}

// authorized runs a harmless command on the caller's session so the server applies its own AUTH check.
func (t *tuner) authorized(p *server.Peer) error {
	var buf bytes.Buffer

	wr := bufio.NewWriter(&buf)
	tp := server.NewPeer(wr)
	tp.Ctx = p.Ctx
	tp.ClientName = internalPeerName

	t.srv.Dispatch(tp, []string{"DBSIZE"})
	tp.Flush()

	_, err := server.ParseReply(bufio.NewReader(&buf))

	return err
}

func (t *tuner) selectDB(p *server.Peer, ps *peerState, args []string, cfg *Settings) bool {
	if len(args) != 1 {
		return false
	}

	idx, err := strconv.Atoi(args[0])
	if err != nil {
		return false
	}

	if idx < 0 || idx >= cfg.Databases {
		p.WriteError("ERR DB index is out of range")

		return true
	}

	t.dispatch(p, "SELECT", args)

	if !ps.multi {
		ps.db = idx
	}

	return true
}

func (t *tuner) slowlogCmd(p *server.Peer, args []string) {
	if len(args) == 0 {
		p.WriteError("ERR wrong number of arguments for 'slowlog' command")

		return
	}

	switch strings.ToUpper(args[0]) {
	case "LEN":
		p.WriteInt(len(t.slow.get(-1)))
	case "RESET":
		t.slow.reset()
		p.WriteOK()
	case "GET":
		t.slowlogGet(p, args[1:])
	default:
		p.WriteError(fmt.Sprintf("ERR unknown subcommand '%s'. Try SLOWLOG HELP.", args[0]))
	}
}

func (t *tuner) slowlogGet(p *server.Peer, rest []string) {
	count := 10

	if len(rest) > 0 {
		n, err := strconv.Atoi(rest[0])
		if err != nil || n < -1 {
			p.WriteError("ERR value is not an integer or out of range")

			return
		}

		count = n
	}

	entries := t.slow.get(count)
	p.WriteLen(len(entries))

	for _, e := range entries {
		const fields = 6

		p.WriteLen(fields)
		p.WriteInt(int(e.id))
		p.WriteInt(int(e.at.Unix()))
		p.WriteInt(int(e.dur.Microseconds()))
		p.WriteStrings(e.args)
		p.WriteBulk("")
		p.WriteBulk(e.name)
	}
}

// info extends INFO with the memory, keyspace and server sections and eviction counters; other sections fall
// through to the server.
func (t *tuner) info(p *server.Peer, args []string, cfg *Settings) bool {
	if len(args) > 1 {
		return false
	}

	section := ""
	if len(args) == 1 {
		section = strings.ToLower(args[0])
	}

	switch section {
	case "memory":
		p.WriteBulk(t.memorySection(cfg))
	case "keyspace":
		p.WriteBulk(t.keyspaceSection())
	case "server":
		p.WriteBulk(serverSection(cfg))
	default:
		if !isFullInfo(section) && section != "stats" && section != "clients" {
			return false
		}

		t.infoMerged(p, section, cfg)
	}

	return true
}

func isFullInfo(section string) bool {
	switch section {
	case "", "default", "all", "everything":
		return true
	default:
		return false
	}
}

func (t *tuner) infoMerged(p *server.Peer, section string, cfg *Settings) {
	base, err := t.innerInfo(p, section)
	if err != nil {
		p.WriteError(err.Error())

		return
	}

	t.mu.Lock()
	evicted, expired := t.evicted, t.expired
	t.mu.Unlock()

	var b strings.Builder

	b.WriteString(strings.TrimRight(base, "\r\n"))

	if section == "stats" || isFullInfo(section) {
		fmt.Fprintf(&b, "\r\nexpired_keys:%d\r\nevicted_keys:%d", expired, evicted)
	}

	if isFullInfo(section) {
		b.WriteString("\r\n\r\n" + serverSection(cfg) + "\r\n" + t.memorySection(cfg) + "\r\n" + t.keyspaceSection())
	}

	b.WriteString("\r\n")
	p.WriteBulk(b.String())
}

func (t *tuner) innerInfo(p *server.Peer, section string) (string, error) {
	var buf bytes.Buffer

	wr := bufio.NewWriter(&buf)
	tp := server.NewPeer(wr)
	tp.Ctx = p.Ctx
	tp.ClientName = internalPeerName

	args := []string{"INFO"}
	if !isFullInfo(section) {
		args = append(args, section)
	}

	t.srv.Dispatch(tp, args)
	tp.Flush()

	v, err := server.ParseReply(bufio.NewReader(&buf))
	if err != nil {
		return "", err
	}

	return replyString(v), nil
}

func (t *tuner) usedMemory() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.mem.dirty {
		t.rebuild()
	}

	return t.mem.total
}

func (t *tuner) memorySection(cfg *Settings) string {
	used := t.usedMemory()

	return fmt.Sprintf(
		"# Memory\r\nused_memory:%d\r\nused_memory_human:%s\r\nmaxmemory:%d\r\nmaxmemory_human:%s\r\n"+
			"maxmemory_policy:%s\r\nmem_fragmentation_ratio:1.00\r\nactive_defrag_running:0\r\n"+
			"lazyfree_pending_objects:0\r\nlazyfreed_objects:0\r\n",
		used, humanBytes(used), cfg.MaxMemory, humanBytes(cfg.MaxMemory), cfg.Policy,
	)
}

func humanBytes(n int64) string {
	switch {
	case n >= kib*kib*kib:
		return fmt.Sprintf("%.2fG", float64(n)/(kib*kib*kib))
	case n >= kib*kib:
		return fmt.Sprintf("%.2fM", float64(n)/(kib*kib))
	case n >= kib:
		return fmt.Sprintf("%.2fK", float64(n)/kib)
	default:
		return strconv.FormatInt(n, 10) + "B"
	}
}

func serverSection(cfg *Settings) string {
	if cfg.RedisVersion == "" {
		return "# Server\r\n"
	}

	return "# Server\r\nredis_version:" + cfg.RedisVersion + "\r\n"
}

func (t *tuner) keyspaceSection() string {
	var b strings.Builder

	b.WriteString("# Keyspace\r\n")

	for db := range keyspaceDBs {
		d := t.mr.DB(db)

		keys := d.Keys()
		if len(keys) == 0 {
			continue
		}

		expires := 0

		for _, k := range keys {
			if d.TTL(k) > 0 {
				expires++
			}
		}

		fmt.Fprintf(&b, "db%d:keys=%d,expires=%d,avg_ttl=0\r\n", db, len(keys), expires)
	}

	return b.String()
}

func (t *tuner) needSweep(cfg *Settings) bool {
	return cfg.MaxMemory > 0 || parseEventFlags(cfg.NotifyKeyspaceEvents).classes&eventExpired != 0
}

func (t *tuner) sweepLoop() {
	defer close(t.done)

	tk := time.NewTicker(sweepInterval)
	defer tk.Stop()

	for {
		select {
		case <-t.stop:
			return
		case <-tk.C:
			if cfg := t.cfg.Load(); t.needSweep(cfg) {
				t.mu.Lock()
				t.expireLocked()
				t.mu.Unlock()
			}
		}
	}
}

// expireLocked lets the server's clock run, then reports the keys whose deadline passed and that are gone.
func (t *tuner) expireLocked() {
	cfg := t.cfg.Load()
	now := t.clock()

	_ = t.mr.DB(0)

	for dk, dl := range t.deadline {
		if dl.After(now) {
			continue
		}

		delete(t.deadline, dk)

		if t.mr.DB(dk.db).Exists(dk.key) {
			continue
		}

		t.expired++
		t.mem.total -= t.mem.sizes[dk]
		delete(t.mem.sizes, dk)
		delete(t.mem.meta, dk)
		t.emitLocked(dk.db, dk.key, "expired", eventExpired, cfg)
	}

	if !t.needSweep(cfg) {
		return
	}

	for db := range keyspaceDBs {
		d := t.mr.DB(db)

		for _, k := range d.Keys() {
			dk := dbKey{db: db, key: k}

			if ttl := d.TTL(k); ttl > 0 {
				t.deadline[dk] = now.Add(ttl)
			} else {
				delete(t.deadline, dk)
			}
		}
	}
}

// learnDeadlines records when the keys a command touched will expire, so the sweeper can report their expiry.
func (t *tuner) learnDeadlines(db int, keys []string) {
	now := t.clock()

	t.mu.Lock()
	defer t.mu.Unlock()

	d := t.mr.DB(db)

	for _, k := range keys {
		dk := dbKey{db: db, key: k}

		if ttl := d.TTL(k); ttl > 0 {
			t.deadline[dk] = now.Add(ttl)
		} else {
			delete(t.deadline, dk)
		}
	}
}
