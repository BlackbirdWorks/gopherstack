package azureredis

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"
)

// Inspection limits.
const (
	MaxDatabases = 16
	MaxScan      = 500
	MaxElements  = 100
	MaxValueLen  = 4096
)

// CacheInfo describes one running cache.
type CacheInfo struct {
	ID       string
	Spec     Spec
	Endpoint Endpoint
	Keys     int
}

// KeyEntry is one key found by Scan.
type KeyEntry struct {
	Key  string
	Type string
	TTL  time.Duration
}

// KeyValue is a capped view of one key's value.
type KeyValue struct {
	Key       string
	Type      string
	Text      string
	Elements  []string
	Total     int
	TTL       time.Duration
	Truncated bool
}

// Caches lists the running caches by ID.
func (b *InMemoryBackend) Caches() []CacheInfo {
	b.mu.RLock("Caches")
	defer b.mu.RUnlock()

	out := make([]CacheInfo, 0, len(b.caches))
	for id, c := range b.caches {
		out = append(out, CacheInfo{
			ID: id, Spec: c.spec, Endpoint: c.endpoint(), Keys: len(c.engine.Mini.DB(0).Keys()),
		})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })

	return out
}

func (b *InMemoryBackend) database(key string, db int) (*cache, error) {
	if db < 0 || db >= MaxDatabases {
		return nil, fmt.Errorf("%w: database must be 0-%d", ErrInvalid, MaxDatabases-1)
	}

	b.mu.RLock("database")
	defer b.mu.RUnlock()

	return b.lookup(key)
}

func globRegexp(pattern string) (*regexp.Regexp, error) {
	var sb strings.Builder

	sb.WriteString("^")

	for _, r := range pattern {
		switch r {
		case '*':
			sb.WriteString("(?s:.*)")
		case '?':
			sb.WriteString("(?s:.)")
		default:
			sb.WriteString(regexp.QuoteMeta(string(r)))
		}
	}

	sb.WriteString("$")

	re, err := regexp.Compile(sb.String())
	if err != nil {
		return nil, fmt.Errorf("%w: bad pattern", ErrInvalid)
	}

	return re, nil
}

// Scan lists up to limit keys of database db that match the glob pattern (* and ? only), in key order,
// starting after the cursor key. The returned next is empty once the scan is complete.
func (b *InMemoryBackend) Scan(key string, db int, pattern, after string, limit int) ([]KeyEntry, string, error) {
	c, err := b.database(key, db)
	if err != nil {
		return nil, "", err
	}

	if pattern == "" {
		pattern = "*"
	}

	re, err := globRegexp(pattern)
	if err != nil {
		return nil, "", err
	}

	if limit <= 0 || limit > MaxScan {
		limit = MaxScan
	}

	rdb := c.engine.Mini.DB(db)
	names := rdb.Keys()
	sort.Strings(names)

	var (
		out  []KeyEntry
		next string
	)

	for _, k := range names {
		if k <= after && after != "" || !re.MatchString(k) {
			continue
		}

		if len(out) == limit {
			next = out[len(out)-1].Key

			break
		}

		out = append(out, KeyEntry{Key: k, Type: rdb.Type(k), TTL: rdb.TTL(k)})
	}

	return out, next, nil
}

func capText(s string) (string, bool) {
	if len(s) <= MaxValueLen {
		return s, false
	}

	return strings.ToValidUTF8(s[:MaxValueLen], "?"), true
}

// Value returns a capped view of the key's value without altering it.
func (b *InMemoryBackend) Value(key string, db int, name string) (KeyValue, error) {
	c, err := b.database(key, db)
	if err != nil {
		return KeyValue{}, err
	}

	rdb := c.engine.Mini.DB(db)
	if !rdb.Exists(name) {
		return KeyValue{}, ErrNotFound
	}

	out := KeyValue{Key: name, Type: rdb.Type(name), TTL: rdb.TTL(name)}

	var elems []string

	switch out.Type {
	case "string":
		v, _ := rdb.Get(name)
		out.Text, out.Truncated = capText(v)
		out.Total = len(v)

		return out, nil
	case "list":
		elems, _ = rdb.List(name)
	case "set":
		elems, _ = rdb.Members(name)
		sort.Strings(elems)
	case "hash":
		fields, _ := rdb.HKeys(name)
		sort.Strings(fields)

		for _, f := range fields {
			elems = append(elems, f+" = "+rdb.HGet(name, f))
		}
	case "zset":
		set, _ := rdb.SortedSet(name)
		members, _ := rdb.ZMembers(name)

		for _, m := range members {
			elems = append(elems, fmt.Sprintf("%s (score %g)", m, set[m]))
		}
	default:
		out.Text = "(" + out.Type + " values are not shown)"

		return out, nil
	}

	out.Total = len(elems)
	if len(elems) > MaxElements {
		elems, out.Truncated = elems[:MaxElements], true
	}

	for i, e := range elems {
		var cut bool

		elems[i], cut = capText(e)
		out.Truncated = out.Truncated || cut
	}

	out.Elements = elems

	return out, nil
}
