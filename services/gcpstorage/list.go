package gcpstorage

import (
	"encoding/base64"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
)

// ListResult is one page of objects and collapsed prefixes.
type ListResult struct {
	Next     string
	Items    []*Object
	Prefixes []string
}

const (
	rankPrefix = 0
	rankObject = 1
)

// listKey orders list entries; a continuation token is the key of the last entry returned, so a page
// boundary survives objects being created or deleted elsewhere in the namespace.
type listKey struct {
	name string
	gen  int64
	rank int
}

func (k listKey) cmp(o listKey) int {
	if c := strings.Compare(k.name, o.name); c != 0 {
		return c
	}

	if k.rank != o.rank {
		return k.rank - o.rank
	}

	switch {
	case k.gen < o.gen:
		return -1
	case k.gen > o.gen:
		return 1
	default:
		return 0
	}
}

func (k listKey) token() string {
	raw := strconv.Itoa(k.rank) + ":" + strconv.FormatInt(k.gen, 10) + ":" + k.name

	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

func parseListToken(tok string) (listKey, *APIError) {
	raw, err := base64.RawURLEncoding.DecodeString(tok)
	if err != nil {
		return listKey{}, errInvalid("Invalid page token.")
	}

	rank, rest, ok1 := strings.Cut(string(raw), ":")
	gen, name, ok2 := strings.Cut(rest, ":")
	r, errR := strconv.Atoi(rank)
	g, errG := strconv.ParseInt(gen, 10, 64)

	if !ok1 || !ok2 || errR != nil || errG != nil || (r != rankPrefix && r != rankObject) {
		return listKey{}, errInvalid("Invalid page token.")
	}

	return listKey{name: name, gen: g, rank: r}, nil
}

type listEntry struct {
	obj    *Object
	prefix string
	key    listKey
}

// ListObjects lists objects in name order, collapsing delimited prefixes.
func (b *InMemoryBackend) ListObjects(bucket string, opts ListOptions) (ListResult, error) {
	var match *regexp.Regexp

	if opts.MatchGlob != "" {
		if opts.Delimiter != "" && opts.Delimiter != "/" {
			return ListResult{}, errInvalid("delimiter must be empty or '/' when matchGlob is set.")
		}

		var apiErr *APIError

		if match, apiErr = compileGlob(opts.MatchGlob); apiErr != nil {
			return ListResult{}, apiErr
		}
	}

	var after *listKey

	if opts.PageToken != "" {
		k, apiErr := parseListToken(opts.PageToken)
		if apiErr != nil {
			return ListResult{}, apiErr
		}

		after = &k
	}

	b.mu.RLock("ListObjects")
	defer b.mu.RUnlock()

	bk, err := b.bucketLocked(bucket)
	if err != nil {
		return ListResult{}, err
	}

	entries := listEntries(bk, opts, match, after)

	limit := opts.MaxResults
	if limit <= 0 || limit > defaultMaxResults {
		limit = defaultMaxResults
	}

	res := ListResult{}

	if len(entries) > limit {
		entries = entries[:limit]
		res.Next = entries[limit-1].key.token()
	}

	for _, e := range entries {
		if e.obj != nil {
			res.Items = append(res.Items, e.obj)
		} else {
			res.Prefixes = append(res.Prefixes, e.prefix)
		}
	}

	return res, nil
}

func matchingNames(bk *Bucket, opts ListOptions, after *listKey) []string {
	names := make([]string, 0, len(bk.Objects))

	for n := range bk.Objects {
		if !strings.HasPrefix(n, opts.Prefix) || n < opts.StartOffset || (opts.EndOffset != "" && n >= opts.EndOffset) {
			continue
		}

		// A collapsed prefix sorts at or before every name under it, so names before the token's name
		// can be skipped without losing the entries that follow it.
		if after != nil && n < after.name {
			continue
		}

		names = append(names, n)
	}

	sort.Strings(names)

	return names
}

func listEntries(bk *Bucket, opts ListOptions, match *regexp.Regexp, after *listKey) []listEntry {
	var entries []listEntry

	seen := map[string]bool{}

	add := func(e listEntry) {
		if after == nil || e.key.cmp(*after) > 0 {
			entries = append(entries, e)
		}
	}

	for _, n := range matchingNames(bk, opts, after) {
		if cp := collapsedPrefix(n, opts); cp != "" {
			if !seen[cp] {
				seen[cp] = true

				if match == nil || match.MatchString(cp) {
					add(listEntry{prefix: cp, key: listKey{name: cp, rank: rankPrefix}})
				}
			}

			if cp != n || !opts.IncludeTrailingDelimiter {
				continue
			}
		}

		if match != nil && !match.MatchString(n) {
			continue
		}

		for _, o := range bk.Objects[n] {
			if opts.Versions || o.Deleted.IsZero() {
				add(listEntry{obj: bk.out(o), key: listKey{name: n, gen: o.Generation, rank: rankObject}})
			}
		}
	}

	slices.SortStableFunc(entries, func(a, c listEntry) int { return a.key.cmp(c.key) })

	return entries
}
