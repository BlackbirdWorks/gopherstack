package redisacl

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
)

const catAll = "all"

// ErrInvalid wraps every permission string error.
var ErrInvalid = errors.New("redisacl: invalid permissions")

var (
	commandTable     map[string]cmdInfo //nolint:gochecknoglobals // read-only after once.Do
	commandTableOnce sync.Once          //nolint:gochecknoglobals // guards commandTable
)

func commands() map[string]cmdInfo {
	commandTableOnce.Do(func() { commandTable = parseCommandTable() })

	return commandTable
}

func atoiDefault(s string) int {
	n, _ := strconv.Atoi(strings.TrimSpace(s))

	return n
}

var standardCategories = map[string]bool{ //nolint:gochecknoglobals // read-only
	"admin": true, "bitmap": true, "blocking": true, "connection": true, "dangerous": true, "fast": true,
	"geo": true, "hash": true, "hyperloglog": true, "keyspace": true, "list": true, "pubsub": true,
	"read": true, "scripting": true, "set": true, "slow": true, "sortedset": true, "stream": true,
	"string": true, "transaction": true, "write": true,
}

// ACL is a parsed Redis permissions string such as "+@read +set ~Az*".
type ACL struct {
	cmds     map[string]bool
	subs     map[string]bool
	keys     []string
	channels []string
	allKeys  bool
	allChans bool
}

// ParsePermissions parses a Redis ACL permissions string: +@category, -@category, +command, -command,
// +command|subcommand, allcommands, nocommands, ~pattern, allkeys, resetkeys, &pattern, allchannels,
// resetchannels.
func ParsePermissions(s string) (*ACL, error) {
	a := &ACL{cmds: map[string]bool{}, subs: map[string]bool{}, allChans: true}
	table := commands()

	for tok := range strings.FieldsSeq(s) {
		if err := a.apply(tok, table); err != nil {
			return nil, err
		}
	}

	return a, nil
}

func (a *ACL) apply(tok string, table map[string]cmdInfo) error {
	lower := strings.ToLower(tok)

	switch {
	case lower == "allcommands":
		return a.setCategory(catAll, true, table)
	case lower == "nocommands":
		return a.setCategory(catAll, false, table)
	case a.applyScope(tok, lower):
		return nil
	case lower == "on" || lower == "off" || lower == "nopass" || lower == "reset":
		return nil
	case strings.HasPrefix(tok, "+@") || strings.HasPrefix(tok, "-@"):
		return a.setCategory(lower[2:], tok[0] == '+', table)
	case strings.HasPrefix(tok, "+") || strings.HasPrefix(tok, "-"):
		return a.setCommand(lower[1:], tok[0] == '+', table)
	}

	return fmt.Errorf("%w: unrecognised permission %q", ErrInvalid, tok)
}

// applyScope handles the key and channel tokens and reports whether tok was one.
func (a *ACL) applyScope(tok, lower string) bool {
	switch {
	case lower == "allkeys" || tok == "~*":
		a.allKeys, a.keys = true, nil
	case lower == "resetkeys":
		a.allKeys, a.keys = false, nil
	case lower == "allchannels" || tok == "&*":
		a.allChans, a.channels = true, nil
	case lower == "resetchannels":
		a.allChans, a.channels = false, nil
	case strings.HasPrefix(tok, "~"):
		a.keys = append(a.keys, tok[1:])
	case strings.HasPrefix(tok, "&"):
		a.allChans = false
		a.channels = append(a.channels, tok[1:])
	default:
		return false
	}

	return true
}

func (a *ACL) setCategory(cat string, allow bool, table map[string]cmdInfo) error {
	known := false

	for name, info := range table {
		if info.cats[cat] {
			known = true

			a.set(name, allow)
		}
	}

	if !known && cat != catAll && !standardCategories[cat] {
		return fmt.Errorf("%w: unknown command category %q", ErrInvalid, cat)
	}

	if cat == catAll {
		a.cmds["*"] = allow
	}

	return nil
}

func (a *ACL) set(name string, allow bool) {
	a.cmds[name] = allow

	for k := range a.cmds {
		if strings.HasPrefix(k, name+"|") {
			a.cmds[k] = allow
		}
	}

	for k := range a.subs {
		if strings.HasPrefix(k, name+"|") {
			delete(a.subs, k)
		}
	}
}

func (a *ACL) setCommand(name string, allow bool, table map[string]cmdInfo) error {
	base, _, hasSub := strings.Cut(name, "|")
	if _, ok := table[base]; !ok {
		return fmt.Errorf("%w: unknown command %q", ErrInvalid, base)
	}

	if _, direct := table[name]; hasSub && !direct {
		a.subs[name] = allow

		return nil
	}

	if hasSub {
		a.cmds[name] = allow

		return nil
	}

	a.set(name, allow)

	return nil
}

// Check reports whether the ACL admits the command line (cmd and its arguments), or the Redis NOPERM reply.
func (a *ACL) Check(cmd string, args []string) (string, bool) {
	name := strings.ToLower(cmd)
	info, known := commands()[name]

	if !a.commandAllowed(name, args) {
		return fmt.Sprintf("NOPERM this user has no permissions to run the '%s' command", name), false
	}

	if !known {
		return "", true
	}

	if !a.allKeys && !a.keysAllowed(info, args) {
		return "NOPERM this user has no permissions to access one of the keys used as arguments", false
	}

	if !a.allChans && !a.channelsAllowed(name, args) {
		return "NOPERM this user has no permissions to access one of the channels used as arguments", false
	}

	return "", true
}

func (a *ACL) commandAllowed(name string, args []string) bool {
	if len(args) > 0 {
		if v, ok := a.subs[name+"|"+strings.ToLower(args[0])]; ok {
			return v
		}
	}

	if len(args) > 0 {
		if v, ok := a.cmds[name+"|"+strings.ToLower(args[0])]; ok {
			return v
		}
	}

	if v, ok := a.cmds[name]; ok {
		return v
	}

	return a.cmds["*"]
}

func (a *ACL) keysAllowed(info cmdInfo, args []string) bool {
	for _, k := range keyArgs(info, args) {
		if !globAny(a.keys, k) {
			return false
		}
	}

	return true
}

func (a *ACL) channelsAllowed(name string, args []string) bool {
	switch name {
	case "publish":
		return len(args) == 0 || globAny(a.channels, args[0])
	case "subscribe", "psubscribe":
		for _, c := range args {
			if !globAny(a.channels, c) {
				return false
			}
		}
	}

	return true
}

func keyArgs(info cmdInfo, args []string) []string {
	switch {
	case info.streams:
		return streamKeys(args)
	case info.numKeysAt > 0:
		return numKeys(info, args)
	case info.first == 0:
		return nil
	}

	first, last := info.first-1, info.last
	if last < 0 {
		last = len(args) + last + 1
	}

	var out []string

	for i := first; i < last && i < len(args); i += max(info.step, 1) {
		out = append(out, args[i])
	}

	return out
}

func numKeys(info cmdInfo, args []string) []string {
	var out []string

	if info.destKey && len(args) > 0 {
		out = append(out, args[0])
	}

	at := info.numKeysAt - 1
	if at < 0 || at >= len(args) {
		return out
	}

	n := atoiDefault(args[at])
	for i := at + 1; i < len(args) && i <= at+n; i++ {
		out = append(out, args[i])
	}

	return out
}

func streamKeys(args []string) []string {
	for i, a := range args {
		if strings.EqualFold(a, "STREAMS") {
			rest := args[i+1:]

			return rest[:len(rest)/2]
		}
	}

	return nil
}

func globAny(patterns []string, s string) bool {
	for _, p := range patterns {
		if glob(p, s) {
			return true
		}
	}

	return false
}

// glob matches Redis-style patterns: * ? [set] and backslash escapes.
func glob(p, s string) bool {
	for len(p) > 0 {
		var ok bool

		switch p[0] {
		case '*':
			return globStar(p, s)
		case '[':
			p, s, ok = globClass(p, s)
		case '?':
			p, s, ok = p[1:], s[min(1, len(s)):], s != ""
		default:
			p, s, ok = globLiteral(p, s)
		}

		if !ok {
			return false
		}
	}

	return s == ""
}

func globStar(p, s string) bool {
	for len(p) > 1 && p[1] == '*' {
		p = p[1:]
	}

	if len(p) == 1 {
		return true
	}

	for i := 0; i <= len(s); i++ {
		if glob(p[1:], s[i:]) {
			return true
		}
	}

	return false
}

func globClass(p, s string) (string, string, bool) {
	end := strings.IndexByte(p, ']')
	if s == "" || end < 0 || !inSet(p[1:end], s[0]) {
		return p, s, false
	}

	return p[end+1:], s[1:], true
}

func globLiteral(p, s string) (string, string, bool) {
	if p[0] == '\\' && len(p) > 1 {
		p = p[1:]
	}

	if s == "" || s[0] != p[0] {
		return p, s, false
	}

	return p[1:], s[1:], true
}

func inSet(set string, c byte) bool {
	neg := strings.HasPrefix(set, "^")
	set = strings.TrimPrefix(set, "^")
	hit := false

	for i := 0; i < len(set); i++ {
		if i+2 < len(set) && set[i+1] == '-' {
			hit = hit || (c >= set[i] && c <= set[i+2])
			i += 2

			continue
		}

		hit = hit || set[i] == c
	}

	return hit != neg
}
