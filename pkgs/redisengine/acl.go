package redisengine

import (
	"crypto/sha256"
	"crypto/subtle"
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/alicebob/miniredis/v2/server"
)

// UserAccess is one ACL user: the AUTH password and the Redis ACL rule string (for example
// "on ~app:* &* +@read") enforced on that user's connections.
type UserAccess struct {
	Name         string
	Password     string
	AccessString string
}

const defaultUser = "default"

type aclRule struct {
	cat   string
	cmd   string
	sub   string
	allow bool
}

type keyPattern struct {
	pattern string
	read    bool
	write   bool
}

type userRules struct {
	password string
	rules    []aclRule
	keys     []keyPattern
	channels []string
	on       bool
	allKeys  bool
	allChans bool
}

// aclGuard enforces access strings on every command that reaches the server.
type aclGuard struct {
	users map[string]*userRules
	peers map[*server.Peer]string
	mu    sync.Mutex
}

// SetUserAccess enforces the users' access strings: connections authenticate with AUTH, and each command, key
// and channel is checked against the authenticated user's rules (NOPERM on denial). Connections that never
// authenticate act as user "default", which is unrestricted unless listed. It installs the server's
// pre-command hook, so it cannot be combined with Tune. An empty list removes enforcement.
func (e *Engine) SetUserAccess(users []UserAccess) error {
	if e == nil || e.Mini == nil || e.Mini.Server() == nil {
		return fmt.Errorf("%w: engine not running", ErrInvalidSettings)
	}

	if e.tuner != nil {
		return fmt.Errorf("%w: user access cannot be combined with tuning", ErrInvalidSettings)
	}

	if len(users) == 0 {
		e.Mini.Server().SetPreHook(nil)
		e.guard = nil

		return nil
	}

	parsed := make(map[string]*userRules, len(users))

	for _, u := range users {
		r, err := parseAccessString(u.AccessString)
		if err != nil {
			return fmt.Errorf("user %q: %w", u.Name, err)
		}

		r.password = u.Password
		parsed[u.Name] = r
	}

	if e.guard == nil {
		e.guard = &aclGuard{peers: map[*server.Peer]string{}}
		e.Mini.Server().SetPreHook(e.guard.hook)
	}

	e.guard.mu.Lock()
	e.guard.users = parsed
	e.guard.mu.Unlock()

	return nil
}

func parseAccessString(s string) (*userRules, error) {
	r := &userRules{}

	for tok := range strings.FieldsSeq(s) {
		if err := r.apply(tok); err != nil {
			return nil, err
		}
	}

	return r, nil
}

func (r *userRules) apply(tok string) error {
	if r.applyKeyword(strings.ToLower(tok)) {
		return nil
	}

	switch tok[0] {
	case '>', '<', '#', '!', '(':
	case '&':
		r.addChannel(tok[1:])
	case '~', '%':
		return r.addKey(tok)
	case '+', '-':
		return r.addCommand(tok)
	default:
		return fmt.Errorf("%w: unrecognized access rule %q", ErrInvalidSettings, tok)
	}

	return nil
}

// applyKeyword handles the argument-less rules; credentials (nopass, resetpass) are managed by the caller.
func (r *userRules) applyKeyword(low string) bool {
	switch low {
	case "on", "off":
		r.on = low == "on"
	case "allkeys":
		r.allKeys, r.keys = true, nil
	case "resetkeys":
		r.allKeys, r.keys = false, nil
	case "allchannels":
		r.allChans, r.channels = true, nil
	case "resetchannels":
		r.allChans, r.channels = false, nil
	case "allcommands":
		r.rules = append(r.rules, aclRule{cat: categoryAll, allow: true})
	case "nocommands":
		r.rules = append(r.rules, aclRule{cat: categoryAll})
	case "reset":
		*r = userRules{password: r.password}
	case "nopass", "resetpass":
	default:
		return false
	}

	return true
}

func (r *userRules) addChannel(p string) {
	if p == "*" {
		r.allChans, r.channels = true, nil

		return
	}

	r.channels = append(r.channels, p)
}

func (r *userRules) addKey(tok string) error {
	kp := keyPattern{read: true, write: true}
	rest := tok

	if strings.HasPrefix(tok, "%") {
		mode, pat, ok := strings.Cut(tok[1:], "~")
		if !ok {
			return fmt.Errorf("%w: unrecognized key rule %q", ErrInvalidSettings, tok)
		}

		kp.read = strings.Contains(strings.ToUpper(mode), "R")
		kp.write = strings.Contains(strings.ToUpper(mode), "W")
		rest = "~" + pat
	}

	kp.pattern = rest[1:]

	if kp.pattern == "*" && kp.read && kp.write {
		r.allKeys, r.keys = true, nil

		return nil
	}

	r.keys = append(r.keys, kp)

	return nil
}

func (r *userRules) addCommand(tok string) error {
	allow := tok[0] == '+'
	body := strings.ToLower(tok[1:])

	if cat, ok := strings.CutPrefix(body, "@"); ok {
		if !knownCategory(cat) {
			return fmt.Errorf("%w: unsupported ACL category %q", ErrInvalidSettings, tok)
		}

		r.rules = append(r.rules, aclRule{cat: cat, allow: allow})

		return nil
	}

	cmd, sub, _ := strings.Cut(body, "|")
	r.rules = append(r.rules, aclRule{cmd: cmd, sub: sub, allow: allow})

	return nil
}

// ValidateAccessString reports whether SetUserAccess can enforce the access string.
func ValidateAccessString(s string) error {
	_, err := parseAccessString(s)

	return err
}

func (r *userRules) commandAllowed(cmd string, args []string, info cmdInfo) bool {
	name := strings.ToLower(cmd)
	allowed := false

	for _, rule := range r.rules {
		switch {
		case rule.cat != "":
			if inCategory(rule.cat, name, info) {
				allowed = rule.allow
			}
		case rule.cmd == name && rule.sub == "":
			allowed = rule.allow
		case rule.cmd == name && len(args) > 0 && strings.EqualFold(args[0], rule.sub):
			allowed = rule.allow
		}
	}

	return allowed
}

func (r *userRules) keyAllowed(key string, write bool) bool {
	if r.allKeys {
		return true
	}

	for _, kp := range r.keys {
		if (write && !kp.write) || (!write && !kp.read) {
			continue
		}

		if globMatch(kp.pattern, key) {
			return true
		}
	}

	return false
}

func (r *userRules) channelAllowed(ch string, literal bool) bool {
	if r.allChans {
		return true
	}

	for _, p := range r.channels {
		if (literal && p == ch) || (!literal && globMatch(p, ch)) {
			return true
		}
	}

	return false
}

func (g *aclGuard) rulesFor(user string) *userRules {
	g.mu.Lock()
	defer g.mu.Unlock()

	return g.users[user]
}

func (g *aclGuard) userOf(p *server.Peer) string {
	g.mu.Lock()
	defer g.mu.Unlock()

	if u, ok := g.peers[p]; ok {
		return u
	}

	return defaultUser
}

func (g *aclGuard) login(p *server.Peer, user string) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if _, ok := g.peers[p]; !ok {
		p.OnDisconnect(func() {
			g.mu.Lock()
			delete(g.peers, p)
			g.mu.Unlock()
		})
	}

	g.peers[p] = user
}

const (
	noAuth        = "NOAUTH Authentication required."
	wrongPass     = "WRONGPASS invalid username-password pair or user is disabled."
	authWithUser  = 2
	escapeLen     = 2
	sortLimitArgs = 2
)

// credentials extracts the AUTH/HELLO credentials of a command.
func credentials(cmd string, args []string) (string, string, bool) {
	if cmd == "AUTH" {
		switch len(args) {
		case 1:
			return defaultUser, args[0], true
		case authWithUser:
			return args[0], args[1], true
		}

		return "", "", false
	}

	i := slices.IndexFunc(args, func(a string) bool { return strings.EqualFold(a, "AUTH") })
	if cmd != "HELLO" || i < 0 || len(args) < i+3 {
		return "", "", false
	}

	return args[i+1], args[i+2], true
}

func (g *aclGuard) hook(p *server.Peer, cmd string, args ...string) bool {
	if p.ClientName == internalPeerName {
		return false
	}

	if user, pass, ok := credentials(cmd, args); ok {
		return g.authenticate(p, user, pass)
	}

	if cmd == "QUIT" {
		return false
	}

	user := g.userOf(p)

	r := g.rulesFor(user)
	if r == nil {
		return false
	}

	if !r.on {
		p.WriteError(noAuth)

		return true
	}

	if cmd == "HELLO" {
		return false
	}

	if msg := r.denial(user, cmd, args); msg != "" {
		p.WriteError(msg)

		return true
	}

	return false
}

// authenticate records the login when the credentials match; otherwise the server's own AUTH answers.
func (g *aclGuard) authenticate(p *server.Peer, user, pass string) bool {
	r := g.rulesFor(user)
	if r == nil {
		return false
	}

	if !r.on || r.password == "" || !passwordMatch(r.password, pass) {
		p.WriteError(wrongPass)

		return true
	}

	g.login(p, user)

	return false
}

func (r *userRules) denial(user, cmd string, args []string) string {
	info := commandTable()[strings.ToLower(cmd)]

	if !r.commandAllowed(cmd, args, info) {
		return fmt.Sprintf("NOPERM User %s has no permissions to run the '%s' command", user, strings.ToLower(cmd))
	}

	if isScripting(cmd) && !r.scriptSafe() {
		return fmt.Sprintf("NOPERM User %s has no permissions to run the '%s' command: scripts run unchecked", user,
			strings.ToLower(cmd))
	}

	if msg := r.sortDenial(cmd, args); msg != "" {
		return msg
	}

	keys, write := aclKeys(info, cmd, args)

	for _, k := range keys {
		if !r.keyAllowed(k, write) {
			return fmt.Sprintf("NOPERM No permissions to access a key (%s)", user)
		}
	}

	for _, ch := range channelsOf(cmd, args) {
		if !r.channelAllowed(ch.name, ch.literal) {
			return fmt.Sprintf("NOPERM User %s has no permissions to access the '%s' channel", user, ch.name)
		}
	}

	return ""
}

// sortDenial refuses SORT BY/GET patterns for users without full key access: the patterns name keys that
// cannot be checked up front, and Redis rejects them the same way.
func (r *userRules) sortDenial(cmd string, args []string) string {
	if r.allKeys || (cmd != cmdSort && cmd != cmdSortRO) {
		return ""
	}

	for i := 1; i < len(args); i++ {
		switch strings.ToUpper(args[i]) {
		case "LIMIT":
			i += sortLimitArgs
		case "STORE":
			i++
		case "BY":
			if i+1 < len(args) && strings.Contains(args[i+1], "*") {
				return "ERR BY option of SORT denied due to insufficient ACL permissions."
			}

			i++
		case "GET":
			return "ERR GET option of SORT denied due to insufficient ACL permissions."
		}
	}

	return ""
}

func passwordMatch(want, got string) bool {
	a, b := sha256.Sum256([]byte(want)), sha256.Sum256([]byte(got))

	return subtle.ConstantTimeCompare(a[:], b[:]) == 1
}

// scriptSafe reports whether the user may run scripts: commands a script issues bypass the access checks,
// so only users with unrestricted keys, channels and commands qualify.
func (r *userRules) scriptSafe() bool {
	if !r.allKeys || !r.allChans {
		return false
	}

	all := false

	for _, rule := range r.rules {
		if !rule.allow {
			return false
		}

		all = all || rule.cat == categoryAll
	}

	return all
}

type channelRef struct {
	name    string
	literal bool
}

func channelsOf(cmd string, args []string) []channelRef {
	switch cmd {
	case "PUBLISH", "SPUBLISH":
		if len(args) > 0 {
			return []channelRef{{name: args[0]}}
		}
	case "SUBSCRIBE", "SSUBSCRIBE":
		return refs(args, false)
	case "PSUBSCRIBE":
		return refs(args, true)
	}

	return nil
}

func refs(names []string, literal bool) []channelRef {
	out := make([]channelRef, 0, len(names))
	for _, n := range names {
		out = append(out, channelRef{name: n, literal: literal})
	}

	return out
}

// globMatch implements Redis glob matching: * ? [set] [^set] [a-z] and backslash escapes.
func globMatch(pattern, s string) bool {
	px, sx := 0, 0
	starP, starS := -1, 0

	for sx < len(s) {
		if px < len(pattern) && pattern[px] == '*' {
			starP, starS = px, sx
			px++

			continue
		}

		if px < len(pattern) {
			if next, ok := matchOne(pattern, px, s[sx]); ok {
				px, sx = next, sx+1

				continue
			}
		}

		if starP < 0 {
			return false
		}

		starS++
		px, sx = starP+1, starS
	}

	for px < len(pattern) && pattern[px] == '*' {
		px++
	}

	return px == len(pattern)
}

// matchOne matches c against the single pattern element at pattern[px] and returns the index after it.
func matchOne(pattern string, px int, c byte) (int, bool) {
	switch pattern[px] {
	case '?':
		return px + 1, true
	case '[':
		return matchClass(pattern, px, c)
	case '\\':
		if px+1 < len(pattern) && pattern[px+1] == c {
			return px + escapeLen, true
		}

		return px, false
	default:
		return px + 1, pattern[px] == c
	}
}

// matchClass matches c against the [..] class starting at pattern[px]; it returns the index after the class.
func matchClass(pattern string, px int, c byte) (int, bool) {
	i := px + 1
	negate := i < len(pattern) && pattern[i] == '^'

	if negate {
		i++
	}

	matched := false

	for i < len(pattern) && pattern[i] != ']' {
		lo := pattern[i]
		if lo == '\\' && i+1 < len(pattern) {
			i++
			lo = pattern[i]
		}

		hi := lo

		if i+2 < len(pattern) && pattern[i+1] == '-' && pattern[i+2] != ']' {
			hi = pattern[i+2]
			i += 2
		}

		if lo > hi {
			lo, hi = hi, lo
		}

		if c >= lo && c <= hi {
			matched = true
		}

		i++
	}

	return i + 1, matched != negate
}
