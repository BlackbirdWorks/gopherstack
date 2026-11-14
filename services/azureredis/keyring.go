package azureredis

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/alicebob/miniredis/v2/server"

	"github.com/blackbirdworks/gopherstack/pkgs/redisacl"
)

const (
	cmdAuth  = "AUTH"
	cmdHello = "HELLO"
	cmdQuit  = "QUIT"
	keyBytes = 32
	// helloAuthSpan is the distance from AUTH to the password in HELLO ... AUTH username password.
	helloAuthSpan = 2
)

func newKey() (string, error) {
	var b [keyBytes]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err //nolint:wrapcheck // crypto/rand failure is fatal and self-describing
	}

	return base64.StdEncoding.EncodeToString(b[:]), nil
}

func newKeys() (Keys, error) {
	p, err := newKey()
	if err != nil {
		return Keys{}, err
	}

	s, err := newKey()

	return Keys{Primary: p, Secondary: s}, err
}

// TokenValidator checks a Microsoft Entra access token for the Redis audience and returns its principal's object ID.
type TokenValidator func(token string) (objectID string, ok bool)

type grant struct {
	acl   *redisacl.ACL
	alias string
}

type authPolicy struct {
	validate    TokenValidator
	grants      map[string]grant
	disableKeys bool
	entra       bool
}

func newGrants(assignments []AccessAssignment) map[string]grant {
	out := make(map[string]grant, len(assignments))

	for _, a := range assignments {
		acl, err := redisacl.ParsePermissions(a.Permissions)
		if err != nil {
			acl, _ = redisacl.ParsePermissions("")
		}

		out[strings.ToLower(a.ObjectID)] = grant{acl: acl, alias: a.Alias}
	}

	return out
}

// keyring holds a cache's access keys and authentication policy for the AUTH pre-hook.
type keyring struct {
	keys   atomic.Pointer[Keys]
	policy atomic.Pointer[authPolicy]
	peers  sync.Map
}

func (k *keyring) setPolicy(spec Spec, validate TokenValidator) {
	k.policy.Store(&authPolicy{
		disableKeys: spec.DisableAccessKeys, entra: spec.EntraEnabled, validate: validate,
		grants: newGrants(spec.Assignments),
	})
}

func (k *keyring) set(v Keys) { k.keys.Store(&v) }

func (k *keyring) get() Keys {
	if p := k.keys.Load(); p != nil {
		return *p
	}

	return Keys{}
}

func same(a, b string) bool { return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1 }

// canonicalize rewrites the credentials of an AUTH or HELLO command, in place, so the miniredis password check
// (primary key, user "default") admits exactly what the cache's authentication policy allows: either access key
// unless keys are disabled, and an Entra token whose object ID is the user name.
func (k *keyring) canonicalize(peer *server.Peer, cmd string, args []string) {
	pi := passwordIndex(cmd, args)
	if pi < 0 {
		return
	}

	k.peers.Delete(peer)

	pol := k.policy.Load()
	if pol == nil {
		pol = &authPolicy{}
	}

	keys := k.get()
	ui := userIndex(cmd, args)

	if ui >= 0 && !strings.EqualFold(args[ui], "default") {
		if pol.entra && pol.validate != nil {
			if oid, ok := pol.validate(args[pi]); ok && pol.admits(oid, args[ui]) {
				args[ui], args[pi] = "default", keys.Primary
				k.peers.Store(peer, strings.ToLower(oid))
				peer.OnDisconnect(func() { k.peers.Delete(peer) })

				return
			}
		}

		args[pi] = ""

		return
	}

	switch {
	case pol.disableKeys:
		args[pi] = ""
	case keys.Secondary != "" && same(args[pi], keys.Secondary):
		args[pi] = keys.Primary
	}
}

// admits reports whether the principal holds an assignment and names itself by object ID or alias.
func (p *authPolicy) admits(oid, user string) bool {
	g, ok := p.grants[strings.ToLower(oid)]

	return ok && (strings.EqualFold(oid, user) || (g.alias != "" && strings.EqualFold(g.alias, user)))
}

// authorize applies the data access policy of the principal that authenticated the connection with Entra.
func (k *keyring) authorize(peer *server.Peer, cmd string, args []string) bool {
	v, ok := k.peers.Load(peer)
	if !ok || cmd == cmdAuth || cmd == cmdHello || cmd == cmdQuit {
		return true
	}

	pol := k.policy.Load()

	var g grant

	if oid, _ := v.(string); pol != nil {
		g = pol.grants[oid]
	}

	if g.acl == nil {
		g.acl, _ = redisacl.ParsePermissions("")
	}

	msg, allowed := g.acl.Check(cmd, args)
	if !allowed {
		peer.WriteError(msg)
	}

	return allowed
}

func userIndex(cmd string, args []string) int {
	switch cmd {
	case cmdAuth:
		if len(args) == helloAuthSpan {
			return 0
		}
	case cmdHello:
		for i := 1; i+helloAuthSpan < len(args); i++ {
			if strings.EqualFold(args[i], cmdAuth) {
				return i + 1
			}
		}
	}

	return -1
}

func passwordIndex(cmd string, args []string) int {
	switch cmd {
	case cmdAuth:
		if len(args) == 0 {
			return -1
		}

		return len(args) - 1
	case cmdHello:
		for i := 1; i+helloAuthSpan < len(args); i++ {
			if strings.EqualFold(args[i], cmdAuth) {
				return i + helloAuthSpan
			}
		}
	}

	return -1
}

// install makes the engine's server require the primary key and accept the secondary through canonicalize.
func (k *keyring) install(srv *server.Server) {
	srv.SetPreHook(func(peer *server.Peer, cmd string, args ...string) bool {
		k.canonicalize(peer, cmd, args)

		return !k.authorize(peer, cmd, args)
	})
}
