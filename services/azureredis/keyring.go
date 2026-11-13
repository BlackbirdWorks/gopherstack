package azureredis

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"strings"
	"sync/atomic"

	"github.com/alicebob/miniredis/v2/server"
)

const (
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

type authPolicy struct {
	validate    TokenValidator
	disableKeys bool
	entra       bool
}

// keyring holds a cache's access keys and authentication policy for the AUTH pre-hook.
type keyring struct {
	keys   atomic.Pointer[Keys]
	policy atomic.Pointer[authPolicy]
}

func (k *keyring) setPolicy(spec Spec, validate TokenValidator) {
	k.policy.Store(&authPolicy{disableKeys: spec.DisableAccessKeys, entra: spec.EntraEnabled, validate: validate})
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
func (k *keyring) canonicalize(cmd string, args []string) {
	pi := passwordIndex(cmd, args)
	if pi < 0 {
		return
	}

	pol := k.policy.Load()
	if pol == nil {
		pol = &authPolicy{}
	}

	keys := k.get()
	ui := userIndex(cmd, args)

	if ui >= 0 && !strings.EqualFold(args[ui], "default") {
		if pol.entra && pol.validate != nil {
			if oid, ok := pol.validate(args[pi]); ok && strings.EqualFold(oid, args[ui]) {
				args[ui], args[pi] = "default", keys.Primary

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

func userIndex(cmd string, args []string) int {
	switch cmd {
	case "AUTH":
		if len(args) == helloAuthSpan {
			return 0
		}
	case "HELLO":
		for i := 1; i+helloAuthSpan < len(args); i++ {
			if strings.EqualFold(args[i], "AUTH") {
				return i + 1
			}
		}
	}

	return -1
}

func passwordIndex(cmd string, args []string) int {
	switch cmd {
	case "AUTH":
		if len(args) == 0 {
			return -1
		}

		return len(args) - 1
	case "HELLO":
		for i := 1; i+helloAuthSpan < len(args); i++ {
			if strings.EqualFold(args[i], "AUTH") {
				return i + helloAuthSpan
			}
		}
	}

	return -1
}

// install makes the engine's server require the primary key and accept the secondary through canonicalize.
func (k *keyring) install(srv *server.Server) {
	srv.SetPreHook(func(_ *server.Peer, cmd string, args ...string) bool {
		k.canonicalize(cmd, args)

		return false
	})
}
