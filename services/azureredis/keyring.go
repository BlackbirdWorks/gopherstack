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

// keyring holds a cache's access keys for the AUTH pre-hook.
type keyring struct {
	keys atomic.Pointer[Keys]
}

func (k *keyring) set(v Keys) { k.keys.Store(&v) }

func (k *keyring) get() Keys {
	if p := k.keys.Load(); p != nil {
		return *p
	}

	return Keys{}
}

func same(a, b string) bool { return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1 }

// canonicalize rewrites the secondary key to the primary one in an AUTH or HELLO command's arguments, in place, so
// the miniredis password check (primary only) admits both keys.
func (k *keyring) canonicalize(cmd string, args []string) {
	i := passwordIndex(cmd, args)
	if i < 0 {
		return
	}

	keys := k.get()
	if keys.Secondary != "" && same(args[i], keys.Secondary) {
		args[i] = keys.Primary
	}
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
