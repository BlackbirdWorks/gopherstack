package registryauth

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"sync"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// PullFunc pulls ref using the base64url-JSON registryAuth (may be empty).
type PullFunc func(ctx context.Context, ref, registryAuth string) error

// TagFunc aliases an already-pulled source image as target.
type TagFunc func(ctx context.Context, source, target string) error

// Resolver chooses credentials and mirrors for image pulls.
type Resolver struct {
	explicit map[string]credential
	mirrors  map[string]string
	emulated map[string]credential
	mu       sync.RWMutex
}

// New builds a Resolver from settings.
func New(s Settings) (*Resolver, error) {
	auth, err := parseAuth(s.Auth)
	if err != nil {
		return nil, err
	}

	mirrors, err := parseMirrors(s.Mirror)
	if err != nil {
		return nil, err
	}

	return &Resolver{explicit: auth, mirrors: mirrors, emulated: make(map[string]credential)}, nil
}

// RegisterEmulated marks host as an in-process registry (e.g. the ECR
// emulator) that accepts the given credentials without further configuration.
func (r *Resolver) RegisterEmulated(host, user, pass string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.emulated[normalizeHost(host)] = credential{user: user, pass: pass}
}

func encodeAuth(ac authConfig) string {
	raw, err := json.Marshal(ac) //nolint:gosec // credentials are the payload of the X-Registry-Auth header
	if err != nil {
		return ""
	}

	return base64.URLEncoding.EncodeToString(raw)
}

func fromCredential(host string, c credential) authConfig {
	return authConfig{Username: c.user, Password: c.pass, ServerAddress: host}
}

// AuthFor returns the registryAuth header value for host, or "" when none is known.
func (r *Resolver) AuthFor(ctx context.Context, host string) string {
	host = normalizeHost(host)

	r.mu.RLock()
	c, ok := r.explicit[host]
	e, emu := r.emulated[host]
	r.mu.RUnlock()

	switch {
	case ok:
		return encodeAuth(fromCredential(host, c))
	case emu:
		return encodeAuth(fromCredential(host, e))
	}

	if ac, found := fromDockerConfig(ctx, host); found {
		return encodeAuth(ac)
	}

	return ""
}

func (r *Resolver) mirrorFor(ref imageRef) (string, bool) {
	if ref.digest != "" {
		return "", false
	}

	m, ok := r.mirrors[ref.host]

	return m, ok
}

// Pull pulls ref with per-host credentials, trying a configured mirror first
// (retagging to ref on success) and falling back to the original registry.
func (r *Resolver) Pull(ctx context.Context, ref string, pull PullFunc, tag TagFunc) error {
	parsed := parseRef(ref)

	if mirror, ok := r.mirrorFor(parsed); ok && tag != nil {
		if r.pullViaMirror(ctx, parsed, ref, mirror, pull, tag) {
			return nil
		}
	}

	return pull(ctx, ref, r.AuthFor(ctx, parsed.host))
}

func (r *Resolver) pullViaMirror(
	ctx context.Context, parsed imageRef, ref, mirror string, pull PullFunc, tag TagFunc,
) bool {
	log := logger.Load(ctx)
	mirrorRef := parsed.withHost(mirror)

	err := pull(ctx, mirrorRef, r.AuthFor(ctx, normalizeHost(mirror)))
	if err == nil {
		err = tag(ctx, mirrorRef, parsed.tagged())
	}

	if err != nil {
		log.WarnContext(ctx, "registry mirror failed; falling back to original registry",
			slog.String("registry", parsed.host), slog.String("mirror", mirror), slog.Any("error", err))

		return false
	}

	log.DebugContext(ctx, "image pulled via registry mirror", slog.String("image", ref), slog.String("mirror", mirror))

	return true
}
