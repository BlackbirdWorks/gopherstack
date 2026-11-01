package ssm

import (
	"context"
	"crypto/subtle"
	"io"
	"maps"
	"net/http"
	"slices"
	"strings"
)

// Shell is an interactive terminal attached to a managed node.
type Shell interface {
	io.ReadWriteCloser
	Resize(ctx context.Context, cols, rows uint) error
}

// ShellOpener starts an interactive shell in an instance container.
type ShellOpener interface {
	OpenShell(ctx context.Context, containerID string, cols, rows uint) (Shell, error)
}

// CommandShellOpener runs an arbitrary command in a container with a TTY.
type CommandShellOpener interface {
	OpenCommand(ctx context.Context, containerID string, cmd []string, cols, rows uint) (Shell, error)
}

type streamBaseKey struct{}

const schemeHTTPS = "https"

// SetShellOpener enables Session Manager shells on Compute-backed instances.
func (b *InMemoryBackend) SetShellOpener(o ShellOpener) {
	b.mu.Lock("SetShellOpener")
	defer b.mu.Unlock()

	b.shells = o
}

// ShellsEnabled reports whether command sessions in containers can be opened.
func (b *InMemoryBackend) ShellsEnabled() bool {
	_, ok := b.shellOpener().(CommandShellOpener)

	return ok
}

func (b *InMemoryBackend) shellOpener() ShellOpener {
	b.mu.RLock("shellOpener")
	defer b.mu.RUnlock()

	return b.shells
}

// streamURL is the data channel URL of a session on this server.
func streamURL(ctx context.Context, sessionID string) string {
	base, ok := ctx.Value(streamBaseKey{}).(string)
	if !ok || base == "" {
		return "wss://gopherstack-ssm-session/" + sessionID
	}

	return base + dataChannelPath + sessionID + "?role=publish_subscribe"
}

// authorizeDataChannel returns the connected session whose token matches, and its region.
func (b *InMemoryBackend) authorizeDataChannel(sessionID, token string) (Session, string, bool) {
	if token == "" {
		return Session{}, "", false
	}

	b.mu.RLock("authorizeDataChannel")
	defer b.mu.RUnlock()

	b.tableMu.Lock()
	regions := slices.Collect(maps.Keys(b.sessions))
	b.tableMu.Unlock()

	for _, region := range regions {
		s, ok := b.sessionsStore(region).Get(sessionID)
		if !ok {
			continue
		}

		if s.Status != sessionStatusConnected ||
			subtle.ConstantTimeCompare([]byte(s.TokenValue), []byte(token)) != 1 {
			return Session{}, "", false
		}

		return *s, region, true
	}

	return Session{}, "", false
}

func (b *InMemoryBackend) shellContainer(region, instanceID string) (string, bool) {
	r := b.commandRunner()
	if r == nil || r.Instances == nil {
		return "", false
	}

	for _, c := range r.Instances.ContainerInstances(region, []string{instanceID}) {
		if c.InstanceID == instanceID && c.ContainerID != "" {
			return c.ContainerID, true
		}
	}

	return "", false
}

func (b *InMemoryBackend) trackChannel(sessionID string, cancel context.CancelFunc) {
	b.execMu.Lock()
	defer b.execMu.Unlock()

	if b.dataChannels == nil {
		b.dataChannels = make(map[string]context.CancelFunc)
	}

	if prev := b.dataChannels[sessionID]; prev != nil {
		prev()
	}

	b.dataChannels[sessionID] = cancel
}

func (b *InMemoryBackend) untrackChannel(sessionID string) {
	b.execMu.Lock()
	defer b.execMu.Unlock()

	delete(b.dataChannels, sessionID)
}

func (b *InMemoryBackend) closeChannel(sessionID string) {
	b.execMu.Lock()
	cancel := b.dataChannels[sessionID]
	b.execMu.Unlock()

	if cancel != nil {
		cancel()
	}
}

// streamBase derives the ws(s):// base of this server from the request.
func streamBase(scheme, host string) string {
	if host == "" {
		return ""
	}

	if scheme == schemeHTTPS {
		return "wss://" + host
	}

	return "ws://" + strings.TrimSpace(host)
}

func requestScheme(r *http.Request) string {
	if r.TLS != nil || r.Header.Get("X-Forwarded-Proto") == schemeHTTPS {
		return schemeHTTPS
	}

	return "http"
}
