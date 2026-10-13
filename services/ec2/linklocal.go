package ec2

import (
	"context"
	"net"
	"net/http"
	"strings"
	"sync"

	"github.com/blackbirdworks/gopherstack/internal/linklocal"
)

const (
	imdsLinkLocalInstance = "linklocal"
)

// LinkLocal makes the default IMDS address reachable from instance containers.
type LinkLocal interface {
	Active(ctx context.Context) bool
	Attach(ctx context.Context, containerID, owner string) error
	Release(containerID string)
	OwnerOf(ctx context.Context, ip string) (string, bool)
}

type linkLocalState struct {
	l  LinkLocal
	mu sync.RWMutex
}

func (s *linkLocalState) get() LinkLocal {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.l
}

type linkLocalSetter interface {
	SetLinkLocal(l LinkLocal)
}

// SetLinkLocal serves IMDS at 169.254.169.254 to docker-backed instances through l.
func (h *Handler) SetLinkLocal(l LinkLocal) {
	h.imds.linkLocal.mu.Lock()
	h.imds.linkLocal.l = l
	h.imds.linkLocal.mu.Unlock()

	for _, bk := range h.imdsBackends() {
		if s, ok := bk.Compute().(linkLocalSetter); ok {
			s.SetLinkLocal(l)
		}
	}
}

// SetLinkLocal installs the link-local relay shared by every region view of this provider.
func (d *DockerCompute) SetLinkLocal(l LinkLocal) {
	d.ll.mu.Lock()
	defer d.ll.mu.Unlock()

	d.ll.l = l
}

func (d *DockerCompute) activeLinkLocal(ctx context.Context) LinkLocal {
	l := d.ll.get()
	if l == nil || !l.Active(ctx) {
		return nil
	}

	return l
}

func (d *DockerCompute) releaseLinkLocal(containerID string) {
	if l := d.ll.get(); l != nil {
		l.Release(containerID)
	}
}

// serveLinkLocalIMDS serves the request relayed from 169.254.169.254; the relay names the connecting container.
func (h *Handler) serveLinkLocalIMDS(w http.ResponseWriter, r *http.Request) {
	l := h.imds.linkLocal.get()
	if l == nil {
		imdsReply(w, http.StatusNotFound, "instance not found")

		return
	}

	peer := strings.TrimSpace(r.Header.Get(linklocal.PeerHeader))
	if net.ParseIP(peer) == nil {
		imdsReply(w, http.StatusNotFound, "instance not found")

		return
	}

	// The relay half-closes its upstream request, which cancels the server-side request context.
	r = r.WithContext(context.WithoutCancel(r.Context()))

	instanceID, ok := l.OwnerOf(r.Context(), peer)
	if !ok {
		imdsReply(w, http.StatusNotFound, "instance not found")

		return
	}

	h.serveIMDS(w, r, instanceID)
}
