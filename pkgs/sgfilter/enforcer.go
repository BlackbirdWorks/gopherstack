package sgfilter

import (
	"net"
	"net/netip"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

// Source resolves live security-group state; EC2 implements it so rule changes take effect on the next
// connection without any push.
type Source interface {
	// SecurityGroups returns the named groups; unknown IDs are omitted.
	SecurityGroups(ids []string) []Group
	// GroupsOfAddr lists the security groups attached to the interface owning addr.
	GroupsOfAddr(addr netip.Addr) []string
}

// Admitter decides whether a remote client may connect.
type Admitter func(remote net.Addr) bool

// Enforcer evaluates flows against the groups its Source reports. A nil Enforcer admits everything, so
// services hold one unconditionally and the operator flag decides whether it is set.
type Enforcer struct {
	src Source
	mu  *lockmetrics.RWMutex
}

// New returns an Enforcer with no Source; until SetSource, every group is empty and ingress is denied.
func New() *Enforcer {
	return &Enforcer{mu: lockmetrics.New("sgfilter")}
}

// SetSource installs the security-group source.
func (e *Enforcer) SetSource(s Source) {
	if e == nil {
		return
	}

	e.mu.Lock("SetSource")
	e.src = s
	e.mu.Unlock()
}

func (e *Enforcer) source() Source {
	e.mu.RLock("source")
	defer e.mu.RUnlock()

	return e.src
}

// AdmitIngress reports whether a client at remote may open proto/port on a target carrying sgIDs.
func (e *Enforcer) AdmitIngress(sgIDs []string, proto string, port int, remote net.Addr) bool {
	if e == nil {
		return true
	}

	src := e.source()
	if src == nil {
		return false
	}

	addr := AddrOf(remote)
	peer := Peer{Addr: addr}

	if addr.IsValid() {
		peer.Groups = src.GroupsOfAddr(addr)
	}

	return AllowIngress(src.SecurityGroups(sgIDs), Flow{Protocol: proto, Port: port, Peer: peer})
}

// Ingress returns an Admitter that re-reads the target's group IDs on every connection, so membership and
// rule edits apply live. A nil Enforcer yields a nil Admitter, which Proxy treats as admit-all.
func (e *Enforcer) Ingress(sgIDs func() []string, proto string, port int) Admitter {
	if e == nil {
		return nil
	}

	return func(remote net.Addr) bool {
		return e.AdmitIngress(sgIDs(), proto, port, remote)
	}
}
