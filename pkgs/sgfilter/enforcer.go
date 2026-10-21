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

// DefaultGroupSource is optionally implemented by a Source that knows each VPC's default security group,
// which AWS attaches to resources created without one.
type DefaultGroupSource interface {
	// DefaultGroupID returns the default group of vpcID; an empty vpcID means the default VPC.
	DefaultGroupID(vpcID string) string
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
	return e.AdmitIngressInVPC("", sgIDs, proto, port, remote)
}

// AdmitIngressInVPC is AdmitIngress for a target in vpcID; with no sgIDs the VPC's default group applies,
// as AWS does for a resource created without one. An empty vpcID means the default VPC.
func (e *Enforcer) AdmitIngressInVPC(vpcID string, sgIDs []string, proto string, port int, remote net.Addr) bool {
	if e == nil {
		return true
	}

	src := e.source()
	if src == nil {
		return false
	}

	if len(sgIDs) == 0 {
		sgIDs = defaultGroupIDs(src, vpcID)
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

func defaultGroupIDs(src Source, vpcID string) []string {
	ds, ok := src.(DefaultGroupSource)
	if !ok {
		return nil
	}

	if id := ds.DefaultGroupID(vpcID); id != "" {
		return []string{id}
	}

	return nil
}
