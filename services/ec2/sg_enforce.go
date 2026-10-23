package ec2

import "github.com/blackbirdworks/gopherstack/pkgs/sgfilter"

// SetSecurityGroupEnforcer makes Compute-backed instances reachable only by clients their security groups
// admit; it reaches every region backend built so far and, through inheritWiring, later ones.
func (h *Handler) SetSecurityGroupEnforcer(e *sgfilter.Enforcer) {
	for _, b := range h.memBackends() {
		b.SetSecurityGroupEnforcer(e)
	}
}

// SetSecurityGroupEnforcer installs the enforcer; nil leaves instance ports published as before.
func (b *InMemoryBackend) SetSecurityGroupEnforcer(e *sgfilter.Enforcer) {
	b.mu.Lock("SetSecurityGroupEnforcer")
	defer b.mu.Unlock()

	b.sgEnforcer = e
}

// SecurityGroupEnforcer returns the installed enforcer, nil when security groups are not enforced.
func (b *InMemoryBackend) SecurityGroupEnforcer() *sgfilter.Enforcer {
	b.mu.RLock("SecurityGroupEnforcer")
	defer b.mu.RUnlock()

	return b.sgEnforcer
}

// InstanceSecurityGroupIDs returns the instance's current groups, read live so ModifyInstanceAttribute
// applies to the next connection.
func (b *InMemoryBackend) InstanceSecurityGroupIDs(instanceID string) []string {
	b.mu.RLock("InstanceSecurityGroupIDs")
	defer b.mu.RUnlock()

	inst, ok := b.instances.Get(instanceID)
	if !ok {
		return nil
	}

	return append([]string(nil), inst.SecurityGroups...)
}

type sgEnforcedBackend interface {
	SecurityGroupEnforcer() *sgfilter.Enforcer
	InstanceSecurityGroupIDs(instanceID string) []string
}

// admitFactory returns the per-port admit hook for an instance, nil when enforcement is off.
func admitFactory(cb computeCapableBackend, instanceID string) func(string, int) sgfilter.Admitter {
	sb, ok := cb.(sgEnforcedBackend)
	if !ok {
		return nil
	}

	enf := sb.SecurityGroupEnforcer()
	if enf == nil {
		return nil
	}

	ids := func() []string { return sb.InstanceSecurityGroupIDs(instanceID) }

	return func(proto string, port int) sgfilter.Admitter { return enf.Ingress(ids, proto, port) }
}
