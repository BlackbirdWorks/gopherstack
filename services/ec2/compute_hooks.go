package ec2

import (
	"context"
	"fmt"
	"strconv"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// computeCapableBackend is implemented by backends that can be enriched with
// a Compute provider (currently only *InMemoryBackend). The handler probes
// for it via type assertion so non-compute backends keep working unchanged.
type computeCapableBackend interface {
	Compute() Compute
	LookupKeyPairAuthorizedKey(name string) string
	LookupInstanceProviderID(instanceID string) string
	SetComputeResult(instanceID string, r LaunchResult) error
	DNSRegistrar() DNSRegistrar
	CreateTags(resourceIDs []string, tags map[string]string) error
}

// computeBackend returns the configured Compute provider, or nil when the
// backend cannot host one or no provider is installed.
func (h *Handler) computeBackend() (computeCapableBackend, Compute) {
	cb, ok := h.Backend.(computeCapableBackend)
	if !ok {
		return nil, nil
	}

	c := cb.Compute()
	if c == nil {
		return cb, nil
	}

	return cb, c
}

// launchOnCompute calls the Compute provider for each freshly created instance
// and merges the result back onto the backend. Errors are logged and the
// instance is left in a "running" state with in-memory defaults so callers can
// still observe and terminate it; full rollback would race with concurrent
// state changes from the caller (e.g. tag creation, attribute mods).
func (h *Handler) launchOnCompute(
	ctx context.Context,
	cb computeCapableBackend,
	c Compute,
	instances []*Instance,
	keyName, userData string,
) {
	log := logger.Load(ctx)

	authorized := ""
	if keyName != "" {
		authorized = cb.LookupKeyPairAuthorizedKey(keyName)
	}

	for _, inst := range instances {
		req := LaunchRequest{
			InstanceID:    inst.ID,
			ImageID:       inst.ImageID,
			InstanceType:  inst.InstanceType,
			KeyName:       keyName,
			AuthorizedKey: authorized,
			UserData:      userData,
		}

		res, err := c.Launch(ctx, req)
		if err != nil {
			log.WarnContext(ctx, "ec2 compute launch failed", "instance", inst.ID, "error", err)

			continue
		}

		if setErr := cb.SetComputeResult(inst.ID, res); setErr != nil {
			log.WarnContext(ctx, "ec2 compute result merge failed", "instance", inst.ID, "error", setErr)

			continue
		}

		// Mirror result back onto the in-flight pointer the caller already
		// holds so the response XML reflects the docker-assigned IPs/ports.
		mergeLaunchResultIntoInstance(inst, res)

		publishComputeMetadata(ctx, cb, inst)
	}
}

// publishComputeMetadata registers the synthetic public DNS hostname with the
// embedded DNS server (when wired) and stamps gopherstack-specific tags on
// the instance so callers (UI, CLI, demo scripts) outside the gopherstack
// process can discover the SSH endpoint without parsing custom XML fields.
func publishComputeMetadata(ctx context.Context, cb computeCapableBackend, inst *Instance) {
	log := logger.Load(ctx)

	if reg := cb.DNSRegistrar(); reg != nil && inst.PublicDNSName != "" {
		reg.Register(inst.PublicDNSName)
	}

	if inst.SSHPort == 0 && inst.PublicIPAddress == "" {
		return
	}

	tags := map[string]string{}
	if inst.SSHPort != 0 {
		tags[tagKeySSHPort] = strconv.Itoa(inst.SSHPort)
	}

	if inst.PublicIPAddress != "" {
		tags[tagKeySSHHost] = inst.PublicIPAddress
	}

	if err := cb.CreateTags([]string{inst.ID}, tags); err != nil {
		log.WarnContext(ctx, "ec2 compute tag write failed", "instance", inst.ID, "error", err)
	}
}

// Tag keys used by the compute hook to surface SSH connection metadata.
const (
	tagKeySSHPort = "gopherstack:ssh-port"
	tagKeySSHHost = "gopherstack:ssh-host"
)

// mergeLaunchResultIntoInstance applies the LaunchResult fields to the
// in-flight instance pointer the handler returns to the response builder.
func mergeLaunchResultIntoInstance(inst *Instance, r LaunchResult) {
	if r.ProviderID != "" {
		inst.ProviderID = r.ProviderID
	}

	if r.PrivateIP != "" {
		inst.PrivateIP = r.PrivateIP
	}

	if r.PublicIPAddress != "" {
		inst.PublicIPAddress = r.PublicIPAddress
	}

	if r.PublicDNSName != "" {
		inst.PublicDNSName = r.PublicDNSName
	}

	if r.SSHPort != 0 {
		inst.SSHPort = r.SSHPort
	}
}

// terminateOnCompute issues Terminate against each instance's Compute provider.
// providerIDs are looked up before the in-memory state is mutated. Errors are
// logged so a partial Docker failure does not break the EC2 API contract.
func (h *Handler) terminateOnCompute(
	ctx context.Context,
	cb computeCapableBackend,
	c Compute,
	providerIDs map[string]string,
	dnsNames map[string]string,
) {
	log := logger.Load(ctx)

	reg := cb.DNSRegistrar()

	for instanceID, providerID := range providerIDs {
		if reg != nil {
			if name := dnsNames[instanceID]; name != "" {
				reg.Deregister(name)
			}
		}

		if providerID == "" {
			continue
		}

		if err := c.Terminate(ctx, instanceID, providerID); err != nil {
			log.WarnContext(ctx, "ec2 compute terminate failed",
				"instance", instanceID, "provider", providerID, "error", err)
		}
	}
}

// computeStartOrStop dispatches Start/Stop hooks for the given instance IDs.
func (h *Handler) computeStartOrStop(
	ctx context.Context,
	cb computeCapableBackend,
	c Compute,
	ids []string,
	start bool,
) {
	log := logger.Load(ctx)

	for _, id := range ids {
		providerID := cb.LookupInstanceProviderID(id)
		if providerID == "" {
			continue
		}

		var err error
		if start {
			err = c.Start(ctx, id, providerID)
		} else {
			err = c.Stop(ctx, id, providerID)
		}

		if err != nil {
			log.WarnContext(ctx, "ec2 compute lifecycle hook failed",
				"instance", id, "provider", providerID, "start", start, "error", err)
		}
	}
}

// snapshotProviderIDs returns a map[instanceID]providerID for the given IDs.
// Unknown instances are silently skipped (matching the lenient AWS terminate
// semantics in this mock).
func snapshotProviderIDs(cb computeCapableBackend, ids []string) map[string]string {
	out := make(map[string]string, len(ids))
	for _, id := range ids {
		out[id] = cb.LookupInstanceProviderID(id)
	}

	return out
}

// snapshotPublicDNSNames returns a map[instanceID]publicDNSName for the
// given IDs so terminateOnCompute can deregister them from the embedded DNS
// after the in-memory state is removed.
func snapshotPublicDNSNames(b instanceLookup, ids []string) map[string]string {
	out := make(map[string]string, len(ids))
	for _, id := range ids {
		if name := b.LookupInstancePublicDNSName(id); name != "" {
			out[id] = name
		}
	}

	return out
}

// instanceLookup is the read side of the backend used by snapshotPublicDNSNames.
type instanceLookup interface {
	LookupInstancePublicDNSName(instanceID string) string
}

// DNSRegistrar can register and deregister hostnames with an embedded DNS
// server. It mirrors the optional registrar interfaces used by RDS, OpenSearch
// and friends so the EC2 docker-compute provider can publish synthetic
// AWS-style instance hostnames (e.g. ec2-1-2-3-4.compute-1.amazonaws.com) that
// resolve to the host the container is reachable on.
type DNSRegistrar interface {
	Register(hostname string)
	Deregister(hostname string)
}

// WithCompute installs an optional Compute provider. When non-nil, RunInstances,
// TerminateInstances, StartInstances and StopInstances will call into the
// provider after updating in-memory state. Passing nil disables the hook.
func (b *InMemoryBackend) WithCompute(c Compute) {
	b.mu.Lock("WithCompute")
	defer b.mu.Unlock()
	b.compute = c
}

// Compute returns the currently installed Compute provider (may be nil).
//
//nolint:ireturn // returning the configured interface is the intent
func (b *InMemoryBackend) Compute() Compute {
	b.mu.RLock("Compute")
	defer b.mu.RUnlock()

	return b.compute
}

// SetDNSRegistrar wires an embedded DNS server so synthetic instance
// hostnames produced by the Compute provider can be resolved by callers
// outside the gopherstack process.
func (b *InMemoryBackend) SetDNSRegistrar(r DNSRegistrar) {
	b.mu.Lock("SetDNSRegistrar")
	defer b.mu.Unlock()
	b.dnsRegistrar = r
}

// DNSRegistrar returns the configured DNS registrar (may be nil).
//
//nolint:ireturn // returning the configured interface is the intent
func (b *InMemoryBackend) DNSRegistrar() DNSRegistrar {
	b.mu.RLock("DNSRegistrar")
	defer b.mu.RUnlock()

	return b.dnsRegistrar
}

// LookupKeyPairAuthorizedKey returns the OpenSSH authorized_keys-format public
// key for the named key pair, or empty string when the key pair is unknown or
// has no derivable public key. Used by Compute providers to seed the launched
// container's authorized_keys.
func (b *InMemoryBackend) LookupKeyPairAuthorizedKey(name string) string {
	b.mu.RLock("LookupKeyPairAuthorizedKey")
	defer b.mu.RUnlock()

	kp, ok := b.keyPairs.Get(name)
	if !ok {
		return ""
	}

	return kp.PublicKey
}

// SetComputeResult merges a LaunchResult onto an existing instance and its
// primary ENI. Empty string fields in the result are ignored. Returns
// ErrInstanceNotFound when the instance has been removed before the compute
// provider returned.
func (b *InMemoryBackend) SetComputeResult(instanceID string, r LaunchResult) error {
	b.mu.Lock("SetComputeResult")
	defer b.mu.Unlock()

	inst, ok := b.instances.Get(instanceID)
	if !ok {
		return fmt.Errorf("%w: %s", ErrInstanceNotFound, instanceID)
	}

	if r.ProviderID != "" {
		inst.ProviderID = r.ProviderID
	}

	if r.PrivateIP != "" {
		oldPrivate := inst.PrivateIP
		inst.PrivateIP = r.PrivateIP
		// keep primary ENI in sync so DescribeNetworkInterfaces shows the
		// container's actual address
		for eniID := range b.eniIDsByInstance[instanceID] {
			if eni, exists := b.networkInterfaces.Get(eniID); exists && eni.PrivateIP == oldPrivate {
				eni.PrivateIP = r.PrivateIP
			}
		}
	}

	if r.PublicIPAddress != "" {
		inst.PublicIPAddress = r.PublicIPAddress
	}

	if r.PublicDNSName != "" {
		inst.PublicDNSName = r.PublicDNSName
	}

	if r.SSHPort != 0 {
		inst.SSHPort = r.SSHPort
	}

	return nil
}

// LookupInstanceProviderID returns the ProviderID stored on an instance, or
// empty string when the instance is unknown.
func (b *InMemoryBackend) LookupInstanceProviderID(instanceID string) string {
	b.mu.RLock("LookupInstanceProviderID")
	defer b.mu.RUnlock()

	inst, ok := b.instances.Get(instanceID)
	if !ok {
		return ""
	}

	return inst.ProviderID
}

// LookupInstancePublicDNSName returns the synthetic public DNS name assigned
// to an instance, or empty string when the instance is unknown or has none.
func (b *InMemoryBackend) LookupInstancePublicDNSName(instanceID string) string {
	b.mu.RLock("LookupInstancePublicDNSName")
	defer b.mu.RUnlock()

	inst, ok := b.instances.Get(instanceID)
	if !ok {
		return ""
	}

	return inst.PublicDNSName
}
