package ec2

import (
	"context"

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
	}
}

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
func (h *Handler) terminateOnCompute(ctx context.Context, c Compute, providerIDs map[string]string) {
	log := logger.Load(ctx)

	for instanceID, providerID := range providerIDs {
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
