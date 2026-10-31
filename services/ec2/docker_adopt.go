package ec2

import (
	"context"

	mobyclient "github.com/moby/moby/client"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const ec2Service = "ec2"

// AdoptSpec identifies a restored instance whose container may have survived a crash.
type AdoptSpec struct {
	InstanceID string
	ProviderID string
	ImageID    string
	VPCID      string
}

// instanceAdopter is implemented by Compute providers that can take over a surviving instance container.
type instanceAdopter interface {
	AdoptInstance(ctx context.Context, spec AdoptSpec) (LaunchResult, bool)
}

// managedInstanceLabels identify an instance container to a restarted gopherstack. A container behind a
// security-group relay carries no resource key: the relay dies with the process, so it is never adopted.
func managedInstanceLabels(req LaunchRequest) map[string]string {
	if req.Admit != nil {
		return container.ManagedLabels(ec2Service, "")
	}

	return container.ManagedLabels(ec2Service, req.InstanceID)
}

// AdoptInstance takes over the running container a crashed gopherstack left for spec's instance and reports the
// addresses it still has. It declines, removing the container, when it is not the recorded container of the same
// AMI, is not running, or the provider maps EBS volumes (their mounts cannot be rebuilt from the container).
func (d *DockerCompute) AdoptInstance(ctx context.Context, spec AdoptSpec) (LaunchResult, bool) {
	claim := d.claim
	if claim == nil {
		claim = container.ClaimOrphan
	}

	oc, ok := claim(ec2Service, spec.InstanceID)
	if !ok {
		return LaunchResult{}, false
	}

	res, adopted := d.adoptable(ctx, spec, oc)
	if !adopted {
		d.discardOrphan(ctx, oc.ID)

		return LaunchResult{}, false
	}

	logger.Load(ctx).InfoContext(ctx, "ec2: adopted instance container from an earlier run",
		"instance", spec.InstanceID, "container", oc.ID)

	return res, true
}

func (d *DockerCompute) adoptable(
	ctx context.Context, spec AdoptSpec, oc container.ManagedContainer,
) (LaunchResult, bool) {
	if d.EBSEnabled() || oc.ID != spec.ProviderID || oc.Labels[dockerLabelPrefix+"image"] != spec.ImageID {
		return LaunchResult{}, false
	}

	res, err := d.api.ContainerInspect(ctx, oc.ID, mobyclient.ContainerInspectOptions{})
	if err != nil || res.Container.State == nil || !res.Container.State.Running {
		return LaunchResult{}, false
	}

	preferred := d.cfg.Network
	if spec.VPCID != "" {
		preferred = vpcNetworkName(spec.VPCID)
	}

	ip, port := extractInspectAddrs(res.Container, preferred)
	if ip == "" {
		return LaunchResult{}, false
	}

	d.reservePort(port)

	return LaunchResult{
		ProviderID:     oc.ID,
		PrivateIP:      ip,
		SSHPort:        port,
		PublishedPorts: publishedOf(res.Container),
	}, true
}

// reservePort marks an adopted container's ssh port as taken when it lies in the configured range.
func (d *DockerCompute) reservePort(port int) {
	if port < d.cfg.SSHPortMin || port > d.cfg.SSHPortMax {
		return
	}

	d.ports.mu.Lock()
	defer d.ports.mu.Unlock()

	d.ports.used[port] = struct{}{}
}

func (d *DockerCompute) discardOrphan(ctx context.Context, id string) {
	timeout := stopTimeoutSec
	_, _ = d.api.ContainerStop(ctx, id, mobyclient.ContainerStopOptions{Timeout: &timeout})
	_, _ = d.api.ContainerRemove(ctx, id, mobyclient.ContainerRemoveOptions{Force: true, RemoveVolumes: true})
}

// adoptCandidates lists the restored pending or running instances that were on a provider container.
func (b *InMemoryBackend) adoptCandidates() []AdoptSpec {
	b.mu.RLock("adoptCandidates")
	defer b.mu.RUnlock()

	var out []AdoptSpec

	for _, inst := range b.instances.All() {
		if inst.ProviderID == "" || (inst.State.Name != StateRunning.Name && inst.State.Name != StatePending.Name) {
			continue
		}

		out = append(out, AdoptSpec{
			InstanceID: inst.ID, ProviderID: inst.ProviderID, ImageID: inst.ImageID, VPCID: inst.VPCID,
		})
	}

	return out
}

func (b *InMemoryBackend) instanceCopy(id string) (Instance, bool) {
	b.mu.RLock("instanceCopy")
	defer b.mu.RUnlock()

	inst, ok := b.instances.Get(id)
	if !ok {
		return Instance{}, false
	}

	return *inst, true
}

// adoptComputeInstances reattaches restored running instances to their surviving containers.
func (h *Handler) adoptComputeInstances(ctx context.Context) {
	cb, c := h.computeBackend()

	ad, ok := c.(instanceAdopter)
	mem, isMem := h.Backend.(*InMemoryBackend)

	if cb == nil || !ok || !isMem {
		return
	}

	for _, spec := range mem.adoptCandidates() {
		res, adopted := ad.AdoptInstance(ctx, spec)
		if !adopted {
			continue
		}

		if err := cb.SetComputeResult(spec.InstanceID, res); err != nil {
			logger.Load(ctx).WarnContext(ctx, "ec2: adopted instance result merge failed",
				"instance", spec.InstanceID, "error", err)

			continue
		}

		h.imdsEndpointFor(ctx, spec.InstanceID)
		publishNetworkMetadata(ctx, cb, spec.InstanceID, res)

		if inst, found := mem.instanceCopy(spec.InstanceID); found {
			publishComputeMetadata(ctx, cb, &inst)
		}
	}
}
