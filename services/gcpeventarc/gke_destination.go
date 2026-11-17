package gcpeventarc

import (
	"context"

	"cloud.google.com/go/eventarc/apiv1/eventarcpb"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// checkCluster requires the cluster of a GKE destination to exist when a GKE emulator is connected. It must
// run without the backend lock held.
func (b *InMemoryBackend) checkCluster(project string, d *eventarcpb.Destination) error {
	g := d.GetGke()
	if g == nil {
		return nil
	}

	b.mu.RLock("checkCluster")
	clusters := b.svc.Clusters
	b.mu.RUnlock()

	name := "projects/" + project + "/locations/" + g.GetLocation() + "/clusters/" + g.GetCluster()
	if clusters != nil && !clusters.ClusterExists(name) {
		return errPrecondition("GKE cluster %q does not exist.", name)
	}

	return nil
}

// deliverToGKE records the delivery: the emulated clusters run no pods, so there is no Kubernetes service to
// post to.
func deliverToGKE(ctx context.Context, t *eventarcpb.Trigger, ev cloudEvent) {
	g := t.GetDestination().GetGke()
	logger.Load(ctx).InfoContext(ctx, "gcpeventarc: would deliver event to GKE service (no data plane)",
		"trigger", t.GetName(), "cluster", g.GetCluster(), "namespace", g.GetNamespace(),
		"service", g.GetService(), "path", g.GetPath(), "id", ev.id, "type", ev.typ)
}
