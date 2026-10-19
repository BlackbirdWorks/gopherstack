package eks

import (
	"context"
	"time"
)

const (
	addonCoreDNS    = "coredns"
	addonKubeProxy  = "kube-proxy"
	addonReadyLimit = 10 * time.Minute

	issueNoReplicas = "InsufficientNumberOfReplicas"
)

// engineBackedAddon reports whether k3s runs a real equivalent of the add-on, so its status can be measured.
func engineBackedAddon(name string) bool {
	switch name {
	case addonCoreDNS, addonVPCCNI, addonKubeProxy:
		return true
	default:
		return false
	}
}

// addonHealthy measures a real k3s equivalent: coredns is the built-in deployment, vpc-cni maps to k3s's
// flannel (a node reports Ready only once the CNI is configured), kube-proxy runs inside k3s.
func (r *reconciler) addonHealthy(ctx context.Context, name string, nodes []k8sNode) (bool, string, error) {
	switch name {
	case addonCoreDNS:
		ok, err := r.api.deploymentAvailable(ctx, "kube-system", "coredns")

		return ok, "deployment kube-system/coredns has no available replicas", err
	case addonVPCCNI, addonKubeProxy:
		for i := range nodes {
			if nodes[i].ready() {
				return true, "", nil
			}
		}

		return false, "no node is Ready", nil
	default:
		return true, "", nil
	}
}

// nextAddonStatus applies the measured health to an add-on's current status; changed is false when it stays.
func nextAddonStatus(cur string, healthy bool, age time.Duration) (string, bool) {
	switch cur {
	case statusCreating:
		switch {
		case healthy:
			return statusActive, true
		case age > addonReadyLimit:
			return statusCreateFail, true
		}
	case statusActive:
		if !healthy {
			return statusDegraded, true
		}
	case statusDegraded:
		if healthy {
			return statusActive, true
		}
	}

	return cur, false
}

type addonUpdate struct {
	status string
	detail string
}

func (r *reconciler) syncAddons(ctx context.Context, addons []addonDesired, nodes []k8sNode) {
	updates := map[string]addonUpdate{}

	for _, a := range addons {
		if !engineBackedAddon(a.name) {
			continue
		}

		healthy, detail, err := r.addonHealthy(ctx, a.name, nodes)
		if err != nil {
			r.note(ctx, "addon/"+a.name, err)

			continue
		}

		if st, changed := nextAddonStatus(a.status, healthy, time.Since(a.createdAt)); changed {
			updates[a.name] = addonUpdate{status: st, detail: detail}
		}
	}

	r.b.applyAddonStatuses(r, updates)
}

func (b *InMemoryBackend) applyAddonStatuses(r *reconciler, updates map[string]addonUpdate) {
	if len(updates) == 0 {
		return
	}

	b.mu.Lock("applyAddonStatuses")
	defer b.mu.Unlock()

	if !r.currentLocked() {
		return
	}

	for name, u := range updates {
		a, ok := b.addons.Get(addonKey(r.name, name))
		if !ok {
			continue
		}

		a.Status = u.status
		a.ModifiedAt = time.Now().UTC()
		a.Health = &AddonHealth{Issues: []map[string]string{}}

		if u.status == statusDegraded || u.status == statusCreateFail {
			a.Health.Issues = append(a.Health.Issues, map[string]string{keyCode: issueNoReplicas, keyMessage: u.detail})
		}
	}
}
