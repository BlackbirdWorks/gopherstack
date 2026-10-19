package eks

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

const (
	nodegroupLabel       = "eks.amazonaws.com/nodegroup"
	capacityTypeLabel    = "eks.amazonaws.com/capacityType"
	defaultCapacityType  = "ON_DEMAND"
	nodegroupReadyWindow = 10 * time.Minute
	maxNodeNameLen       = 63
	firstFieldCount      = 1
	agentBaseArgs        = 4
)

var (
	errExecUnsupported = errors.New("container runtime cannot exec; agent nodes need the server container IP")
	errNoServerIP      = errors.New("could not determine the k3s server container IP")
	nonDNSLabelChars   = regexp.MustCompile(`[^a-z0-9-]+`)
)

// agentNode is one k3s agent container backing a managed nodegroup node.
type agentNode struct {
	cluster     string
	created     time.Time
	labels      map[string]string
	nodegroup   string
	nodeName    string
	containerID string
}

// clusterExecer is the optional runtime capability used to discover the server container IP.
type clusterExecer interface {
	Exec(ctx context.Context, containerID string, cmd []string) (container.ExecResult, error)
}

// Exec forwards to the wrapped runtime when it can exec.
func (s sharedRuntime) Exec(ctx context.Context, id string, cmd []string) (container.ExecResult, error) {
	if x, ok := s.ClusterRuntime.(clusterExecer); ok {
		return x.Exec(ctx, id, cmd)
	}

	return container.ExecResult{}, container.ErrExecUnsupported
}

func k8sNodeName(cluster, nodegroup string) string {
	base := nonDNSLabelChars.ReplaceAllString(strings.ToLower(cluster+"-"+nodegroup), "-")
	base = strings.Trim(base, "-")

	suffix := "-" + uuid.NewString()[:8]
	if len(base) > maxNodeNameLen-len(suffix) {
		base = strings.Trim(base[:maxNodeNameLen-len(suffix)], "-")
	}

	return base + suffix
}

func k8sTaintEffect(eksEffect string) string {
	switch eksEffect {
	case "NO_EXECUTE":
		return "NoExecute"
	case "PREFER_NO_SCHEDULE":
		return "PreferNoSchedule"
	default:
		return "NoSchedule"
	}
}

func toK8sTaints(in []NodegroupTaint) []k8sTaint {
	out := make([]k8sTaint, len(in))
	for i, t := range in {
		out[i] = k8sTaint{Key: t.Key, Value: t.Value, Effect: k8sTaintEffect(t.Effect)}
	}

	return out
}

func taintFlag(t k8sTaint) string {
	s := t.Key
	if t.Value != "" {
		s += "=" + t.Value
	}

	return s + ":" + t.Effect
}

func eksNodeLabels(ng ngDesired) map[string]string {
	capacity := ng.capacityType
	if capacity == "" {
		capacity = defaultCapacityType
	}

	return map[string]string{nodegroupLabel: ng.name, capacityTypeLabel: capacity}
}

func desiredNodeLabels(ng ngDesired) map[string]string {
	out := maps.Clone(ng.labels)
	if out == nil {
		out = map[string]string{}
	}

	maps.Copy(out, eksNodeLabels(ng))

	return out
}

func (e *clusterEngine) agentSpec(
	cluster string,
	lc *liveCluster,
	ng ngDesired,
	nodeName, serverIP string,
) container.Spec {
	labels := eksNodeLabels(ng)
	taints := toK8sTaints(ng.taints)
	args := make([]string, 0, agentBaseArgs+2*(len(labels)+len(taints)))
	args = append(args, "/bin/k3s", "agent", "--node-name", nodeName)

	for _, k := range slices.Sorted(maps.Keys(labels)) {
		args = append(args, "--node-label", k+"="+labels[k])
	}

	for _, t := range taints {
		args = append(args, "--node-taint", taintFlag(t))
	}

	return container.Spec{
		Name:       "gopherstack-eks-" + cluster + "-" + ng.name + "-" + uuid.NewString()[:8],
		Image:      lc.image,
		Entrypoint: args,
		Env: []string{
			"K3S_URL=https://" + net.JoinHostPort(serverIP, strconv.Itoa(k3sAPIPort)),
			"K3S_TOKEN=" + lc.joinToken,
		},
		Tmpfs:      []string{"/run", "/var/run"},
		Privileged: true,
	}
}

func (r *reconciler) serverIP(ctx context.Context) (string, error) {
	if ip := r.lc.serverIP.Load(); ip != nil {
		return *ip, nil
	}

	x, ok := r.e.cfg.Runtime.(clusterExecer)
	if !ok {
		return "", errExecUnsupported
	}

	res, err := x.Exec(ctx, r.lc.containerID, []string{"/bin/sh", "-c", "hostname -i"})
	if err != nil {
		return "", fmt.Errorf("discover server IP: %w", err)
	}

	fields := strings.Fields(res.Stdout)
	if res.ExitCode != 0 || len(fields) < firstFieldCount || net.ParseIP(fields[0]) == nil {
		return "", fmt.Errorf("%w: %q", errNoServerIP, strings.TrimSpace(res.Stdout+res.Stderr))
	}

	r.lc.serverIP.Store(&fields[0])

	return fields[0], nil
}

func (r *reconciler) startAgent(ctx context.Context, ng ngDesired) error {
	ip, err := r.serverIP(ctx)
	if err != nil {
		return err
	}

	nodeName := k8sNodeName(r.name, ng.name)

	id, err := r.e.cfg.Runtime.CreateAndStart(ctx, r.e.agentSpec(r.name, r.lc, ng, nodeName, ip))
	if err != nil {
		return fmt.Errorf("start agent container: %w", err)
	}

	r.e.agents.Set(id, &agentNode{
		cluster:     r.name,
		nodegroup:   ng.name,
		nodeName:    nodeName,
		containerID: id,
		created:     time.Now(),
		labels:      eksNodeLabels(ng),
	})

	if r.lc.dead.Load() {
		r.dropAgent(id)
	}

	return nil
}

// dropAgent removes an agent container and forgets it.
func (r *reconciler) dropAgent(containerID string) {
	if a, ok := r.e.agents.Get(containerID); ok {
		r.e.agents.Delete(containerID)
		r.e.remove(containerID, 0)

		ctx, cancel := context.WithTimeout(context.Background(), clusterStopTimeout)
		defer cancel()

		_ = r.api.remove(ctx, "/api/v1/nodes/"+a.nodeName)
	}
}

func (r *reconciler) agentsByNodegroup() map[string][]*agentNode {
	out := map[string][]*agentNode{}

	for _, a := range r.e.agents.Values() {
		if a.cluster == r.name {
			out[a.nodegroup] = append(out[a.nodegroup], a)
		}
	}

	for _, list := range out {
		slices.SortFunc(list, func(x, y *agentNode) int { return x.created.Compare(y.created) })
	}

	return out
}

func (r *reconciler) syncNodegroups(ctx context.Context, ngs []ngDesired, nodes []k8sNode) {
	byNode := map[string]*k8sNode{}
	for i := range nodes {
		byNode[nodes[i].Metadata.Name] = &nodes[i]
	}

	agents := r.agentsByNodegroup()
	want := map[string]bool{}
	updates := map[string]string{}

	for _, ng := range ngs {
		want[ng.name] = true
		list := r.scaleNodegroup(ctx, ng, agents[ng.name])

		ready := 0

		for _, a := range list {
			if n, ok := byNode[a.nodeName]; ok {
				r.syncNodeMetadata(ctx, ng, a, n)

				if n.ready() {
					ready++
				}
			}
		}

		if st := nodegroupNextStatus(ng, ready); st != "" {
			updates[ng.name] = st
		}
	}

	for name, list := range agents {
		if want[name] {
			continue
		}

		for _, a := range list {
			r.dropAgent(a.containerID)
		}
	}

	r.b.applyNodegroupStatuses(r, updates)
}

// scaleNodegroup converges the agent containers of ng to its desired size and returns the resulting set.
func (r *reconciler) scaleNodegroup(ctx context.Context, ng ngDesired, have []*agentNode) []*agentNode {
	for len(have) > ng.desired {
		last := have[len(have)-1]
		have = have[:len(have)-1]

		r.dropAgent(last.containerID)
	}

	key := "nodegroup/" + ng.name

	for len(have) < ng.desired {
		if err := r.startAgent(ctx, ng); err != nil {
			if ctx.Err() == nil && r.warned[key] != err.Error() {
				r.warned[key] = err.Error()
				r.e.log(ctx).WarnContext(ctx, "eks: nodegroup agent start failed",
					"cluster", r.name, "nodegroup", ng.name, "error", err)
			}

			return r.agentsByNodegroup()[ng.name]
		}

		have = r.agentsByNodegroup()[ng.name]
	}

	r.clear(key)

	return have
}

// nodegroupNextStatus returns the status ng should move to, or "" to leave it.
func nodegroupNextStatus(ng ngDesired, ready int) string {
	if ng.status != statusCreating {
		return ""
	}

	switch {
	case ready >= ng.desired:
		return statusActive
	case time.Since(ng.createdAt) > nodegroupReadyWindow:
		return statusCreateFail
	default:
		return ""
	}
}

func (b *InMemoryBackend) applyNodegroupStatuses(r *reconciler, updates map[string]string) {
	if len(updates) == 0 {
		return
	}

	b.mu.Lock("applyNodegroupStatuses")
	defer b.mu.Unlock()

	if !r.currentLocked() {
		return
	}

	for name, st := range updates {
		if ng, ok := b.nodegroups.Get(nodegroupKey(r.name, name)); ok && ng.Status == statusCreating {
			ng.Status = st
			ng.ModifiedAt = time.Now().UTC()
		}
	}
}

// syncNodeMetadata applies user labels and taints that the kubelet is not allowed to self-register.
func (r *reconciler) syncNodeMetadata(ctx context.Context, ng ngDesired, a *agentNode, n *k8sNode) {
	path := "/api/v1/nodes/" + a.nodeName
	want := desiredNodeLabels(ng)

	patch := map[string]*string{}

	for k, v := range want {
		if a.labels[k] != v || n.Metadata.Labels[k] != v {
			patch[k] = &v
		}
	}

	for k := range a.labels {
		if _, keep := want[k]; !keep {
			patch[k] = nil
		}
	}

	if len(patch) > 0 {
		body := map[string]any{keyMetadata: map[string]any{keyLabels: patch}}
		if err := r.api.mergePatch(ctx, path, body); err != nil {
			r.note(ctx, "labels/"+a.nodeName, err)
		} else {
			a.labels = want
		}
	}

	r.syncTaints(ctx, ng, a, n, path)
}

func (r *reconciler) syncTaints(ctx context.Context, ng ngDesired, a *agentNode, n *k8sNode, path string) {
	want := toK8sTaints(ng.taints)

	var keep, have []k8sTaint

	for _, t := range n.Spec.Taints {
		if strings.HasPrefix(t.Key, "node.kubernetes.io/") || strings.HasPrefix(t.Key, "node-role.kubernetes.io/") {
			keep = append(keep, t)

			continue
		}

		have = append(have, t)
	}

	less := func(x, y k8sTaint) int { return strings.Compare(taintFlag(x), taintFlag(y)) }
	slices.SortFunc(want, less)
	slices.SortFunc(have, less)

	if slices.Equal(want, have) {
		return
	}

	body := map[string]any{"spec": map[string]any{"taints": append(keep, want...)}}
	if err := r.api.mergePatch(ctx, path, body); err != nil {
		r.note(ctx, "taints/"+a.nodeName, err)
	}
}
