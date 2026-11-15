package ocicontainerengine

import (
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/k3sengine"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

var errNoServerIP = errors.New("could not determine the k3s server container IP")

// agentNode is the k3s agent container backing one worker node.
type agentNode struct {
	cluster     string
	containerID string
	starting    bool
}

type agentWant struct {
	id, name string
	labels   []KeyValue
}

func (b *InMemoryBackend) wantedAgentsLocked(clusterID string) []agentWant {
	var out []agentWant

	for _, np := range b.pools {
		if np.ClusterID != clusterID || np.State == stateDeleting || np.State == stateDeleted {
			continue
		}

		for _, n := range np.Nodes {
			if n.State == stateDeleting || n.State == stateFailing {
				continue
			}

			if _, have := b.engine.nodes[n.ID]; !have {
				out = append(out, agentWant{id: n.ID, name: n.Name, labels: np.Labels})
			}
		}
	}

	return out
}

// syncAgentsLocked starts a k3s agent container for every worker node of the cluster that lacks one.
func (b *InMemoryBackend) syncAgentsLocked(clusterID string) {
	if b.engine == nil {
		return
	}

	lc, ok := b.engine.live[clusterID]
	if !ok || !lc.ready {
		return
	}

	for _, w := range b.wantedAgentsLocked(clusterID) {
		b.engine.nodes[w.id] = &agentNode{cluster: clusterID, starting: true}
		b.engine.wg.Go(func() { b.runAgent(clusterID, lc, w) })
	}
}

func (b *InMemoryBackend) runAgent(clusterID string, lc *liveCluster, w agentWant) {
	e := b.engine
	ctx := b.runCtx

	cid, err := b.startAgent(ctx, lc, w)

	b.mu.Lock("runAgent")
	defer b.mu.Unlock()

	a, tracked := e.nodes[w.id]
	if err != nil || !tracked || e.live[clusterID] != lc {
		if tracked && err != nil {
			delete(e.nodes, w.id)
		}

		if err != nil {
			logger.Load(ctx).WarnContext(ctx, "ocicontainerengine: k3s agent start failed",
				"cluster", clusterID, "node", w.id, "error", err)
		} else {
			e.removeAgents([]string{cid})
		}

		return
	}

	a.containerID, a.starting = cid, false
}

func (b *InMemoryBackend) startAgent(ctx context.Context, lc *liveCluster, w agentWant) (string, error) {
	e := b.engine

	ip, err := b.serverIP(ctx, lc)
	if err != nil {
		return "", err
	}

	args := []string{"/bin/k3s", "agent", "--node-name", w.name}
	for _, l := range w.labels {
		args = append(args, "--node-label", l.Key+"="+l.Value)
	}

	id, err := e.core.Runtime().CreateAndStart(ctx, container.Spec{
		Name:       "gopherstack-oke-agent-" + w.name,
		Labels:     container.ServiceLabels("k3s-oke-agent"),
		Image:      lc.image,
		Entrypoint: args,
		Env: []string{
			"K3S_URL=https://" + net.JoinHostPort(ip, strconv.Itoa(k3sengine.APIPort)),
			"K3S_TOKEN=" + lc.joinToken,
		},
		Tmpfs:      []string{"/run", "/var/run"},
		Privileged: true,
	})
	if err != nil {
		return "", fmt.Errorf("start agent container: %w", err)
	}

	return id, nil
}

func (b *InMemoryBackend) serverIP(ctx context.Context, lc *liveCluster) (string, error) {
	b.mu.RLock("serverIP")
	ip, containerID := lc.serverIP, lc.containerID
	b.mu.RUnlock()

	if ip != "" {
		return ip, nil
	}

	res, err := b.engine.core.Exec(ctx, containerID, []string{"/bin/sh", "-c", "hostname -i"}, nil)
	if err != nil {
		return "", fmt.Errorf("discover server IP: %w", err)
	}

	fields := strings.Fields(res.Stdout)
	if res.ExitCode != 0 || len(fields) == 0 || net.ParseIP(fields[0]) == nil {
		return "", fmt.Errorf("%w: %q", errNoServerIP, strings.TrimSpace(res.Stdout+res.Stderr))
	}

	b.mu.Lock("serverIP")
	lc.serverIP = fields[0]
	b.mu.Unlock()

	return fields[0], nil
}

// dropAgentLocked stops the agent container of a node that is being deleted.
func (b *InMemoryBackend) dropAgentLocked(nodeID string) {
	if b.engine == nil {
		return
	}

	if a, ok := b.engine.nodes[nodeID]; ok {
		delete(b.engine.nodes, nodeID)
		b.engine.removeAgents([]string{a.containerID})
	}
}

// takeAgentsLocked forgets the agents of a cluster and returns their container IDs.
func (b *InMemoryBackend) takeAgentsLocked(clusterID string) []string {
	var ids []string

	for nodeID, a := range b.engine.nodes {
		if a.cluster == clusterID {
			ids = append(ids, a.containerID)

			delete(b.engine.nodes, nodeID)
		}
	}

	return ids
}

func (e *clusterEngine) removeAgents(ids []string) {
	for _, id := range slices.DeleteFunc(slices.Clone(ids), func(s string) bool { return s == "" }) {
		e.wg.Go(func() { e.core.Remove(id) })
	}
}
