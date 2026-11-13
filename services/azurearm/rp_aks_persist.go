package azurearm

import (
	"encoding/json/v2"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

type containerServiceSnapshot struct {
	Clusters  map[string]*storedAKSCluster  `json:"clusters"`
	Snapshots map[string]*storedAKSSnapshot `json:"snapshots,omitempty"`
}

func (p *ContainerServiceProvider) snapshot() *containerServiceSnapshot {
	p.mu.RLock("snapshot")
	defer p.mu.RUnlock()

	buf, err := json.Marshal(&containerServiceSnapshot{Clusters: p.clusters, Snapshots: p.snapshots}, jsonx.V1())
	if err != nil {
		return &containerServiceSnapshot{}
	}

	var out containerServiceSnapshot
	if err = json.Unmarshal(buf, &out, jsonx.V1()); err != nil {
		return &containerServiceSnapshot{}
	}

	return &out
}

// restore replaces the stored clusters; with the engine on, each running cluster gets a fresh node in the background.
func (p *ContainerServiceProvider) restore(snap *containerServiceSnapshot) {
	p.opMu.Lock("restore")
	defer p.opMu.Unlock()

	p.eng.removeAll()

	p.mu.Lock("restore")
	p.clusters = map[string]*storedAKSCluster{}
	p.snapshots = map[string]*storedAKSSnapshot{}

	if snap != nil {
		for k, s := range snap.Snapshots {
			if s != nil {
				p.snapshots[k] = s
			}
		}

		for k, c := range snap.Clusters {
			if c == nil {
				continue
			}

			if c.Pools == nil {
				c.Pools = map[string]*storedAKSPool{}
			}

			c.interruptCommands(p.now().UTC())

			if c.ProvState != aksStateCanceled {
				c.ProvState = ""
			}

			p.clusters[k] = c
		}
	}

	known := make([]string, 0, len(p.clusters))
	for k := range p.clusters {
		known = append(known, k)
	}

	p.mu.Unlock()

	p.eng.sweepVolumes(known)
	p.relaunchAll()
}
