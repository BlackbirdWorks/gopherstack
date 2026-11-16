package ocicontainerengine

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

type virtualCyclingSettings struct {
	Enabled        *bool   `json:"isVirtualNodeCyclingEnabled"`
	MaxUnavailable *string `json:"maximumUnavailable"`
	MaxSurge       *string `json:"maximumSurge"`
}

// checkVirtualCycling validates virtualNodePoolCyclingDetails.
func checkVirtualCycling(cycling jsontext.Value, size int) *APIError {
	cy, ok := decodeSettings[virtualCyclingSettings](cycling)
	if !ok {
		return nil
	}

	return firstErr(
		checkCycleLimit("virtualNodePoolCyclingDetails.maximumUnavailable", cy.MaxUnavailable, size),
		checkCycleLimit("virtualNodePoolCyclingDetails.maximumSurge", cy.MaxSurge, size),
	)
}

// virtualNodeSpec fingerprints the pool settings a virtual node is created from.
func virtualNodeSpec(p *VirtualNodePool) string {
	nsgs := slices.Sorted(slices.Values(p.NsgIDs))

	raw, err := json.Marshal(struct {
		Pod    *PodConfig
		Tags   *Tags
		Ver    string
		Nsgs   []string
		Labels []KeyValue
		Taints []Taint
	}{p.PodConfig, p.NodeTags, p.Version, nsgs, p.Labels, p.Taints}, jsonx.V1())
	if err != nil {
		return ""
	}

	sum := sha256.Sum256(raw)

	return hex.EncodeToString(sum[:specHashBytes])
}

// cycleVirtualNodes replaces virtual nodes created from out-of-date pool settings when virtual node cycling is on.
func (b *InMemoryBackend) cycleVirtualNodes(p *VirtualNodePool) {
	cy, ok := decodeSettings[virtualCyclingSettings](p.Cycling)
	if !ok || cy.Enabled == nil || !*cy.Enabled {
		return
	}

	want := virtualNodeSpec(p)

	for _, n := range slices.Clone(p.Nodes) {
		if n.State == stateDeleting || n.Spec == "" || n.Spec == want {
			continue
		}

		n.State, n.TransitionAt = stateDeleting, p.TransitionAt
		p.Nodes = append(p.Nodes, b.newVirtualNode(p, placementOf(p, n)))
	}
}

func placementOf(p *VirtualNodePool, n *VirtualNode) VPlacement {
	for _, pl := range p.Placements {
		if pl.AvailabilityDomain == n.AD && pl.SubnetID == n.SubnetID {
			return pl
		}
	}

	return p.Placements[0]
}
