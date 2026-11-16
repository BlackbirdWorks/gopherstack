package ocicontainerengine

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	cycleInstanceReplace   = "INSTANCE_REPLACE"
	cycleBootVolumeReplace = "BOOT_VOLUME_REPLACE"
	specHashBytes          = 8
	percentMax             = 100
)

var cycleLimitRe = regexp.MustCompile(`^(\d{1,9})(%?)$`)

type evictionSettings struct {
	Grace *string `json:"evictionGraceDuration"`
}

type cyclingSettings struct {
	Enabled        *bool    `json:"isNodeCyclingEnabled"`
	MaxUnavailable *string  `json:"maximumUnavailable"`
	MaxSurge       *string  `json:"maximumSurge"`
	Modes          []string `json:"cycleModes"`
}

func decodeSettings[T any](raw jsontext.Value) (T, bool) {
	var out T
	if len(raw) == 0 || string(raw) == "null" {
		return out, false
	}

	return out, json.Unmarshal(raw, &out, jsonx.V1()) == nil
}

func checkCycleLimit(name string, v *string, size int) *APIError {
	if v == nil {
		return nil
	}

	m := cycleLimitRe.FindStringSubmatch(*v)
	if m == nil {
		return errParam(name + " must be an integer or a percentage")
	}

	n, _ := strconv.Atoi(m[1])
	if m[2] == "%" && n > percentMax || m[2] == "" && n > size {
		return errParam(name + " must range from 0 to the node pool size or from 0% to 100%")
	}

	return nil
}

// checkNodePoolSettings validates nodeEvictionNodePoolSettings and nodePoolCyclingDetails.
func checkNodePoolSettings(eviction, cycling jsontext.Value, size int) *APIError {
	if ev, ok := decodeSettings[evictionSettings](eviction); ok {
		if aerr := validEviction("nodeEvictionNodePoolSettings.evictionGraceDuration", ev.Grace); aerr != nil {
			return aerr
		}
	}

	cy, ok := decodeSettings[cyclingSettings](cycling)
	if !ok {
		return nil
	}

	for _, m := range cy.Modes {
		if m != cycleInstanceReplace && m != cycleBootVolumeReplace {
			return errParam(
				"nodePoolCyclingDetails.cycleModes must be " + cycleInstanceReplace + " or " + cycleBootVolumeReplace,
			)
		}
	}

	return firstErr(
		checkCycleLimit("nodePoolCyclingDetails.maximumUnavailable", cy.MaxUnavailable, size),
		checkCycleLimit("nodePoolCyclingDetails.maximumSurge", cy.MaxSurge, size),
	)
}

func nodeSpec(np *NodePool) string {
	var sb strings.Builder

	fmt.Fprint(&sb, np.Version, "|", np.Shape, "|", np.SSHKey)

	if np.NodeSource != nil {
		fmt.Fprint(&sb, "|", np.NodeSource.ImageID)

		if np.NodeSource.BootVolumeGBs != nil {
			fmt.Fprint(&sb, "|", *np.NodeSource.BootVolumeGBs)
		}
	}

	if np.ShapeConfig != nil {
		if np.ShapeConfig.Ocpus != nil {
			fmt.Fprint(&sb, "|", *np.ShapeConfig.Ocpus)
		}

		if np.ShapeConfig.MemoryInGBs != nil {
			fmt.Fprint(&sb, "|", *np.ShapeConfig.MemoryInGBs)
		}
	}

	for _, k := range slices.Sorted(maps.Keys(np.NodeMetadata)) {
		fmt.Fprint(&sb, "|", k, "=", np.NodeMetadata[k])
	}

	sum := sha256.Sum256([]byte(sb.String()))

	return hex.EncodeToString(sum[:specHashBytes])
}

// cycleNodes brings nodes whose settings are out of date up to the pool's current settings when node cycling is on.
func (b *InMemoryBackend) cycleNodes(np *NodePool, c *Cluster) {
	cy, ok := decodeSettings[cyclingSettings](np.Cycling)
	if !ok || cy.Enabled == nil || !*cy.Enabled {
		return
	}

	want := nodeSpec(np)
	bootOnly := len(cy.Modes) > 0 && cy.Modes[0] == cycleBootVolumeReplace

	for _, n := range slices.Clone(np.Nodes) {
		if n.State == stateDeleting || n.State == stateFailing || n.Spec == "" || n.Spec == want {
			continue
		}

		if bootOnly {
			n.Version, n.Spec, n.State, n.TransitionAt = np.Version, want, stateUpdating, np.TransitionAt

			continue
		}

		b.releaseNode(np, n)
		n.State, n.TransitionAt = stateDeleting, np.TransitionAt
		np.Nodes = append(np.Nodes, b.newNode(np, c, slot{n.AD, n.SubnetID}))
	}
}

func (b *InMemoryBackend) logEviction(np *NodePool, n *Node) {
	ev, ok := decodeSettings[evictionSettings](np.Eviction)
	if !ok {
		return
	}

	logger.Load(b.runCtx).InfoContext(b.runCtx, "ocicontainerengine: would cordon and drain node before deleting it",
		"node", n.ID, "evictionGraceDuration", ptrStr(ev.Grace))
}
