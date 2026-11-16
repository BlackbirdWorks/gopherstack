package ocicontainerengine

import (
	"net/http"
	"strconv"
)

// Default Kubernetes Engine limits of an Oracle Universal Credits tenancy in a commercial realm, from
// docs.oracle.com/en-us/iaas/Content/General/Concepts/servicelimits.htm.
const (
	limitBasicClusters       = 15
	limitEnhancedClusters    = 15
	limitNodesBasic          = 1000
	limitNodesEnhancedFlannl = 5000
	limitNodesEnhancedNative = 2000
	limitVirtualNodes        = 9
)

func errLimit(what string, limit int) *APIError {
	return newErr(http.StatusBadRequest, "LimitExceeded",
		"Fulfilling this request exceeds the limit of "+strconv.Itoa(limit)+" for "+what+".")
}

func (b *InMemoryBackend) clustersOfType(typ string) int {
	n := 0

	for _, c := range b.clusters {
		if c.Type == typ && c.State != stateDeleted {
			n++
		}
	}

	return n
}

func (b *InMemoryBackend) checkClusterLimit(typ string) *APIError {
	limit, what := limitBasicClusters, "basic clusters per region"
	if typ == typeEnhanced {
		limit, what = limitEnhancedClusters, "enhanced clusters per region"
	}

	if b.clustersOfType(typ) >= limit {
		return errLimit(what, limit)
	}

	return nil
}

func clusterNodeLimit(c *Cluster) int {
	switch {
	case c.Type == typeBasic:
		return limitNodesBasic
	case len(c.PodNetworks) > 0 && c.PodNetworks[0].CniType == cniNative:
		return limitNodesEnhancedNative
	}

	return limitNodesEnhancedFlannl
}

// checkNodeLimit refuses a node pool size that takes the cluster over its managed node limit.
func (b *InMemoryBackend) checkNodeLimit(c *Cluster, poolID string, size int) *APIError {
	total := size

	for _, np := range b.pools {
		if np.ClusterID == c.ID && np.ID != poolID && np.State != stateDeleting && np.State != stateDeleted {
			total += b.targetSize(np)
		}
	}

	if limit := clusterNodeLimit(c); total > limit {
		return errLimit("managed nodes per cluster", limit)
	}

	return nil
}

// checkVirtualNodeLimit refuses a virtual node pool size that takes the region over its virtual node limit.
func (b *InMemoryBackend) checkVirtualNodeLimit(poolID string, size int) *APIError {
	total := size

	for _, p := range b.vpools {
		if p.ID != poolID && p.State != stateDeleting && p.State != stateDeleted {
			total += p.Size
		}
	}

	if total > limitVirtualNodes {
		return errLimit("virtual nodes per region", limitVirtualNodes)
	}

	return nil
}
