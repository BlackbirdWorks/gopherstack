package redshift

import (
	"crypto/sha256"
	"encoding/hex"
	"net/url"
	"strconv"
	"strings"
)

// Redshift Advisor recommendation types and impact rankings.
const (
	recTypeCostOptimization = "CostOptimization"
	recTypeSecurity         = "Security"
	recTypeHighAvailability = "HighAvailability"
	recTypeManagement       = "Management"

	impactHigh   = "HIGH"
	impactMedium = "MEDIUM"
	impactLow    = "LOW"

	recActionTypeUpdate  = "Update"
	recActionTypeRestore = "Restore"

	// Node-configuration action types accepted by DescribeNodeConfigurationOptions.
	nodeActionRestore   = "restore-cluster"
	nodeActionRecommend = "recommend-node-config"
	nodeActionResize    = "resize-cluster"

	// Node counts offered for a generated node-configuration option set.
	nodeCountFour  = 4
	nodeCountEight = 8

	// Estimated per-node disk utilization for generated node options.
	nodeUtilTwoNodes   = 65.0
	nodeUtilFourNodes  = 35.0
	nodeUtilEightNodes = 20.0

	// recommendationCreatedAt is the fixed observation time (RFC3339) used for
	// derived recommendations so responses are deterministic across reads.
	recommendationCreatedAt = "2024-01-01T00:00:00Z"
)

// recommendationID returns a stable, opaque recommendation id for a
// (cluster, type) pair so repeated calls yield identical ids (as AWS does).
func recommendationID(clusterID, recType string) string {
	sum := sha256.Sum256([]byte(clusterID + "|" + recType))

	return "rec-" + hex.EncodeToString(sum[:8])
}

// deriveClusterRecommendations synthesizes the Advisor recommendations AWS would
// surface for a cluster given its configuration.
func deriveClusterRecommendations(c *Cluster) []recommendationXML {
	created := recommendationCreatedAt

	var recs []recommendationXML

	// Unencrypted clusters get a security recommendation to enable encryption.
	if !c.Encrypted {
		recs = append(recs, recommendationXML{
			RecommendationID:   recommendationID(c.ClusterIdentifier, recTypeSecurity),
			ClusterIdentifier:  c.ClusterIdentifier,
			Type:               recTypeSecurity,
			Title:              "Enable database encryption",
			Description:        "This cluster is not encrypted at rest.",
			Observation:        "Cluster " + c.ClusterIdentifier + " has encryption disabled.",
			ImpactRanking:      impactHigh,
			RecommendationText: "Enable KMS encryption to protect data at rest.",
			CreatedAt:          created,
			RecommendedActions: []recommendedActionXML{
				{Text: "Modify the cluster to enable encryption.", Type: recActionTypeUpdate},
			},
		})
	}

	// Legacy node families (dc2/ds2) get a cost/RA3 migration recommendation.
	if isLegacyNodeType(c.NodeType) {
		recs = append(recs, recommendationXML{
			RecommendationID:   recommendationID(c.ClusterIdentifier, recTypeCostOptimization),
			ClusterIdentifier:  c.ClusterIdentifier,
			Type:               recTypeCostOptimization,
			Title:              "Migrate to RA3 nodes",
			Description:        "Node type " + c.NodeType + " predates RA3 managed storage.",
			Observation:        "Cluster " + c.ClusterIdentifier + " runs on legacy node type " + c.NodeType + ".",
			ImpactRanking:      impactMedium,
			RecommendationText: "Resize to an RA3 node type to decouple compute and storage.",
			CreatedAt:          created,
			RecommendedActions: []recommendedActionXML{
				{Text: "Resize the cluster to an RA3 node type.", Type: recActionTypeRestore},
			},
		})
	}

	// Single-node clusters get a high-availability recommendation.
	if c.ClusterType == clusterTypeSingleNode || c.NumberOfNodes <= 1 {
		recs = append(recs, recommendationXML{
			RecommendationID:   recommendationID(c.ClusterIdentifier, recTypeHighAvailability),
			ClusterIdentifier:  c.ClusterIdentifier,
			Type:               recTypeHighAvailability,
			Title:              "Use a multi-node cluster",
			Description:        "Single-node clusters have no data redundancy.",
			Observation:        "Cluster " + c.ClusterIdentifier + " is a single-node cluster.",
			ImpactRanking:      impactMedium,
			RecommendationText: "Resize to a multi-node configuration for high availability.",
			CreatedAt:          created,
			RecommendedActions: []recommendedActionXML{
				{Text: "Resize the cluster to multiple nodes.", Type: recActionTypeUpdate},
			},
		})
	}

	// Clusters without a maintenance window get a management recommendation.
	if c.PreferredMaintenanceWindow == "" {
		recs = append(recs, recommendationXML{
			RecommendationID:   recommendationID(c.ClusterIdentifier, recTypeManagement),
			ClusterIdentifier:  c.ClusterIdentifier,
			Type:               recTypeManagement,
			Title:              "Set a preferred maintenance window",
			Description:        "No preferred maintenance window is configured.",
			Observation:        "Cluster " + c.ClusterIdentifier + " has no maintenance window set.",
			ImpactRanking:      impactLow,
			RecommendationText: "Configure a maintenance window to control when updates apply.",
			CreatedAt:          created,
			RecommendedActions: []recommendedActionXML{
				{Text: "Modify the cluster to set a maintenance window.", Type: recActionTypeUpdate},
			},
		})
	}

	return recs
}

// isLegacyNodeType reports whether a node type is a pre-RA3 family.
func isLegacyNodeType(nodeType string) bool {
	return strings.HasPrefix(nodeType, "dc2.") ||
		strings.HasPrefix(nodeType, "dc1.") ||
		strings.HasPrefix(nodeType, "ds2.")
}

// validNodeConfigActionType reports whether the ActionType is one AWS accepts.
func validNodeConfigActionType(actionType string) bool {
	switch actionType {
	case nodeActionRestore, nodeActionRecommend, nodeActionResize:
		return true
	default:
		return false
	}
}

// nodeConfigurationOptions computes valid target node configurations for the
// requested action. Legacy source node types are mapped to their RA3 successor
// for restore/resize so the emulator recommends the modern family, matching how
// AWS surfaces RA3 targets for dc2/ds2 sources.
func nodeConfigurationOptions(actionType, baseNodeType string) []nodeConfigOptionXML {
	target := baseNodeType
	if target == "" || isLegacyNodeType(target) {
		target = "ra3.xlplus"
	}

	mode := ""
	if actionType == nodeActionResize {
		mode = "elastic"
	}

	return []nodeConfigOptionXML{
		{
			NodeType:                        target,
			Mode:                            mode,
			NumberOfNodes:                   minClusterNodes,
			EstimatedDiskUtilizationPercent: nodeUtilTwoNodes,
		},
		{
			NodeType:                        target,
			Mode:                            mode,
			NumberOfNodes:                   nodeCountFour,
			EstimatedDiskUtilizationPercent: nodeUtilFourNodes,
		},
		{
			NodeType:                        target,
			Mode:                            mode,
			NumberOfNodes:                   nodeCountEight,
			EstimatedDiskUtilizationPercent: nodeUtilEightNodes,
		},
	}
}

// nodeConfigFilterValue returns the first requested value for a node-config
// filter. It accepts both the AWS Filter.member.N.Name/Values.member.M encoding
// and a plain query parameter of the same name for convenience.
func nodeConfigFilterValue(vals url.Values, name string) string {
	if v := vals.Get(name); v != "" {
		return v
	}

	for key, filterName := range vals {
		if !strings.HasSuffix(key, ".Name") || len(filterName) == 0 || filterName[0] != name {
			continue
		}

		prefix := strings.TrimSuffix(key, ".Name")
		for vk, vv := range vals {
			if strings.HasPrefix(vk, prefix+".Values.") && len(vv) > 0 {
				return vv[0]
			}
		}
	}

	return ""
}

// nodeConfigFilterInt returns a node-config filter value parsed as an int, or 0.
func nodeConfigFilterInt(vals url.Values, name string) int {
	s := nodeConfigFilterValue(vals, name)
	if s == "" {
		return 0
	}

	n, err := strconv.Atoi(s)
	if err != nil {
		return 0
	}

	return n
}
