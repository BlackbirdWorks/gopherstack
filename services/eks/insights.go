package eks

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// gopherstack-wf8f item 2: honest insight derivation.
//
// Real EKS insights (types.Insight/InsightSummary, verified against
// aws-sdk-go-v2/service/eks@v1.98.0's types.go and deserializers.go) are
// computed by EKS from live analysis of the cluster's Kubernetes API server
// -- categories UPGRADE_READINESS (deprecated-API usage, add-on version
// compatibility, control-plane-vs-node-group version skew) and
// MISCONFIGURATION (EKS Hybrid Nodes networking problems). This backend has
// no Kubernetes API server to analyze and no hybrid-nodes model, so neither
// category's real checks can be run.
//
// What this backend DOES have, honestly: each cluster's real Version field
// and the static supported-Kubernetes-version table DescribeClusterVersions
// already exposes (clusters.go's clusterVersionSupportTable). Two
// UPGRADE_READINESS-shaped insights are derivable from exactly that data
// without inventing anything:
//   - version-support: is the cluster's Kubernetes version still within
//     (or nearing/past) its standard support window?
//   - version-behind-latest: how many minor versions behind this backend's
//     newest supported version is the cluster?
//
// A cluster whose Version isn't in the static table (any version string a
// caller supplied that isn't one of the ones this backend tracks support
// dates for) gets neither insight -- there is nothing honest to compute,
// so nothing is fabricated in its place. Everything else the real API
// models and this pass does NOT provide is disclosed in PARITY.md's gaps:
// deprecated-API-usage insights, AddonCompatibilityDetails,
// CategorySpecificSummary.DeprecationDetails, Resources[]/InsightResourceDetail,
// and the entire MISCONFIGURATION category.

const (
	insightStatusWarning = "WARNING"
	insightStatusError   = "ERROR"

	// eolWarningWindow is how far ahead of a version's real published
	// end-of-standard-support date this backend starts surfacing WARNING
	// instead of PASSING. Not itself an AWS-published number (AWS does not
	// document the exact lead time its own insight uses) -- a reasonable,
	// disclosed backend policy, not a fabricated AWS fact.
	eolWarningWindow = 90 * 24 * time.Hour
)

// kubernetesVersionParts is "major.minor" -- Kubernetes version strings
// never carry a patch component here (verified against every entry in
// clusterVersionSupportTable).
const kubernetesVersionParts = 2

// minorVersion parses a "1.NN"-shaped Kubernetes version string's minor
// component. ok is false for anything else (a caller-supplied Version this
// backend cannot honestly compare).
func minorVersion(v string) (int, bool) {
	parts := strings.SplitN(v, ".", kubernetesVersionParts)
	if len(parts) < kubernetesVersionParts {
		return 0, false
	}

	n, err := strconv.Atoi(parts[1])
	if err != nil {
		return 0, false
	}

	return n, true
}

// versionSupportInsight checks whether version is still within its real
// published standard-support window (clusterVersionSupportTable).
func versionSupportInsight(clusterName, version string, sup clusterVersionSupport, now time.Time) *Insight {
	remaining := sup.EndOfStandardSupport.Sub(now)
	supportDate := sup.EndOfStandardSupport.Format(time.DateOnly)

	var status, reason, recommendation string

	switch {
	case remaining <= 0:
		status = insightStatusError
		reason = fmt.Sprintf("Kubernetes %s reached the end of standard support on %s.", version, supportDate)
		recommendation = "Upgrade the cluster to a Kubernetes version that is still within standard support."
	case remaining <= eolWarningWindow:
		status = insightStatusWarning
		reason = fmt.Sprintf("Kubernetes %s reaches the end of standard support on %s.", version, supportDate)
		recommendation = "Plan a cluster upgrade before standard support for this version ends."
	default:
		status = statusPassing
		reason = fmt.Sprintf("Kubernetes %s is within standard support until %s.", version, supportDate)
	}

	return &Insight{
		ID:                stableID(clusterName + "/upgrade-readiness/version-support"),
		ClusterName:       clusterName,
		Category:          typeUpgradeReadiness,
		Name:              "Kubernetes version end of standard support",
		KubernetesVersion: version,
		Status:            status,
		StatusReason:      reason,
		Description:       "Checks whether the cluster's Kubernetes version is within standard support.",
		Recommendation:    recommendation,
		LastRefreshTime:   now,
		LastTransition:    now,
	}
}

// versionBehindLatestInsight compares version against this backend's newest
// supported version (clusterVersionSupportTable's Default-flagged row).
func versionBehindLatestInsight(clusterName, version, latest string, now time.Time) *Insight {
	cur, curOK := minorVersion(version)
	lat, latOK := minorVersion(latest)

	if !curOK || !latOK {
		return nil
	}

	behind := lat - cur

	status := statusPassing
	reason := fmt.Sprintf("Kubernetes %s is this backend's latest supported version.", version)
	recommendation := ""

	if behind > 0 {
		status = insightStatusWarning
		reason = fmt.Sprintf("Kubernetes %s is %d minor version(s) behind the latest supported version %s.",
			version, behind, latest)
		recommendation = fmt.Sprintf("Upgrade the cluster toward Kubernetes %s.", latest)
	}

	return &Insight{
		ID:                stableID(clusterName + "/upgrade-readiness/version-behind-latest"),
		ClusterName:       clusterName,
		Category:          typeUpgradeReadiness,
		Name:              "Cluster Kubernetes version behind latest supported version",
		KubernetesVersion: version,
		Status:            status,
		StatusReason:      reason,
		Description:       "Checks how many Kubernetes minor versions behind the latest supported version the cluster is.",
		Recommendation:    recommendation,
		LastRefreshTime:   now,
		LastTransition:    now,
	}
}

// deriveUpgradeReadinessInsights returns every UPGRADE_READINESS insight
// honestly derivable for a cluster from its real Version plus the static
// supported-version table. Returns an empty slice (never fabricated
// content) when version isn't a table entry this backend has support dates
// for.
func deriveUpgradeReadinessInsights(clusterName, version string, now time.Time) []*Insight {
	sup, ok := clusterVersionSupportFor(version)
	if !ok {
		return nil
	}

	insights := []*Insight{versionSupportInsight(clusterName, version, sup, now)}

	if behind := versionBehindLatestInsight(clusterName, version, latestSupportedClusterVersion(), now); behind != nil {
		insights = append(insights, behind)
	}

	return insights
}

// DescribeInsight returns one derived insight for a cluster by ID.
func (b *InMemoryBackend) DescribeInsight(clusterName, insightID string) (*Insight, error) {
	b.mu.RLock("DescribeInsight")
	defer b.mu.RUnlock()

	c, ok := b.clusters.Get(clusterName)
	if !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	for _, ins := range deriveUpgradeReadinessInsights(clusterName, c.Version, time.Now().UTC()) {
		if ins.ID == insightID {
			return ins, nil
		}
	}

	return nil, fmt.Errorf("%w: insight %s not found in cluster %s", ErrNotFound, insightID, clusterName)
}

// ListInsights returns every insight honestly derivable for a cluster.
func (b *InMemoryBackend) ListInsights(clusterName string) ([]*Insight, error) {
	b.mu.RLock("ListInsights")
	defer b.mu.RUnlock()

	c, ok := b.clusters.Get(clusterName)
	if !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	return deriveUpgradeReadinessInsights(clusterName, c.Version, time.Now().UTC()), nil
}

// StartInsightsRefresh starts the (cluster-level singleton) insights refresh
// operation for a cluster.
func (b *InMemoryBackend) StartInsightsRefresh(clusterName string) (*InsightsRefresh, error) {
	b.mu.RLock("StartInsightsRefresh")
	defer b.mu.RUnlock()

	if _, ok := b.clusters.Get(clusterName); !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	now := time.Now().UTC()

	return &InsightsRefresh{
		ClusterName: clusterName,
		Status:      "COMPLETED",
		Message:     "Insights refresh completed successfully",
		StartedAt:   now,
		EndedAt:     now,
	}, nil
}

// DescribeInsightsRefresh returns the status of the (cluster-level singleton)
// insights refresh operation for a cluster.
func (b *InMemoryBackend) DescribeInsightsRefresh(clusterName string) (*InsightsRefresh, error) {
	b.mu.RLock("DescribeInsightsRefresh")
	defer b.mu.RUnlock()

	if _, ok := b.clusters.Get(clusterName); !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	now := time.Now().UTC()

	return &InsightsRefresh{
		ClusterName: clusterName,
		Status:      "COMPLETED",
		Message:     "Insights refresh completed successfully",
		StartedAt:   now,
		EndedAt:     now,
	}, nil
}
