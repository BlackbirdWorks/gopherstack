package eks

import "fmt"

// Hard-coded resource caps enforced at the Create*/Register* ops whose real
// error catalog declares ResourceLimitExceededException -- confirmed per-op
// by reading aws-sdk-go-v2/service/eks@v1.98.0/deserializers.go's own
// awsRestjson1_deserializeOpError<Op> switches (grepped every
// awsRestjson1_deserializeOpError<Op> func body for the string
// "ResourceLimitExceededException"): CreateAccessEntry, CreateCapability,
// CreateCluster, CreateEksAnywhereSubscription, CreateFargateProfile,
// CreateNodegroup, CreatePodIdentityAssociation, RegisterCluster. (Two more
// ops declare it too -- CreateCertificateAuthority and
// UpdatePodIdentityAssociation -- but CreateCertificateAuthority and its
// four ActivateCertificateAuthority/DeleteCertificateAuthority/
// DescribeCertificateAuthority/ListCertificateAuthorities siblings are new
// ops in this pinned SDK version this service has never implemented at all
// -- a pre-existing gap discovered during this pass, out of scope for
// gopherstack-wf8f, disclosed in PARITY.md rather than silently added.
// UpdatePodIdentityAssociation modifies an existing association and cannot
// itself push any count over a cardinality quota, so it has nothing to
// enforce despite declaring the exception on the wire.)
//
// Values are the real, published EKS defaults from
// https://docs.aws.amazon.com/general/latest/gr/eks.html#limits_eks
// (WebFetch'd 2026-09-11) "Service quotas" table, plus
// https://docs.aws.amazon.com/eks/latest/userguide/pod-identities.html#pod-id-limits
// (WebFetch'd 2026-09-11, "Up to 5,000 EKS Pod Identity associations per
// cluster") and
// https://docs.aws.amazon.com/eks/latest/userguide/capabilities.html
// (WebFetch'd 2026-09-11, "You can create one capability resource of each
// type ... for a given cluster. You cannot create multiple capability
// resources of the same type on the same cluster.") for capabilities, which
// carries no numeric Service Quotas entry because it is a fixed structural
// rule, not an adjustable per-account cap.
const (
	defaultMaxClustersPerAccount            = 100  // "Clusters"
	defaultMaxRegisteredClustersPerAccount  = 10   // "Registered clusters"
	defaultMaxSecurityGroupsPerCluster      = 4    // "Control plane security groups per cluster" (not adjustable)
	defaultMaxPublicAccessCIDRsPerCluster   = 40   // "Public endpoint access CIDR ranges per cluster" (not adjustable)
	defaultMaxNodegroupsPerCluster          = 30   // "Managed node groups per cluster"
	defaultMaxFargateProfilesPerCluster     = 10   // "Fargate profiles per cluster"
	defaultMaxSelectorsPerFargateProfile    = 5    // "Selectors per Fargate profile"
	defaultMaxLabelsPerFargateSelector      = 5    // "Label pairs per Fargate profile selector"
	defaultMaxAccessEntriesPerCluster       = 3000 // "Access entries per cluster" (not adjustable)
	defaultMaxAnywhereSubscriptionsPerAccnt = 10   // "EKS Anywhere Enterprise Subscriptions"
	defaultMaxPodIdentityAssocsPerCluster   = 5000 // pod-identities.md "Limits"
)

// resourceLimits holds the caps InMemoryBackend enforces, defaulted to the
// real EKS values above (defaultResourceLimits) and overridable via
// WithResourceLimits -- the same constructor-option pattern
// services/efs/limits.go and services/glue/limits.go use -- so tests
// exercising a 100/3,000/5,000-sized cap don't have to create that many real
// resources. The one-capability-per-type-per-cluster rule is not part of
// this struct: it is a fixed structural constant (always 1, not an
// AWS-adjustable quota), enforced directly in capabilities.go.
type resourceLimits struct {
	clustersPerAccount           int
	registeredClustersPerAccount int
	securityGroupsPerCluster     int
	publicAccessCIDRsPerCluster  int
	nodegroupsPerCluster         int
	fargateProfilesPerCluster    int
	selectorsPerFargateProfile   int
	labelsPerFargateSelector     int
	accessEntriesPerCluster      int
	anywhereSubscriptionsPerAcct int
	podIdentityAssocsPerCluster  int
}

func defaultResourceLimits() resourceLimits {
	return resourceLimits{
		clustersPerAccount:           defaultMaxClustersPerAccount,
		registeredClustersPerAccount: defaultMaxRegisteredClustersPerAccount,
		securityGroupsPerCluster:     defaultMaxSecurityGroupsPerCluster,
		publicAccessCIDRsPerCluster:  defaultMaxPublicAccessCIDRsPerCluster,
		nodegroupsPerCluster:         defaultMaxNodegroupsPerCluster,
		fargateProfilesPerCluster:    defaultMaxFargateProfilesPerCluster,
		selectorsPerFargateProfile:   defaultMaxSelectorsPerFargateProfile,
		labelsPerFargateSelector:     defaultMaxLabelsPerFargateSelector,
		accessEntriesPerCluster:      defaultMaxAccessEntriesPerCluster,
		anywhereSubscriptionsPerAcct: defaultMaxAnywhereSubscriptionsPerAccnt,
		podIdentityAssocsPerCluster:  defaultMaxPodIdentityAssocsPerCluster,
	}
}

// ResourceLimits overrides the resource caps a *InMemoryBackend enforces
// with ResourceLimitExceededException. A zero field keeps its real-EKS
// default -- used by tests that need to trip a large cap without actually
// creating that many resources.
type ResourceLimits struct {
	ClustersPerAccount           int
	RegisteredClustersPerAccount int
	SecurityGroupsPerCluster     int
	PublicAccessCIDRsPerCluster  int
	NodegroupsPerCluster         int
	FargateProfilesPerCluster    int
	SelectorsPerFargateProfile   int
	LabelsPerFargateSelector     int
	AccessEntriesPerCluster      int
	AnywhereSubscriptionsPerAcct int
	PodIdentityAssocsPerCluster  int
}

func applyResourceLimitOverrides(rl *resourceLimits, l ResourceLimits) {
	if l.ClustersPerAccount > 0 {
		rl.clustersPerAccount = l.ClustersPerAccount
	}

	if l.RegisteredClustersPerAccount > 0 {
		rl.registeredClustersPerAccount = l.RegisteredClustersPerAccount
	}

	if l.SecurityGroupsPerCluster > 0 {
		rl.securityGroupsPerCluster = l.SecurityGroupsPerCluster
	}

	if l.PublicAccessCIDRsPerCluster > 0 {
		rl.publicAccessCIDRsPerCluster = l.PublicAccessCIDRsPerCluster
	}

	if l.NodegroupsPerCluster > 0 {
		rl.nodegroupsPerCluster = l.NodegroupsPerCluster
	}

	if l.FargateProfilesPerCluster > 0 {
		rl.fargateProfilesPerCluster = l.FargateProfilesPerCluster
	}

	if l.SelectorsPerFargateProfile > 0 {
		rl.selectorsPerFargateProfile = l.SelectorsPerFargateProfile
	}

	if l.LabelsPerFargateSelector > 0 {
		rl.labelsPerFargateSelector = l.LabelsPerFargateSelector
	}

	if l.AccessEntriesPerCluster > 0 {
		rl.accessEntriesPerCluster = l.AccessEntriesPerCluster
	}

	if l.AnywhereSubscriptionsPerAcct > 0 {
		rl.anywhereSubscriptionsPerAcct = l.AnywhereSubscriptionsPerAcct
	}

	if l.PodIdentityAssocsPerCluster > 0 {
		rl.podIdentityAssocsPerCluster = l.PodIdentityAssocsPerCluster
	}
}

func resourceLimitExceededErr(resource string, limit int) error {
	return fmt.Errorf("%w: reached the maximum of %d %s", ErrResourceLimitExceeded, limit, resource)
}

// countClusters returns the number of non-connected (real EKS-provisioned,
// ConnectorConfig == nil) or connected (RegisterCluster'd,
// ConnectorConfig != nil) clusters, matching ListClusters' own
// includeExternal distinction (clusters.go) -- the "Clusters: 100" and
// "Registered clusters: 10" quotas are separate counters in the real
// service quotas table, not one shared number.
func (b *InMemoryBackend) countClusters(connected bool) int {
	n := 0

	for _, c := range b.clusters.All() {
		if (c.ConnectorConfig != nil) == connected {
			n++
		}
	}

	return n
}
