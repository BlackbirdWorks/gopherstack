package quicksight

import (
	"maps"
	"slices"
)

// ---- Tags ----

// TagResource attaches tags to resourceARN. Real AWS returns
// ResourceNotFoundException when the ARN doesn't identify an existing
// taggable resource, so this backend checks arnExists before writing --
// callers can't tag a resource into existing just by tagging it.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if !b.arnExists(resourceARN) {
		return ErrTaggableResourceNotFound
	}

	if b.tags[resourceARN] == nil {
		b.tags[resourceARN] = make(map[string]string)
	}
	maps.Copy(b.tags[resourceARN], tags)

	return nil
}

// UntagResource removes tags from resourceARN. See TagResource for the
// existence-check rationale.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if !b.arnExists(resourceARN) {
		return ErrTaggableResourceNotFound
	}

	if b.tags[resourceARN] == nil {
		return nil
	}
	for _, k := range tagKeys {
		delete(b.tags[resourceARN], k)
	}

	return nil
}

// ListTagsForResource lists the tags on resourceARN. See TagResource for the
// existence-check rationale.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	if !b.arnExists(resourceARN) {
		return nil, ErrTaggableResourceNotFound
	}

	result := make(map[string]string)
	if t := b.tags[resourceARN]; t != nil {
		maps.Copy(result, t)
	}

	return result, nil
}

// arnCollectorFuncs lists, one closure per taggable resource family, how to
// gather every currently-live ARN of that family. Data-driven (rather than
// one hand-written loop per family inlined into arnExists) so arnExists
// itself stays trivially simple as new taggable families are added --
// mirrors the tableRegistrations pattern in store_setup.go. Only resource
// types that are independently taggable in real AWS QuickSight (i.e. carry
// their own Arn, not just a reference to another resource's ARN) are
// included: namespace, group, user, data source, dataset, dashboard,
// analysis, folder, template, theme, topic, VPC connection, brand, custom
// permissions profile, OAuth client application, asset bundle export/import
// job, dashboard snapshot job, action connector, automation job, flow,
// agent, knowledge base, space.
//
//nolint:gochecknoglobals // registration table, analogous to tableRegistrations/errCodeLookup elsewhere
var arnCollectorFuncs = []func(*InMemoryBackend) []string{
	func(b *InMemoryBackend) []string {
		return arnsOf(b.namespaces.All(), func(v *storedNamespace) string { return v.Arn })
	},
	func(b *InMemoryBackend) []string {
		return arnsOf(b.groups.All(), func(v *storedGroup) string { return v.Arn })
	},
	func(b *InMemoryBackend) []string {
		return arnsOf(b.users.All(), func(v *storedUser) string { return v.Arn })
	},
	func(b *InMemoryBackend) []string {
		return arnsOf(b.dataSources.All(), func(v *storedDataSource) string { return v.Arn })
	},
	func(b *InMemoryBackend) []string {
		return arnsOf(b.dataSets.All(), func(v *storedDataSet) string { return v.Arn })
	},
	func(b *InMemoryBackend) []string {
		return arnsOf(b.dashboards.All(), func(v *storedDashboard) string { return v.Arn })
	},
	func(b *InMemoryBackend) []string {
		return arnsOf(b.analyses.All(), func(v *storedAnalysis) string { return v.Arn })
	},
	func(b *InMemoryBackend) []string {
		return arnsOf(b.folders.All(), func(v *storedFolder) string { return v.Arn })
	},
	func(b *InMemoryBackend) []string {
		return arnsOf(b.templates.All(), func(v *storedTemplate) string { return v.Arn })
	},
	func(b *InMemoryBackend) []string {
		return arnsOf(b.themes.All(), func(v *storedTheme) string { return v.Arn })
	},
	func(b *InMemoryBackend) []string {
		return arnsOf(b.topics.All(), func(v *storedTopic) string { return v.Arn })
	},
	func(b *InMemoryBackend) []string {
		return arnsOf(b.vpcConnections.All(), func(v *storedVPCConnection) string { return v.Arn })
	},
	func(b *InMemoryBackend) []string {
		return arnsOf(b.brands.All(), func(v *storedBrand) string { return v.Arn })
	},
	func(b *InMemoryBackend) []string {
		return arnsOf(b.customPermissions.All(), func(v *storedCustomPermissions) string { return v.Arn })
	},
	func(b *InMemoryBackend) []string {
		return arnsOf(b.oauthClientApps.All(), func(v *storedOAuthApp) string { return v.Arn })
	},
	func(b *InMemoryBackend) []string {
		return arnsOf(b.assetBundleExportJobs.All(), func(v *storedAssetBundleExportJob) string { return v.Arn })
	},
	func(b *InMemoryBackend) []string {
		return arnsOf(b.assetBundleImportJobs.All(), func(v *storedAssetBundleImportJob) string { return v.Arn })
	},
	func(b *InMemoryBackend) []string {
		return arnsOf(b.dashboardSnapshotJobs.All(), func(v *storedDashboardSnapshotJob) string { return v.Arn })
	},
	func(b *InMemoryBackend) []string {
		return arnsOf(b.actionConnectors.All(), func(v *storedActionConnector) string { return v.Arn })
	},
	func(b *InMemoryBackend) []string {
		return arnsOf(b.automationJobs.All(), func(v *storedAutomationJob) string { return v.Arn })
	},
	func(b *InMemoryBackend) []string {
		return arnsOf(b.flows.All(), func(v *storedFlow) string { return v.Arn })
	},
	func(b *InMemoryBackend) []string {
		return arnsOf(b.agents.All(), func(v *storedAgent) string { return v.Arn })
	},
	func(b *InMemoryBackend) []string {
		return arnsOf(b.knowledgeBases.All(), func(v *storedKnowledgeBase) string { return v.Arn })
	},
	func(b *InMemoryBackend) []string {
		return arnsOf(b.spaces.All(), func(v *storedSpace) string { return v.Arn })
	},
}

// arnsOf projects a slice of stored values down to their Arn field via get,
// so arnCollectorFuncs can stay a flat one-liner per resource family.
func arnsOf[T any](all []*T, get func(*T) string) []string {
	out := make([]string, len(all))
	for i, v := range all {
		out[i] = get(v)
	}

	return out
}

// arnExists reports whether target is the ARN of a resource this backend
// currently holds, across every independently-taggable resource family (see
// arnCollectorFuncs). Caller must hold b.mu (read or write lock).
func (b *InMemoryBackend) arnExists(target string) bool {
	if target == "" {
		return false
	}

	for _, collect := range arnCollectorFuncs {
		if slices.Contains(collect(b), target) {
			return true
		}
	}

	return false
}
