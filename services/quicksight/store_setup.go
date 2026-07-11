package quicksight

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T resource field on InMemoryBackend that has a pure per-value
// identity is registered exactly once, here, as a *store.Table[T] on
// b.registry. See pkgs/store's package doc and the services/ec2 (commit
// 12e611a4), services/sqs (commit 0f09d77c), and services/ssm (commit
// 4c686770) conversions this follows.
//
// QuickSight's InMemoryBackend, like ec2/sqs, holds exactly one (accountID,
// region) pair per instance (see provider.go: one backend is constructed per
// service.AccountRegionOrDefault call), so the flat, statically-registered
// pattern applies -- not ssm's per-region lazy registration.
//
// # Composite keys
//
// Several stored types are keyed externally by accountID+"/"+<id...> (e.g.
// nsKey, groupKey, folderKey) even though accountID is not itself a field on
// the stored struct. Because every value ever placed in one of these tables
// was created by this same backend instance, accountID is always b.accountID
// (see provider.go), so each keyFn below is a closure over b that reconstructs
// the exact same key string the old map used, from b.accountID plus the
// value's own identity field(s).
//
// # What is NOT converted here, and why
//
// The following resource fields are deliberately left as plain maps, matching
// the ec2/ssm precedent of leaving non-identity-bearing collections raw:
//   - accountSettings, accountSubscriptions, ipRestrictions,
//     defaultQBusinessApps: value type carries no identity field of its own
//     (no AccountID field); each is a single per-account slot keyed externally
//     by accountID alone -- same rationale as ec2's instanceIMDSOptions
//     exclusion.
//   - groupMembers, publicSharing, roleMemberships: map[string]bool, a bare
//     membership flag with no identity.
//   - tags: map[string]map[string]string, value is itself a map with no
//     identity field.
//   - accountCustomPermissions, qPersonalization, qSearchConfig,
//     dashboardsQAConfig, brandAssignments, roleCustomPermissions,
//     userCustomPermissions, spiceCapacity, selfUpgradeConfig:
//     map[string]string, a bare scalar with no identity.
//   - keyRegistrations: map[string][]storedRegisteredKey, one-to-many
//     (slice-valued) -- there is no single V to key a Table by, same as ec2's
//     spotFleetHistory pattern.
import (
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// registerAllTables registers every convertible resource collection on
// b.registry, assigning each *store.Table[T] to its InMemoryBackend field.
// Called once from NewInMemoryBackend.
func registerAllTables(b *InMemoryBackend) {
	for _, register := range tableRegistrations {
		register(b)
	}
}

// tableRegistrations is the data-driven list registerAllTables walks: one
// closure per resource table, each binding its own store.New/store.Register
// call to the concrete field and value type. A closure list (rather than one
// statement per field in a single function body) keeps registerAllTables
// small regardless of how many resource tables the backend grows to.
//
//nolint:gochecknoglobals // registration table, analogous to errCodeLookup-style lookup tables elsewhere
var tableRegistrations = []func(*InMemoryBackend){
	func(b *InMemoryBackend) {
		b.namespaces = store.Register(b.registry, "namespaces", store.New(func(v *storedNamespace) string {
			return nsKey(b.accountID, v.Name)
		}))
	},
	func(b *InMemoryBackend) {
		b.groups = store.Register(b.registry, "groups", store.New(func(v *storedGroup) string {
			return groupKey(b.accountID, v.Namespace, v.GroupName)
		}))
	},
	func(b *InMemoryBackend) {
		b.users = store.Register(b.registry, "users", store.New(func(v *storedUser) string {
			return userKey(b.accountID, v.Namespace, v.UserName)
		}))
	},
	func(b *InMemoryBackend) {
		b.dataSources = store.Register(b.registry, "dataSources", store.New(func(v *storedDataSource) string {
			return dataSourceKey(b.accountID, v.DataSourceID)
		}))
	},
	func(b *InMemoryBackend) {
		b.dataSets = store.Register(b.registry, "dataSets", store.New(func(v *storedDataSet) string {
			return dataSetKey(b.accountID, v.DataSetID)
		}))
	},
	func(b *InMemoryBackend) {
		b.ingestions = store.Register(b.registry, "ingestions", store.New(func(v *storedIngestion) string {
			return ingestionKey(b.accountID, v.DataSetID, v.IngestionID)
		}))
	},
	func(b *InMemoryBackend) {
		b.dashboards = store.Register(b.registry, "dashboards", store.New(func(v *storedDashboard) string {
			return dashboardKey(b.accountID, v.DashboardID)
		}))
	},
	func(b *InMemoryBackend) {
		b.analyses = store.Register(b.registry, "analyses", store.New(func(v *storedAnalysis) string {
			return analysisKey(b.accountID, v.AnalysisID)
		}))
	},
	func(b *InMemoryBackend) {
		b.folders = store.Register(b.registry, "folders", store.New(func(v *storedFolder) string {
			return folderKey(b.accountID, v.FolderID)
		}))
	},
	func(b *InMemoryBackend) {
		b.folderMembers = store.Register(b.registry, "folderMembers", store.New(func(v *storedFolderMember) string {
			return folderMemberKey(b.accountID, v.FolderID, v.MemberType, v.MemberID)
		}))
	},
	func(b *InMemoryBackend) {
		b.templates = store.Register(b.registry, "templates", store.New(func(v *storedTemplate) string {
			return templateKey(b.accountID, v.TemplateID)
		}))
	},
	func(b *InMemoryBackend) {
		b.themes = store.Register(b.registry, "themes", store.New(func(v *storedTheme) string {
			return themeKey(b.accountID, v.ThemeID)
		}))
	},
	func(b *InMemoryBackend) {
		b.topics = store.Register(b.registry, "topics", store.New(func(v *storedTopic) string {
			return topicKey(b.accountID, v.TopicID)
		}))
	},
	func(b *InMemoryBackend) {
		b.vpcConnections = store.Register(b.registry, "vpcConnections", store.New(func(v *storedVPCConnection) string {
			return vpcConnectionKey(b.accountID, v.VPCConnectionID)
		}))
	},
	func(b *InMemoryBackend) {
		b.iamPolicyAssignments = store.Register(
			b.registry,
			"iamPolicyAssignments",
			store.New(func(v *storedIAMPolicyAssignment) string {
				return iamPolicyAssignmentKey(b.accountID, v.Namespace, v.AssignmentName)
			}),
		)
	},
	func(b *InMemoryBackend) {
		b.accountCustomizations = store.Register(
			b.registry,
			"accountCustomizations",
			store.New(func(v *storedAccountCustomization) string {
				return accountCustomizationKey(b.accountID, v.Namespace)
			}),
		)
	},
	func(b *InMemoryBackend) {
		b.brands = store.Register(b.registry, "brands", store.New(func(v *storedBrand) string {
			return brandKey(b.accountID, v.BrandID)
		}))
	},
	func(b *InMemoryBackend) {
		b.customPermissions = store.Register(
			b.registry,
			"customPermissions",
			store.New(func(v *storedCustomPermissions) string {
				return customPermissionsKey(b.accountID, v.Name)
			}),
		)
	},
	func(b *InMemoryBackend) {
		b.oauthClientApps = store.Register(b.registry, "oauthClientApps", store.New(func(v *storedOAuthApp) string {
			return oauthAppKey(b.accountID, v.ClientID)
		}))
	},
	func(b *InMemoryBackend) {
		b.identityPropagationConfigs = store.Register(
			b.registry,
			"identityPropagationConfigs",
			store.New(func(v *storedIdentityPropagationConfig) string {
				return identityPropagationKey(b.accountID, v.Service)
			}),
		)
	},
	func(b *InMemoryBackend) {
		b.assetBundleExportJobs = store.Register(
			b.registry,
			"assetBundleExportJobs",
			store.New(func(v *storedAssetBundleExportJob) string {
				return assetBundleJobKey(b.accountID, v.JobID)
			}),
		)
	},
	func(b *InMemoryBackend) {
		b.assetBundleImportJobs = store.Register(
			b.registry,
			"assetBundleImportJobs",
			store.New(func(v *storedAssetBundleImportJob) string {
				return assetBundleJobKey(b.accountID, v.JobID)
			}),
		)
	},
	func(b *InMemoryBackend) {
		b.dashboardSnapshotJobs = store.Register(
			b.registry,
			"dashboardSnapshotJobs",
			store.New(func(v *storedDashboardSnapshotJob) string {
				return dashboardSnapshotJobKey(b.accountID, v.DashboardID, v.JobID)
			}),
		)
	},
	func(b *InMemoryBackend) {
		b.actionConnectors = store.Register(
			b.registry,
			"actionConnectors",
			store.New(func(v *storedActionConnector) string {
				return actionConnectorKey(b.accountID, v.ActionConnectorID)
			}),
		)
	},
	func(b *InMemoryBackend) {
		b.automationJobs = store.Register(b.registry, "automationJobs", store.New(func(v *storedAutomationJob) string {
			return automationJobKey(b.accountID, v.AutomationGroupID, v.AutomationID, v.JobID)
		}))
	},
	func(b *InMemoryBackend) {
		b.flows = store.Register(b.registry, "flows", store.New(func(v *storedFlow) string {
			return flowKey(b.accountID, v.FlowID)
		}))
	},
	func(b *InMemoryBackend) {
		b.selfUpgradeRequests = store.Register(
			b.registry,
			"selfUpgradeRequests",
			store.New(func(v *storedSelfUpgradeRequest) string {
				return selfUpgradeRequestKey(b.accountID, v.Namespace, v.UpgradeRequestID)
			}),
		)
	},
}
