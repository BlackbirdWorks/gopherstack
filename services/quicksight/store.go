package quicksight

import (
	"context"
	"encoding/base64"
	"fmt"
	"strconv"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// quicksightSnapshotVersion identifies the shape of backendSnapshot's Tables
// blob (i.e. the set/shape of resources registered on b.registry -- see
// registerAllTables in store_setup.go). It must be bumped whenever a change
// there would make an older snapshot unsafe to decode as the current shape.
// Restore compares this against the persisted value and discards (rather
// than attempts to partially decode) any mismatch -- see Restore below. This
// mirrors the services/ec2 (commit 12e611a4) and services/sqs (commit
// 0f09d77c) conversions.
const quicksightSnapshotVersion = 1

// encodePageToken encodes an integer offset as an opaque base64 token.
func encodePageToken(offset int) string {
	return base64.StdEncoding.EncodeToString([]byte(strconv.Itoa(offset)))
}

// decodePageToken decodes an opaque base64 page token back to an integer
// offset. A token decoding to a negative offset is rejected like any other
// malformed token: every caller only clamps start against the upper bound
// (`if start > len(all) { start = len(all) }`) before slicing all[start:end],
// so a negative offset would otherwise reach the slice and panic.
func decodePageToken(tok string) (int, error) {
	b, err := base64.StdEncoding.DecodeString(tok)
	if err != nil {
		return 0, err
	}

	n, err := strconv.Atoi(string(b))
	if err != nil {
		return 0, err
	}

	if n < 0 {
		return 0, ErrValidation
	}

	return n, nil
}

const (
	defaultNamespace         = "default"
	identityStoreQuickSight  = "QUICKSIGHT"
	statusCreationSuccessful = "CREATION_SUCCESSFUL"
	statusCreationInProgress = "CREATION_IN_PROGRESS"
	statusUpdateSuccessful   = "UPDATE_SUCCESSFUL"
	statusDeleted            = "DELETED"
	statusRunning            = "RUNNING"
	statusCompleted          = "COMPLETED"
	statusCancelled          = "CANCELLED"
	statusFailed             = "FAILED"

	defaultMaxResults = 100
)

// InMemoryBackend is the in-memory implementation of StorageBackend.
type InMemoryBackend struct {
	mu       *lockmetrics.RWMutex
	registry *store.Registry

	namespaces           *store.Table[storedNamespace]
	groups               *store.Table[storedGroup]
	groupMembers         map[string]bool
	users                *store.Table[storedUser]
	dataSources          *store.Table[storedDataSource]
	dataSets             *store.Table[storedDataSet]
	ingestions           *store.Table[storedIngestion]
	dashboards           *store.Table[storedDashboard]
	analyses             *store.Table[storedAnalysis]
	tags                 map[string]map[string]string
	folders              *store.Table[storedFolder]
	folderMembers        *store.Table[storedFolderMember]
	templates            *store.Table[storedTemplate]
	themes               *store.Table[storedTheme]
	topics               *store.Table[storedTopic]
	vpcConnections       *store.Table[storedVPCConnection]
	iamPolicyAssignments *store.Table[storedIAMPolicyAssignment]

	accountSettings          map[string]*storedAccountSettings
	accountSubscriptions     map[string]*storedAccountSubscription
	accountCustomizations    *store.Table[storedAccountCustomization]
	accountCustomPermissions map[string]string
	ipRestrictions           map[string]*storedIPRestriction
	publicSharing            map[string]bool
	keyRegistrations         map[string][]storedRegisteredKey
	defaultQBusinessApps     map[string]*storedDefaultQBusinessApplication
	qPersonalization         map[string]string
	qSearchConfig            map[string]string
	dashboardsQAConfig       map[string]string

	brands                     *store.Table[storedBrand]
	brandAssignments           map[string]string
	customPermissions          *store.Table[storedCustomPermissions]
	roleCustomPermissions      map[string]string
	roleMemberships            map[string]bool
	userCustomPermissions      map[string]string
	oauthClientApps            *store.Table[storedOAuthApp]
	identityPropagationConfigs *store.Table[storedIdentityPropagationConfig]
	assetBundleExportJobs      *store.Table[storedAssetBundleExportJob]
	assetBundleImportJobs      *store.Table[storedAssetBundleImportJob]
	dashboardSnapshotJobs      *store.Table[storedDashboardSnapshotJob]

	actionConnectors *store.Table[storedActionConnector]
	automationJobs   *store.Table[storedAutomationJob]
	flows            *store.Table[storedFlow]
	spiceCapacity    map[string]string

	agents         *store.Table[storedAgent]
	knowledgeBases *store.Table[storedKnowledgeBase]
	spaces         *store.Table[storedSpace]

	selfUpgradeConfig   map[string]string
	selfUpgradeRequests *store.Table[storedSelfUpgradeRequest]

	accountID string
	region    string
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		accountID: accountID,
		region:    region,
		registry:  store.NewRegistry(),

		groupMembers: make(map[string]bool),
		tags:         make(map[string]map[string]string),

		accountSettings:      make(map[string]*storedAccountSettings),
		accountSubscriptions: make(map[string]*storedAccountSubscription),
		ipRestrictions:       make(map[string]*storedIPRestriction),
		publicSharing:        make(map[string]bool),
		keyRegistrations:     make(map[string][]storedRegisteredKey),
		defaultQBusinessApps: make(map[string]*storedDefaultQBusinessApplication),
		qPersonalization:     make(map[string]string),
		qSearchConfig:        make(map[string]string),
		dashboardsQAConfig:   make(map[string]string),

		brandAssignments:      make(map[string]string),
		roleCustomPermissions: make(map[string]string),
		roleMemberships:       make(map[string]bool),
		userCustomPermissions: make(map[string]string),

		spiceCapacity: make(map[string]string),

		selfUpgradeConfig: make(map[string]string),
	}
	b.mu = lockmetrics.New("quicksight")
	registerAllTables(b)

	// Pre-create the default namespace so basic operations work without explicit setup.
	b.namespaces.Put(&storedNamespace{
		Name:           defaultNamespace,
		Arn:            b.buildARN("namespace", defaultNamespace),
		CapacityRegion: region,
		Status:         statusCreationSuccessful,
		IdentityStore:  identityStoreQuickSight,
	})

	return b
}

// AccountID returns the configured account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Region returns the configured region.
func (b *InMemoryBackend) Region() string { return b.region }

// Reset clears all stored state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()

	b.groupMembers = make(map[string]bool)
	b.tags = make(map[string]map[string]string)

	b.accountSettings = make(map[string]*storedAccountSettings)
	b.accountSubscriptions = make(map[string]*storedAccountSubscription)
	b.accountCustomPermissions = make(map[string]string)
	b.ipRestrictions = make(map[string]*storedIPRestriction)
	b.publicSharing = make(map[string]bool)
	b.keyRegistrations = make(map[string][]storedRegisteredKey)
	b.defaultQBusinessApps = make(map[string]*storedDefaultQBusinessApplication)
	b.qPersonalization = make(map[string]string)
	b.qSearchConfig = make(map[string]string)
	b.dashboardsQAConfig = make(map[string]string)

	b.brandAssignments = make(map[string]string)
	b.roleCustomPermissions = make(map[string]string)
	b.roleMemberships = make(map[string]bool)
	b.userCustomPermissions = make(map[string]string)

	b.spiceCapacity = make(map[string]string)

	b.selfUpgradeConfig = make(map[string]string)

	b.namespaces.Put(&storedNamespace{
		Name:           defaultNamespace,
		Arn:            b.buildARN("namespace", defaultNamespace),
		CapacityRegion: b.region,
		Status:         statusCreationSuccessful,
		IdentityStore:  identityStoreQuickSight,
	})
}

// Snapshot serializes backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		// The registered tables are plain JSON-friendly structs, so a marshal
		// failure here would indicate a programming error rather than bad
		// input data. Log and skip the snapshot rather than panic, matching
		// the persistence.Persistable contract (nil is skipped by the Manager).
		logger.Load(ctx).WarnContext(ctx, "quicksight: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version: quicksightSnapshotVersion,
		Tables:  tables,

		GroupMembers: b.groupMembers,
		Tags:         b.tags,

		AccountSettings:          b.accountSettings,
		AccountSubscriptions:     b.accountSubscriptions,
		AccountCustomPermissions: b.accountCustomPermissions,
		IPRestrictions:           b.ipRestrictions,
		PublicSharing:            b.publicSharing,
		KeyRegistrations:         b.keyRegistrations,
		DefaultQBusinessApps:     b.defaultQBusinessApps,
		QPersonalization:         b.qPersonalization,
		QSearchConfig:            b.qSearchConfig,
		DashboardsQAConfig:       b.dashboardsQAConfig,

		BrandAssignments:      b.brandAssignments,
		RoleCustomPermissions: b.roleCustomPermissions,
		RoleMemberships:       b.roleMemberships,
		UserCustomPermissions: b.userCustomPermissions,

		SPICECapacity:     b.spiceCapacity,
		SelfUpgradeConfig: b.selfUpgradeConfig,
	}

	return persistence.MarshalSnapshot(ctx, "quicksight", snap)
}

// Restore deserializes backend state from JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot
	if err := persistence.UnmarshalSnapshot(ctx, "quicksight", data, &snap); err != nil {
		return fmt.Errorf("quicksight: restore: %w", err)
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != quicksightSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption. Mirrors the services/ec2 (12e611a4) and services/sqs
		// (0f09d77c) conversions.
		logger.Load(ctx).WarnContext(ctx,
			"quicksight: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", quicksightSnapshotVersion)

		b.registry.ResetAll()
		b.resetRawMaps()

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("quicksight: restore snapshot tables: %w", err)
	}

	b.restoreRawMaps(&snap)

	return nil
}

// resetRawMaps re-initializes every raw (non-store.Table) map to empty. Used
// when Restore discards an incompatible snapshot version.
func (b *InMemoryBackend) resetRawMaps() {
	b.groupMembers = make(map[string]bool)
	b.tags = make(map[string]map[string]string)

	b.accountSettings = make(map[string]*storedAccountSettings)
	b.accountSubscriptions = make(map[string]*storedAccountSubscription)
	b.accountCustomPermissions = make(map[string]string)
	b.ipRestrictions = make(map[string]*storedIPRestriction)
	b.publicSharing = make(map[string]bool)
	b.keyRegistrations = make(map[string][]storedRegisteredKey)
	b.defaultQBusinessApps = make(map[string]*storedDefaultQBusinessApplication)
	b.qPersonalization = make(map[string]string)
	b.qSearchConfig = make(map[string]string)
	b.dashboardsQAConfig = make(map[string]string)

	b.brandAssignments = make(map[string]string)
	b.roleCustomPermissions = make(map[string]string)
	b.roleMemberships = make(map[string]bool)
	b.userCustomPermissions = make(map[string]string)

	b.spiceCapacity = make(map[string]string)
	b.selfUpgradeConfig = make(map[string]string)
}

// restoreRawMaps copies every raw (non-store.Table) map from snap onto b,
// then fills in any left nil (e.g. a snapshot taken before that field
// existed) via ensureRawMapsInitialized. Split out of Restore, and split
// again from the nil-check pass below, purely to keep each function's
// cyclomatic complexity in budget.
func (b *InMemoryBackend) restoreRawMaps(snap *backendSnapshot) {
	b.groupMembers = snap.GroupMembers
	b.tags = snap.Tags
	b.accountSettings = snap.AccountSettings
	b.accountSubscriptions = snap.AccountSubscriptions
	b.accountCustomPermissions = snap.AccountCustomPermissions
	b.ipRestrictions = snap.IPRestrictions
	b.publicSharing = snap.PublicSharing
	b.keyRegistrations = snap.KeyRegistrations
	b.defaultQBusinessApps = snap.DefaultQBusinessApps
	b.qPersonalization = snap.QPersonalization
	b.qSearchConfig = snap.QSearchConfig
	b.dashboardsQAConfig = snap.DashboardsQAConfig
	b.brandAssignments = snap.BrandAssignments
	b.roleCustomPermissions = snap.RoleCustomPermissions
	b.roleMemberships = snap.RoleMemberships
	b.userCustomPermissions = snap.UserCustomPermissions
	b.spiceCapacity = snap.SPICECapacity
	b.selfUpgradeConfig = snap.SelfUpgradeConfig

	b.ensureRawMapsInitialized()
}

// ensureRawMapsInitialized re-initializes any raw (non-store.Table) map left
// nil after restoreRawMaps copied a snapshot (e.g. a snapshot taken before
// that field existed).
func (b *InMemoryBackend) ensureRawMapsInitialized() {
	if b.groupMembers == nil {
		b.groupMembers = make(map[string]bool)
	}
	if b.tags == nil {
		b.tags = make(map[string]map[string]string)
	}
	if b.accountSettings == nil {
		b.accountSettings = make(map[string]*storedAccountSettings)
	}
	if b.accountSubscriptions == nil {
		b.accountSubscriptions = make(map[string]*storedAccountSubscription)
	}
	if b.accountCustomPermissions == nil {
		b.accountCustomPermissions = make(map[string]string)
	}
	if b.ipRestrictions == nil {
		b.ipRestrictions = make(map[string]*storedIPRestriction)
	}
	if b.publicSharing == nil {
		b.publicSharing = make(map[string]bool)
	}
	if b.keyRegistrations == nil {
		b.keyRegistrations = make(map[string][]storedRegisteredKey)
	}
	if b.defaultQBusinessApps == nil {
		b.defaultQBusinessApps = make(map[string]*storedDefaultQBusinessApplication)
	}
	if b.qPersonalization == nil {
		b.qPersonalization = make(map[string]string)
	}
	if b.qSearchConfig == nil {
		b.qSearchConfig = make(map[string]string)
	}
	if b.dashboardsQAConfig == nil {
		b.dashboardsQAConfig = make(map[string]string)
	}
	b.ensureRoleAndSelfUpgradeMapsInitialized()
}

// ensureRoleAndSelfUpgradeMapsInitialized re-initializes the brand/role/user
// custom-permission and SPICE/self-upgrade raw maps left nil after
// restoreRawMaps. Split out of ensureRawMapsInitialized purely to keep its
// cyclomatic complexity in budget.
func (b *InMemoryBackend) ensureRoleAndSelfUpgradeMapsInitialized() {
	if b.brandAssignments == nil {
		b.brandAssignments = make(map[string]string)
	}
	if b.roleCustomPermissions == nil {
		b.roleCustomPermissions = make(map[string]string)
	}
	if b.roleMemberships == nil {
		b.roleMemberships = make(map[string]bool)
	}
	if b.userCustomPermissions == nil {
		b.userCustomPermissions = make(map[string]string)
	}
	if b.spiceCapacity == nil {
		b.spiceCapacity = make(map[string]string)
	}
	if b.selfUpgradeConfig == nil {
		b.selfUpgradeConfig = make(map[string]string)
	}
}

// ---- key helpers ----

func nsKey(accountID, namespace string) string {
	return accountID + "/" + namespace
}

func groupKey(accountID, namespace, groupName string) string {
	return accountID + "/" + namespace + "/" + groupName
}

func groupMemberKey(accountID, namespace, groupName, memberName string) string {
	return accountID + "/" + namespace + "/" + groupName + "/" + memberName
}

func userKey(accountID, namespace, userName string) string {
	return accountID + "/" + namespace + "/" + userName
}

func dataSourceKey(accountID, dataSourceID string) string {
	return accountID + "/" + dataSourceID
}

func dataSetKey(accountID, dataSetID string) string {
	return accountID + "/" + dataSetID
}

func ingestionKey(accountID, dataSetID, ingestionID string) string {
	return accountID + "/" + dataSetID + "/" + ingestionID
}

func dashboardKey(accountID, dashboardID string) string {
	return accountID + "/" + dashboardID
}

func analysisKey(accountID, analysisID string) string {
	return accountID + "/" + analysisID
}

func folderKey(accountID, folderID string) string {
	return accountID + "/" + folderID
}

func folderMemberKey(accountID, folderID, memberType, memberID string) string {
	return accountID + "/" + folderID + "/" + memberType + "/" + memberID
}

func templateKey(accountID, templateID string) string {
	return accountID + "/" + templateID
}

func themeKey(accountID, themeID string) string {
	return accountID + "/" + themeID
}

func brandKey(accountID, brandID string) string {
	return accountID + "/" + brandID
}

func customPermissionsKey(accountID, name string) string {
	return accountID + "/" + name
}

func roleCustomPermissionKey(accountID, namespace, role string) string {
	return accountID + "/" + namespace + "/" + role
}

func roleMembershipKey(accountID, namespace, role, memberName string) string {
	return accountID + "/" + namespace + "/" + role + "/" + memberName
}

func userCustomPermissionKey(accountID, namespace, userName string) string {
	return accountID + "/" + namespace + "/" + userName
}

func oauthAppKey(accountID, clientID string) string {
	return accountID + "/" + clientID
}

func identityPropagationKey(accountID, service string) string {
	return accountID + "/" + service
}

func assetBundleJobKey(accountID, jobID string) string {
	return accountID + "/" + jobID
}

func dashboardSnapshotJobKey(accountID, dashboardID, jobID string) string {
	return accountID + "/" + dashboardID + "/" + jobID
}

// ---- ARN builder ----

func (b *InMemoryBackend) buildARN(resourceType, resourceID string) string {
	return arn.Build("quicksight", b.region, b.accountID, fmt.Sprintf("%s/%s", resourceType, resourceID))
}
