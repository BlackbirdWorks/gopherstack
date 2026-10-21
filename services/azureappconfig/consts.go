package azureappconfig

// Content types the service and its clients treat specially.
const (
	ContentTypeFeatureFlag = "application/vnd.microsoft.appconfig.ff+json;charset=utf-8"
	ContentTypeKeyVaultRef = "application/vnd.microsoft.appconfig.keyvaultref+json;charset=utf-8"
	FeatureFlagPrefix      = ".appconfig.featureflag/"
)

const (
	mediaKV          = "application/vnd.microsoft.appconfig.kv+json; charset=utf-8"
	mediaKVSet       = "application/vnd.microsoft.appconfig.kvset+json; charset=utf-8"
	mediaKeySet      = "application/vnd.microsoft.appconfig.keyset+json; charset=utf-8"
	mediaLabelSet    = "application/vnd.microsoft.appconfig.labelset+json; charset=utf-8"
	mediaSnapshot    = "application/vnd.microsoft.appconfig.snapshot+json; charset=utf-8"
	mediaSnapshotSet = "application/vnd.microsoft.appconfig.snapshotset+json; charset=utf-8"
	mediaProblem     = "application/problem+json; charset=utf-8"
	mediaJSON        = "application/json; charset=utf-8"
)

const (
	pageSize        = 100
	maxFilterItems  = 5
	maxTagFilters   = 5
	maxKeyLen       = 10 << 10
	maxLabelLen     = 256
	maxItemBytes    = 1 << 20
	maxHistory      = 50_000
	maxSnapshotFilt = 3
	maxBodyBytes    = 2 << 20
	maxNameLen      = 50
	minNameLen      = 5
)

const (
	snapshotReady        = "ready"
	snapshotArchived     = "archived"
	snapshotProvisioning = "provisioning"
	compositionKey       = "key"
	compositionKeyLabel  = "key_label"

	defaultRetention = int64(7 * 24 * 3600)
	minRetention     = int64(3600)
	maxRetention     = int64(90 * 24 * 3600)
)
