package cosmosdb

// DefaultPort is the real Cosmos DB Local Emulator's own fixed,
// protocol-conventional TCP port (8081). Unlike services/azureblob's 10000,
// services/azurequeue's 10001, and services/azuretable's 10002 -- all of
// which mirror Azurite's own port convention -- 8081 mirrors the real Cosmos
// DB Emulator's own published default instead, since Cosmos's emulator
// (unlike Azurite) is a single-service, single-port tool with no analogous
// three-port split to imitate. This deliberately sits OUTSIDE
// --port-range-start/--port-range-end's own default range (10000-10100),
// exactly like services/iot's MQTT broker default (1883) -- see AZURE.md
// section 4 and handler.go's StartWorker for the synchronous-bind,
// no-fallback-pool rationale this follows.
const DefaultPort = 8081

// DefaultMasterKey is the real Cosmos DB Local Emulator's well-known,
// publicly documented fixed master key. Accepting it by default (like
// services/azureblob's/azurequeue's/azuretable's fixed devstoreaccount1
// key) means unmodified SDK configuration pointed at the real emulator's
// documented connection string works out of the box against gopherstack
// too. See masterkey.go and AZURE.md section 5.
const DefaultMasterKey = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw=="

// Settings holds service-level configuration for the Cosmos DB backend.
// Fields are picked up by the Kong CLI parser when this struct is embedded
// in the root CLI command (see cli.go's CLI.CosmosDB field), mirroring
// services/azuretable's Settings pattern.
type Settings struct {
	// MasterKey is the base64-encoded master key authorize verifies request
	// signatures against when ValidateAuth is true. Defaults to the real
	// emulator's own well-known key (DefaultMasterKey).
	MasterKey string `json:"masterKey" env:"COSMOSDB_MASTER_KEY" default:"C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==" name:"master-key" help:"Base64-encoded Cosmos DB master key checked when --cosmosdb-validate-auth is set."` //nolint:lll // config struct tags are intentionally verbose
	// Port is the fixed TCP port for the dedicated Cosmos DB listener. See
	// handler.go's StartWorker for what happens when it's unavailable
	// (fails fast; no fallback pool, matching services/azureblob,
	// services/azurequeue, and services/azuretable).
	Port int `json:"port" env:"COSMOSDB_PORT" default:"8081" name:"port" help:"Fixed TCP port for the dedicated Cosmos DB listener; startup fails if it's unavailable (no fallback pool)."` //nolint:lll // config struct tags are intentionally verbose
	// ValidateAuth opts into cryptographic master-key signature
	// verification of the Authorization header (opt-in, off by default --
	// mirrors services/s3's WithPresignValidation and pkgs/azureauth's
	// VerifySharedKey opt-in pattern). See masterkey.go.
	ValidateAuth bool `json:"validateAuth" env:"COSMOSDB_VALIDATE_AUTH" default:"false" name:"validate-auth" help:"Cryptographically validate Cosmos DB master-key request signatures (opt-in)."` //nolint:lll // config struct tags are intentionally verbose
	// EnforceThroughput opts into 429 throttling against each container's (or shared database's) provisioned RU/s.
	EnforceThroughput bool `json:"enforceThroughput" env:"COSMOSDB_ENFORCE_THROUGHPUT" default:"false" name:"enforce-throughput" help:"Throttle requests with 429 once a container's provisioned RU/s budget is exceeded (opt-in)."` //nolint:lll // config struct tags are intentionally verbose
	// BurstCapacity lets throttled partitions spend banked idle RU/s (opt-in; needs enforce-throughput).
	BurstCapacity bool `json:"burstCapacity" env:"COSMOSDB_BURST_CAPACITY" default:"false" name:"burst-capacity" help:"Let a partition spend up to 300 s of idle RU/s at up to 3000 RU/s before throttling (opt-in; needs --cosmosdb-enforce-throughput)."` //nolint:lll // config struct tags are intentionally verbose
	// Serverless models a serverless account: throughput cannot be provisioned and containers burst to 5000 RU/s.
	Serverless bool `json:"serverless" env:"COSMOSDB_SERVERLESS" default:"false" name:"serverless" help:"Model a serverless Cosmos DB account: reject throughput provisioning and cap containers at 5000 RU/s (needs --cosmosdb-enforce-throughput to throttle)."` //nolint:lll // config struct tags are intentionally verbose
	// ResourceTokenMaxSeconds is the longest accepted resource-token lifetime (600..86400).
	ResourceTokenMaxSeconds int `json:"resourceTokenMaxSeconds" env:"COSMOSDB_RESOURCE_TOKEN_MAX_SECONDS" default:"86400" name:"resource-token-max-seconds" help:"Maximum resource-token lifetime in seconds, 600..86400 (Service quotas: 86400; the 2020 Create Permission page says 18000)."` //nolint:lll // config struct tags are intentionally verbose
}

// DefaultSettings returns the default Settings. Used when no ConfigProvider
// is available at init time (e.g. tests constructing a Provider directly).
func DefaultSettings() Settings {
	return Settings{
		Port: DefaultPort, MasterKey: DefaultMasterKey, ValidateAuth: false,
		ResourceTokenMaxSeconds: maxTokenExpirySeconds,
	}
}
