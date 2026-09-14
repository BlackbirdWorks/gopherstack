package glue

// Hard-coded resource caps enforced at the Create*/Register*/BatchCreate* ops
// whose real error catalog declares ResourceNumberLimitExceededException --
// confirmed per-op by reading aws-sdk-go-v2/service/glue@v1.157.0/
// deserializers.go's own awsAwsjson11_deserializeOpError<Op> switch:
// CreateConnection, CreateCrawler, CreateDatabase, CreateDevEndpoint,
// CreateJob, CreateMLTransform, CreateSecurityConfiguration, CreateTable,
// CreateTrigger, CreateUserDefinedFunction, CreateWorkflow, CreateRegistry,
// BatchCreatePartition/CreatePartition, CreateIntegration,
// RegisterSchemaVersion. Every other op whose switch also carries the
// exception (CreateBlueprint, CreateCatalog, CreateColumnStatisticsTaskSettings,
// CreateCustomEntityType, CreateDataQualityRuleset, CreateSchema,
// CreateUsageProfile, PutSchemaVersionMetadata, PutWorkflowRunProperties,
// RegisterConnectionType, RunStatement, Start*, TestConnection,
// UpdateDataQualityRuleset, UpdateTable) is left unenforced because AWS does
// not publish a distinct per-resource-kind cardinality quota for it (only
// concurrency quotas, which are a different exception's job -- see
// ErrConcurrentRunsExceeded) -- not skipped for lack of effort.
//
// Values are AWS's real, published default quotas from
// https://docs.aws.amazon.com/general/latest/gr/glue.html (WebFetch'd
// 2026-09-11), except schemaRegistries/schemaVersions, whose page marks
// "Adjustable: No" -- still a real numeric default, not invented, just not
// raisable via Service Quotas in real AWS the way the others are.
const (
	defaultMaxDevEndpoints       = 25       // "Max development endpoint per account"
	defaultMaxConnections        = 1000     // "Max connection per account"
	defaultMaxCrawlers           = 1000     // "Number of crawlers per account"
	defaultMaxDatabases          = 10000    // "Max databases per account"
	defaultMaxJobs               = 2000     // "Max jobs per account"
	defaultMaxMLTransforms       = 100      // "Number of machine learning transforms"
	defaultMaxSecurityConfigs    = 250      // "Max security configurations per account"
	defaultMaxTablesPerDatabase  = 200000   // "Max tables per database"
	defaultMaxTriggers           = 1000     // "Max triggers per account"
	defaultMaxFunctionsPerDB     = 100      // "Max functions per database"
	defaultMaxWorkflows          = 1000     // "Number of workflows"
	defaultMaxSchemaRegistries   = 100      // "Number of Schema Registries" (not adjustable)
	defaultMaxPartitionsPerTable = 10000000 // "Max partitions per table"
	defaultMaxIntegrations       = 40       // "Number of integrations"
	defaultMaxSchemaVersions     = 10000    // "Number of Schema Versions" (not adjustable)
)

// resourceLimits holds the caps InMemoryBackend enforces, defaulted to the
// real Glue values above (defaultResourceLimits) and overridable via
// WithResourceLimits (store.go) -- the same constructor-option pattern
// services/ses/limits.go uses -- so tests exercising
// ResourceNumberLimitExceededException on a 10,000/200,000/10,000,000-sized
// cap don't have to create that many real resources.
type resourceLimits struct {
	devEndpoints         int
	connections          int
	crawlers             int
	databases            int
	jobs                 int
	mlTransforms         int
	securityConfigs      int
	tablesPerDatabase    int
	triggers             int
	functionsPerDatabase int
	workflows            int
	schemaRegistries     int
	partitionsPerTable   int
	integrations         int
	schemaVersions       int
}

func defaultResourceLimits() resourceLimits {
	return resourceLimits{
		devEndpoints:         defaultMaxDevEndpoints,
		connections:          defaultMaxConnections,
		crawlers:             defaultMaxCrawlers,
		databases:            defaultMaxDatabases,
		jobs:                 defaultMaxJobs,
		mlTransforms:         defaultMaxMLTransforms,
		securityConfigs:      defaultMaxSecurityConfigs,
		tablesPerDatabase:    defaultMaxTablesPerDatabase,
		triggers:             defaultMaxTriggers,
		functionsPerDatabase: defaultMaxFunctionsPerDB,
		workflows:            defaultMaxWorkflows,
		schemaRegistries:     defaultMaxSchemaRegistries,
		partitionsPerTable:   defaultMaxPartitionsPerTable,
		integrations:         defaultMaxIntegrations,
		schemaVersions:       defaultMaxSchemaVersions,
	}
}

// ResourceLimits overrides the resource caps a *InMemoryBackend enforces with
// ResourceNumberLimitExceededException. A zero field keeps its real-Glue
// default (see WithResourceLimits in store.go) -- used by tests that need to
// trip a large cap without actually creating that many resources.
type ResourceLimits struct {
	DevEndpoints         int
	Connections          int
	Crawlers             int
	Databases            int
	Jobs                 int
	MLTransforms         int
	SecurityConfigs      int
	TablesPerDatabase    int
	Triggers             int
	FunctionsPerDatabase int
	Workflows            int
	SchemaRegistries     int
	PartitionsPerTable   int
	Integrations         int
	SchemaVersions       int
}

// applyResourceLimitOverrides copies every positive field of l onto rl,
// leaving fields left at zero in l unchanged. Split into two halves to stay
// under this repo's cyclop limit (decompose, never nolint) -- see
// applyResourceLimitOverridesPart2.
func applyResourceLimitOverrides(rl *resourceLimits, l ResourceLimits) {
	if l.DevEndpoints > 0 {
		rl.devEndpoints = l.DevEndpoints
	}

	if l.Connections > 0 {
		rl.connections = l.Connections
	}

	if l.Crawlers > 0 {
		rl.crawlers = l.Crawlers
	}

	if l.Databases > 0 {
		rl.databases = l.Databases
	}

	if l.Jobs > 0 {
		rl.jobs = l.Jobs
	}

	if l.MLTransforms > 0 {
		rl.mlTransforms = l.MLTransforms
	}

	if l.SecurityConfigs > 0 {
		rl.securityConfigs = l.SecurityConfigs
	}

	if l.TablesPerDatabase > 0 {
		rl.tablesPerDatabase = l.TablesPerDatabase
	}

	applyResourceLimitOverridesPart2(rl, l)
}

// applyResourceLimitOverridesPart2 is the second half of
// applyResourceLimitOverrides.
func applyResourceLimitOverridesPart2(rl *resourceLimits, l ResourceLimits) {
	if l.Triggers > 0 {
		rl.triggers = l.Triggers
	}

	if l.FunctionsPerDatabase > 0 {
		rl.functionsPerDatabase = l.FunctionsPerDatabase
	}

	if l.Workflows > 0 {
		rl.workflows = l.Workflows
	}

	if l.SchemaRegistries > 0 {
		rl.schemaRegistries = l.SchemaRegistries
	}

	if l.PartitionsPerTable > 0 {
		rl.partitionsPerTable = l.PartitionsPerTable
	}

	if l.Integrations > 0 {
		rl.integrations = l.Integrations
	}

	if l.SchemaVersions > 0 {
		rl.schemaVersions = l.SchemaVersions
	}
}
