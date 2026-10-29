package azurearm

import (
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

const (
	namespaceDBforPostgreSQL = "Microsoft.DBforPostgreSQL"
	namespaceDBforMySQL      = "Microsoft.DBforMySQL"
	flexServersType          = "flexibleServers"
	flexFirewallType         = "firewallRules"
	flexDatabasesType        = "databases"
	flexConfigsType          = "configurations"
	flexReplicasType         = "replicas"
	tierBurstable            = "Burstable"
	tierGeneralPurpose       = "GeneralPurpose"
	tierMemoryOptimized      = "MemoryOptimized"
	flexEnabled              = "Enabled"
	flexDisabled             = "Disabled"
	flexMemMB                = 1024
)

type flexSKU struct {
	name    string
	tier    string
	vcores  int
	memGiB  int
	maxConn int
}

type flexConfigDef struct {
	name     string
	dataType string
	def      string
	allowed  string
	desc     string
	unit     string
	enum     []string
	min      int64
	max      int64
	readOnly bool
	static   bool
}

// flexFlavor is everything that differs between the PostgreSQL and MySQL flexible server resource providers.
type flexFlavor struct {
	loginRE         *regexp.Regexp
	dbNameRE        *regexp.Regexp
	ns              string
	kind            string
	hostSuffix      string
	defaultVersion  string
	defaultCharset  string
	defaultCollate  string
	roleReplica     string
	roleSource      string
	subnetService   string
	apiVersions     []string
	versions        []string
	storageSizes    []int
	reservedLogins  []string
	systemDBs       []string
	skus            []flexSKU
	configs         []flexConfigDef
	minStorage      int
	maxStorage      int
	defaultStorage  int
	minRetention    int
	maxReplicas     int
	maxLoginLen     int
	boolStrings     bool
	hasAuthConfig   bool
	sourceTimeField string
}

func (f *flexFlavor) skuByName(name string) (flexSKU, bool) {
	for _, s := range f.skus {
		if s.name == name {
			return s, true
		}
	}

	return flexSKU{}, false
}

func (f *flexFlavor) config(name string) (flexConfigDef, bool) {
	for _, c := range f.configs {
		if strings.EqualFold(c.name, name) {
			return c, true
		}
	}

	return flexConfigDef{}, false
}

func (f *flexFlavor) isSystemDB(name string) bool {
	return slices.ContainsFunc(f.systemDBs, func(s string) bool { return strings.EqualFold(s, name) })
}

func (f *flexFlavor) validVersion(v string) bool { return slices.Contains(f.versions, v) }

func (f *flexFlavor) validStorage(gb int) bool {
	if f.storageSizes != nil {
		return slices.Contains(f.storageSizes, gb)
	}

	return gb >= f.minStorage && gb <= f.maxStorage
}

func flexNames(ss []flexSKU) []string {
	out := make([]string, len(ss))
	for i, s := range ss {
		out[i] = s.name
	}

	return out
}

// pgConnLimit and the memory sizes follow the compute tables in the PostgreSQL flexible server limits page.
func pgConnLimit(tier string, vcores, memGiB int) int {
	switch {
	case tier == tierBurstable && vcores == 1:
		return 50
	case tier == tierBurstable && memGiB == 4:
		return 429
	case memGiB <= 8:
		return 859
	case memGiB <= 16:
		return 1718
	case memGiB <= 32:
		return 3437
	}

	return 5000
}

func pgSKUs() []flexSKU {
	var out []flexSKU

	for _, b := range []struct {
		n    string
		v, m int
	}{{"B1ms", 1, 2}, {"B2s", 2, 4}, {"B2ms", 2, 8}, {"B4ms", 4, 16}, {"B8ms", 8, 32}, {"B12ms", 12, 48},
		{"B16ms", 16, 64}, {"B20ms", 20, 80}} {
		out = append(out, flexSKU{"Standard_" + b.n, tierBurstable, b.v, b.m, pgConnLimit(tierBurstable, b.v, b.m)})
	}

	suffixes := []string{"s_v3", "ds_v4", "ds_v5", "ads_v5"}

	for _, v := range []int{2, 4, 8, 16, 32, 48, 64} {
		for _, s := range suffixes {
			out = append(out, flexSKU{fmt.Sprintf("Standard_D%d%s", v, s), tierGeneralPurpose, v, v * 4,
				pgConnLimit(tierGeneralPurpose, v, v*4)})
		}
	}

	for _, s := range []string{"ds_v5", "ads_v5"} {
		out = append(out, flexSKU{"Standard_D96" + s, tierGeneralPurpose, 96, 384, 5000})
	}

	for _, v := range []int{2, 4, 8, 16, 20, 32, 48, 64} {
		mem := v * 8
		if v == 64 {
			mem = 432
		}

		for _, s := range suffixes {
			if v == 20 && s == "s_v3" {
				continue
			}

			out = append(out, flexSKU{fmt.Sprintf("Standard_E%d%s", v, s), tierMemoryOptimized, v, mem,
				pgConnLimit(tierMemoryOptimized, v, mem)})
		}
	}

	for _, s := range []string{"ds_v5", "ads_v5"} {
		out = append(out, flexSKU{"Standard_E96" + s, tierMemoryOptimized, 96, 672, 5000})
	}

	return out
}

func mysqlSKUs() []flexSKU {
	var out []flexSKU

	for _, b := range []struct {
		n    string
		v, m int
	}{{"B1s", 1, 1}, {"B1ms", 1, 2}, {"B2s", 2, 4}, {"B2ms", 2, 8}, {"B4ms", 4, 16}, {"B8ms", 8, 32},
		{"B12ms", 12, 48}, {"B16ms", 16, 64}, {"B20ms", 20, 80}} {
		out = append(out, flexSKU{"Standard_" + b.n, tierBurstable, b.v, b.m, 0})
	}

	for _, v := range []int{2, 4, 8, 16, 32, 48, 64} {
		out = append(out, flexSKU{fmt.Sprintf("Standard_D%dds_v4", v), tierGeneralPurpose, v, v * 4, 0},
			flexSKU{fmt.Sprintf("Standard_D%dads_v5", v), tierGeneralPurpose, v, v * 4, 0})
	}

	out = append(out, flexSKU{"Standard_D96ads_v5", tierGeneralPurpose, 96, 384, 0})

	for _, v := range []int{2, 4, 8, 16, 20, 32, 48, 64} {
		out = append(out, flexSKU{fmt.Sprintf("Standard_E%dds_v4", v), tierMemoryOptimized, v, v * 8, 0})
	}

	for _, v := range []int{2, 4, 8, 16, 32, 48, 64, 96} {
		out = append(out, flexSKU{fmt.Sprintf("Standard_E%dads_v5", v), tierMemoryOptimized, v, v * 8, 0})
	}

	return out
}

func pgExtensions() string {
	return "AGE,ANON,AZURE_AI,AZURE_STORAGE,BLOOM,BTREE_GIN,BTREE_GIST,CITEXT,CUBE,DBLINK,DICT_INT,DICT_XSYN," +
		"EARTHDISTANCE,FUZZYSTRMATCH,HSTORE,INTARRAY,ISN,LO,LTREE,PG_BUFFERCACHE,PG_CRON,PG_FREESPACEMAP," +
		"PG_PARTMAN,PG_PREWARM,PG_REPACK,PG_SQUEEZE,PG_STAT_STATEMENTS,PG_TRGM,PG_VISIBILITYMAP,PGAUDIT," +
		"PGCRYPTO,PGROWLOCKS,PGSTATTUPLE,PGVECTOR,PLPGSQL,POSTGIS,POSTGIS_RASTER,POSTGIS_TOPOLOGY,POSTGRES_FDW," +
		"SEG,TABLEFUNC,TCN,TIMESCALEDB,TSM_SYSTEM_ROWS,TSM_SYSTEM_TIME,UNACCENT,UUID-OSSP,VECTOR"
}

func pgConfigs() []flexConfigDef {
	return []flexConfigDef{
		{name: "max_connections", dataType: "Integer", def: "50", min: 25, max: 262143, static: true,
			desc: "Sets the maximum number of concurrent connections."},
		{name: "work_mem", dataType: "Integer", def: "4096", min: 4, max: 2097151, unit: "kB",
			desc: "Sets the amount of memory to be used by internal sort operations and hash tables."},
		{name: "maintenance_work_mem", dataType: "Integer", def: "65536", min: 1024, max: 2097151, unit: "kB",
			desc: "Sets the maximum memory to be used for maintenance operations."},
		{name: "shared_buffers", dataType: "Integer", def: "16384", min: 16, max: 1073741823, unit: "8kB",
			static: true, desc: "Sets the number of shared memory buffers used by the server."},
		{name: "effective_cache_size", dataType: "Integer", def: "524288", min: 1, max: 2147483647, unit: "8kB",
			desc: "Sets the planner's assumption about the total size of the data caches."},
		{name: "statement_timeout", dataType: "Integer", def: "0", min: 0, max: 2147483647, unit: "ms",
			desc: "Sets the maximum allowed duration of any statement."},
		{name: "idle_in_transaction_session_timeout", dataType: "Integer", def: "0", min: 0, max: 2147483647,
			unit: "ms", desc: "Sets the maximum allowed idle time between queries, when in a transaction."},
		{name: "lock_timeout", dataType: "Integer", def: "0", min: 0, max: 2147483647, unit: "ms",
			desc: "Sets the maximum allowed duration of any wait for a lock."},
		{name: "log_min_duration_statement", dataType: "Integer", def: "-1", min: -1, max: 2147483647, unit: "ms",
			desc: "Sets the minimum execution time above which statements will be logged."},
		{name: "checkpoint_timeout", dataType: "Integer", def: "600", min: 30, max: 86400, unit: "s",
			desc: "Sets the maximum time between automatic WAL checkpoints."},
		{name: "track_activity_query_size", dataType: "Integer", def: "1024", min: 100, max: 1048576, unit: "B",
			static: true, desc: "Sets the size reserved for pg_stat_activity.query."},
		{name: "random_page_cost", dataType: "Numeric", def: "4", min: 0, max: 1000000,
			desc: "Sets the planner's estimate of the cost of a nonsequentially fetched disk page."},
		{name: "log_statement", dataType: "Enumeration", def: "none", enum: []string{"none", "ddl", "mod", "all"},
			desc: "Sets the type of statements logged."},
		{name: "wal_level", dataType: "Enumeration", def: "replica", enum: []string{"replica", "logical"},
			static: true, desc: "Sets the level of information written to the WAL."},
		{name: "password_encryption", dataType: "Enumeration", def: "scram-sha-256",
			enum: []string{"md5", "scram-sha-256"}, desc: "Chooses the algorithm for encrypting passwords."},
		{name: "log_connections", dataType: "Boolean", def: "off", enum: []string{"on", "off"},
			desc: "Logs each successful connection."},
		{name: "log_disconnections", dataType: "Boolean", def: "off", enum: []string{"on", "off"},
			desc: "Logs end of a session, including duration."},
		{name: "autovacuum", dataType: "Boolean", def: "on", enum: []string{"on", "off"},
			desc: "Starts the autovacuum subprocess."},
		{name: "azure.extensions", dataType: "Set", def: "", allowed: pgExtensions(), static: false,
			desc: "Specifies which extensions are allowed to be created in the server."},
		{name: "server_version", dataType: "String", def: "", readOnly: true,
			desc: "Shows the server version."},
	}
}

func mysqlConfigs() []flexConfigDef {
	return []flexConfigDef{
		{name: "max_connections", dataType: "Integer", def: "151", min: 10, max: 100000,
			desc: "The number of simultaneous client connections."},
		{name: "wait_timeout", dataType: "Integer", def: "28800", min: 1, max: 31536000, unit: "s",
			desc: "The number of seconds the server waits for activity on a noninteractive connection."},
		{name: "interactive_timeout", dataType: "Integer", def: "28800", min: 1, max: 31536000, unit: "s",
			desc: "The number of seconds the server waits for activity on an interactive connection."},
		{name: "max_allowed_packet", dataType: "Integer", def: "536870912", min: 1024, max: 1073741824, unit: "B",
			desc: "The maximum size of one packet or any generated/intermediate string."},
		{name: "innodb_lock_wait_timeout", dataType: "Integer", def: "50", min: 1, max: 1073741824, unit: "s",
			desc: "The length of time in seconds an InnoDB transaction waits for a row lock."},
		{name: "long_query_time", dataType: "Numeric", def: "10", min: 0, max: 31536000, unit: "s",
			desc: "Queries slower than this many seconds are counted as slow."},
		{name: "slow_query_log", dataType: "Enumeration", def: "OFF", enum: []string{"ON", "OFF"},
			desc: "Whether the slow query log is enabled."},
		{name: "event_scheduler", dataType: "Enumeration", def: "OFF", enum: []string{"ON", "OFF"},
			desc: "Indicates the status of the Event Scheduler."},
		{name: "require_secure_transport", dataType: "Enumeration", def: "ON", enum: []string{"ON", "OFF"},
			desc: "Whether client connections to the server are required to use some form of secure transport."},
		{name: "log_bin_trust_function_creators", dataType: "Enumeration", def: "OFF", enum: []string{"ON", "OFF"},
			desc: "Whether stored function creators can be trusted not to create stored functions that cause unsafe events."},
		{name: "sql_generate_invisible_primary_key", dataType: "Enumeration", def: "ON",
			enum: []string{"ON", "OFF"}, desc: "Whether GIPK mode is in effect."},
		{name: "tls_version", dataType: "Set", def: "TLSv1.2", allowed: "TLSv1.2,TLSv1.3", static: true,
			desc: "Which protocols the server permits for encrypted connections."},
		{name: "sql_mode", dataType: "Set",
			def: "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO," +
				"NO_ENGINE_SUBSTITUTION",
			allowed: "ALLOW_INVALID_DATES,ANSI_QUOTES,ERROR_FOR_DIVISION_BY_ZERO,HIGH_NOT_PRECEDENCE," +
				"IGNORE_SPACE,NO_AUTO_VALUE_ON_ZERO,NO_BACKSLASH_ESCAPES,NO_DIR_IN_CREATE,NO_ENGINE_SUBSTITUTION," +
				"NO_UNSIGNED_SUBTRACTION,NO_ZERO_DATE,NO_ZERO_IN_DATE,ONLY_FULL_GROUP_BY,PAD_CHAR_TO_FULL_LENGTH," +
				"PIPES_AS_CONCAT,REAL_AS_FLOAT,STRICT_ALL_TABLES,STRICT_TRANS_TABLES,TIME_TRUNCATE_FRACTIONAL",
			desc: "The current server SQL mode."},
		{name: "character_set_server", dataType: "Enumeration", def: "utf8mb4", static: true,
			enum: []string{"utf8mb4", "utf8mb3", "utf8", "latin1", "ascii", "binary"},
			desc: "The server's default character set."},
		{name: "time_zone", dataType: "String", def: "+00:00", desc: "The server time zone."},
		{name: "lower_case_table_names", dataType: "Integer", def: "1", min: 1, max: 2, readOnly: true,
			desc: "Whether table names are stored in lowercase."},
	}
}

func newPostgresFlavor() *flexFlavor {
	return &flexFlavor{
		ns: namespaceDBforPostgreSQL, kind: "postgres", hostSuffix: ".postgres.database.azure.com",
		apiVersions: []string{
			"2025-08-01", "2025-06-01-preview", "2024-11-01-preview", "2024-08-01", "2024-03-01-preview",
			"2023-12-01-preview", "2023-06-01-preview", "2023-03-01-preview", "2022-12-01", "2022-03-08-preview",
			"2021-06-01",
		},
		versions:       []string{"11", "12", "13", "14", "15", "16", "17", "18"},
		defaultVersion: "16", defaultCharset: "UTF8", defaultCollate: "en_US.utf8",
		storageSizes: []int{32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768}, defaultStorage: 128,
		minStorage: 32, maxStorage: 32768, minRetention: 7, maxReplicas: 5, maxLoginLen: 63,
		loginRE:  regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,62}$`),
		dbNameRE: regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_$]{0,62}$`),
		reservedLogins: []string{
			"azure_superuser", "azure_pg_admin", "admin", "administrator", "root", "guest", "public",
		},
		systemDBs: []string{"azure_maintenance", "azure_sys", "postgres"}, skus: pgSKUs(), configs: pgConfigs(),
		roleReplica: "AsyncReplica", roleSource: "Primary", subnetService: "Microsoft.DBforPostgreSQL/flexibleServers",
		hasAuthConfig: true, sourceTimeField: "pointInTimeUTC",
	}
}

func newMySQLFlavor() *flexFlavor {
	return &flexFlavor{
		ns: namespaceDBforMySQL, kind: "mysql", hostSuffix: ".mysql.database.azure.com",
		apiVersions: []string{
			"2024-12-30", "2024-10-01-preview", "2024-06-01-preview", "2023-12-30", "2023-12-01-preview",
			"2023-10-01-preview", "2023-06-30", "2022-12-01", "2022-01-01", "2021-12-01-preview", "2021-05-01",
		},
		versions:       []string{"5.7", "8.0.21", "8.4"},
		defaultVersion: "8.0.21", defaultCharset: "utf8mb4", defaultCollate: "utf8mb4_0900_ai_ci",
		minStorage: 20, maxStorage: 16384, defaultStorage: 20, minRetention: 1, maxReplicas: 10, maxLoginLen: 32,
		loginRE:  regexp.MustCompile(`^[A-Za-z0-9_]{1,32}$`),
		dbNameRE: regexp.MustCompile(`^[A-Za-z0-9_$-]{1,64}$`),
		reservedLogins: []string{
			"azure_superuser", "admin", "administrator", "root", "guest", "public",
		},
		systemDBs: []string{"information_schema", "mysql", "performance_schema", "sys"}, skus: mysqlSKUs(),
		configs: mysqlConfigs(), roleReplica: "Replica", roleSource: "Source",
		subnetService: "Microsoft.DBforMySQL/flexibleServers", boolStrings: true, sourceTimeField: "restorePointInTime",
	}
}

// pgStorageTier maps a PostgreSQL storage size to its disk tier and provisioned IOPS (Premium SSD sizes).
func pgStorageTier(gb int) (string, int) {
	tiers := []struct {
		gb   int
		tier string
		iops int
	}{{32, "P4", 120}, {64, "P6", 240}, {128, "P10", 500}, {256, "P15", 1100}, {512, "P20", 2300},
		{1024, "P30", 5000}, {2048, "P40", 7500}, {4096, "P50", 7500}, {8192, "P60", 16000},
		{16384, "P70", 18000}, {32768, "P80", 20000}}

	for _, t := range tiers {
		if t.gb == gb {
			return t.tier, t.iops
		}
	}

	return "", 0
}

const (
	mysqlBaseIops   = 360
	mysqlIopsPerGiB = 3
	mysqlMaxIops    = 20000
)

func mysqlIops(gb int) int { return min(max(mysqlBaseIops, gb*mysqlIopsPerGiB), mysqlMaxIops) }

func validConfigValue(def flexConfigDef, value string) error {
	switch def.dataType {
	case "Integer":
		n, err := strconv.ParseInt(value, 10, 64)
		if err != nil || n < def.min || n > def.max {
			return fmt.Errorf("allowed values are %d-%d", def.min, def.max)
		}
	case "Numeric":
		f, err := strconv.ParseFloat(value, 64)
		if err != nil || f < float64(def.min) || f > float64(def.max) {
			return fmt.Errorf("allowed values are %d-%d", def.min, def.max)
		}
	case "Boolean", "Enumeration":
		if !slices.ContainsFunc(def.enum, func(e string) bool { return strings.EqualFold(e, value) }) {
			return fmt.Errorf("allowed values are %s", strings.Join(def.enum, ","))
		}
	case "Set":
		allowed := strings.Split(strings.ToUpper(def.allowed), ",")

		for _, v := range strings.Split(value, ",") {
			v = strings.TrimSpace(strings.ToUpper(v))
			if v != "" && !slices.Contains(allowed, v) {
				return fmt.Errorf("%q is not an allowed value", v)
			}
		}
	}

	return nil
}

func (c flexConfigDef) allowedValues() string {
	switch {
	case c.allowed != "":
		return c.allowed
	case len(c.enum) > 0:
		return strings.Join(c.enum, ",")
	case c.dataType == "Integer" || c.dataType == "Numeric":
		return fmt.Sprintf("%d-%d", c.min, c.max)
	}

	return ""
}
