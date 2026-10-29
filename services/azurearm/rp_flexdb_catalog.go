package azurearm

import (
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

const (
	namespaceDBforPostgreSQL  = "Microsoft.DBforPostgreSQL"
	namespaceDBforMySQL       = "Microsoft.DBforMySQL"
	flexServersType           = "flexibleServers"
	flexFirewallType          = "firewallRules"
	flexDatabasesType         = "databases"
	flexConfigsType           = "configurations"
	flexReplicasType          = "replicas"
	tierBurstable             = "Burstable"
	tierGeneralPurpose        = "GeneralPurpose"
	tierMemoryOptimized       = "MemoryOptimized"
	flexEnabled               = "Enabled"
	flexDisabled              = "Disabled"
	flexMemMB                 = 1024
	flexKindPostgres          = "postgres"
	flexKindMySQL             = "mysql"
	flexAPI20221201           = "2022-12-01"
	flexLoginAdmin            = "admin"
	flexRolePrimary           = "Primary"
	flexAvailable             = "Available"
	flexPropCollation         = "collation"
	flexPropFQDN              = "fullyQualifiedDomainName"
	flexLoginAzureSuper       = "azure_superuser"
	suffixADS                 = "ads_v5"
	cfgInteger                = "Integer"
	cfgNumeric                = "Numeric"
	cfgEnum                   = "Enumeration"
	cfgBoolean                = "Boolean"
	cfgSet                    = "Set"
	cfgOn                     = "ON"
	cfgOff                    = "OFF"
	cfgMaxConn                = "max_connections"
	gpMemPerCore              = 4
	moMemPerCore              = 8
	moMem64                   = 432
	moMem96                   = 672
	gpMem96                   = 384
	moVcores64                = 64
	flexBackupHours           = 24
	flexTierCount             = 3
	flexPropSupportedVersions = "supportedServerVersions"
	flexPropStorageEditions   = "supportedStorageEditions"
	flexPropServerEditions    = "supportedFlexibleServerEditions"
	suffixDS5                 = "ds_v5"
	flexRangeNonNeg           = "0..2147483647"
	flexOff                   = "off"
	flexOn                    = "on"
	flexUTF8MB4               = "utf8mb4"
	flexLoginAdministrator    = "administrator"
	flexLoginGuest            = "guest"
	flexMaxConns              = 5000
	pgDefaultStorageGiB       = 128
	pgMinStorageGiB           = 32
	mysqlMinStorageGiB        = 20
	flexMaxVcores             = 96
	flexVcores20              = 20
	pgSKUCount                = 90
	mysqlSKUCount             = 40
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
	rng      string
	enum     []string
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
	sourceTimeField string
	versions        []string
	storageSizes    []int
	reservedLogins  []string
	systemDBs       []string
	skus            []flexSKU
	configs         []flexConfigDef
	apiVersions     []string
	maxStorage      int
	defaultStorage  int
	minRetention    int
	maxReplicas     int
	maxLoginLen     int
	minStorage      int
	boolStrings     bool
	hasAuthConfig   bool
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

// pgConnLimit follows the default max_connections table in the PostgreSQL flexible server limits page; the
// default depends on the compute memory.
func pgConnLimit(memGiB int) int {
	for _, l := range []struct{ upToGiB, conns int }{
		{2, 50}, {4, 429}, {8, 859}, {16, 1718}, {32, 3437},
	} {
		if memGiB <= l.upToGiB {
			return l.conns
		}
	}

	return flexMaxConns
}

func pgSKUs() []flexSKU {
	out := make([]flexSKU, 0, pgSKUCount)
	add := func(name, tier string, vcores, mem int) {
		out = append(out, flexSKU{name, tier, vcores, mem, pgConnLimit(mem)})
	}

	for _, b := range []struct {
		n    string
		v, m int
	}{{"B1ms", 1, 2}, {"B2s", 2, 4}, {"B2ms", 2, 8}, {"B4ms", 4, 16}, {"B8ms", 8, 32}, {"B12ms", 12, 48},
		{"B16ms", 16, 64}, {"B20ms", 20, 80}} {
		add("Standard_"+b.n, tierBurstable, b.v, b.m)
	}

	suffixes := []string{"s_v3", "ds_v4", suffixDS5, suffixADS}

	for _, v := range []int{2, 4, 8, 16, 32, 48, 64} {
		for _, s := range suffixes {
			add(fmt.Sprintf("Standard_D%d%s", v, s), tierGeneralPurpose, v, v*gpMemPerCore)
		}
	}

	for _, s := range []string{suffixDS5, suffixADS} {
		add("Standard_D96"+s, tierGeneralPurpose, flexMaxVcores, gpMem96)
	}

	for _, v := range []int{2, 4, 8, 16, 20, 32, 48, moVcores64} {
		mem := v * moMemPerCore
		if v == moVcores64 {
			mem = moMem64
		}

		for _, s := range suffixes {
			if v == flexVcores20 && s == "s_v3" {
				continue
			}

			add(fmt.Sprintf("Standard_E%d%s", v, s), tierMemoryOptimized, v, mem)
		}
	}

	for _, s := range []string{suffixDS5, suffixADS} {
		add("Standard_E96"+s, tierMemoryOptimized, flexMaxVcores, moMem96)
	}

	return out
}

func mysqlSKUs() []flexSKU {
	out := make([]flexSKU, 0, mysqlSKUCount)
	add := func(name, tier string, vcores, mem int) {
		out = append(out, flexSKU{name, tier, vcores, mem, 0})
	}

	for _, b := range []struct {
		n    string
		v, m int
	}{{"B1s", 1, 1}, {"B1ms", 1, 2}, {"B2s", 2, 4}, {"B2ms", 2, 8}, {"B4ms", 4, 16}, {"B8ms", 8, 32},
		{"B12ms", 12, 48}, {"B16ms", 16, 64}, {"B20ms", 20, 80}} {
		add("Standard_"+b.n, tierBurstable, b.v, b.m)
	}

	for _, v := range []int{2, 4, 8, 16, 32, 48, moVcores64} {
		add(fmt.Sprintf("Standard_D%dds_v4", v), tierGeneralPurpose, v, v*gpMemPerCore)
		add(fmt.Sprintf("Standard_D%d%s", v, suffixADS), tierGeneralPurpose, v, v*gpMemPerCore)
	}

	add("Standard_D96"+suffixADS, tierGeneralPurpose, flexMaxVcores, gpMem96)

	for _, v := range []int{2, 4, 8, 16, flexVcores20, 32, 48, moVcores64} {
		add(fmt.Sprintf("Standard_E%dds_v4", v), tierMemoryOptimized, v, v*moMemPerCore)
	}

	for _, v := range []int{2, 4, 8, 16, 32, 48, moVcores64, flexMaxVcores} {
		add(fmt.Sprintf("Standard_E%d%s", v, suffixADS), tierMemoryOptimized, v, v*moMemPerCore)
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
		{name: cfgMaxConn, dataType: cfgInteger, def: "50", rng: "25..262143", static: true,
			desc: "Sets the maximum number of concurrent connections."},
		{name: "work_mem", dataType: cfgInteger, def: "4096", rng: "4..2097151", unit: "kB",
			desc: "Sets the amount of memory to be used by internal sort operations and hash tables."},
		{name: "maintenance_work_mem", dataType: cfgInteger, def: "65536", rng: "1024..2097151", unit: "kB",
			desc: "Sets the maximum memory to be used for maintenance operations."},
		{name: "shared_buffers", dataType: cfgInteger, def: "16384", rng: "16..1073741823", unit: "8kB",
			static: true, desc: "Sets the number of shared memory buffers used by the server."},
		{name: "effective_cache_size", dataType: cfgInteger, def: "524288", rng: "1..2147483647", unit: "8kB",
			desc: "Sets the planner's assumption about the total size of the data caches."},
		{name: "statement_timeout", dataType: cfgInteger, def: "0", rng: flexRangeNonNeg, unit: "ms",
			desc: "Sets the maximum allowed duration of any statement."},
		{name: "idle_in_transaction_session_timeout", dataType: cfgInteger, def: "0", rng: flexRangeNonNeg,
			unit: "ms", desc: "Sets the maximum allowed idle time between queries, when in a transaction."},
		{name: "lock_timeout", dataType: cfgInteger, def: "0", rng: flexRangeNonNeg, unit: "ms",
			desc: "Sets the maximum allowed duration of any wait for a lock."},
		{name: "log_min_duration_statement", dataType: cfgInteger, def: "-1", rng: "-1..2147483647", unit: "ms",
			desc: "Sets the minimum execution time above which statements will be logged."},
		{name: "checkpoint_timeout", dataType: cfgInteger, def: "600", rng: "30..86400", unit: "s",
			desc: "Sets the maximum time between automatic WAL checkpoints."},
		{name: "track_activity_query_size", dataType: cfgInteger, def: "1024", rng: "100..1048576", unit: "B",
			static: true, desc: "Sets the size reserved for pg_stat_activity.query."},
		{name: "random_page_cost", dataType: cfgNumeric, def: "4", rng: "0..1000000",
			desc: "Sets the planner's estimate of the cost of a nonsequentially fetched disk page."},
		{
			name:     "log_statement",
			dataType: cfgEnum,
			def:      aksPluginNone,
			enum:     []string{aksPluginNone, "ddl", "mod", "all"},
			desc:     "Sets the type of statements logged.",
		},
		{name: "wal_level", dataType: cfgEnum, def: "replica", enum: []string{"replica", "logical"},
			static: true, desc: "Sets the level of information written to the WAL."},
		{name: "password_encryption", dataType: cfgEnum, def: "scram-sha-256",
			enum: []string{"md5", "scram-sha-256"}, desc: "Chooses the algorithm for encrypting passwords."},
		{name: "log_connections", dataType: cfgBoolean, def: flexOff, enum: []string{flexOn, flexOff},
			desc: "Logs each successful connection."},
		{name: "log_disconnections", dataType: cfgBoolean, def: flexOff, enum: []string{flexOn, flexOff},
			desc: "Logs end of a session, including duration."},
		{name: "autovacuum", dataType: cfgBoolean, def: flexOn, enum: []string{flexOn, flexOff},
			desc: "Starts the autovacuum subprocess."},
		{name: "azure.extensions", dataType: cfgSet, def: "", allowed: pgExtensions(), static: false,
			desc: "Specifies which extensions are allowed to be created in the server."},
		{name: "server_version", dataType: "String", def: "", readOnly: true,
			desc: "Shows the server version."},
	}
}

func mysqlConfigs() []flexConfigDef {
	return []flexConfigDef{
		{name: cfgMaxConn, dataType: cfgInteger, def: "151", rng: "10..100000",
			desc: "The number of simultaneous client connections."},
		{name: "wait_timeout", dataType: cfgInteger, def: "28800", rng: "1..31536000", unit: "s",
			desc: "The number of seconds the server waits for activity on a noninteractive connection."},
		{name: "interactive_timeout", dataType: cfgInteger, def: "28800", rng: "1..31536000", unit: "s",
			desc: "The number of seconds the server waits for activity on an interactive connection."},
		{name: "max_allowed_packet", dataType: cfgInteger, def: "536870912", rng: "1024..1073741824", unit: "B",
			desc: "The maximum size of one packet or any generated/intermediate string."},
		{name: "innodb_lock_wait_timeout", dataType: cfgInteger, def: "50", rng: "1..1073741824", unit: "s",
			desc: "The length of time in seconds an InnoDB transaction waits for a row lock."},
		{name: "long_query_time", dataType: cfgNumeric, def: "10", rng: "0..31536000", unit: "s",
			desc: "Queries slower than this many seconds are counted as slow."},
		{name: "slow_query_log", dataType: cfgEnum, def: cfgOff, enum: []string{cfgOn, cfgOff},
			desc: "Whether the slow query log is enabled."},
		{name: "event_scheduler", dataType: cfgEnum, def: cfgOff, enum: []string{cfgOn, cfgOff},
			desc: "Indicates the status of the Event Scheduler."},
		{name: "require_secure_transport", dataType: cfgEnum, def: cfgOn, enum: []string{cfgOn, cfgOff},
			desc: "Whether client connections to the server are required to use some form of secure transport."},
		{
			name:     "log_bin_trust_function_creators",
			dataType: cfgEnum,
			def:      cfgOff,
			enum:     []string{cfgOn, cfgOff},
			desc:     "Whether stored function creators can be trusted not to create stored functions that cause unsafe events.",
		},
		{name: "sql_generate_invisible_primary_key", dataType: cfgEnum, def: cfgOn,
			enum: []string{cfgOn, cfgOff}, desc: "Whether GIPK mode is in effect."},
		{name: "tls_version", dataType: cfgSet, def: "TLSv1.2", allowed: "TLSv1.2,TLSv1.3", static: true,
			desc: "Which protocols the server permits for encrypted connections."},
		{name: "sql_mode", dataType: cfgSet,
			def: "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO," +
				"NO_ENGINE_SUBSTITUTION",
			allowed: "ALLOW_INVALID_DATES,ANSI_QUOTES,ERROR_FOR_DIVISION_BY_ZERO,HIGH_NOT_PRECEDENCE," +
				"IGNORE_SPACE,NO_AUTO_VALUE_ON_ZERO,NO_BACKSLASH_ESCAPES,NO_DIR_IN_CREATE,NO_ENGINE_SUBSTITUTION," +
				"NO_UNSIGNED_SUBTRACTION,NO_ZERO_DATE,NO_ZERO_IN_DATE,ONLY_FULL_GROUP_BY,PAD_CHAR_TO_FULL_LENGTH," +
				"PIPES_AS_CONCAT,REAL_AS_FLOAT,STRICT_ALL_TABLES,STRICT_TRANS_TABLES,TIME_TRUNCATE_FRACTIONAL",
			desc: "The current server SQL mode."},
		{name: "character_set_server", dataType: cfgEnum, def: flexUTF8MB4, static: true,
			enum: []string{flexUTF8MB4, "utf8mb3", "utf8", "latin1", "ascii", "binary"},
			desc: "The server's default character set."},
		{name: "time_zone", dataType: "String", def: "+00:00", desc: "The server time zone."},
		{name: "lower_case_table_names", dataType: cfgInteger, def: "1", rng: "1..2", readOnly: true,
			desc: "Whether table names are stored in lowercase."},
	}
}

func newPostgresFlavor() *flexFlavor {
	return &flexFlavor{
		ns:         namespaceDBforPostgreSQL,
		kind:       flexKindPostgres,
		hostSuffix: ".postgres.database.azure.com",
		apiVersions: []string{
			"2025-08-01", "2025-06-01-preview", "2024-11-01-preview", "2024-08-01", "2024-03-01-preview",
			"2023-12-01-preview", "2023-06-01-preview", "2023-03-01-preview", flexAPI20221201, "2022-03-08-preview",
			"2021-06-01",
		},
		versions:       []string{"11", "12", "13", "14", "15", "16", "17", "18"},
		defaultVersion: "16",
		defaultCharset: "UTF8",
		defaultCollate: "en_US.utf8",
		storageSizes: []int{
			32,
			64,
			128,
			256,
			512,
			1024,
			2048,
			4096,
			8192,
			16384,
			32768,
		},
		defaultStorage: pgDefaultStorageGiB,
		minStorage:     pgMinStorageGiB,
		maxStorage:     pgMaxStorageGiB,
		minRetention:   pgMinRetentionDays,
		maxReplicas:    pgMaxReplicas,
		maxLoginLen:    pgMaxLoginLen,
		loginRE:        regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,62}$`),
		dbNameRE:       regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_$]{0,62}$`),
		reservedLogins: []string{
			flexLoginAzureSuper,
			"azure_pg_admin",
			flexLoginAdmin,
			flexLoginAdministrator,
			flexLoginRoot,
			flexLoginGuest,
			flexLoginPublic,
		},
		systemDBs:       []string{"azure_maintenance", "azure_sys", flexKindPostgres},
		skus:            pgSKUs(),
		configs:         pgConfigs(),
		roleReplica:     "AsyncReplica",
		roleSource:      flexRolePrimary,
		subnetService:   "Microsoft.DBforPostgreSQL/flexibleServers",
		hasAuthConfig:   true,
		sourceTimeField: "pointInTimeUTC",
	}
}

func newMySQLFlavor() *flexFlavor {
	return &flexFlavor{
		ns: namespaceDBforMySQL, kind: flexKindMySQL, hostSuffix: ".mysql.database.azure.com",
		apiVersions: []string{
			"2024-12-30", "2024-10-01-preview", "2024-06-01-preview", "2023-12-30", "2023-12-01-preview",
			"2023-10-01-preview", "2023-06-30", flexAPI20221201, "2022-01-01", "2021-12-01-preview", "2021-05-01",
		},
		versions:       []string{"5.7", "8.0.21", "8.4"},
		defaultVersion: "8.0.21", defaultCharset: flexUTF8MB4, defaultCollate: "utf8mb4_0900_ai_ci",
		minStorage: mysqlMinStorageGiB, maxStorage: mysqlMaxStorageGiB, defaultStorage: mysqlMinStorageGiB,
		minRetention: 1, maxReplicas: mysqlMaxReplicas, maxLoginLen: mysqlMaxLoginLen,
		loginRE:  regexp.MustCompile(`^[A-Za-z0-9_]{1,32}$`),
		dbNameRE: regexp.MustCompile(`^[A-Za-z0-9_$-]{1,64}$`),
		reservedLogins: []string{
			flexLoginAzureSuper, flexLoginAdmin, flexLoginAdministrator, flexLoginRoot, flexLoginGuest, flexLoginPublic,
		},
		systemDBs: []string{"information_schema", flexKindMySQL, "performance_schema", "sys"}, skus: mysqlSKUs(),
		configs: mysqlConfigs(), roleReplica: "Replica", roleSource: "Source",
		subnetService: "Microsoft.DBforMySQL/flexibleServers", boolStrings: true, sourceTimeField: "restorePointInTime",
	}
}

// pgStorageTier maps a PostgreSQL storage size to its disk tier and provisioned IOPS (Premium SSD sizes).
func pgStorageTier(gb int) (string, int) {
	tiers := []struct {
		tier string
		gb   int
		iops int
	}{{"P4", 32, 120}, {"P6", 64, 240}, {"P10", 128, 500}, {"P15", 256, 1100}, {"P20", 512, 2300},
		{"P30", 1024, 5000}, {"P40", 2048, 7500}, {"P50", 4096, 7500}, {"P60", 8192, 16000},
		{"P70", 16384, 18000}, {"P80", 32768, 20000}}

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

func (c flexConfigDef) bounds() (float64, float64) {
	l, h, _ := strings.Cut(c.rng, "..")
	lo, _ := strconv.ParseFloat(l, 64)
	hi, _ := strconv.ParseFloat(h, 64)

	return lo, hi
}

// configValueProblem returns why value is not allowed for def, or "" when it is.
func configValueProblem(def flexConfigDef, value string) string {
	switch def.dataType {
	case cfgInteger, cfgNumeric:
		lo, hi := def.bounds()

		f, err := strconv.ParseFloat(value, 64)
		if err == nil && def.dataType == cfgInteger {
			_, err = strconv.ParseInt(value, 10, 64)
		}

		if err != nil || f < lo || f > hi {
			return "allowed values are " + def.allowedValues()
		}
	case cfgBoolean, cfgEnum:
		if !slices.ContainsFunc(def.enum, func(e string) bool { return strings.EqualFold(e, value) }) {
			return "allowed values are " + strings.Join(def.enum, ",")
		}
	case cfgSet:
		allowed := strings.Split(strings.ToUpper(def.allowed), ",")

		for v := range strings.SplitSeq(value, ",") {
			if v = strings.TrimSpace(strings.ToUpper(v)); v != "" && !slices.Contains(allowed, v) {
				return strconv.Quote(v) + " is not an allowed value"
			}
		}
	}

	return ""
}

func (c flexConfigDef) allowedValues() string {
	switch {
	case c.allowed != "":
		return c.allowed
	case len(c.enum) > 0:
		return strings.Join(c.enum, ",")
	case c.rng != "":
		lo, hi := c.bounds()

		return strconv.FormatFloat(lo, 'f', -1, 64) + "-" + strconv.FormatFloat(hi, 'f', -1, 64)
	}

	return ""
}

const (
	mysqlMaxStorageGiB = 16384
	pgMaxStorageGiB    = 32768
	flexLoginRoot      = "root"
	flexLoginPublic    = "public"
)

const (
	pgMinRetentionDays = 7
	pgMaxReplicas      = 5
	pgMaxLoginLen      = 63
	mysqlMaxReplicas   = 10
	mysqlMaxLoginLen   = 32
)
