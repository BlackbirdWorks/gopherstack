package timestreaminfluxdb

import (
	"encoding/json"
	"regexp"
	"slices"
	"strings"
	"time"
)

const (
	minNameLen     = 3
	maxNameLen     = 40
	minPassword    = 8
	maxPassword    = 64
	maxUsername    = 64
	maxOrgLen      = 64
	minBucketLen   = 2
	maxBucketLen   = 64
	minStorage     = 20
	maxStorage     = 16384
	minPort        = 1024
	maxPort        = 65535
	defaultPort    = 8086
	maxSubnets     = 3
	maxSecGroups   = 5
	maxTags        = 50
	maxTagKeyLen   = 128
	maxTagValueLen = 256
	maxRetention   = 365
	defaultMaxList = 20
	maxList        = 100
	minList        = 1
	maxDescription = 255
)

var (
	nameRe   = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9]*(-[a-zA-Z0-9]+)*$`)
	userRe   = regexp.MustCompile(`^[a-zA-Z]([a-zA-Z0-9]*(_[a-zA-Z0-9]+)*)?$`)
	passRe   = regexp.MustCompile(`^[a-zA-Z0-9]+$`)
	bucketRe = regexp.MustCompile(`^[^_][^"]*$`)
	subnetRe = regexp.MustCompile(`^subnet-[a-z0-9]+$`)
	sgRe     = regexp.MustCompile(`^sg-[a-z0-9]+$`)
	windowRe = regexp.MustCompile(
		`^(Mon|Tue|Wed|Thu|Fri|Sat|Sun):([01][0-9]|2[0-3]):[0-5][0-9]-` +
			`(Mon|Tue|Wed|Thu|Fri|Sat|Sun):([01][0-9]|2[0-3]):[0-5][0-9]$`,
	)
	bucketArn = regexp.MustCompile(`^[0-9a-z][0-9a-z.-]{2,62}$`)
)

func oneOf(field, v string, allowed []string) error {
	if !slices.Contains(allowed, v) {
		return invalid("%s must be one of %s", field, strings.Join(allowed, ", "))
	}

	return nil
}

func validateName(name string) error {
	if len(name) < minNameLen || len(name) > maxNameLen || !nameRe.MatchString(name) {
		return invalid("name must be %d-%d characters of letters, digits and single hyphens, starting with a letter",
			minNameLen, maxNameLen)
	}

	return nil
}

func validateCredentials(user, pass, org, bucket string) error {
	if user != "" && (len(user) > maxUsername || !userRe.MatchString(user)) {
		return invalid("username must be at most %d characters and start with a letter", maxUsername)
	}

	if pass != "" && (len(pass) < minPassword || len(pass) > maxPassword || !passRe.MatchString(pass)) {
		return invalid("password must be %d-%d alphanumeric characters", minPassword, maxPassword)
	}

	if org != "" && len(org) > maxOrgLen {
		return invalid("organization must be at most %d characters", maxOrgLen)
	}

	if bucket != "" && (len(bucket) < minBucketLen || len(bucket) > maxBucketLen || !bucketRe.MatchString(bucket)) {
		return invalid("bucket must be %d-%d characters and must not start with an underscore or contain quotes",
			minBucketLen, maxBucketLen)
	}

	return nil
}

func validatePort(p int32) error {
	if p < minPort || p > maxPort {
		return invalid("port must be between %d and %d", minPort, maxPort)
	}

	for _, r := range blockedPrt() {
		if p >= r[0] && p <= r[1] {
			return invalid("port %d is reserved", p)
		}
	}

	return nil
}

func validateNetwork(subnets, groups []string) error {
	if len(subnets) < 1 || len(subnets) > maxSubnets {
		return invalid("vpcSubnetIds must contain between 1 and %d subnets", maxSubnets)
	}

	if len(groups) < 1 || len(groups) > maxSecGroups {
		return invalid("vpcSecurityGroupIds must contain between 1 and %d security groups", maxSecGroups)
	}

	for _, s := range subnets {
		if !subnetRe.MatchString(s) {
			return invalid("%q is not a valid subnet id", s)
		}
	}

	for _, g := range groups {
		if !sgRe.MatchString(g) {
			return invalid("%q is not a valid security group id", g)
		}
	}

	return nil
}

func validateStorage(gib int32) error {
	if gib < minStorage || gib > maxStorage {
		return invalid("allocatedStorage must be between %d and %d GiB", minStorage, maxStorage)
	}

	return nil
}

func validateLogDelivery(l *LogDelivery) error {
	if l != nil && !bucketArn.MatchString(l.Bucket) {
		return invalid("logDeliveryConfiguration bucketName %q is not a valid S3 bucket name", l.Bucket)
	}

	return nil
}

func validateMaintenance(m *Maintenance) error {
	if m == nil {
		return nil
	}

	if m.Window != "" && !windowRe.MatchString(m.Window) {
		return invalid("preferredMaintenanceWindow must look like Sun:02:00-Sun:06:00")
	}

	if m.Timezone != "UTC" && !validZone(m.Timezone) {
		return invalid("timezone %q is not a valid IANA timezone", m.Timezone)
	}

	return nil
}

func validZone(name string) bool {
	if name == "" || !strings.Contains(name, "/") {
		return false
	}

	_, err := time.LoadLocation(name)

	return err == nil
}

func validateBackupConfigs(cfgs []BackupConfig) error {
	for _, c := range cfgs {
		if err := oneOf("dbBackupConfigurations type", c.Type, backupTypes()); err != nil {
			return err
		}

		if c.RetentionDays < 1 || c.RetentionDays > maxRetention {
			return invalid("backup retentionDays must be between 1 and %d", maxRetention)
		}

		if (c.Type == backupCustomSchedule) != (c.CustomSchedule != "") {
			return invalid("customSchedule is required for, and only valid with, CUSTOM_SCHEDULE backups")
		}

		if c.Type == backupCustomSchedule {
			if _, ok := parseCustomSchedule(c.CustomSchedule); !ok {
				return invalid(
					"customSchedule must be a valid cron(minute hour day-of-month month day-of-week year) expression",
				)
			}
		}
	}

	return nil
}

func validateTags(tags map[string]string) error {
	if len(tags) > maxTags {
		return fail(ErrQuotaExceeded, "a resource can have at most %d tags", maxTags)
	}

	for k, v := range tags {
		switch {
		case k == "" || len(k) > maxTagKeyLen:
			return invalid("tag keys must be between 1 and %d characters", maxTagKeyLen)
		case len(v) > maxTagValueLen:
			return invalid("tag values must be at most %d characters", maxTagValueLen)
		case strings.HasPrefix(strings.ToLower(k), "aws:"):
			return invalid("tag keys must not begin with aws:")
		}
	}

	return nil
}

func validateListArgs(maxResults int32) error {
	if maxResults != 0 && (maxResults < minList || maxResults > maxList) {
		return invalid("maxResults must be between %d and %d", minList, maxList)
	}

	return nil
}

// validateParameters checks the union shape: one engine key holding a parameter object.
func validateParameters(raw json.RawMessage) error {
	if len(raw) == 0 {
		return nil
	}

	var union map[string]json.RawMessage
	if err := json.Unmarshal(raw, &union); err != nil || len(union) != 1 {
		return invalid("parameters must set exactly one of %s", strings.Join(paramGroups(), ", "))
	}

	for engine, body := range union {
		if !slices.Contains(paramGroups(), engine) {
			return invalid("parameters must set exactly one of %s", strings.Join(paramGroups(), ", "))
		}

		var fields map[string]json.RawMessage
		if err := json.Unmarshal(body, &fields); err != nil {
			return invalid("%s parameters must be an object", engine)
		}

		if err := validateParameterFields(engine, fields); err != nil {
			return err
		}
	}

	return nil
}

func validateParameterFields(engine string, fields map[string]json.RawMessage) error {
	for name, val := range fields {
		if !paramKeys[name] {
			return invalid("%s has no parameter named %q", engine, name)
		}

		switch name {
		case "logLevel":
			if err := checkEnum(name, val, logLevels()); err != nil {
				return err
			}
		case "tracingType":
			if err := checkEnum(name, val, tracingTypes()); err != nil {
				return err
			}
		default:
			if isDurationParam(name) {
				if err := checkDuration(name, val); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func checkEnum(name string, raw json.RawMessage, allowed []string) error {
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return invalid("%s must be a string", name)
	}

	return oneOf(name, s, allowed)
}

func checkDuration(name string, raw json.RawMessage) error {
	var d struct {
		Value        *int64 `json:"value"`
		DurationType string `json:"durationType"`
	}
	if err := json.Unmarshal(raw, &d); err != nil || d.Value == nil {
		return invalid("%s must be a duration with durationType and value", name)
	}

	if *d.Value < 0 {
		return invalid("%s value must not be negative", name)
	}

	return oneOf(name+" durationType", d.DurationType, durationKind())
}

func isDurationParam(name string) bool {
	switch name {
	case "httpIdleTimeout", "httpReadHeaderTimeout", "httpReadTimeout", "httpWriteTimeout",
		"storageCacheSnapshotWriteColdDuration", "storageCompactFullWriteColdDuration",
		"storageRetentionCheckInterval", "storageWalMaxWriteDelay":
		return true
	}

	return false
}

var paramKeys = func() map[string]bool { //nolint:gochecknoglobals // immutable lookup built once
	keys := []string{
		"fluxLogEnabled", "httpIdleTimeout", "httpReadHeaderTimeout", "httpReadTimeout", "httpWriteTimeout",
		"influxqlMaxSelectBuckets", "influxqlMaxSelectPoint", "influxqlMaxSelectSeries", "logLevel",
		"metricsDisabled", "noTasks", "pprofDisabled", "queryConcurrency", "queryInitialMemoryBytes",
		"queryMaxMemoryBytes", "queryMemoryBytes", "queryQueueSize", "sessionLength", "sessionRenewDisabled",
		"storageCacheMaxMemorySize", "storageCacheSnapshotMemorySize", "storageCacheSnapshotWriteColdDuration",
		"storageCompactFullWriteColdDuration", "storageCompactThroughputBurst", "storageMaxConcurrentCompactions",
		"storageMaxIndexLogFileSize", "storageNoValidateFieldSize", "storageRetentionCheckInterval",
		"storageSeriesFileMaxConcurrentSnapshotCompactions", "storageSeriesIdSetCacheSize",
		"storageWalMaxConcurrentWrites", "storageWalMaxWriteDelay", "tracingType", "uiDisabled",
		"catalogSyncInterval", "compactionCheckInterval", "compactionCleanupWait", "compactionGen2Duration",
		"compactionMaxNumFilesPerPlan", "compactionMultipliers", "compactionRowLimit", "dataFusionConfig",
		"dataFusionMaxParquetFanout", "dataFusionNumThreads", "dataFusionRuntimeDisableLifoSlot",
		"dataFusionRuntimeEventInterval", "dataFusionRuntimeGlobalQueueInterval",
		"dataFusionRuntimeMaxBlockingThreads", "dataFusionRuntimeMaxIoEventsPerTick",
		"dataFusionRuntimeThreadKeepAlive", "dataFusionRuntimeThreadPriority", "dataFusionRuntimeType",
		"dataFusionUseCachedParquetLoader", "deleteGracePeriod", "disableParquetMemCache",
		"distinctCacheEvictionInterval", "distinctValueCacheDisableFromHistory", "execMemPoolBytes",
		"forceSnapshotMemThreshold", "gen1Duration", "gen1LookbackDuration", "hardDeleteDefaultDuration",
		"lastCacheEvictionInterval", "lastValueCacheDisableFromHistory", "logFilter", "logFormat",
		"maxHttpRequestSize", "parquetMemCachePruneInterval", "parquetMemCachePrunePercentage",
		"parquetMemCacheQueryPathDuration", "parquetMemCacheSize", "pluginRepositorySecretArn",
		"pluginRepositoryUrl", "preemptiveCacheAge", "queryFileLimit", "queryLogSize", "replicationInterval",
		"retentionCheckInterval", "snapshottedWalFilesToKeep", "tableIndexCacheConcurrencyLimit",
		"tableIndexCacheMaxEntries", "walMaxWriteBufferSize", "walReplayConcurrencyLimit", "walReplayFailOnError",
		"walSnapshotSize",
	}
	m := make(map[string]bool, len(keys))

	for _, k := range keys {
		m[k] = true
	}

	return m
}()

func blockedPrt() [][2]int32 {
	return [][2]int32{{2375, 2376}, {7788, 7799}, {8090, 8090}, {51678, 51680}}
}

func instanceTypes() []string {
	return []string{
		"db.influx.medium", "db.influx.large", "db.influx.xlarge", "db.influx.2xlarge", "db.influx.4xlarge",
		"db.influx.8xlarge", "db.influx.12xlarge", "db.influx.16xlarge", "db.influx.24xlarge",
	}
}

func storageTypes() []string {
	return []string{"InfluxIOIncludedT1", "InfluxIOIncludedT2", "InfluxIOIncludedT3"}
}

func networkTypes() []string {
	return []string{"IPV4", "DUAL"}
}

func failoverMods() []string {
	return []string{"AUTOMATIC", "NO_FAILOVER"}
}

func backupTypes() []string {
	return []string{"HOURLY", "DAILY", "WEEKLY", "MONTHLY", backupCustomSchedule, "CONTINUOUS"}
}

func logLevels() []string {
	return []string{"debug", "info", "error"}
}

func tracingTypes() []string {
	return []string{"log", "jaeger", "disabled"}
}

func durationKind() []string {
	return []string{"hours", "minutes", "seconds", "milliseconds", "days"}
}

func paramGroups() []string {
	return []string{"InfluxDBv2", "InfluxDBv3Core", "InfluxDBv3Enterprise"}
}
