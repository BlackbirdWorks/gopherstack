package gcpsql

import (
	"strconv"
	"strings"
)

// FlagInfo is one entry of flags.list.
type FlagInfo struct {
	Name                string   `json:"name"`
	Type                string   `json:"type"`
	Kind                string   `json:"kind"`
	MinValue            string   `json:"minValue,omitempty"`
	MaxValue            string   `json:"maxValue,omitempty"`
	AppliesTo           []string `json:"appliesTo"`
	AllowedStringValues []string `json:"allowedStringValues,omitempty"`
	RequiresRestart     bool     `json:"requiresRestart"`
}

func pgVersions() []string {
	return []string{"POSTGRES_14", "POSTGRES_15", "POSTGRES_16", "POSTGRES_17"}
}

func myVersions() []string { return []string{"MYSQL_8_0", "MYSQL_8_4"} }

func flagCatalog() []FlagInfo {
	pg := func(name, typ, lo, hi string, restart bool, allowed ...string) FlagInfo {
		return FlagInfo{Name: name, Type: typ, Kind: "sql#flag", MinValue: lo, MaxValue: hi, AppliesTo: pgVersions(),
			RequiresRestart: restart, AllowedStringValues: allowed}
	}

	my := func(name, typ, lo, hi string, restart bool) FlagInfo {
		return FlagInfo{Name: name, Type: typ, Kind: "sql#flag", MinValue: lo, MaxValue: hi, AppliesTo: myVersions(),
			RequiresRestart: restart}
	}

	return []FlagInfo{
		pg("max_connections", "INTEGER", "14", "262142", true),
		pg("shared_buffers", "INTEGER", "16", "1073741823", true),
		pg("work_mem", "INTEGER", "64", "2147483647", false),
		pg("maintenance_work_mem", "INTEGER", "1024", "2147483647", false),
		pg("effective_cache_size", "INTEGER", "1", "2147483647", false),
		pg("random_page_cost", "FLOAT", "0", "2147483647", false),
		pg("statement_timeout", "INTEGER", "0", "2147483647", false),
		pg("idle_in_transaction_session_timeout", "INTEGER", "0", "2147483647", false),
		pg("log_min_duration_statement", "INTEGER", "-1", "2147483647", false),
		pg("log_statement", "STRING", "", "", false, "none", "ddl", "mod", "all"),
		pg("log_connections", "BOOLEAN", "", "", false),
		pg("timezone", "STRING", "", "", false),
		pg("autovacuum", "BOOLEAN", "", "", false),
		pg("autovacuum_naptime", "INTEGER", "1", "2147483", false),
		pg("autovacuum_max_workers", "INTEGER", "1", "262143", true),
		pg("checkpoint_timeout", "INTEGER", "30", "86400", false),
		pg("max_wal_size", "INTEGER", "2", "2147483647", false),
		pg("min_wal_size", "INTEGER", "2", "2147483647", false),
		pg("lock_timeout", "INTEGER", "0", "2147483647", false),
		pg("default_statistics_target", "INTEGER", "1", "10000", false),
		pg("effective_io_concurrency", "INTEGER", "0", "1000", false),
		pg("max_worker_processes", "INTEGER", "0", "262143", true),
		pg("max_parallel_workers", "INTEGER", "0", "1024", false),
		pg("max_parallel_workers_per_gather", "INTEGER", "0", "1024", false),
		pg("log_min_messages", "STRING", "", "", false, "debug5", "debug4", "debug3", "debug2", "debug1", "info",
			"notice", "warning", "error", "log", "fatal", "panic"),
		pg("log_checkpoints", "BOOLEAN", "", "", false),
		pg("log_disconnections", "BOOLEAN", "", "", false),
		pg("log_lock_waits", "BOOLEAN", "", "", false),
		pg("log_temp_files", "INTEGER", "-1", "2147483647", false),
		pg("temp_file_limit", "INTEGER", "-1", "2147483647", false),
		pg("track_activity_query_size", "INTEGER", "100", "1048576", true),
		pg("idle_session_timeout", "INTEGER", "0", "2147483647", false),
		my("max_connections", "INTEGER", "1", "100000", false),
		my("innodb_buffer_pool_size", "INTEGER", "5242880", "9223372036854775807", true),
		my("character_set_server", "STRING", "", "", true),
		my("sql_mode", "STRING", "", "", false),
		my("slow_query_log", "BOOLEAN", "", "", false),
		my("long_query_time", "FLOAT", "0", "31536000", false),
		my("wait_timeout", "INTEGER", "1", "31536000", false),
		my("max_allowed_packet", "INTEGER", "1024", "1073741824", false),
		my("group_concat_max_len", "INTEGER", "4", "9223372036854775807", false),
		my("local_infile", "BOOLEAN", "", "", false),
		my("event_scheduler", "BOOLEAN", "", "", false),
		my("innodb_lock_wait_timeout", "INTEGER", "1", "1073741824", false),
		my("interactive_timeout", "INTEGER", "1", "31536000", false),
		my("net_read_timeout", "INTEGER", "1", "31536000", false),
		my("net_write_timeout", "INTEGER", "1", "31536000", false),
		my("tmp_table_size", "INTEGER", "1024", "9223372036854775807", false),
		my("max_heap_table_size", "INTEGER", "16384", "9223372036854775807", false),
		my("sort_buffer_size", "INTEGER", "32768", "9223372036854775807", false),
		my("join_buffer_size", "INTEGER", "128", "9223372036854775807", false),
		my("table_open_cache", "INTEGER", "1", "524288", false),
		my("thread_cache_size", "INTEGER", "0", "16384", false),
		my("innodb_flush_log_at_trx_commit", "INTEGER", "0", "2", false),
		my("sync_binlog", "INTEGER", "0", "4294967295", false),
		my("binlog_expire_logs_seconds", "INTEGER", "0", "4294967295", false),
		my("log_bin_trust_function_creators", "BOOLEAN", "", "", false),
		my("general_log", "BOOLEAN", "", "", false),
		my("time_zone", "STRING", "", "", false),
	}
}

// ListFlags returns the flags that apply to databaseVersion (all when empty).
func ListFlags(databaseVersion string) []FlagInfo {
	var out []FlagInfo

	for _, f := range flagCatalog() {
		if databaseVersion == "" || contains(f.AppliesTo, strings.ToUpper(databaseVersion)) {
			out = append(out, f)
		}
	}

	return out
}

// TierInfo is one entry of tiers.list.
type TierInfo struct {
	Tier      string   `json:"tier"`
	Kind      string   `json:"kind"`
	RAM       string   `json:"RAM"`
	DiskQuota string   `json:"DiskQuota"`
	Region    []string `json:"region"`
}

const (
	mib       = int64(1 << 20)
	microMiB  = 614
	smallMiB  = 1740
	tierCount = 17
	diskQuota = "30000000000000"
)

var tierRegions = []string{ //nolint:gochecknoglobals // immutable
	"us-central1", "us-east1", "us-east4", "us-west1", "europe-west1", "europe-west4", "asia-east1", "asia-northeast1",
}

// ListTiers returns the machine tiers the emulator knows.
func ListTiers() []TierInfo {
	tiers := make([]TierInfo, 0, tierCount)
	tiers = append(tiers,
		TierInfo{Tier: "db-f1-micro", RAM: fmtInt(microMiB * mib)},
		TierInfo{Tier: "db-g1-small", RAM: fmtInt(smallMiB * mib)})

	for _, n := range []int64{1, 2, 4, 8, 16, 32, 64, 96} {
		tiers = append(tiers, TierInfo{Tier: "db-n1-standard-" + fmtInt(n), RAM: fmtInt(n * standardMiB * mib)})
	}

	for _, n := range []int64{2, 4, 8, 16, 32, 64, 96} {
		tiers = append(tiers, TierInfo{Tier: "db-n1-highmem-" + fmtInt(n), RAM: fmtInt(n * highmemMiB * mib)})
	}

	for i := range tiers {
		tiers[i].Kind, tiers[i].DiskQuota, tiers[i].Region = "sql#tier", diskQuota, tierRegions
	}

	return tiers
}

func fmtInt(n int64) string { return strconv.FormatInt(n, 10) }
