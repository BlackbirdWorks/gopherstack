package ocimysql

import (
	"crypto/sha256"
	"encoding/hex"
	"slices"
	"strconv"
	"strings"
)

const (
	supportDBSystem = "DBSYSTEM"
	supportHeatWave = "HEATWAVECLUSTER"
	shapeFree       = "MySQL.Free"
	gib             = int64(1) << 30
	tenancyOwner    = "ocid1.tenancy.oc1..mysqldefaults"

	minConnections      = 100
	connectionsPerGB    = 16
	bufferPoolShare     = 2
	defaultBinlogExpiry = 3600
)

type shapeDef struct {
	name    string
	support string
	cpus    int
	memGB   int
}

//nolint:gochecknoglobals // immutable catalogue of documented MySQL shapes
var shapeDefs = []shapeDef{
	{shapeFree, supportDBSystem, 2, 8},
	{"MySQL.2", supportDBSystem, 2, 16},
	{"MySQL.4", supportDBSystem, 4, 32},
	{"MySQL.8", supportDBSystem, 8, 64},
	{"MySQL.16", supportDBSystem, 16, 128},
	{"MySQL.32", supportDBSystem, 32, 256},
	{"MySQL.48", supportDBSystem, 48, 384},
	{"MySQL.64", supportDBSystem, 64, 512},
	{"MySQL.256", supportDBSystem, 256, 2048},
	{"MySQL.VM.Standard.E3.1.8GB", supportDBSystem, 1, 8},
	{"MySQL.VM.Standard.E3.1.16GB", supportDBSystem, 1, 16},
	{"MySQL.VM.Standard.E3.2.32GB", supportDBSystem, 2, 32},
	{"MySQL.VM.Standard.E3.4.64GB", supportDBSystem, 4, 64},
	{"MySQL.VM.Standard.E3.8.128GB", supportDBSystem, 8, 128},
	{"MySQL.VM.Standard.E3.16.256GB", supportDBSystem, 16, 256},
	{"MySQL.VM.Standard.E3.24.384GB", supportDBSystem, 24, 384},
	{"MySQL.VM.Standard.E3.32.512GB", supportDBSystem, 32, 512},
	{"MySQL.VM.Standard.E3.48.768GB", supportDBSystem, 48, 768},
	{"MySQL.VM.Standard.E3.64.1024GB", supportDBSystem, 64, 1024},
	{"MySQL.HeatWave.VM.Standard", supportHeatWave, 32, 512},
	{"MySQL.HeatWave.VM.Standard.E3", supportHeatWave, 32, 512},
	{"MySQL.HeatWave.BM.Standard", supportHeatWave, 128, 1024},
	{"MySQL.HeatWave.BM.Standard.E3", supportHeatWave, 128, 2048},
}

func shapeByName(name string) (shapeDef, bool) {
	for _, s := range shapeDefs {
		if s.name == name {
			return s, true
		}
	}

	return shapeDef{}, false
}

type versionFamily struct {
	family   string
	image    string
	versions []string
}

//nolint:gochecknoglobals // immutable catalogue of representative MySQL releases
var versionFamilies = []versionFamily{
	{"8.0", "mysql:8.0", []string{"8.0.39", "8.0.40", "8.0.41", "8.0.42"}},
	{"8.4", "mysql:8.4", []string{"8.4.3", "8.4.4", "8.4.5", "8.4.6"}},
	{"9.x", "mysql:9", []string{"9.1.0", "9.2.0", "9.3.0", "9.4.0"}},
}

const defaultVersion = "8.4.6"

func familyOf(version string) (versionFamily, bool) {
	for _, f := range versionFamilies {
		if slices.Contains(f.versions, version) {
			return f, true
		}
	}

	return versionFamily{}, false
}

// versionTuple parses "major.minor.patch" for ordering; ok is false for anything else.
func versionTuple(v string) [3]int {
	var out [3]int

	for i, p := range strings.SplitN(v, ".", len(out)) {
		out[i], _ = strconv.Atoi(p)
	}

	return out
}

func versionBefore(a, b string) bool {
	ta, tb := versionTuple(a), versionTuple(b)

	return slices.Compare(ta[:], tb[:]) < 0
}

func defaultConfigID(shape string, ha bool) string {
	sum := sha256.Sum256([]byte(shape + strconv.FormatBool(ha)))

	return "ocid1.mysqlconfiguration.oc1..default" + hex.EncodeToString(sum[:])[:24]
}

// defaultConfigs builds the DEFAULT configurations: one standalone and, except for the free shape, one HA per
// DB system shape.
func defaultConfigs() []*Configuration {
	var out []*Configuration

	for _, s := range shapeDefs {
		if s.support != supportDBSystem {
			continue
		}

		for _, ha := range []bool{false, true} {
			if ha && s.name == shapeFree {
				continue
			}

			kind := "Standalone"
			if ha {
				kind = "HA"
			}

			out = append(out, &Configuration{
				ID:            defaultConfigID(s.name, ha),
				CompartmentID: tenancyOwner,
				Shape:         s.name,
				Type:          configDefault,
				State:         stateActive,
				Name:          s.name + "." + kind,
				Description:   "Default " + kind + " configuration for " + s.name,
				Variables:     defaultVariables(s),
				Etag:          "default",
			})
		}
	}

	return out
}

func defaultVariables(s shapeDef) map[string]any {
	return map[string]any{
		"maxConnections":          max(minConnections, s.memGB*connectionsPerGB),
		"innodbBufferPoolSize":    int64(s.memGB) * gib / bufferPoolShare,
		"autocommit":              true,
		"binlogExpireLogsSeconds": defaultBinlogExpiry,
		"sqlMode": "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE," +
			"ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION",
		"characterSetServer":   "UTF8MB4",
		"transactionIsolation": "REPEATABLE-READ",
	}
}
