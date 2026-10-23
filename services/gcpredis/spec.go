package gcpredis

import (
	"fmt"
	"hash/fnv"
	"strings"
	"time"

	"cloud.google.com/go/redis/apiv1/redispb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	minMemoryGB       = 1
	maxMemoryGB       = 300
	maxReplicas       = 5
	defaultReadRepl   = 2
	maxDisplayName    = 80
	maxLabels         = 64
	maxLabelLen       = 63
	maxPolicyDesc     = 512
	maxWindows        = 1
	hoursPerDay       = 24
	projectNumberMod  = 1_000_000_000_000
	reservedRangeBits = 256
	sixHours          = 6 * time.Hour
	twelveHours       = 12 * time.Hour
	readReplicaMin    = 5
)

func derefClone(in *redispb.Instance) *redispb.Instance {
	out, _ := proto.Clone(in).(*redispb.Instance)

	return out
}

func clearOutputOnly(pb *redispb.Instance) {
	pb.Host, pb.Port, pb.StatusMessage = "", 0, ""
	pb.ServerCaCerts, pb.Nodes = nil, nil
	pb.ReadEndpoint, pb.ReadEndpointPort = "", 0
	pb.PersistenceIamIdentity, pb.MaintenanceSchedule = "", nil
	pb.SuspensionReasons, pb.AvailableMaintenanceVersions = nil, nil
}

// buildInstance turns a CreateInstance payload into a CREATING instance with defaults applied.
func buildInstance(n resName, in *redispb.Instance, now time.Time) (*redispb.Instance, error) {
	pb := derefClone(in)
	clearOutputOnly(pb)

	pb.Name = n.full()
	pb.CreateTime = timestamppb.New(now)
	pb.State = redispb.Instance_CREATING

	if err := applyDefaults(n, pb, now); err != nil {
		return nil, err
	}

	if err := validateSpec(n.location, pb); err != nil {
		return nil, err
	}

	pb.PersistenceIamIdentity = persistenceIdentity(n.project)
	stampPolicy(pb.GetMaintenancePolicy(), now)

	return pb, nil
}

func applyDefaults(n resName, pb *redispb.Instance, now time.Time) error {
	if pb.GetTier() != redispb.Instance_BASIC && pb.GetTier() != redispb.Instance_STANDARD_HA {
		return errInvalid("tier must be BASIC or STANDARD_HA.")
	}

	setDefaultStrings(n, pb)

	if pb.GetConnectMode() == redispb.Instance_CONNECT_MODE_UNSPECIFIED {
		pb.ConnectMode = redispb.Instance_DIRECT_PEERING
	}

	if pb.GetTransitEncryptionMode() == redispb.Instance_TRANSIT_ENCRYPTION_MODE_UNSPECIFIED {
		pb.TransitEncryptionMode = redispb.Instance_DISABLED
	}

	if pb.GetReadReplicasMode() == redispb.Instance_READ_REPLICAS_MODE_UNSPECIFIED {
		pb.ReadReplicasMode = redispb.Instance_READ_REPLICAS_DISABLED
	}

	applyReplicaDefaults(pb)

	if pb.GetConnectMode() == redispb.Instance_DIRECT_PEERING && pb.GetReservedIpRange() == "" {
		pb.ReservedIpRange = reservedRange(pb.GetName())
	}

	return applyPersistenceDefaults(pb, now)
}

func setDefaultStrings(n resName, pb *redispb.Instance) {
	if pb.GetRedisVersion() == "" {
		pb.RedisVersion = defaultVersion
	}

	if pb.GetAuthorizedNetwork() == "" {
		pb.AuthorizedNetwork = "projects/" + n.project + "/global/networks/default"
	}

	if pb.GetLocationId() == "" {
		pb.LocationId = n.location + "-" + zoneSuffixes[0]
	}

	if pb.GetTier() == redispb.Instance_STANDARD_HA && pb.GetAlternativeLocationId() == "" {
		pb.AlternativeLocationId = n.location + "-" + zoneSuffixes[1]
	}

	pb.CurrentLocationId = pb.GetLocationId()
}

func applyReplicaDefaults(pb *redispb.Instance) {
	if pb.GetReplicaCount() != 0 || pb.GetTier() != redispb.Instance_STANDARD_HA {
		return
	}

	if pb.GetReadReplicasMode() == redispb.Instance_READ_REPLICAS_ENABLED {
		pb.ReplicaCount = defaultReadRepl
	} else {
		pb.ReplicaCount = 1
	}
}

func applyPersistenceDefaults(pb *redispb.Instance, now time.Time) error {
	pc := pb.GetPersistenceConfig()
	if pc == nil {
		pb.PersistenceConfig = &redispb.PersistenceConfig{PersistenceMode: redispb.PersistenceConfig_DISABLED}

		return nil
	}

	switch pc.GetPersistenceMode() {
	case redispb.PersistenceConfig_PERSISTENCE_MODE_UNSPECIFIED, redispb.PersistenceConfig_DISABLED:
		pb.PersistenceConfig = &redispb.PersistenceConfig{PersistenceMode: redispb.PersistenceConfig_DISABLED}
	case redispb.PersistenceConfig_RDB:
		period := snapshotPeriods[pc.GetRdbSnapshotPeriod()]
		if period == 0 {
			return errInvalid("rdb_snapshot_period is required when persistence_mode is RDB.")
		}

		start := now
		if pc.GetRdbSnapshotStartTime() != nil {
			start = pc.GetRdbSnapshotStartTime().AsTime()
		}

		pc.RdbSnapshotStartTime = timestamppb.New(start)
		pc.RdbNextSnapshotTime = timestamppb.New(start.Add(period))
	}

	return nil
}

//nolint:gochecknoglobals // immutable catalog
var snapshotPeriods = map[redispb.PersistenceConfig_SnapshotPeriod]time.Duration{
	redispb.PersistenceConfig_ONE_HOUR:                    time.Hour,
	redispb.PersistenceConfig_SIX_HOURS:                   sixHours,
	redispb.PersistenceConfig_TWELVE_HOURS:                twelveHours,
	redispb.PersistenceConfig_SNAPSHOT_PERIOD_UNSPECIFIED: 0,
	redispb.PersistenceConfig_TWENTY_FOUR_HOURS:           hoursPerDay * time.Hour,
}

// validateSpec checks every user-settable field of an instance in region.
func validateSpec(region string, pb *redispb.Instance) error {
	checks := []func() error{
		func() error { return validateBasics(pb) },
		func() error { return validateLocations(region, pb) },
		func() error { return validateReplicas(pb) },
		func() error { return validateConfigs(pb.GetRedisConfigs(), pb.GetRedisVersion(), pb.GetMemorySizeGb()) },
		func() error { return validateMaintenance(pb.GetMaintenancePolicy()) },
	}

	for _, check := range checks {
		if err := check(); err != nil {
			return err
		}
	}

	return nil
}

func validateBasics(pb *redispb.Instance) error {
	if pb.GetMemorySizeGb() < minMemoryGB || pb.GetMemorySizeGb() > maxMemoryGB {
		return errInvalid("memory_size_gb must be between %d and %d.", minMemoryGB, maxMemoryGB)
	}

	if versionRank(pb.GetRedisVersion()) < 0 {
		return errInvalid("redis_version %q is not supported.", pb.GetRedisVersion())
	}

	if len(pb.GetDisplayName()) > maxDisplayName {
		return errInvalid("display_name must be at most %d characters.", maxDisplayName)
	}

	if len(pb.GetLabels()) > maxLabels {
		return errInvalid("At most %d labels are allowed.", maxLabels)
	}

	for k, v := range pb.GetLabels() {
		if k == "" || len(k) > maxLabelLen || len(v) > maxLabelLen {
			return errInvalid("Label %q is invalid: keys and values are at most %d characters.", k, maxLabelLen)
		}
	}

	return nil
}

func validateLocations(region string, pb *redispb.Instance) error {
	if !zoneIn(pb.GetLocationId(), region) {
		return errInvalid("location_id %q is not a zone of %s.", pb.GetLocationId(), region)
	}

	alt := pb.GetAlternativeLocationId()
	if pb.GetTier() == redispb.Instance_BASIC {
		if alt != "" {
			return errInvalid("alternative_location_id is only valid for the STANDARD_HA tier.")
		}

		return nil
	}

	if !zoneIn(alt, region) || alt == pb.GetLocationId() {
		return errInvalid("alternative_location_id must be a zone of %s other than location_id.", region)
	}

	return nil
}

func validateReplicas(pb *redispb.Instance) error {
	enabled := pb.GetReadReplicasMode() == redispb.Instance_READ_REPLICAS_ENABLED

	switch {
	case pb.GetTier() == redispb.Instance_BASIC && (enabled || pb.GetReplicaCount() != 0):
		return errInvalid("Replicas and read replicas require the STANDARD_HA tier.")
	case enabled && (pb.GetReplicaCount() < 1 || pb.GetReplicaCount() > maxReplicas):
		return errInvalid("replica_count must be between 1 and %d when read replicas are enabled.", maxReplicas)
	case enabled && majorVersion(pb.GetRedisVersion()) < readReplicaMin:
		return errInvalid("Read replicas require Redis 5.0 or newer.")
	case !enabled && pb.GetTier() == redispb.Instance_STANDARD_HA && pb.GetReplicaCount() != 1:
		return errInvalid("replica_count must be 1 unless read replicas are enabled.")
	}

	return nil
}

func validateMaintenance(mp *redispb.MaintenancePolicy) error {
	if mp == nil {
		return nil
	}

	if len(mp.GetDescription()) > maxPolicyDesc {
		return errInvalid("maintenance_policy.description must be at most %d characters.", maxPolicyDesc)
	}

	if len(mp.GetWeeklyMaintenanceWindow()) > maxWindows {
		return errInvalid("maintenance_policy allows at most %d weekly window.", maxWindows)
	}

	for _, w := range mp.GetWeeklyMaintenanceWindow() {
		if w.GetDay() == 0 {
			return errInvalid("weekly_maintenance_window.day is required.")
		}

		if h := w.GetStartTime().GetHours(); h < 0 || h >= hoursPerDay {
			return errInvalid("weekly_maintenance_window.start_time.hours must be 0-23.")
		}
	}

	return nil
}

func persistenceIdentity(project string) string {
	h := fnv.New64a()
	_, _ = h.Write([]byte(project))

	return fmt.Sprintf("serviceAccount:service-%012d@cloud-redis.iam.gserviceaccount.com", h.Sum64()%projectNumberMod)
}

func reservedRange(name string) string {
	h := fnv.New32a()
	_, _ = h.Write([]byte(name))
	sum := h.Sum32()

	return fmt.Sprintf("10.%d.%d.0/29", sum%reservedRangeBits, sum>>8%reservedRangeBits)
}

// deriveTopology recomputes nodes and the read endpoint from the tier, replicas and host.
func deriveTopology(pb *redispb.Instance) {
	pb.Nodes = nil
	pb.ReadEndpoint, pb.ReadEndpointPort = "", 0

	if pb.GetTier() != redispb.Instance_STANDARD_HA {
		return
	}

	region := regionOfZone(pb.GetLocationId())
	zones := []string{pb.GetCurrentLocationId(), otherZone(pb)}

	for i := range int(pb.GetReplicaCount()) + 1 {
		zone := zones[min(i, 1)]
		if i > 1 {
			zone = zonesOf(region)[i%len(zoneSuffixes)]
		}

		pb.Nodes = append(pb.Nodes, &redispb.NodeInfo{Id: fmt.Sprintf("node-%d", i), Zone: zone})
	}

	if pb.GetReadReplicasMode() == redispb.Instance_READ_REPLICAS_ENABLED {
		pb.ReadEndpoint, pb.ReadEndpointPort = pb.GetHost(), pb.GetPort()
	}
}

func otherZone(pb *redispb.Instance) string {
	if pb.GetCurrentLocationId() == pb.GetLocationId() {
		return pb.GetAlternativeLocationId()
	}

	return pb.GetLocationId()
}

func regionOfZone(zone string) string {
	if i := strings.LastIndex(zone, "-"); i > 0 {
		return zone[:i]
	}

	return zone
}
