package gcpcontainer

import (
	"crypto/sha256"
	"encoding/base64"
	"maps"
	"net/netip"
	"regexp"
	"slices"
	"strings"
	"time"

	"cloud.google.com/go/container/apiv1/containerpb"
)

const (
	maxLabels      = 64
	maxNodeCount   = 1000
	minDiskGb      = 10
	maxDiskGb      = 65536
	defaultDiskGb  = 100
	defaultPodsMax = 110
	maxAuthNets    = 50
	minPodsMax     = 8
	maxPodsMax     = 256
	minPodPrefix   = 8
	maxPodPrefix   = 21
	minSvcPrefix   = 16
	maxSvcPrefix   = 27
	masterPrefix   = 28
	minPassword    = 16
	maxPassword    = 32
)

var (
	labelKeyRE   = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,62}$`)
	labelValueRE = regexp.MustCompile(`^[a-z0-9_-]{0,63}$`)
	machineRE    = regexp.MustCompile(`^[a-z][a-z0-9]*(-[a-z0-9]+)+$`)
	k8sLabelRE   = regexp.MustCompile(
		`^([A-Za-z0-9]([-A-Za-z0-9_.]{0,61}[A-Za-z0-9])?/)?[A-Za-z0-9]([-A-Za-z0-9_.]{0,61}[A-Za-z0-9])?$`,
	)
	k8sValueRE = regexp.MustCompile(`^([A-Za-z0-9]([-A-Za-z0-9_.]{0,61}[A-Za-z0-9])?)?$`)
	clockRE    = regexp.MustCompile(`^([01][0-9]|2[0-3]):[0-5][0-9]$`)
)

func validLabels(field string, labels map[string]string) error {
	if len(labels) > maxLabels {
		return errInvalid(field, "At most %d labels are allowed.", maxLabels)
	}

	for k, v := range labels {
		if !labelKeyRE.MatchString(k) {
			return errInvalid(
				field,
				"Invalid label key %q: start with a lowercase letter, then at most 62 lowercase letters, "+
					"digits, underscores or hyphens.",
				k,
			)
		}

		if !labelValueRE.MatchString(v) {
			return errInvalid(
				field,
				"Invalid value %q for label %q: use at most 63 lowercase letters, digits, underscores or hyphens.",
				v,
				k,
			)
		}
	}

	return nil
}

func validNodeLabels(labels map[string]string) error {
	for k, v := range labels {
		if !k8sLabelRE.MatchString(k) || !k8sValueRE.MatchString(v) {
			return errInvalid("node_config.labels", "Invalid Kubernetes label %q=%q.", k, v)
		}
	}

	return nil
}

func validTaints(taints []*containerpb.NodeTaint) error {
	for _, t := range taints {
		if !k8sLabelRE.MatchString(t.GetKey()) || !k8sValueRE.MatchString(t.GetValue()) {
			return errInvalid("node_config.taints", "Invalid taint %q=%q.", t.GetKey(), t.GetValue())
		}

		if t.GetEffect() == containerpb.NodeTaint_EFFECT_UNSPECIFIED {
			return errInvalid("node_config.taints", "Taint %q must have an effect.", t.GetKey())
		}
	}

	return nil
}

func parseCIDR(field, s string, minBits, maxBits int) (netip.Prefix, error) {
	p, err := netip.ParsePrefix(s)
	if err != nil || !p.Addr().Is4() || p.Masked() != p {
		return netip.Prefix{}, errInvalid(field, "Invalid CIDR block %q: want an IPv4 network such as 10.0.0.0/14.", s)
	}

	if p.Bits() < minBits || p.Bits() > maxBits {
		return netip.Prefix{}, errInvalid(
			field,
			"CIDR block %q must have a prefix length between /%d and /%d.",
			s,
			minBits,
			maxBits,
		)
	}

	return p, nil
}

func validMachineType(c Compute, mt string) error {
	if mt == "" {
		return nil
	}

	if c != nil {
		if !c.MachineTypeExists(mt) {
			return errInvalid("node_config.machine_type", "Machine type %q does not exist.", mt)
		}

		return nil
	}

	if !machineRE.MatchString(mt) {
		return errInvalid("node_config.machine_type", "Invalid machine type %q.", mt)
	}

	return nil
}

func validDiskType(t string) bool {
	switch t {
	case "pd-standard", "pd-balanced", "pd-ssd":
		return true
	default:
		return strings.HasPrefix(t, "hyperdisk-")
	}
}

func validImageType(t string) bool {
	for _, v := range validImageTypes() {
		if strings.EqualFold(v, t) {
			return true
		}
	}

	return false
}

func validAutoscaling(a *containerpb.NodePoolAutoscaling) error {
	if a == nil {
		return nil
	}

	perZone := a.GetMinNodeCount() != 0 || a.GetMaxNodeCount() != 0
	total := a.GetTotalMinNodeCount() != 0 || a.GetTotalMaxNodeCount() != 0

	if !a.GetEnabled() {
		if perZone || total {
			return errInvalid("autoscaling", "Autoscaling limits can only be set when autoscaling is enabled.")
		}

		return nil
	}

	if perZone && total {
		return errInvalid("autoscaling", "Per-zone and total node limits cannot both be set.")
	}

	return validAutoscalingLimits(a, perZone || total)
}

func validAutoscalingLimits(a *containerpb.NodePoolAutoscaling, limited bool) error {
	top := max(a.GetMaxNodeCount(), a.GetTotalMaxNodeCount())

	switch {
	case a.GetMinNodeCount() < 0 || a.GetTotalMinNodeCount() < 0:
		return errInvalid("autoscaling", "Minimum node count must not be negative.")
	case !limited || top < 1:
		return errInvalid("autoscaling", "Maximum node count must be greater than 0.")
	case a.GetMaxNodeCount() < a.GetMinNodeCount() || a.GetTotalMaxNodeCount() < a.GetTotalMinNodeCount():
		return errInvalid("autoscaling", "Maximum node count must not be less than the minimum node count.")
	case top > maxNodeCount:
		return errInvalid("autoscaling", "Maximum node count must not exceed %d.", maxNodeCount)
	}

	return nil
}

func validNodeConfig(c Compute, nc *containerpb.NodeConfig) error {
	if err := validMachineType(c, nc.GetMachineType()); err != nil {
		return err
	}

	if d := int64(nc.GetDiskSizeGb()); d != 0 && (d < minDiskGb || d > maxDiskGb) {
		return errInvalid("node_config.disk_size_gb", "Disk size must be between %d and %d GB.", minDiskGb, maxDiskGb)
	}

	if t := nc.GetDiskType(); t != "" && !validDiskType(t) {
		return errInvalid("node_config.disk_type", "Invalid disk type %q.", t)
	}

	if t := nc.GetImageType(); t != "" && !validImageType(t) {
		return errInvalid("node_config.image_type", "Image type %q is not supported.", t)
	}

	if nc.GetSpot() && nc.GetPreemptible() {
		return errInvalid("node_config.spot", "Spot and preemptible cannot both be set.")
	}

	if nc.GetLocalSsdCount() < 0 {
		return errInvalid("node_config.local_ssd_count", "Local SSD count must not be negative.")
	}

	if err := validNodeLabels(nc.GetLabels()); err != nil {
		return err
	}

	if err := validLabels("node_config.resource_labels", nc.GetResourceLabels()); err != nil {
		return err
	}

	return validTaints(nc.GetTaints())
}

// podPrefixes maps the largest max-pods-per-node a per-node pod range of that size can serve.
var podPrefixes = []struct { //nolint:gochecknoglobals // immutable catalog
	maxPods int64
	prefix  int32
}{{8, 28}, {16, 27}, {32, 26}, {64, 25}, {128, 24}}

const widePodPrefix = 23

// podPrefix maps max pods per node to the per-node pod range size.
func podPrefix(maxPods int64) int32 {
	for _, p := range podPrefixes {
		if maxPods <= p.maxPods {
			return p.prefix
		}
	}

	return widePodPrefix
}

func validMaxPods(field string, c *containerpb.MaxPodsConstraint) error {
	if c == nil {
		return nil
	}

	if n := c.GetMaxPodsPerNode(); n != 0 && (n < minPodsMax || n > maxPodsMax) {
		return errInvalid(field, "Max pods per node must be between %d and %d.", minPodsMax, maxPodsMax)
	}

	return nil
}

func validMaintenance(m *containerpb.MaintenancePolicy) error {
	w := m.GetWindow()
	if w == nil {
		return nil
	}

	if d := w.GetDailyMaintenanceWindow(); d != nil && !clockRE.MatchString(d.GetStartTime()) {
		return errInvalid("maintenance_policy.window.daily_maintenance_window.start_time",
			"Invalid start time %q: want HH:MM in UTC.", d.GetStartTime())
	}

	if r := w.GetRecurringWindow(); r != nil {
		if err := validRecurring(r); err != nil {
			return err
		}
	}

	for id, ex := range w.GetMaintenanceExclusions() {
		win := ex.GetStartTime() != nil && ex.GetEndTime() != nil
		if !win || !ex.GetEndTime().AsTime().After(ex.GetStartTime().AsTime()) {
			return errInvalid("maintenance_policy.window.maintenance_exclusions",
				"Maintenance exclusion %q must end after it starts.", id)
		}
	}

	return nil
}

func validRecurring(r *containerpb.RecurringTimeWindow) error {
	win := r.GetWindow()
	if win == nil || win.GetStartTime() == nil || win.GetEndTime() == nil {
		return errInvalid("maintenance_policy.window.recurring_window.window", "Start and end times must be set.")
	}

	if !win.GetEndTime().AsTime().After(win.GetStartTime().AsTime()) {
		return errInvalid("maintenance_policy.window.recurring_window.window", "End time must be after start time.")
	}

	if r.GetRecurrence() == "" || !strings.Contains(r.GetRecurrence(), "FREQ=") {
		return errInvalid(
			"maintenance_policy.window.recurring_window.recurrence",
			"Recurrence must be an RFC 5545 RRULE.",
		)
	}

	return nil
}

func validMasterAuthNets(m *containerpb.MasterAuthorizedNetworksConfig) error {
	if len(m.GetCidrBlocks()) > maxAuthNets {
		return errInvalid(
			"master_authorized_networks_config",
			"At most %d authorized networks are allowed.",
			maxAuthNets,
		)
	}

	for _, blk := range m.GetCidrBlocks() {
		p, err := netip.ParsePrefix(blk.GetCidrBlock())
		if err != nil || !p.Addr().Is4() {
			return errInvalid(
				"master_authorized_networks_config.cidr_blocks",
				"Invalid CIDR block %q.",
				blk.GetCidrBlock(),
			)
		}
	}

	return nil
}

func validPassword(p string) error {
	if len(p) < minPassword || len(p) > maxPassword {
		return errInvalid("update.password", "Password must be between %d and %d characters.", minPassword, maxPassword)
	}

	return nil
}

func rfc3339(t time.Time) string { return t.UTC().Format("2006-01-02T15:04:05+00:00") }

// fingerprint identifies a label set so concurrent SetLabels calls can be detected.
func fingerprint(labels map[string]string) string {
	var sb strings.Builder

	for _, k := range slices.Sorted(maps.Keys(labels)) {
		sb.WriteString(k + "=" + labels[k] + ";")
	}

	sum := sha256.Sum256([]byte(sb.String()))

	return base64.StdEncoding.EncodeToString(sum[:etagBytes])
}
