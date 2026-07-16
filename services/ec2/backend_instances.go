package ec2

import (
	"encoding/base64"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
)

// GetConsoleOutput returns a base64-encoded console output for an instance.
// Instances in this mock don't generate real console output; returns empty string.
func (b *InMemoryBackend) GetConsoleOutput(instanceID string) (string, time.Time, error) {
	if instanceID == "" {
		return "", time.Time{}, fmt.Errorf("%w: InstanceId is required", ErrInvalidParameter)
	}

	b.mu.RLock("GetConsoleOutput")
	defer b.mu.RUnlock()

	if _, ok := b.instances.Get(instanceID); !ok {
		return "", time.Time{}, fmt.Errorf("%w: %s", ErrInstanceNotFound, instanceID)
	}

	output := base64.StdEncoding.EncodeToString([]byte("[ EC2 mock console ]\n"))

	return output, time.Now().UTC(), nil
}

// ---- ModifyInstanceMetadataOptions ----

// IMDSOptions holds per-instance IMDS configuration.
type IMDSOptions struct {
	HTTPTokens              string `json:"httpTokens,omitempty"`
	HTTPEndpoint            string `json:"httpEndpoint,omitempty"`
	InstanceMetadataTags    string `json:"instanceMetadataTags,omitempty"`
	State                   string `json:"state,omitempty"`
	HTTPPutResponseHopLimit int    `json:"httpPutResponseHopLimit,omitempty"`
}

// ModifyInstanceMetadataOptions updates IMDS settings for an instance.
func (b *InMemoryBackend) ModifyInstanceMetadataOptions(
	instanceID, httpTokens, httpEndpoint, instanceMetadataTags string,
	hopLimit int,
) (*IMDSOptions, error) {
	if instanceID == "" {
		return nil, fmt.Errorf("%w: InstanceId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyInstanceMetadataOptions")
	defer b.mu.Unlock()

	inst, ok := b.instances.Get(instanceID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrInstanceNotFound, instanceID)
	}

	// Persist tokens setting on instance for DescribeInstances accuracy.
	if httpTokens != "" {
		inst.MetadataOptionsTokens = httpTokens
	}
	if httpEndpoint != "" {
		inst.MetadataOptionsState = httpEndpoint
	}

	opts := &IMDSOptions{
		HTTPTokens:              inst.MetadataOptionsTokens,
		HTTPEndpoint:            inst.MetadataOptionsState,
		HTTPPutResponseHopLimit: hopLimit,
		InstanceMetadataTags:    instanceMetadataTags,
		State:                   "applied",
	}
	if opts.HTTPTokens == "" {
		opts.HTTPTokens = "optional"
	}
	if opts.HTTPEndpoint == "" {
		opts.HTTPEndpoint = "enabled"
	}
	if opts.HTTPPutResponseHopLimit == 0 {
		opts.HTTPPutResponseHopLimit = 1
	}

	b.instanceIMDSOptions[instanceID] = opts

	return opts, nil
}

// ---- InstanceMetadata account defaults ----

// GetInstanceMetadataDefaults returns account-level IMDS defaults.
func (b *InMemoryBackend) GetInstanceMetadataDefaults() *InstanceMetadataDefaults {
	b.mu.RLock("GetInstanceMetadataDefaults")
	defer b.mu.RUnlock()

	if b.instanceMetadataDefaults == nil {
		return &InstanceMetadataDefaults{
			HTTPTokens:              "no-preference",
			HTTPEndpoint:            "enabled",
			HTTPPutResponseHopLimit: 0,
			InstanceMetadataTags:    "no-preference",
		}
	}
	cp := *b.instanceMetadataDefaults

	return &cp
}

// ModifyInstanceMetadataDefaults updates account-level IMDS defaults.
func (b *InMemoryBackend) ModifyInstanceMetadataDefaults(
	httpTokens, httpEndpoint, instanceMetadataTags string,
	hopLimit int,
) error {
	b.mu.Lock("ModifyInstanceMetadataDefaults")
	defer b.mu.Unlock()

	d := &InstanceMetadataDefaults{}
	if b.instanceMetadataDefaults != nil {
		*d = *b.instanceMetadataDefaults
	}
	if httpTokens != "" {
		d.HTTPTokens = httpTokens
	}
	if httpEndpoint != "" {
		d.HTTPEndpoint = httpEndpoint
	}
	if instanceMetadataTags != "" {
		d.InstanceMetadataTags = instanceMetadataTags
	}
	if hopLimit > 0 {
		d.HTTPPutResponseHopLimit = hopLimit
	}
	b.instanceMetadataDefaults = d

	return nil
}

// ---- Instance credit specifications ----

// InstanceCreditSpec holds the CPU credit configuration for burstable instances.
type InstanceCreditSpec struct {
	InstanceID string `json:"instanceID,omitempty"`
	CPUCredits string `json:"cpuCredits,omitempty"` // "standard" or "unlimited"
}

// DescribeInstanceCreditSpecifications returns CPU credit specs for the given instances.
func (b *InMemoryBackend) DescribeInstanceCreditSpecifications(ids []string) []InstanceCreditSpec {
	b.mu.RLock("DescribeInstanceCreditSpecifications")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(ids))
	for _, id := range ids {
		filter[id] = true
	}

	var out []InstanceCreditSpec
	for _, inst := range b.instances.All() {
		if len(filter) > 0 && !filter[inst.ID] {
			continue
		}
		credits := b.instanceCreditSpecs[inst.ID]
		if credits == "" {
			// Default: burstable types use "standard", others return nothing in real AWS
			// but we return "standard" to keep responses non-empty.
			credits = "standard"
		}
		out = append(out, InstanceCreditSpec{InstanceID: inst.ID, CPUCredits: credits})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].InstanceID < out[j].InstanceID })

	return out
}

// ModifyInstanceCreditSpecification updates the CPU credit spec for an instance.
func (b *InMemoryBackend) ModifyInstanceCreditSpecification(
	instanceID, cpuCredits string,
) error {
	if instanceID == "" {
		return fmt.Errorf("%w: InstanceId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyInstanceCreditSpecification")
	defer b.mu.Unlock()

	if _, ok := b.instances.Get(instanceID); !ok {
		return fmt.Errorf("%w: %s", ErrInstanceNotFound, instanceID)
	}
	b.instanceCreditSpecs[instanceID] = cpuCredits

	return nil
}

// ---- DescribeInstanceTopology ----

// InstanceTopologyItem holds topology info for an instance.
type InstanceTopologyItem struct {
	InstanceID       string   `json:"instanceID,omitempty"`
	InstanceType     string   `json:"instanceType,omitempty"`
	GroupName        string   `json:"groupName,omitempty"`
	AvailabilityZone string   `json:"availabilityZone,omitempty"`
	ZoneID           string   `json:"zoneID,omitempty"`
	NetworkNodes     []string `json:"networkNodes,omitempty"`
}

// DescribeInstanceTopology returns placement topology info for instances.
func (b *InMemoryBackend) DescribeInstanceTopology(ids []string) []InstanceTopologyItem {
	b.mu.RLock("DescribeInstanceTopology")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(ids))
	for _, id := range ids {
		filter[id] = true
	}

	var out []InstanceTopologyItem
	for _, inst := range b.instances.All() {
		if len(filter) > 0 && !filter[inst.ID] {
			continue
		}
		az := b.Region + "a"
		if sub, ok := b.subnets.Get(inst.SubnetID); ok && sub.AvailabilityZone != "" {
			az = sub.AvailabilityZone
		}
		out = append(out, InstanceTopologyItem{
			InstanceID:       inst.ID,
			InstanceType:     inst.InstanceType,
			AvailabilityZone: az,
			ZoneID:           az + "1",
			NetworkNodes:     []string{"nn-" + inst.ID[:8]},
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].InstanceID < out[j].InstanceID })

	return out
}

// ---- MonitorInstances / UnmonitorInstances ----

// MonitoringState holds the monitoring state for an instance.
type MonitoringState struct {
	InstanceID string `json:"instanceID,omitempty"`
	State      string `json:"state,omitempty"` // "pending" | "enabled" | "disabling" | "disabled"
}

// MonitorInstances enables detailed monitoring for the given instances.
func (b *InMemoryBackend) MonitorInstances(instanceIDs []string) ([]MonitoringState, error) {
	if len(instanceIDs) == 0 {
		return nil, fmt.Errorf("%w: at least one InstanceId is required", ErrInvalidParameter)
	}

	b.mu.Lock("MonitorInstances")
	defer b.mu.Unlock()

	var out []MonitoringState
	for _, id := range instanceIDs {
		if _, ok := b.instances.Get(id); !ok {
			return nil, fmt.Errorf("%w: %s", ErrInstanceNotFound, id)
		}
		b.instanceMonitoring[id] = stateMonitoringEnabled
		out = append(out, MonitoringState{InstanceID: id, State: stateMonitoringEnabled})
	}

	return out, nil
}

// UnmonitorInstances disables detailed monitoring for the given instances.
func (b *InMemoryBackend) UnmonitorInstances(instanceIDs []string) ([]MonitoringState, error) {
	if len(instanceIDs) == 0 {
		return nil, fmt.Errorf("%w: at least one InstanceId is required", ErrInvalidParameter)
	}

	b.mu.Lock("UnmonitorInstances")
	defer b.mu.Unlock()

	var out []MonitoringState
	for _, id := range instanceIDs {
		if _, ok := b.instances.Get(id); !ok {
			return nil, fmt.Errorf("%w: %s", ErrInstanceNotFound, id)
		}
		b.instanceMonitoring[id] = stateMonitoringDisabled
		out = append(out, MonitoringState{InstanceID: id, State: stateMonitoringDisabled})
	}

	return out, nil
}

// ---- Network Interface attribute ----

// NIAttributeResult holds the result of DescribeNetworkInterfaceAttribute.
type NIAttributeResult struct {
	NetworkInterfaceID string   `json:"networkInterfaceID,omitempty"`
	Description        string   `json:"description,omitempty"`
	GroupIDs           []string `json:"groupIDs,omitempty"`
	SourceDestCheck    bool     `json:"sourceDestCheck,omitempty"`
}

// EnableSerialConsoleAccess enables serial console access for the account.
func (b *InMemoryBackend) EnableSerialConsoleAccess() {
	b.mu.Lock("EnableSerialConsoleAccess")
	defer b.mu.Unlock()

	b.serialConsoleAccess = true
}

// DisableSerialConsoleAccess disables serial console access for the account.
func (b *InMemoryBackend) DisableSerialConsoleAccess() {
	b.mu.Lock("DisableSerialConsoleAccess")
	defer b.mu.Unlock()

	b.serialConsoleAccess = false
}

// GetSerialConsoleAccessStatus returns whether serial console access is enabled.
func (b *InMemoryBackend) GetSerialConsoleAccessStatus() bool {
	b.mu.RLock("GetSerialConsoleAccessStatus")
	defer b.mu.RUnlock()

	return b.serialConsoleAccess
}

// ---- VGW route propagation ----

// GetDefaultCreditSpecification returns the account-level default CPU credit spec.
func (b *InMemoryBackend) GetDefaultCreditSpecification() string {
	b.mu.RLock("GetDefaultCreditSpecification")
	defer b.mu.RUnlock()

	if b.defaultCreditSpec == "" {
		return stateDefaultCredit
	}

	return b.defaultCreditSpec
}

// ModifyDefaultCreditSpecification sets the account-level default CPU credit spec.
func (b *InMemoryBackend) ModifyDefaultCreditSpecification(cpuCredits string) error {
	if cpuCredits == "" {
		return fmt.Errorf("%w: CpuCredits is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyDefaultCreditSpecification")
	defer b.mu.Unlock()

	b.defaultCreditSpec = cpuCredits

	return nil
}

// ---- Replace root volume tasks ----

// CreateInstanceConnectEndpoint creates a new Instance Connect Endpoint.
func (b *InMemoryBackend) CreateInstanceConnectEndpoint(
	subnetID string,
	securityGroupIDs []string,
	preserveClientIP bool,
) (*InstanceConnectEndpoint, error) {
	if subnetID == "" {
		return nil, fmt.Errorf("%w: SubnetId is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateInstanceConnectEndpoint")
	defer b.mu.Unlock()

	subnet, ok := b.subnets.Get(subnetID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrSubnetNotFound, subnetID)
	}

	id := "eice-" + uuid.New().String()[:8]
	ep := &InstanceConnectEndpoint{
		InstanceConnectEndpointID:  id,
		InstanceConnectEndpointARN: "arn:aws:ec2:" + b.Region + ":" + b.AccountID + ":instance-connect-endpoint/" + id,
		SubnetID:                   subnetID,
		VPCID:                      subnet.VPCID,
		SecurityGroupIDs:           securityGroupIDs,
		State:                      stateActive,
		PreserveClientIP:           preserveClientIP,
		CreateTime:                 time.Now().UTC(),
	}
	b.instanceConnectEndpoints.Put(ep)

	return ep, nil
}

// DeleteInstanceConnectEndpoint removes an Instance Connect Endpoint.
func (b *InMemoryBackend) DeleteInstanceConnectEndpoint(id string) error {
	if id == "" {
		return fmt.Errorf("%w: InstanceConnectEndpointId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteInstanceConnectEndpoint")
	defer b.mu.Unlock()

	if _, ok := b.instanceConnectEndpoints.Get(id); !ok {
		return fmt.Errorf("%w: %s", ErrInstanceConnectEndpointNotFound, id)
	}
	b.instanceConnectEndpoints.Delete(id)

	return nil
}

// DescribeInstanceConnectEndpoints returns endpoints, optionally filtered by IDs.
func (b *InMemoryBackend) DescribeInstanceConnectEndpoints(
	ids []string,
) []*InstanceConnectEndpoint {
	b.mu.RLock("DescribeInstanceConnectEndpoints")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(ids))
	for _, id := range ids {
		filter[id] = true
	}

	var out []*InstanceConnectEndpoint
	for _, ep := range b.instanceConnectEndpoints.All() {
		if len(filter) > 0 && !filter[ep.InstanceConnectEndpointID] {
			continue
		}
		cp := *ep
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].InstanceConnectEndpointID < out[j].InstanceConnectEndpointID
	})

	return out
}

// ModifyInstanceConnectEndpoint modifies preserveClientIP for an endpoint.
func (b *InMemoryBackend) ModifyInstanceConnectEndpoint(id string, preserveClientIP bool) error {
	if id == "" {
		return fmt.Errorf("%w: InstanceConnectEndpointId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyInstanceConnectEndpoint")
	defer b.mu.Unlock()

	ep, ok := b.instanceConnectEndpoints.Get(id)
	if !ok {
		return fmt.Errorf("%w: %s", ErrInstanceConnectEndpointNotFound, id)
	}
	ep.PreserveClientIP = preserveClientIP

	return nil
}

// ---- Instance Event Windows ----

// CreateInstanceEventWindow creates a maintenance event window.
func (b *InMemoryBackend) CreateInstanceEventWindow(
	name, cronExpression string,
) (*InstanceEventWindow, error) {
	b.mu.Lock("CreateInstanceEventWindow")
	defer b.mu.Unlock()

	ew := &InstanceEventWindow{
		InstanceEventWindowID: "iew-" + uuid.New().String()[:8],
		Name:                  name,
		CronExpression:        cronExpression,
		State:                 stateActive,
	}
	b.instanceEventWindows.Put(ew)

	return ew, nil
}

// DeleteInstanceEventWindow removes an event window.
func (b *InMemoryBackend) DeleteInstanceEventWindow(id string) error {
	if id == "" {
		return fmt.Errorf("%w: InstanceEventWindowId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteInstanceEventWindow")
	defer b.mu.Unlock()

	if _, ok := b.instanceEventWindows.Get(id); !ok {
		return fmt.Errorf("%w: %s", ErrInstanceEventWindowNotFound, id)
	}
	b.instanceEventWindows.Delete(id)

	return nil
}

// DescribeInstanceEventWindows returns event windows, optionally filtered by IDs.
func (b *InMemoryBackend) DescribeInstanceEventWindows(ids []string) []*InstanceEventWindow {
	b.mu.RLock("DescribeInstanceEventWindows")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(ids))
	for _, id := range ids {
		filter[id] = true
	}

	var out []*InstanceEventWindow
	for _, ew := range b.instanceEventWindows.All() {
		if len(filter) > 0 && !filter[ew.InstanceEventWindowID] {
			continue
		}
		cp := *ew
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].InstanceEventWindowID < out[j].InstanceEventWindowID
	})

	return out
}

// ModifyInstanceEventWindow updates the cron expression for an event window.
func (b *InMemoryBackend) ModifyInstanceEventWindow(id, name, cronExpression string) error {
	if id == "" {
		return fmt.Errorf("%w: InstanceEventWindowId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyInstanceEventWindow")
	defer b.mu.Unlock()

	ew, ok := b.instanceEventWindows.Get(id)
	if !ok {
		return fmt.Errorf("%w: %s", ErrInstanceEventWindowNotFound, id)
	}
	if name != "" {
		ew.Name = name
	}
	if cronExpression != "" {
		ew.CronExpression = cronExpression
	}

	return nil
}

// ---- Spot Datafeed ----

// GetPasswordData returns the password data for a Windows instance (empty in mock).
func (b *InMemoryBackend) GetPasswordData(instanceID string) (string, time.Time, error) {
	if instanceID == "" {
		return "", time.Time{}, fmt.Errorf("%w: InstanceId is required", ErrInvalidParameter)
	}

	b.mu.RLock("GetPasswordData")
	defer b.mu.RUnlock()

	if _, ok := b.instances.Get(instanceID); !ok {
		return "", time.Time{}, fmt.Errorf("%w: %s", ErrInstanceNotFound, instanceID)
	}

	return "", time.Now().UTC(), nil
}

// ---- GetConsoleScreenshot ----

// GetConsoleScreenshot returns an empty base64-encoded console screenshot.
func (b *InMemoryBackend) GetConsoleScreenshot(instanceID string) (string, error) {
	if instanceID == "" {
		return "", fmt.Errorf("%w: InstanceId is required", ErrInvalidParameter)
	}

	b.mu.RLock("GetConsoleScreenshot")
	defer b.mu.RUnlock()

	if _, ok := b.instances.Get(instanceID); !ok {
		return "", fmt.Errorf("%w: %s", ErrInstanceNotFound, instanceID)
	}

	return "", nil
}

// ---- GetInstanceTypesFromInstanceRequirements ----

// GetInstanceTypesFromInstanceRequirements returns a static list of instance types
// matching the given requirements (simplified mock).
func (b *InMemoryBackend) GetInstanceTypesFromInstanceRequirements() []string {
	return []string{
		instanceTypeT3Micro, instanceTypeT3Small, instanceTypeT3Medium, "m5.large",
		instanceTypeM5Xlarge,
	}
}

// ---- GetSubnetCidrReservations ----

// ReportInstanceStatus records a custom status for an instance.
func (b *InMemoryBackend) ReportInstanceStatus(instanceID string, _ string, _ string) error {
	if instanceID == "" {
		return fmt.Errorf("%w: InstanceId is required", ErrInvalidParameter)
	}

	b.mu.RLock("ReportInstanceStatus")
	defer b.mu.RUnlock()

	if _, ok := b.instances.Get(instanceID); !ok {
		return fmt.Errorf("%w: %s", ErrInstanceNotFound, instanceID)
	}

	return nil
}

// ---- VPN operations ----

// DescribeInstancesByVPC returns running instances in a given VPC using the secondary index.
func (b *InMemoryBackend) DescribeInstancesByVPC(vpcID string) []*Instance {
	b.mu.RLock("DescribeInstancesByVPC")
	defer b.mu.RUnlock()

	ids, ok := b.instanceIDsByVPC[vpcID]
	if !ok {
		return nil
	}

	out := make([]*Instance, 0, len(ids))

	for id := range ids {
		if inst, exists := b.instances.Get(id); exists {
			cp := *inst
			out = append(out, &cp)
		}
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].ID < out[j].ID
	})

	return out
}

// DescribeInstanceTypeOfferings returns a static list of instance type / AZ pairs.
func (b *InMemoryBackend) DescribeInstanceTypeOfferings() []InstanceTypeOffering {
	azs := b.DescribeAvailabilityZones(b.Region)
	types := []string{
		"t3.nano", instanceTypeT3Micro, "t3.small", instanceTypeT3Medium, "t3.large",
		"t3.xlarge", "t3.2xlarge",
		"m5.large", instanceTypeM5Xlarge, "m5.2xlarge", "m5.4xlarge",
		"c5.large", "c5.xlarge", "c5.2xlarge",
		"r5.large", "r5.xlarge",
		"p3.2xlarge",
		"inf1.xlarge",
	}

	out := make([]InstanceTypeOffering, 0, len(types)*len(azs))

	for _, t := range types {
		for _, az := range azs {
			out = append(out, InstanceTypeOffering{
				InstanceType:     t,
				AvailabilityZone: az,
				Location:         az,
				LocationType:     "availability-zone",
			})
		}
	}

	return out
}

// ---- VPC peering: create / delete (accept is in backend_accept_ops.go) ----

// SendDiagnosticInterrupt validates that the instance exists. Real AWS sends
// a diagnostic NMI/QMP interrupt to the underlying hypervisor guest, which
// has no observable effect this backend can emulate beyond the existence
// check every real call also performs.
func (b *InMemoryBackend) SendDiagnosticInterrupt(instanceID string) error {
	if instanceID == "" {
		return fmt.Errorf("%w: InstanceId is required", ErrInvalidParameter)
	}

	b.mu.RLock("SendDiagnosticInterrupt")
	defer b.mu.RUnlock()

	if _, ok := b.instances.Get(instanceID); !ok {
		return fmt.Errorf("%w: %s", ErrInstanceNotFound, instanceID)
	}

	return nil
}

// DescribeElasticGpus returns Elastic Graphics accelerators. No API in this
// backend attaches Elastic Graphics accelerators to instances (AWS retired
// the feature in 2023), so this always returns an empty, correctly shaped
// list — the same "no API populates this" precedent already established for
// ClientVpnConnection elsewhere in this package.
func (b *InMemoryBackend) DescribeElasticGpus(_ []string) []*ElasticGpuStub {
	return []*ElasticGpuStub{}
}

// ElasticGpuStub is the wire shape for DescribeElasticGpus entries. No API in
// this backend attaches Elastic Graphics accelerators to instances (AWS
// retired the feature in 2023), so DescribeElasticGpus never populates one —
// this type exists purely so the response stays correctly shaped, mirroring
// the ClientVpnConnection precedent elsewhere in this package.
type ElasticGpuStub struct {
	ElasticGpuID   string
	InstanceID     string
	ElasticGpuType string
}
