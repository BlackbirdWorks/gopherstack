package networkmonitor

import (
	"context"
	"fmt"
	"maps"
	"strings"
	"time"
)

// CreateProbe adds a probe to an existing monitor.
func (b *InMemoryBackend) CreateProbe(
	ctx context.Context,
	monitorName string,
	pi *probeInput,
	tags map[string]string,
) (*Probe, error) {
	if pi == nil {
		return nil, fmt.Errorf("%w: probe is required", ErrValidation)
	}

	proto, err := validateProbeInput(pi.Destination, pi.Protocol, pi.SourceArn, pi.DestinationPort, pi.PacketSize)
	if err != nil {
		return nil, err
	}

	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock()
	defer b.mu.Unlock()

	m, exists := b.monitors.Get(regionKey(region, monitorName))
	if !exists {
		return nil, fmt.Errorf("%w: monitor %q not found", ErrNotFound, monitorName)
	}

	if quotaErr := validateProbeQuotas(m.Probes, []string{pi.SourceArn}); quotaErr != nil {
		return nil, quotaErr
	}

	now := time.Now().UTC()
	probeID := b.nextProbeID()
	probeARN := b.buildProbeARN(region, monitorName, probeID)
	af := detectAddressFamily(pi.Destination)

	tagsCopy := make(map[string]string, len(pi.Tags)+len(tags))
	maps.Copy(tagsCopy, pi.Tags)
	maps.Copy(tagsCopy, tags)

	probe := &Probe{
		ProbeID:         probeID,
		ProbeArn:        probeARN,
		SourceArn:       pi.SourceArn,
		Destination:     pi.Destination,
		Protocol:        proto,
		DestinationPort: pi.DestinationPort,
		PacketSize:      pi.PacketSize,
		State:           probeStateActive,
		AddressFamily:   af,
		CreatedAt:       &now,
		ModifiedAt:      &now,
		Tags:            tagsCopy,
	}

	m.Probes = append(m.Probes, probe)
	m.ModifiedAt = &now

	return probeCopy(probe), nil
}

// DeleteProbe removes a probe from a monitor.
func (b *InMemoryBackend) DeleteProbe(ctx context.Context, monitorName, probeID string) error {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock()
	defer b.mu.Unlock()

	m, exists := b.monitors.Get(regionKey(region, monitorName))
	if !exists {
		return fmt.Errorf("%w: monitor %q not found", ErrNotFound, monitorName)
	}

	idx := findProbeIndex(m.Probes, probeID)
	if idx < 0 {
		return fmt.Errorf("%w: probe %q not found in monitor %q", ErrNotFound, probeID, monitorName)
	}

	now := time.Now().UTC()
	m.Probes = append(m.Probes[:idx], m.Probes[idx+1:]...)
	m.ModifiedAt = &now

	return nil
}

// GetProbe returns a probe from a monitor.
func (b *InMemoryBackend) GetProbe(
	ctx context.Context,
	monitorName, probeID string,
) (*Probe, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock()
	defer b.mu.RUnlock()

	m, exists := b.monitors.Get(regionKey(region, monitorName))
	if !exists {
		return nil, fmt.Errorf("%w: monitor %q not found", ErrNotFound, monitorName)
	}

	idx := findProbeIndex(m.Probes, probeID)
	if idx < 0 {
		return nil, fmt.Errorf(
			"%w: probe %q not found in monitor %q",
			ErrNotFound,
			probeID,
			monitorName,
		)
	}

	return probeCopy(m.Probes[idx]), nil
}

// UpdateProbe updates a probe's attributes.
func (b *InMemoryBackend) UpdateProbe(
	ctx context.Context,
	monitorName, probeID string,
	req *updateProbeRequest,
) (*Probe, error) {
	if req == nil {
		return nil, fmt.Errorf("%w: update request is required", ErrValidation)
	}

	normalizedProtocol, err := validateUpdateProbeRequest(req)
	if err != nil {
		return nil, err
	}

	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock()
	defer b.mu.Unlock()

	m, exists := b.monitors.Get(regionKey(region, monitorName))
	if !exists {
		return nil, fmt.Errorf("%w: monitor %q not found", ErrNotFound, monitorName)
	}

	idx := findProbeIndex(m.Probes, probeID)
	if idx < 0 {
		return nil, fmt.Errorf(
			"%w: probe %q not found in monitor %q",
			ErrNotFound,
			probeID,
			monitorName,
		)
	}

	probe := m.Probes[idx]

	if resultErr := validateProbeUpdateResult(probe, req, normalizedProtocol); resultErr != nil {
		return nil, resultErr
	}

	now := time.Now().UTC()
	applyProbeUpdate(probe, req, normalizedProtocol, now)
	m.ModifiedAt = &now

	return probeCopy(probe), nil
}

// validateUpdateProbeRequest validates an UpdateProbe request's optional
// fields in isolation, before any resource lookup, and returns the
// canonicalised (upper-cased) protocol if one was supplied ("" otherwise).
func validateUpdateProbeRequest(req *updateProbeRequest) (string, error) {
	if err := validateDestinationPort(req.DestinationPort); err != nil {
		return "", err
	}

	if err := validatePacketSize(req.PacketSize); err != nil {
		return "", err
	}

	if err := validateProbeState(req.State); err != nil {
		return "", err
	}

	if req.Protocol == "" {
		return "", nil
	}

	normalizedProtocol := strings.ToUpper(req.Protocol)
	if normalizedProtocol != protocolTCP && normalizedProtocol != protocolICMP {
		return "", fmt.Errorf("%w: probe protocol must be TCP or ICMP", ErrValidation)
	}

	return normalizedProtocol, nil
}

// validateProbeUpdateResult enforces the same TCP-requires-destinationPort
// invariant CreateProbe enforces, against the *effective* post-update values
// (an update may switch protocol to TCP without also supplying a port, or may
// clear an existing port on an already-TCP probe -- neither is a valid end
// state).
func validateProbeUpdateResult(probe *Probe, req *updateProbeRequest, normalizedProtocol string) error {
	effectiveProtocol := probe.Protocol
	if normalizedProtocol != "" {
		effectiveProtocol = normalizedProtocol
	}

	effectiveDestPort := probe.DestinationPort
	if req.DestinationPort != nil {
		effectiveDestPort = req.DestinationPort
	}

	if effectiveProtocol == protocolTCP && effectiveDestPort == nil {
		return fmt.Errorf("%w: destinationPort is required when protocol is TCP", ErrValidation)
	}

	return nil
}

// applyProbeUpdate mutates probe in place with the already-validated fields
// present in req, substituting normalizedProtocol for req.Protocol.
func applyProbeUpdate(probe *Probe, req *updateProbeRequest, normalizedProtocol string, now time.Time) {
	if req.Destination != "" {
		probe.Destination = req.Destination
		probe.AddressFamily = detectAddressFamily(req.Destination)
	}

	if normalizedProtocol != "" {
		probe.Protocol = normalizedProtocol
	}

	if req.DestinationPort != nil {
		probe.DestinationPort = req.DestinationPort
	}

	if req.PacketSize != nil {
		probe.PacketSize = req.PacketSize
	}

	if req.State != "" {
		probe.State = strings.ToUpper(req.State)
	}

	probe.ModifiedAt = &now
}

func findProbeIndex(probes []*Probe, probeID string) int {
	for i, p := range probes {
		if p.ProbeID == probeID {
			return i
		}
	}

	return -1
}

// validateProbeInput validates a full probe definition supplied at creation
// time (CreateMonitor's nested probes and CreateProbe). It returns the
// canonicalised (upper-cased) protocol so callers persist "TCP"/"ICMP"
// rather than whatever case the caller sent, matching the AWS enum wire
// contract.
func validateProbeInput(destination, protocol, sourceARN string, destPort, packetSize *int32) (string, error) {
	if destination == "" {
		return "", fmt.Errorf("%w: probe destination is required", ErrValidation)
	}

	if protocol == "" {
		return "", fmt.Errorf("%w: probe protocol is required", ErrValidation)
	}

	proto := strings.ToUpper(protocol)
	if proto != protocolTCP && proto != protocolICMP {
		return "", fmt.Errorf("%w: probe protocol must be TCP or ICMP", ErrValidation)
	}

	if sourceARN == "" {
		return "", fmt.Errorf("%w: probe sourceArn is required", ErrValidation)
	}

	if proto == protocolTCP && destPort == nil {
		return "", fmt.Errorf("%w: destinationPort is required when protocol is TCP", ErrValidation)
	}

	if err := validateDestinationPort(destPort); err != nil {
		return "", err
	}

	if err := validatePacketSize(packetSize); err != nil {
		return "", err
	}

	return proto, nil
}

// validateDestinationPort enforces the 1-65535 range documented for probe
// destinationPort. A nil port (not provided) is always valid here; the
// TCP-requires-destinationPort rule is enforced separately by callers.
func validateDestinationPort(port *int32) error {
	if port == nil {
		return nil
	}

	if *port < minDestinationPort || *port > maxDestinationPort {
		return fmt.Errorf("%w: destinationPort must be between 1 and 65535", ErrValidation)
	}

	return nil
}

// validatePacketSize enforces the 56-8500 byte range documented for probe
// packetSize.
func validatePacketSize(size *int32) error {
	if size == nil {
		return nil
	}

	if *size < minPacketSize || *size > maxPacketSize {
		return fmt.Errorf("%w: packetSize must be between 56 and 8500", ErrValidation)
	}

	return nil
}

// validateProbeQuotas enforces the two probe-related Network Synthetic
// Monitor service quotas (see the max*PerMonitor constants in store.go) for
// a set of newSourceArns about to be added to a monitor that already holds
// existingProbes: at most maxProbesPerMonitor probes total, and at most
// maxProbesPerSubnetPerMonitor probes sharing the same sourceArn (subnet)
// within that monitor. Called for both CreateMonitor's nested probes
// (existingProbes is nil) and CreateProbe (existingProbes is the monitor's
// current probe list).
func validateProbeQuotas(existingProbes []*Probe, newSourceArns []string) error {
	if len(existingProbes)+len(newSourceArns) > maxProbesPerMonitor {
		return fmt.Errorf(
			"%w: a monitor cannot have more than %d probes",
			ErrServiceQuotaExceeded,
			maxProbesPerMonitor,
		)
	}

	perSubnet := make(map[string]int, len(existingProbes)+len(newSourceArns))
	for _, p := range existingProbes {
		perSubnet[p.SourceArn]++
	}

	for _, sourceARN := range newSourceArns {
		perSubnet[sourceARN]++

		if perSubnet[sourceARN] > maxProbesPerSubnetPerMonitor {
			return fmt.Errorf(
				"%w: a subnet cannot have more than %d probes in a monitor",
				ErrServiceQuotaExceeded,
				maxProbesPerSubnetPerMonitor,
			)
		}
	}

	return nil
}

// validateProbeState enforces that a caller-supplied probe state (UpdateProbe's
// optional state field) is one of the known ProbeState enum values. An empty
// string (not provided) is always valid.
func validateProbeState(state string) error {
	if state == "" {
		return nil
	}

	if !isValidProbeState(strings.ToUpper(state)) {
		return fmt.Errorf(
			"%w: state must be one of PENDING, ACTIVE, INACTIVE, ERROR, DELETING, DELETED",
			ErrValidation,
		)
	}

	return nil
}

// detectAddressFamily returns "IPV4" or "IPV6" based on destination format.
func detectAddressFamily(destination string) string {
	if strings.Contains(destination, ":") {
		return "IPV6"
	}

	return "IPV4"
}

func probeCopy(p *Probe) *Probe {
	if p == nil {
		return nil
	}

	cp := *p
	cp.Tags = maps.Clone(p.Tags)

	if p.CreatedAt != nil {
		t := *p.CreatedAt
		cp.CreatedAt = &t
	}

	if p.ModifiedAt != nil {
		t := *p.ModifiedAt
		cp.ModifiedAt = &t
	}

	if p.DestinationPort != nil {
		v := *p.DestinationPort
		cp.DestinationPort = &v
	}

	if p.PacketSize != nil {
		v := *p.PacketSize
		cp.PacketSize = &v
	}

	return &cp
}
