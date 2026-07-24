package networkmonitor

import "context"

// ProbeInputForTest is a test alias for probeInput so external test packages can create probes.
type ProbeInputForTest = probeInput

// CreateMonitorWithProbesForTest creates a monitor with nested probes,
// converting a []ProbeInputForTest into the internal createMonitorProbeInput
// shape CreateMonitor's probes parameter expects, so external test packages
// can exercise CreateMonitor's nested-probe path (e.g. quota enforcement)
// without reaching into the unexported createMonitorProbeInput type.
func CreateMonitorWithProbesForTest(
	ctx context.Context,
	b *InMemoryBackend,
	name string,
	probes []ProbeInputForTest,
) (*Monitor, error) {
	converted := make([]createMonitorProbeInput, len(probes))

	for i, p := range probes {
		converted[i] = createMonitorProbeInput(p)
	}

	return b.CreateMonitor(ctx, name, nil, converted, nil)
}

// UpdateProbeRequestForTest is a test-visible mirror of updateProbeRequest so
// external test packages can exercise UpdateProbe without reaching into
// unexported wire types. It has no Tags field, matching updateProbeRequest
// (see models.go): the real UpdateProbeInput has no Tags member.
type UpdateProbeRequestForTest struct {
	DestinationPort *int32
	PacketSize      *int32
	Destination     string
	Protocol        string
	State           string
}

// ToUpdateProbeRequest converts to the internal updateProbeRequest type
// accepted by StorageBackend.UpdateProbe.
func (r UpdateProbeRequestForTest) ToUpdateProbeRequest() *updateProbeRequest {
	return &updateProbeRequest{
		DestinationPort: r.DestinationPort,
		PacketSize:      r.PacketSize,
		Destination:     r.Destination,
		Protocol:        r.Protocol,
		State:           r.State,
	}
}

// MonitorCount returns the number of monitors stored across all regions.
func MonitorCount(b *InMemoryBackend) int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.monitors.Len()
}

// HandlerOpsLen returns the number of operations the handler supports.
func HandlerOpsLen(h *Handler) int {
	return len(h.GetSupportedOperations())
}

// WithRegion returns a context with the given region set.
func WithRegion(region string) context.Context {
	return context.WithValue(context.Background(), regionContextKey{}, region)
}
