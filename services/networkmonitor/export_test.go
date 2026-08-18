package networkmonitor

import "context"

// ProbeInputForTest is a test alias for probeInput so external test packages can create probes.
type ProbeInputForTest = probeInput

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
	b.mu.RLock("MonitorCount")
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
