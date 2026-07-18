package xray

// PutTelemetryRecords stores telemetry records in a ring buffer.
func (b *InMemoryBackend) PutTelemetryRecords(records []TelemetryRecord) {
	b.mu.Lock("PutTelemetryRecords")
	defer b.mu.Unlock()

	for i := range records {
		b.telemetry[b.telemetryIdx%telemetryRingSize] = &records[i]
		b.telemetryIdx++
	}
}

const (
	// telemetryRingSize is the capacity of the telemetry ring buffer.
	telemetryRingSize = 100
)
