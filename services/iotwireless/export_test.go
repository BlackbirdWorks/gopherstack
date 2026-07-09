package iotwireless

// countScoped counts entries in a store.Table-backed resource whose
// AccountID/Region match the given scope. It is generic over the 8
// account/region-scoped ("dirty") table value types -- see store_setup.go.
func countScoped[T any](all []*T, accountID, region string, scope func(*T) (string, string)) int {
	count := 0

	for _, v := range all {
		if a, r := scope(v); a == accountID && r == region {
			count++
		}
	}

	return count
}

// DeviceCount returns the number of wireless devices in the backend for the given account and region.
// Intended for test assertions only.
func DeviceCount(b *InMemoryBackend, accountID, region string) int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return countScoped(b.devices.All(), accountID, region, func(v *WirelessDevice) (string, string) {
		return v.AccountID, v.Region
	})
}

// GatewayCount returns the number of wireless gateways in the backend for the given account and region.
// Intended for test assertions only.
func GatewayCount(b *InMemoryBackend, accountID, region string) int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return countScoped(b.gateways.All(), accountID, region, func(v *WirelessGateway) (string, string) {
		return v.AccountID, v.Region
	})
}

// ServiceProfileCount returns the number of service profiles in the backend for the given account and region.
// Intended for test assertions only.
func ServiceProfileCount(b *InMemoryBackend, accountID, region string) int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return countScoped(b.serviceProfiles.All(), accountID, region, func(v *ServiceProfile) (string, string) {
		return v.AccountID, v.Region
	})
}

// DestinationCount returns the number of destinations in the backend for the given account and region.
// Intended for test assertions only.
func DestinationCount(b *InMemoryBackend, accountID, region string) int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return countScoped(b.destinations.All(), accountID, region, func(v *Destination) (string, string) {
		return v.AccountID, v.Region
	})
}

// DeviceProfileCount returns the number of device profiles in the backend for the given account and region.
// Intended for test assertions only.
func DeviceProfileCount(b *InMemoryBackend, accountID, region string) int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return countScoped(b.deviceProfiles.All(), accountID, region, func(v *DeviceProfile) (string, string) {
		return v.AccountID, v.Region
	})
}

// FuotaTaskCount returns the number of FUOTA tasks in the backend for the given account and region.
// Intended for test assertions only.
func FuotaTaskCount(b *InMemoryBackend, accountID, region string) int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return countScoped(b.fuotaTasks.All(), accountID, region, func(v *FuotaTask) (string, string) {
		return v.AccountID, v.Region
	})
}

// MulticastGroupCount returns the number of multicast groups in the backend for the given account and region.
// Intended for test assertions only.
func MulticastGroupCount(b *InMemoryBackend, accountID, region string) int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return countScoped(b.multicastGroups.All(), accountID, region, func(v *MulticastGroup) (string, string) {
		return v.AccountID, v.Region
	})
}

// NetworkAnalyzerConfigCount returns the number of network analyzer configs in the backend.
// Intended for test assertions only.
func NetworkAnalyzerConfigCount(b *InMemoryBackend, accountID, region string) int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return countScoped(
		b.networkAnalyzerConfigs.All(), accountID, region,
		func(v *NetworkAnalyzerConfig) (string, string) { return v.AccountID, v.Region },
	)
}

// ImportTaskCount returns the number of import tasks in the backend.
// Intended for test assertions only.
func ImportTaskCount(b *InMemoryBackend) int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.importTasks.Len()
}
