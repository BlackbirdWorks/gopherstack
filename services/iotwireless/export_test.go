package iotwireless

// DeviceCount returns the number of wireless devices in the backend for the given account and region.
// Intended for test assertions only.
func DeviceCount(b *InMemoryBackend, accountID, region string) int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	count := 0

	for k := range b.devices {
		if k.AccountID == accountID && k.Region == region {
			count++
		}
	}

	return count
}

// GatewayCount returns the number of wireless gateways in the backend for the given account and region.
// Intended for test assertions only.
func GatewayCount(b *InMemoryBackend, accountID, region string) int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	count := 0

	for k := range b.gateways {
		if k.AccountID == accountID && k.Region == region {
			count++
		}
	}

	return count
}

// ServiceProfileCount returns the number of service profiles in the backend for the given account and region.
// Intended for test assertions only.
func ServiceProfileCount(b *InMemoryBackend, accountID, region string) int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	count := 0

	for k := range b.serviceProfiles {
		if k.AccountID == accountID && k.Region == region {
			count++
		}
	}

	return count
}

// DestinationCount returns the number of destinations in the backend for the given account and region.
// Intended for test assertions only.
func DestinationCount(b *InMemoryBackend, accountID, region string) int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	count := 0

	for k := range b.destinations {
		if k.AccountID == accountID && k.Region == region {
			count++
		}
	}

	return count
}

// DeviceProfileCount returns the number of device profiles in the backend for the given account and region.
// Intended for test assertions only.
func DeviceProfileCount(b *InMemoryBackend, accountID, region string) int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	count := 0

	for k := range b.deviceProfiles {
		if k.AccountID == accountID && k.Region == region {
			count++
		}
	}

	return count
}

// FuotaTaskCount returns the number of FUOTA tasks in the backend for the given account and region.
// Intended for test assertions only.
func FuotaTaskCount(b *InMemoryBackend, accountID, region string) int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	count := 0

	for k := range b.fuotaTasks {
		if k.AccountID == accountID && k.Region == region {
			count++
		}
	}

	return count
}

// MulticastGroupCount returns the number of multicast groups in the backend for the given account and region.
// Intended for test assertions only.
func MulticastGroupCount(b *InMemoryBackend, accountID, region string) int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	count := 0

	for k := range b.multicastGroups {
		if k.AccountID == accountID && k.Region == region {
			count++
		}
	}

	return count
}

// NetworkAnalyzerConfigCount returns the number of network analyzer configs in the backend.
// Intended for test assertions only.
func NetworkAnalyzerConfigCount(b *InMemoryBackend, accountID, region string) int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	count := 0

	for k := range b.networkAnalyzerConfigs {
		if k.AccountID == accountID && k.Region == region {
			count++
		}
	}

	return count
}

// ImportTaskCount returns the number of import tasks in the backend.
// Intended for test assertions only.
func ImportTaskCount(b *InMemoryBackend) int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return len(b.importTasks)
}
