package medialive

// ChannelCount returns the number of stored channels.
func ChannelCount(b *InMemoryBackend) int {
	b.mu.RLock("ChannelCount")
	defer b.mu.RUnlock()

	return len(b.channels)
}

// InputCount returns the number of stored inputs.
func InputCount(b *InMemoryBackend) int {
	b.mu.RLock("InputCount")
	defer b.mu.RUnlock()

	return len(b.inputs)
}

// InputSecurityGroupCount returns the number of stored input security groups.
func InputSecurityGroupCount(b *InMemoryBackend) int {
	b.mu.RLock("InputSecurityGroupCount")
	defer b.mu.RUnlock()

	return len(b.inputSecurityGroups)
}

// InputDeviceCount returns the number of stored input devices.
func InputDeviceCount(b *InMemoryBackend) int {
	b.mu.RLock("InputDeviceCount")
	defer b.mu.RUnlock()

	return len(b.inputDevices)
}

// MultiplexCount returns the number of stored multiplexes.
func MultiplexCount(b *InMemoryBackend) int {
	b.mu.RLock("MultiplexCount")
	defer b.mu.RUnlock()

	return len(b.multiplexes)
}

// MultiplexProgramCount returns the number of programs in a multiplex.
func MultiplexProgramCount(b *InMemoryBackend, multiplexID string) int {
	b.mu.RLock("MultiplexProgramCount")
	defer b.mu.RUnlock()

	m, ok := b.multiplexes[multiplexID]
	if !ok {
		return 0
	}

	return len(m.Programs)
}
