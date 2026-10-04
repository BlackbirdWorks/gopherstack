package kinesisvideo

import "github.com/blackbirdworks/gopherstack/pkgs/store"

func streamKeyFn(v *Stream) string { return v.Name }

func channelKeyFn(v *Channel) string { return v.Name }

// registerAllTables registers every backend resource table exactly once.
// Must be called during construction only -- store.Register panics on a
// duplicate name.
func registerAllTables(b *InMemoryBackend) {
	b.streams = store.Register(b.registry, "streams", store.New(streamKeyFn))
	b.channels = store.Register(b.registry, "channels", store.New(channelKeyFn))
}
