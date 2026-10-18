package globalaccelerator

import "github.com/blackbirdworks/gopherstack/pkgs/store"

func acceleratorKey(v *Accelerator) string     { return v.ARN }
func listenerKey(v *Listener) string           { return v.ARN }
func endpointGroupKey(v *EndpointGroup) string { return v.ARN }
func attachmentKey(v *Attachment) string       { return v.ARN }
func byoipKey(v *ByoipCidr) string             { return v.Cidr }

func registerAllTables(b *InMemoryBackend) {
	b.accelerators = store.Register(b.registry, "accelerators", store.New(acceleratorKey))
	b.listeners = store.Register(b.registry, "listeners", store.New(listenerKey))
	b.endpointGroups = store.Register(b.registry, "endpointGroups", store.New(endpointGroupKey))
	b.attachments = store.Register(b.registry, "attachments", store.New(attachmentKey))
	b.byoip = store.Register(b.registry, "byoipCidrs", store.New(byoipKey))
}
