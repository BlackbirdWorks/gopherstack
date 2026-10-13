package ssooidc

import "github.com/blackbirdworks/gopherstack/pkgs/store"

func registerAllTables(b *InMemoryBackend) {
	b.clients = store.Register(b.registry, "clients", store.New(func(v *Client) string { return v.ClientID }))
	b.devices = store.Register(b.registry, "devices", store.New(func(v *Device) string { return v.DeviceCode }))
	b.codes = store.Register(b.registry, "codes", store.New(func(v *AuthCode) string { return v.Code }))
	b.tokens = store.Register(b.registry, "tokens", store.New(func(v *Token) string { return v.Token }))
}
