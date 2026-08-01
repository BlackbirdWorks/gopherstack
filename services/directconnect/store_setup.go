package directconnect

import (
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

func connectionKeyFn(v *Connection) string { return v.ConnectionID }

func lagKeyFn(v *Lag) string { return v.LagID }

func interconnectKeyFn(v *Interconnect) string { return v.InterconnectID }

func vifKeyFn(v *VirtualInterface) string { return v.VirtualInterfaceID }

func vifConnectionIndexKeyFn(v *VirtualInterface) string { return v.ConnectionID }

func gatewayKeyFn(v *DirectConnectGateway) string { return v.DirectConnectGatewayID }

func associationKeyFn(v *GatewayAssociation) string { return v.AssociationID }

func proposalKeyFn(v *GatewayAssociationProposal) string { return v.ProposalID }

func vifTestKeyFn(v *VifTestHistory) string { return v.TestID }

func vifTestVifIndexKeyFn(v *VifTestHistory) string { return v.VirtualInterfaceID }

// registerAllTables registers every resource collection exactly once. Must
// be called during construction only -- see services/outposts/store_setup.go's
// doc comment for why (store.Register panics on a duplicate name).
func registerAllTables(b *InMemoryBackend) {
	b.connections = store.Register(b.registry, "connections", store.New(connectionKeyFn))
	b.lags = store.Register(b.registry, "lags", store.New(lagKeyFn))
	b.interconnects = store.Register(b.registry, "interconnects", store.New(interconnectKeyFn))

	b.virtualInterfaces = store.Register(b.registry, "virtualInterfaces", store.New(vifKeyFn))
	b.vifsByConnection = b.virtualInterfaces.AddIndex("byConnection", vifConnectionIndexKeyFn)

	b.gateways = store.Register(b.registry, "gateways", store.New(gatewayKeyFn))
	b.associations = store.Register(b.registry, "associations", store.New(associationKeyFn))
	b.proposals = store.Register(b.registry, "proposals", store.New(proposalKeyFn))

	b.vifTests = store.Register(b.registry, "vifTests", store.New(vifTestKeyFn))
	b.vifTestsByVif = b.vifTests.AddIndex("byVif", vifTestVifIndexKeyFn)
}
