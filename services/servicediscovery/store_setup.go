package servicediscovery

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T resource field on InMemoryBackend is replaced with a
// *store.Table[T], following the pattern established by services/sesv2 /
// services/opsworks (data-driven, direct registration -- see pkgs/store's
// package doc for the underlying primitive).
//
// Namespace.ID, Service.ID, and Operation.ID are already globally unique
// (minted by nextNsID/nextSvcID/nextOpID), so those three tables are "clean"
// direct registrations keyed by their own real ID field. The former
// nsARNIndex/nsNameIndex/svcARNIndex/svcByNsAndName reverse-lookup maps become
// secondary *store.Index values on the namespaces/services tables -- each is a
// pure function of a value's own field(s), so [store.Table.AddIndex]'s purity
// requirement holds trivially.
//
// Instance is the one composite-key table here: an instance's ID
// (Instance.ID) is only unique within its owning service, not globally (the
// same instance ID may be registered against two different services), so the
// table is keyed by instanceKey(ServiceID, ID) exactly as the pre-conversion
// b.instances map was. Both ServiceID and ID are already real (non-json:"-")
// fields, so no hidden field or DTO wrapper is needed -- the composite keyFn
// reads straight off the value. The former instancesByService grouping map
// becomes a byService *store.Index on this table.
//
// Left as plain maps (not store.Table-backed): serviceAttributes
// (map[string]map[string]string) and instanceHealthStatuses
// (map[string]string) -- their values are plain maps/strings, not *T, so
// neither fits store.Table's keyed-by-identity-value shape. See persistence.go.
import (
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

func namespaceKeyFn(v *Namespace) string     { return v.ID }
func namespaceARNKeyFn(v *Namespace) string  { return v.ARN }
func namespaceNameKeyFn(v *Namespace) string { return v.Name }

func serviceKeyFn(v *Service) string    { return v.ID }
func serviceARNKeyFn(v *Service) string { return v.ARN }

// serviceNsNameKeyFn indexes services by "namespaceID:name", mirroring the
// pre-conversion svcByNsAndName map's key shape exactly (including services
// with an empty NamespaceID, which key under ":name" and are never looked up
// that way since real namespace IDs are never empty).
func serviceNsNameKeyFn(v *Service) string { return v.NamespaceID + ":" + v.Name }

// instanceKeyFn returns the composite "serviceID/instanceID" primary key --
// see the file doc comment for why this table needs a composite key.
func instanceKeyFn(v *Instance) string { return instanceKey(v.ServiceID, v.ID) }

func instanceServiceKeyFn(v *Instance) string { return v.ServiceID }

func operationKeyFn(v *Operation) string { return v.ID }

// registerAllTables registers every backend resource table (and its
// secondary indexes) exactly once. It must be called during construction
// only, immediately after b.registry is created -- store.Register panics on
// a duplicate name, so runtime resets go through b.registry.ResetAll()
// instead (see InMemoryBackend.Reset in backend.go).
func registerAllTables(b *InMemoryBackend) {
	b.namespaces = store.Register(b.registry, "namespaces", store.New(namespaceKeyFn))
	b.namespacesByARN = b.namespaces.AddIndex("byARN", namespaceARNKeyFn)
	b.namespacesByName = b.namespaces.AddIndex("byName", namespaceNameKeyFn)

	b.services = store.Register(b.registry, "services", store.New(serviceKeyFn))
	b.servicesByARN = b.services.AddIndex("byARN", serviceARNKeyFn)
	b.servicesByNsName = b.services.AddIndex("byNsName", serviceNsNameKeyFn)

	b.instances = store.Register(b.registry, "instances", store.New(instanceKeyFn))
	b.instancesByService = b.instances.AddIndex("byService", instanceServiceKeyFn)

	b.operations = store.Register(b.registry, "operations", store.New(operationKeyFn))
}
