package acmpca

// Code in this file supports Phase 3.3 of the datalayer refactor: four
// resource collections that were previously nested by region (outer key =
// region, e.g. map[string]map[string]*CertificateAuthority) are each replaced
// by a single flat *store.Table[T], keyed by the composite "region|id" string
// (see regionKey in backend.go). Certificates and permissions additionally
// get a companion *store.Index grouping entries by (region, CA ARN) so the
// CA-scoped list operations (ListCertificates, ListPermissions) no longer
// need a linear scan-and-filter over every entry in the region -- the same
// region-qualified-table pattern services/emr uses.
//
// certsByCASerial (a derived, rebuilt-on-Restore lookup of bare strings with
// no identity of their own) and policies (a bare string value with no
// identity struct) are deliberately NOT converted: store.Table requires a *V
// value with its own identity, which neither has. They remain plain
// region-nested maps, unchanged by this refactor (see certsByCASerialStore
// and policiesStore in backend.go).
import (
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

func caKeyFn(v *CertificateAuthority) string            { return regionKey(v.region, v.ARN) }
func caRegionIndexKeyFn(v *CertificateAuthority) string { return v.region }

func certKeyFn(v *IssuedCertificate) string          { return regionKey(v.region, v.ARN) }
func certByCAIndexKeyFn(v *IssuedCertificate) string { return regionKey(v.region, v.CAARN) }

// permissionKeyFn reuses the existing permissionKey helper (backend.go) for
// the inner composite (CA ARN + principal + source account), qualified by
// region -- the same three-part identity DeletePermission/CreatePermission
// already used as their map key pre-refactor.
func permissionKeyFn(v *Permission) string {
	return regionKey(v.region, permissionKey(v.CertificateAuthorityArn, v.Principal, v.SourceAccount))
}
func permissionByCAIndexKeyFn(v *Permission) string {
	return regionKey(v.region, v.CertificateAuthorityArn)
}

// auditReportKeyFn has no companion CA-index key function: unlike
// certs/permissions, audit reports are only ever looked up directly by
// (region, AuditReportId) via DescribeCertificateAuthorityAuditReport --
// there is no ListCertificateAuthorityAuditReports operation that would need
// a per-CA scan.
func auditReportKeyFn(v *AuditReport) string { return regionKey(v.region, v.AuditReportID) }

// registerAllTables registers every converted resource collection on
// b.registry exactly once. It must be called during construction only
// (immediately after b.registry is created -- see NewInMemoryBackend), never
// on every Reset() -- store.Register panics on a duplicate name, so runtime
// resets go through b.registry.ResetAll() instead (see InMemoryBackend.Reset
// in backend.go).
func registerAllTables(b *InMemoryBackend) {
	b.cas = store.Register(b.registry, "cas", store.New(caKeyFn))
	b.casByRegion = b.cas.AddIndex("byRegion", caRegionIndexKeyFn)

	b.certs = store.Register(b.registry, "certs", store.New(certKeyFn))
	b.certsByCA = b.certs.AddIndex("byCA", certByCAIndexKeyFn)

	b.permissions = store.Register(b.registry, "permissions", store.New(permissionKeyFn))
	b.permissionsByCA = b.permissions.AddIndex("byCA", permissionByCAIndexKeyFn)

	b.auditReports = store.Register(b.registry, "auditReports", store.New(auditReportKeyFn))
}
