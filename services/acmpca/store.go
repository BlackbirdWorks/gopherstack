package acmpca

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// getRegion extracts the region from ctx, falling back to defaultRegion when unset.
func getRegion(ctx context.Context, defaultRegion string) string {
	if r, ok := ctx.Value(regionContextKey{}).(string); ok && r != "" {
		return r
	}

	return defaultRegion
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		certsByCASerial: make(map[string]map[string]string),
		policies:        make(map[string]map[string]string),
		accountID:       accountID,
		region:          region,
		mu:              lockmetrics.New("acmpca"),
		registry:        store.NewRegistry(),
	}
	registerAllTables(b)

	return b
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// regionKey builds the composite store.Table primary key ("region|id") shared
// by every region-qualified table registered in store_setup.go.
func regionKey(region, id string) string { return region + "|" + id }

// The following Get/Put/Delete/InRegion/ForCA helpers replace the old lazy
// per-region map accessors (casStore(region) etc.) with store.Table /
// store.Index operations. Callers must still hold b.mu, exactly as before --
// store.Table performs no locking of its own (see pkgs/store's package doc).

func (b *InMemoryBackend) caGet(region, caARN string) (*CertificateAuthority, bool) {
	return b.cas.Get(regionKey(region, caARN))
}

func (b *InMemoryBackend) caPut(v *CertificateAuthority) { b.cas.Put(v) }

func (b *InMemoryBackend) casInRegion(region string) []*CertificateAuthority {
	return b.casByRegion.Get(region)
}

func (b *InMemoryBackend) certGet(region, certARN string) (*IssuedCertificate, bool) {
	return b.certs.Get(regionKey(region, certARN))
}

func (b *InMemoryBackend) certPut(v *IssuedCertificate) { b.certs.Put(v) }

func (b *InMemoryBackend) certsForCA(region, caARN string) []*IssuedCertificate {
	return b.certsByCA.Get(regionKey(region, caARN))
}

func (b *InMemoryBackend) certsByCASerialStore(region string) map[string]string {
	if b.certsByCASerial[region] == nil {
		b.certsByCASerial[region] = make(map[string]string)
	}

	return b.certsByCASerial[region]
}

func (b *InMemoryBackend) permissionGet(region, key string) (*Permission, bool) {
	return b.permissions.Get(regionKey(region, key))
}

func (b *InMemoryBackend) permissionPut(v *Permission) { b.permissions.Put(v) }

func (b *InMemoryBackend) permissionDelete(region, key string) bool {
	return b.permissions.Delete(regionKey(region, key))
}

func (b *InMemoryBackend) permissionsForCA(region, caARN string) []*Permission {
	return b.permissionsByCA.Get(regionKey(region, caARN))
}

func (b *InMemoryBackend) auditReportGet(region, id string) (*AuditReport, bool) {
	return b.auditReports.Get(regionKey(region, id))
}

func (b *InMemoryBackend) auditReportPut(v *AuditReport) { b.auditReports.Put(v) }

func (b *InMemoryBackend) policiesStore(region string) map[string]string {
	if b.policies[region] == nil {
		b.policies[region] = make(map[string]string)
	}

	return b.policies[region]
}

// validateRequiredParameter returns ErrInvalidParameter when a required field is empty.
func validateRequiredParameter(value, fieldName string) error {
	if value == "" {
		return fmt.Errorf("%w: %s is required", ErrInvalidParameter, fieldName)
	}

	return nil
}

// Reset clears all backend state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
	b.certsByCASerial = make(map[string]map[string]string)
	b.policies = make(map[string]map[string]string)
}
