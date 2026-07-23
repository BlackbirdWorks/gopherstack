package acmpca

import (
	"context"
	"fmt"
	"time"

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
		idempotency:     make(map[string]idempotencyRecord),
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

// caGet returns the live CA for (region, caARN). A DELETED CA whose
// RestorableUntil deadline has passed is treated as not-found here, the single
// choke point every read/write path in this package goes through -- matching
// real AWS, which permanently and irrevocably deletes a CA once its
// restoration window ends (see caPastRestorationWindow). This also makes
// RestoreCertificateAuthority correctly reject a restore attempted after the
// deadline: its own b.caGet lookup simply misses, returning ErrCANotFound,
// same as for any other nonexistent CA.
func (b *InMemoryBackend) caGet(region, caARN string) (*CertificateAuthority, bool) {
	ca, ok := b.cas.Get(regionKey(region, caARN))
	if !ok || caPastRestorationWindow(ca, time.Now().UTC()) {
		return nil, false
	}

	return ca, true
}

func (b *InMemoryBackend) caPut(v *CertificateAuthority) { b.cas.Put(v) }

// casInRegion returns every live CA in region, applying the same
// past-restoration-window filter as caGet so ListCertificateAuthorities never
// reports a permanently-deleted CA.
func (b *InMemoryBackend) casInRegion(region string) []*CertificateAuthority {
	all := b.casByRegion.Get(region)
	now := time.Now().UTC()
	live := make([]*CertificateAuthority, 0, len(all))

	for _, ca := range all {
		if !caPastRestorationWindow(ca, now) {
			live = append(live, ca)
		}
	}

	return live
}

// caPastRestorationWindow reports whether ca is a DELETED CA whose
// RestorableUntil deadline has already passed.
func caPastRestorationWindow(ca *CertificateAuthority, now time.Time) bool {
	return ca != nil && ca.Status == caStatusDeleted && !ca.RestorableUntil.IsZero() && now.After(ca.RestorableUntil)
}

// idempotencyCacheKey scopes an idempotency-token lookup by region, operation
// name, and the token itself, so a token reused across different regions or
// operations is never conflated.
func idempotencyCacheKey(region, op, token string) string {
	return region + "|" + op + "|" + token
}

// idempotentResourceARN returns the resourceARN previously cached for
// (region, op, token) if the token is non-empty and its 5-minute window has
// not yet expired. Callers hold either b.mu.Lock (Create paths, which also
// populate the cache via rememberIdempotency).
func (b *InMemoryBackend) idempotentResourceARN(region, op, token string, now time.Time) (string, bool) {
	if token == "" {
		return "", false
	}

	rec, ok := b.idempotency[idempotencyCacheKey(region, op, token)]
	if !ok || now.After(rec.expiresAt) {
		return "", false
	}

	return rec.resourceARN, true
}

// idempotencyWindow is the duration within which a repeated call bearing the
// same IdempotencyToken is recognized as a duplicate of the original request,
// matching CreateCertificateAuthority/IssueCertificate's documented 5-minute
// idempotency window.
const idempotencyWindow = 5 * time.Minute

// rememberIdempotency caches resourceARN under (region, op, token) for
// idempotencyWindow. A no-op when token is empty (nothing to dedupe).
func (b *InMemoryBackend) rememberIdempotency(region, op, token, resourceARN string, now time.Time) {
	if token == "" {
		return
	}

	b.idempotency[idempotencyCacheKey(region, op, token)] = idempotencyRecord{
		resourceARN: resourceARN,
		expiresAt:   now.Add(idempotencyWindow),
	}
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
	b.idempotency = make(map[string]idempotencyRecord)
}
