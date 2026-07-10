package acmpca

import (
	"context"
	"crypto/ecdsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

var (
	errDecodePEM   = errors.New("failed to decode private key PEM")
	errNotECDSAKey = errors.New("private key is not *ecdsa.PrivateKey")
)

// acmpcaSnapshotVersion identifies the shape of [backendSnapshot]. It must be
// bumped whenever a change to the DTOs/backendSnapshot itself would make an
// older snapshot unsafe to decode as the current shape (e.g. a field's
// meaning or type changes). Restore compares this against the persisted value
// and discards (rather than attempts to partially decode) any mismatch -- see
// Restore below. This is the first version: earlier (pre-Phase-3.3)
// snapshots carried no version field at all, so they also fail this check
// and are discarded rather than misread, which is the safe behaviour across
// any snapshot-format change.
const acmpcaSnapshotVersion = 1

func marshalPrivKey(key *ecdsa.PrivateKey) (string, error) {
	if key == nil {
		return "", nil
	}

	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return "", fmt.Errorf("marshal private key: %w", err)
	}

	return string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})), nil
}

func unmarshalPrivKey(pemStr string) (*ecdsa.PrivateKey, error) {
	block, _ := pem.Decode([]byte(pemStr))
	if block == nil {
		return nil, errDecodePEM
	}

	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse private key: %w", err)
	}

	ecKey, ok := key.(*ecdsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("%w: got %T", errNotECDSAKey, key)
	}

	return ecKey, nil
}

// caDTO wraps a CertificateAuthority for JSON round-tripping through
// store.Registry. store.Table[CertificateAuthority].Snapshot's plain
// json.Marshal(CertificateAuthority) cannot see any of its unexported fields
// -- region (used only for the live table's composite key, see store_setup.go)
// and privKey (never JSON-tagged, even pre-refactor) -- so both are carried
// alongside Value here instead. ARN mirrors the CA's own ARN so the DTO table
// has a stable composite key ("Region|ARN") independent of Value's own
// (unmarshaled) fields.
type caDTO struct {
	Value      *CertificateAuthority `json:"value"`
	Region     string                `json:"region"`
	ARN        string                `json:"arn"`
	PrivKeyPEM string                `json:"privKeyPEM,omitempty"`
}

func caDTOKeyFn(d *caDTO) string { return regionKey(d.Region, d.ARN) }

// regionalDTO wraps a region-nested resource for JSON round-tripping through
// store.Registry. It is shared by every converted resource whose only hidden
// field is region (IssuedCertificate, Permission, AuditReport). ID mirrors
// the resource's own composite-key component (its ARN, or the permissionKey
// composite for Permission) so the DTO table itself has a stable composite
// key ("Region|ID").
type regionalDTO[V any] struct {
	Value  *V     `json:"value"`
	Region string `json:"region"`
	ID     string `json:"id"`
}

// regionalDTOKeyFn is the shared [store.Table] key function for every
// regionalDTO[V] table below; it mirrors the "region|id" composite key each
// live table uses (see regionKey in backend.go).
func regionalDTOKeyFn[V any](d *regionalDTO[V]) string { return regionKey(d.Region, d.ID) }

// backendSnapshot is the top-level on-disk shape for the ACM PCA backend.
//
// Tables holds one JSON-encoded array per registered DTO table, produced by
// [store.Registry.SnapshotAll] -- the four converted resource collections,
// each wrapped in a DTO to carry its hidden field(s) through. Policies is
// still a region-nested plain map (see store_setup.go for why it was not
// converted to a store.Table) and is persisted directly, as before.
// certsByCASerial is NOT persisted -- it is rebuilt from the restored certs
// table in Restore, matching pre-refactor behaviour. Version guards against
// decoding a snapshot from an incompatible (older or newer) build of this
// backend as though it were the current shape; see Restore.
type backendSnapshot struct {
	Tables    map[string]json.RawMessage   `json:"tables"`
	Policies  map[string]map[string]string `json:"policies"`
	AccountID string                       `json:"accountID"`
	Region    string                       `json:"region"`
	Version   int                          `json:"version"`
}

// persistenceDTOTables groups every ephemeral DTO table
// buildPersistenceDTORegistry constructs, so Snapshot/Restore can pass them
// around as one value instead of four separate return values.
type persistenceDTOTables struct {
	registry     *store.Registry
	cas          *store.Table[caDTO]
	certs        *store.Table[regionalDTO[IssuedCertificate]]
	permissions  *store.Table[regionalDTO[Permission]]
	auditReports *store.Table[regionalDTO[AuditReport]]
}

// buildPersistenceDTORegistry constructs the ephemeral DTO registry used by
// both Snapshot and Restore. It is built fresh on every call (rather than
// reusing b.registry) because the DTO value types differ from the live table
// value types (e.g. regionalDTO[V] vs V).
func buildPersistenceDTORegistry() persistenceDTOTables {
	dtoReg := store.NewRegistry()

	return persistenceDTOTables{
		registry: dtoReg,
		cas:      store.Register(dtoReg, "cas", store.New(caDTOKeyFn)),
		certs:    store.Register(dtoReg, "certs", store.New(regionalDTOKeyFn[IssuedCertificate])),
		permissions: store.Register(
			dtoReg, "permissions", store.New(regionalDTOKeyFn[Permission]),
		),
		auditReports: store.Register(
			dtoReg, "auditReports", store.New(regionalDTOKeyFn[AuditReport]),
		),
	}
}

// Snapshot serialises the backend state to JSON. It implements
// persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	dtos := buildPersistenceDTORegistry()

	for _, v := range b.cas.Snapshot() {
		pemStr, err := marshalPrivKey(v.privKey)
		if err != nil {
			pemStr = ""
		}
		dtos.cas.Put(&caDTO{Value: v, Region: v.region, ARN: v.ARN, PrivKeyPEM: pemStr})
	}

	for _, v := range b.certs.Snapshot() {
		dtos.certs.Put(&regionalDTO[IssuedCertificate]{Region: v.region, ID: v.ARN, Value: v})
	}

	for _, v := range b.permissions.Snapshot() {
		id := permissionKey(v.CertificateAuthorityArn, v.Principal, v.SourceAccount)
		dtos.permissions.Put(&regionalDTO[Permission]{Region: v.region, ID: id, Value: v})
	}

	for _, v := range b.auditReports.Snapshot() {
		dtos.auditReports.Put(&regionalDTO[AuditReport]{Region: v.region, ID: v.AuditReportID, Value: v})
	}

	tables, err := dtos.registry.SnapshotAll()
	if err != nil {
		// The DTOs above are plain JSON-friendly structs, so a marshal
		// failure here would indicate a programming error rather than bad
		// input data. Log and skip the snapshot rather than panic, matching
		// the persistence.Persistable contract (nil is skipped by the
		// Manager).
		logger.Load(ctx).WarnContext(ctx, "acmpca: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version:   acmpcaSnapshotVersion,
		Tables:    tables,
		Policies:  b.policies,
		AccountID: b.accountID,
		Region:    b.region,
	}

	return persistence.MarshalSnapshot(ctx, "acmpca", snap)
}

// Restore loads backend state from a JSON snapshot. It implements
// persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "acmpca", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != acmpcaSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"acmpca: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", acmpcaSnapshotVersion)

		b.registry.ResetAll()
		b.certsByCASerial = make(map[string]map[string]string)
		b.policies = make(map[string]map[string]string)
		b.accountID = snap.AccountID
		b.region = snap.Region

		return nil
	}

	if err := b.restoreResourceTables(snap.Tables); err != nil {
		return err
	}

	if snap.Policies == nil {
		snap.Policies = make(map[string]map[string]string)
	}
	b.policies = snap.Policies

	b.accountID = snap.AccountID
	b.region = snap.Region

	// Rebuild the per-region certsByCASerial index from restored certificates.
	b.certsByCASerial = make(map[string]map[string]string)
	for _, cert := range b.certs.All() {
		b.certsByCASerialStore(cert.region)[cert.CAARN+"#"+cert.Serial] = cert.ARN
	}

	return nil
}

// unwrapRegionalDTOs converts every regionalDTO[V] in dtos into its live *V,
// restoring the unexported region field each carries via setRegion (a plain
// generic type parameter V has no field access, so the region assignment is
// supplied by the caller, one per concrete V; see restoreResourceTables). A
// DTO whose Value is nil -- not producible by Snapshot, but a defensive guard
// against a hand-edited or corrupted snapshot -- is skipped rather than
// dereferenced.
func unwrapRegionalDTOs[V any](dtos *store.Table[regionalDTO[V]], setRegion func(*V, string)) []*V {
	items := make([]*V, 0, dtos.Len())

	for _, d := range dtos.All() {
		if d.Value == nil {
			continue
		}

		setRegion(d.Value, d.Region)
		items = append(items, d.Value)
	}

	return items
}

// restoreResourceTables rebuilds every store.Table on b from snap's
// per-table JSON, factored out of Restore to keep Restore's own cognitive
// complexity low. Callers must hold b.mu.Lock.
func (b *InMemoryBackend) restoreResourceTables(tables map[string]json.RawMessage) error {
	dtos := buildPersistenceDTORegistry()

	if err := dtos.registry.RestoreAll(tables); err != nil {
		return fmt.Errorf("acmpca: restore snapshot tables: %w", err)
	}

	casItems, err := unwrapCADTOs(dtos.cas)
	if err != nil {
		return err
	}
	b.cas.Restore(casItems)

	b.certs.Restore(unwrapRegionalDTOs(dtos.certs, func(v *IssuedCertificate, r string) { v.region = r }))
	b.permissions.Restore(unwrapRegionalDTOs(dtos.permissions, func(v *Permission, r string) { v.region = r }))
	b.auditReports.Restore(unwrapRegionalDTOs(dtos.auditReports, func(v *AuditReport, r string) { v.region = r }))

	return nil
}

// unwrapCADTOs converts every caDTO in dtos into its live *CertificateAuthority,
// re-hydrating both the region field and the unexported privKey (from its PEM
// encoding) each DTO wraps.
func unwrapCADTOs(dtos *store.Table[caDTO]) ([]*CertificateAuthority, error) {
	items := make([]*CertificateAuthority, 0, dtos.Len())

	for _, d := range dtos.All() {
		if d.Value == nil {
			continue
		}

		ca := d.Value
		ca.region = d.Region

		if d.PrivKeyPEM != "" {
			privKey, err := unmarshalPrivKey(d.PrivKeyPEM)
			if err != nil {
				return nil, fmt.Errorf("restore CA %s private key: %w", d.ARN, err)
			}

			ca.privKey = privKey
		}

		items = append(items, ca)
	}

	return items, nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
