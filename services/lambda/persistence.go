package lambda

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// lambdaSnapshotVersion identifies the shape of backendSnapshot's Tables blob
// (i.e. the set/shape of resources registered on b.registry -- see
// registerAllTables in store_setup.go). It must be bumped whenever a change
// there would make an older snapshot unsafe to decode as the current shape.
// Restore compares this against the persisted value and discards (rather
// than attempts to partially decode) any mismatch -- see Restore below. This
// mirrors the services/sqs pilot (commit 0f09d77c) and the services/ec2
// conversion (commit 12e611a4).
const lambdaSnapshotVersion = 1

// permissionSnapshot is the DTO used to serialise a *FunctionPermission.
// FunctionPermission.FunctionName and .Qualifier are tagged `json:"-"` (they
// are internal bookkeeping the AWS response shape never carries), which means
// they -- along with permissionKeyFn's ability to reconstruct a
// b.permissions key from them -- would be silently lost if a
// *store.Table[FunctionPermission] were round-tripped through
// store.Registry's generic JSON encoding. permissionSnapshot instead gives
// those two fields real json tags purely for the on-disk shape, following the
// DTO-registry pattern the services/sqs pilot (commit 0f09d77c) established
// for exactly this "dirty struct" case.
//
// FunctionURLAuthType and InvokedViaFunctionURL (gopherstack-rluhj) are the
// same case: both carry `json:"-"` on the live type because they are
// input-only on the real AddPermission request (aws-sdk-go-v2/service/lambda
// @v1.107.0 api_op_AddPermission.go:94,98) -- GetPolicy returns them baked
// into the policy JSON's Condition block, never as top-level fields (see
// buildPermissionStatementJSON in permissions.go) -- so the wire tag is
// correct and stays. Without a persisted copy, GetPolicy's Condition block
// went missing these two after a restore.
type permissionSnapshot struct {
	InvokedViaFunctionURL *bool  `json:"invokedViaFunctionUrl,omitempty"`
	StatementID           string `json:"statementId"`
	FunctionName          string `json:"functionName"`
	Qualifier             string `json:"qualifier,omitempty"`
	Action                string `json:"action"`
	Effect                string `json:"effect"`
	Principal             string `json:"principal"`
	SourceAccount         string `json:"sourceAccount,omitempty"`
	SourceArn             string `json:"sourceArn,omitempty"`
	EventSourceToken      string `json:"eventSourceToken,omitempty"`
	PrincipalOrgID        string `json:"principalOrgId,omitempty"`
	FunctionURLAuthType   string `json:"functionUrlAuthType,omitempty"`
}

// permissionSnapshotKey is the store.Table key function for the ephemeral DTO
// table Snapshot/Restore build around permissionSnapshot; it mirrors
// permissionKeyFn exactly so the on-disk table is keyed identically to the
// live b.permissions table.
func permissionSnapshotKey(p *permissionSnapshot) string {
	return permissionMapKey(p.FunctionName, p.Qualifier) + "|" + p.StatementID
}

func permissionToSnapshot(p *FunctionPermission) *permissionSnapshot {
	return &permissionSnapshot{
		StatementID:           p.StatementID,
		FunctionName:          p.FunctionName,
		Qualifier:             p.Qualifier,
		Action:                p.Action,
		Effect:                p.Effect,
		Principal:             p.Principal,
		SourceAccount:         p.SourceAccount,
		SourceArn:             p.SourceArn,
		EventSourceToken:      p.EventSourceToken,
		PrincipalOrgID:        p.PrincipalOrgID,
		FunctionURLAuthType:   p.FunctionURLAuthType,
		InvokedViaFunctionURL: p.InvokedViaFunctionURL,
	}
}

func permissionFromSnapshot(p *permissionSnapshot) *FunctionPermission {
	return &FunctionPermission{
		StatementID:           p.StatementID,
		FunctionName:          p.FunctionName,
		Qualifier:             p.Qualifier,
		Action:                p.Action,
		Effect:                p.Effect,
		Principal:             p.Principal,
		SourceAccount:         p.SourceAccount,
		SourceArn:             p.SourceArn,
		EventSourceToken:      p.EventSourceToken,
		PrincipalOrgID:        p.PrincipalOrgID,
		FunctionURLAuthType:   p.FunctionURLAuthType,
		InvokedViaFunctionURL: p.InvokedViaFunctionURL,
	}
}

// functionConfigurationSnapshot is FunctionConfiguration's persisted twin
// (gopherstack-rluhj, the same wire/persisted conflation as opensearch's
// vpcEndpointSnapshot, gopherstack-8mcb). CreatedAt/S3BucketCode/S3KeyCode
// carry `json:"-"` on the live type; verified against the pinned SDK
// (aws-sdk-go-v2/service/lambda@v1.107.0 types/types.go:1396-1580) that real
// types.FunctionConfiguration has no such members, so that tag is correct for
// the wire and stays. b.functions was registered directly on b.registry with
// no DTO, so SnapshotAll marshaled the live type as-is and dropped all three
// from persistence too: a restored function's zero CreatedAt failed the TTL
// purge's `!fn.CreatedAt.Before(cutoff)` check unconditionally (Purge in
// lifecycle.go), and a restored S3-sourced function lost the bucket/key
// startZipContainer needs to refetch its code (containers.go).
//
// Embedding FunctionConfiguration and re-declaring just these three fields at
// depth 0 shadows the embedded (json:"-") copies for both encode and decode
// -- encoding/json's field resolution always prefers the shallower depth,
// regardless of tags, so there is no duplicate JSON key. Every other
// FunctionConfiguration field therefore flows through unmodified without
// needing to be hand-copied here.
type functionConfigurationSnapshot struct {
	CreatedAt    time.Time `json:"createdAt,omitzero"`
	S3BucketCode string    `json:"s3BucketCode,omitempty"`
	S3KeyCode    string    `json:"s3KeyCode,omitempty"`
	FunctionConfiguration
}

// functionConfigurationSnapshotKey mirrors functionsKeyFn (store_setup.go)
// exactly, so the DTO registry table Snapshot/Restore build is keyed
// identically to the live b.functions table.
func functionConfigurationSnapshotKey(v *functionConfigurationSnapshot) string {
	return v.FunctionName
}

func toFunctionConfigurationSnapshot(fn *FunctionConfiguration) *functionConfigurationSnapshot {
	return &functionConfigurationSnapshot{
		FunctionConfiguration: *fn,
		CreatedAt:             fn.CreatedAt,
		S3BucketCode:          fn.S3BucketCode,
		S3KeyCode:             fn.S3KeyCode,
	}
}

func fromFunctionConfigurationSnapshot(v *functionConfigurationSnapshot) *FunctionConfiguration {
	fn := v.FunctionConfiguration
	fn.CreatedAt = v.CreatedAt
	fn.S3BucketCode = v.S3BucketCode
	fn.S3KeyCode = v.S3KeyCode

	return &fn
}

type backendSnapshot struct {
	// Tables holds one JSON-encoded array per table registered on b.registry
	// (functionURLConfigs, eventSourceMappings, aliases) PLUS a "permissions"
	// entry and a "functions" entry, both built separately from DTOs (see
	// Snapshot/Restore below) -- neither b.permissions nor b.functions is
	// registered on b.registry; see store_setup.go's package doc. Tables registered on
	// b.ephemeralRegistry (codeSigningConfigs, capacityProviders,
	// provisionedConcurrencies) are deliberately NOT included here -- they
	// were never persisted before this refactor and must stay that way.
	Tables map[string]json.RawMessage `json:"tables"`
	// The fields below have no pure, self-contained identity (see
	// store_setup.go's exclusion list) so they cannot become store.Tables, but
	// they WERE persisted before this refactor and must remain so.
	EventInvokeConfigs    map[string]*FunctionEventInvokeConfig                  `json:"eventInvokeConfigs"`
	Versions              map[string][]*FunctionVersion                          `json:"versions"`
	VersionCounters       map[string]int                                         `json:"versionCounters"`
	Layers                map[string][]*LayerVersion                             `json:"layers"`
	LayerVersionCounters  map[string]int64                                       `json:"layerVersionCounters"`
	LayerPolicies         map[string]map[int64]map[string]*LayerVersionStatement `json:"layerPolicies"`
	FunctionConcurrencies map[string]int                                         `json:"functionConcurrencies"`
	AccountID             string                                                 `json:"accountID"`
	Region                string                                                 `json:"region"`
	Version               int                                                    `json:"version"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
// ZipData (code bytes) are not serialised — code must be re-deployed after restore.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		// The registered tables are plain JSON-friendly structs, so a marshal
		// failure here would indicate a programming error rather than bad
		// input data. Log and skip the snapshot rather than panic, matching
		// the persistence.Persistable contract (nil is skipped by the Manager).
		logger.Load(ctx).WarnContext(ctx, "lambda: snapshot table marshal failed", "error", err)

		return nil
	}

	// b.permissions is not on b.registry (see store_setup.go's package doc),
	// so its Tables entry is built from a throwaway DTO registry instead,
	// exactly mirroring the services/sqs pilot's Queue/moveTaskState pattern.
	permDTOReg := store.NewRegistry()
	permDTOs := store.Register(permDTOReg, "permissions", store.New(permissionSnapshotKey))

	for _, p := range b.permissions.Snapshot() {
		permDTOs.Put(permissionToSnapshot(p))
	}

	permTables, err := permDTOReg.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "lambda: snapshot permissions marshal failed", "error", err)

		return nil
	}

	tables["permissions"] = permTables["permissions"]

	// b.functions is likewise not on b.registry (gopherstack-rluhj, see
	// store_setup.go's package doc and functionConfigurationSnapshot above).
	funcDTOReg := store.NewRegistry()
	funcDTOs := store.Register(funcDTOReg, "functions", store.New(functionConfigurationSnapshotKey))

	for _, fn := range b.functions.Snapshot() {
		funcDTOs.Put(toFunctionConfigurationSnapshot(fn))
	}

	funcTables, err := funcDTOReg.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "lambda: snapshot functions marshal failed", "error", err)

		return nil
	}

	tables["functions"] = funcTables["functions"]

	snap := backendSnapshot{
		Version:               lambdaSnapshotVersion,
		Tables:                tables,
		EventInvokeConfigs:    b.eventInvokeConfigs,
		Versions:              b.versions,
		VersionCounters:       b.versionCounters,
		Layers:                b.layers,
		LayerVersionCounters:  b.layerVersionCounters,
		LayerPolicies:         b.layerPolicies,
		FunctionConcurrencies: b.functionConcurrencies,
		AccountID:             b.accountID,
		Region:                b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "persistence: snapshot marshal failed", "service", "lambda", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
// Code bytes are not restored — functions must be re-deployed after restore.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "lambda", data, &snap); err != nil {
		return err
	}

	normalizeSnapshot(&snap)

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != lambdaSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape — that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption. Mirrors the services/sqs pilot (commit 0f09d77c) and the
		// services/ec2 conversion (commit 12e611a4). Only b.registry is reset:
		// b.ephemeralRegistry's tables (codeSigningConfigs, capacityProviders,
		// provisionedConcurrencies) were never persisted, so Restore has never
		// touched them and must not start now.
		logger.Load(ctx).WarnContext(ctx,
			"lambda: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", lambdaSnapshotVersion)

		b.registry.ResetAll()
		b.permissions.Reset()
		b.functions.Reset()

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("lambda: restore snapshot tables: %w", err)
	}

	if err := b.restoreFunctionsFromDTO(snap.Tables); err != nil {
		return err
	}

	if err := b.restorePermissionsFromDTO(snap.Tables); err != nil {
		return err
	}

	b.eventInvokeConfigs = snap.EventInvokeConfigs
	b.versions = snap.Versions
	b.versionCounters = snap.VersionCounters
	b.layers = snap.Layers
	b.layerVersionCounters = snap.LayerVersionCounters
	b.layerPolicies = snap.LayerPolicies
	b.functionConcurrencies = snap.FunctionConcurrencies
	b.accountID = snap.AccountID
	b.region = snap.Region

	restoreSnapshotFunctions(b.functions.All(), b.functionConcurrencies)
	restoreSnapshotLayers(b.layers)

	// versionIndex is a derived lookup (UUID/qualifier -> published version
	// snapshot) built incrementally by PublishVersion; it is not itself
	// serialised. Without rebuilding it here, every published version becomes
	// unreachable by qualifier after a restore (GetFunction/Invoke with a
	// numeric version, or an alias pointing at one, would incorrectly 404)
	// even though the version data survived in b.versions.
	b.versionIndex = make(map[string]map[string]*FunctionVersion)

	for name, versions := range snap.Versions {
		for _, v := range versions {
			if v.Version == "" || v.Version == versionLatest {
				continue
			}

			if b.versionIndex[name] == nil {
				b.versionIndex[name] = make(map[string]*FunctionVersion)
			}

			b.versionIndex[name][v.Version] = v
		}
	}

	// esmByFunctionARN is likewise a derived reverse index over
	// eventSourceMappings; rebuild it so ListEventSourceMappings filtered by
	// FunctionName keeps working after a restore.
	b.esmByFunctionARN = make(map[string]map[string]struct{})

	for _, m := range b.eventSourceMappings.All() {
		if b.esmByFunctionARN[m.FunctionARN] == nil {
			b.esmByFunctionARN[m.FunctionARN] = make(map[string]struct{})
		}

		b.esmByFunctionARN[m.FunctionARN][m.UUID] = struct{}{}
	}

	return nil
}

// restoreFunctionsFromDTO restores b.functions from its "functions" DTO
// entry in tables (gopherstack-rluhj; b.functions is not on b.registry, see
// store_setup.go's package doc and functionConfigurationSnapshot above).
func (b *InMemoryBackend) restoreFunctionsFromDTO(tables map[string]json.RawMessage) error {
	funcDTOReg := store.NewRegistry()
	funcDTOs := store.Register(funcDTOReg, "functions", store.New(functionConfigurationSnapshotKey))

	if err := funcDTOReg.RestoreAll(tables); err != nil {
		return fmt.Errorf("lambda: restore snapshot functions: %w", err)
	}

	liveFunctions := make([]*FunctionConfiguration, 0, funcDTOs.Len())
	for _, v := range funcDTOs.All() {
		liveFunctions = append(liveFunctions, fromFunctionConfigurationSnapshot(v))
	}

	b.functions.Restore(liveFunctions)

	return nil
}

// restorePermissionsFromDTO restores b.permissions from its "permissions"
// DTO entry in tables (b.permissions is not on b.registry, see
// store_setup.go's package doc).
func (b *InMemoryBackend) restorePermissionsFromDTO(tables map[string]json.RawMessage) error {
	permDTOReg := store.NewRegistry()
	permDTOs := store.Register(permDTOReg, "permissions", store.New(permissionSnapshotKey))

	if err := permDTOReg.RestoreAll(tables); err != nil {
		return fmt.Errorf("lambda: restore snapshot permissions: %w", err)
	}

	livePerms := make([]*FunctionPermission, 0, permDTOs.Len())
	for _, p := range permDTOs.All() {
		livePerms = append(livePerms, permissionFromSnapshot(p))
	}

	b.permissions.Restore(livePerms)

	return nil
}

// normalizeSnapshot initialises nil maps in a snapshot to empty maps so callers
// never need to nil-check after a Restore.
func normalizeSnapshot(snap *backendSnapshot) {
	if snap.EventInvokeConfigs == nil {
		snap.EventInvokeConfigs = make(map[string]*FunctionEventInvokeConfig)
	}

	if snap.Versions == nil {
		snap.Versions = make(map[string][]*FunctionVersion)
	}

	if snap.VersionCounters == nil {
		snap.VersionCounters = make(map[string]int)
	}

	if snap.Layers == nil {
		snap.Layers = make(map[string][]*LayerVersion)
	}

	if snap.LayerVersionCounters == nil {
		snap.LayerVersionCounters = make(map[string]int64)
	}

	if snap.LayerPolicies == nil {
		snap.LayerPolicies = make(map[string]map[int64]map[string]*LayerVersionStatement)
	}

	if snap.FunctionConcurrencies == nil {
		snap.FunctionConcurrencies = make(map[string]int)
	}
}

// restoreSnapshotFunctions clears transient fields on restored function configurations
// and re-links ReservedConcurrentExecutions from the concurrency map.
func restoreSnapshotFunctions(fns []*FunctionConfiguration, concurrencies map[string]int) {
	for _, fn := range fns {
		fn.ZipData = nil

		if fn.LastUpdateStatus == "" {
			fn.LastUpdateStatus = LastUpdateStatusSuccessful
		}

		if reserved, ok := concurrencies[fn.FunctionName]; ok {
			v := reserved
			fn.ReservedConcurrentExecutions = &v
		} else {
			fn.ReservedConcurrentExecutions = nil
		}
	}
}

// restoreSnapshotLayers clears zip data from restored layer versions.
func restoreSnapshotLayers(layers map[string][]*LayerVersion) {
	for _, versions := range layers {
		for _, lv := range versions {
			lv.ZipData = nil
		}
	}
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	type snapshotter interface {
		Snapshot(ctx context.Context) []byte
	}
	if s, ok := h.Backend.(snapshotter); ok {
		return s.Snapshot(ctx)
	}

	return nil
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	type restorer interface {
		Restore(context.Context, []byte) error
	}
	if r, ok := h.Backend.(restorer); ok {
		return r.Restore(ctx, data)
	}

	return nil
}
