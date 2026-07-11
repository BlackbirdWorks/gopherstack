package apigateway

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// apigatewaySnapshotVersion identifies the shape of backendSnapshot's Tables
// blob (the set of "clean" tables on b.registry plus the DTO tables built
// below for the "dirty" ones -- see store_setup.go's registerAllTables doc
// for the clean/dirty split). Bump whenever a change to a DTO type, a
// registered table's value type, or backendSnapshot itself would make an
// older snapshot unsafe to decode as the current shape. Restore compares this
// against the persisted value and discards (rather than attempts to
// partially decode) any mismatch -- see Restore below. Mirrors the
// services/sqs pilot (commit 0f09d77c) and services/ec2 (commit 12e611a4).
const apigatewaySnapshotVersion = 1

// resourceSnapshot, deploymentSnapshot, stageSnapshot, authorizerSnapshot,
// requestValidatorSnapshot, documentationPartSnapshot,
// documentationVersionSnapshot, modelSnapshot, and usagePlanKeySnapshot are
// DTOs used ONLY for Snapshot/Restore. Each mirrors its live type field for
// field, except the identity field (RestAPIID / UsagePlanID) is given a real
// JSON tag here instead of the live type's `json:"-"` (see models.go) --
// marshaling the live type directly would silently drop that field, and
// unmarshaling into it would leave it permanently empty, corrupting the flat
// table's composite key on restore. This is the same DTO-registry technique
// services/sqs uses for its Queue/moveTaskState types (commit 0f09d77c),
// applied here for a different reason (a json:"-" identity field rather than
// live-only runtime state like channels/mutexes).
type resourceSnapshot struct {
	ResourceMethods   map[string]*Method `json:"resourceMethods,omitempty"`
	CorsConfiguration *CorsConfiguration `json:"corsConfiguration,omitempty"`
	ID                string             `json:"id"`
	ParentID          string             `json:"parentId,omitempty"`
	PathPart          string             `json:"pathPart,omitempty"`
	Path              string             `json:"path"`
	RestAPIID         string             `json:"restApiId"`
}

func resourceSnapshotKey(v *resourceSnapshot) string { return resourceKey(v.RestAPIID, v.ID) }

func toResourceSnapshot(v *Resource) *resourceSnapshot {
	return &resourceSnapshot{
		ID:                v.ID,
		ParentID:          v.ParentID,
		PathPart:          v.PathPart,
		Path:              v.Path,
		RestAPIID:         v.RestAPIID,
		ResourceMethods:   v.ResourceMethods,
		CorsConfiguration: v.CorsConfiguration,
	}
}

func fromResourceSnapshot(v *resourceSnapshot) *Resource {
	return &Resource{
		ID:                v.ID,
		ParentID:          v.ParentID,
		PathPart:          v.PathPart,
		Path:              v.Path,
		RestAPIID:         v.RestAPIID,
		ResourceMethods:   v.ResourceMethods,
		CorsConfiguration: v.CorsConfiguration,
	}
}

type deploymentSnapshot struct {
	CreatedDate unixEpochTime `json:"createdDate"`
	ID          string        `json:"id"`
	RestAPIID   string        `json:"restApiId"`
	Description string        `json:"description,omitempty"`
}

func deploymentSnapshotKey(v *deploymentSnapshot) string { return deploymentKey(v.RestAPIID, v.ID) }

func toDeploymentSnapshot(v *Deployment) *deploymentSnapshot {
	return &deploymentSnapshot{
		ID:          v.ID,
		RestAPIID:   v.RestAPIID,
		Description: v.Description,
		CreatedDate: v.CreatedDate,
	}
}

func fromDeploymentSnapshot(v *deploymentSnapshot) *Deployment {
	return &Deployment{
		ID:          v.ID,
		RestAPIID:   v.RestAPIID,
		Description: v.Description,
		CreatedDate: v.CreatedDate,
	}
}

type stageSnapshot struct {
	CanarySettings      *CanarySettings          `json:"canarySettings,omitempty"`
	AccessLogSettings   *AccessLogSettings       `json:"accessLogSettings,omitempty"`
	MethodSettings      map[string]MethodSetting `json:"methodSettings,omitempty"`
	Variables           map[string]string        `json:"variables,omitempty"`
	CreatedDate         unixEpochTime            `json:"createdDate"`
	LastUpdatedDate     unixEpochTime            `json:"lastUpdatedDate"`
	StageName           string                   `json:"stageName"`
	RestAPIID           string                   `json:"restApiId"`
	DeploymentID        string                   `json:"deploymentId"`
	Description         string                   `json:"description,omitempty"`
	ClientCertificateID string                   `json:"clientCertificateId,omitempty"`
	CacheClusterSize    string                   `json:"cacheClusterSize,omitempty"`
	CacheClusterStatus  string                   `json:"cacheClusterStatus,omitempty"`
	InvokeURL           string                   `json:"invokeUrl,omitempty"`
	TracingEnabled      bool                     `json:"tracingEnabled,omitempty"`
	CacheClusterEnabled bool                     `json:"cacheClusterEnabled,omitempty"`
}

func stageSnapshotKey(v *stageSnapshot) string { return stageKey(v.RestAPIID, v.StageName) }

func toStageSnapshot(v *Stage) *stageSnapshot {
	return &stageSnapshot{
		CanarySettings:      v.CanarySettings,
		AccessLogSettings:   v.AccessLogSettings,
		MethodSettings:      v.MethodSettings,
		Variables:           v.Variables,
		CreatedDate:         v.CreatedDate,
		LastUpdatedDate:     v.LastUpdatedDate,
		StageName:           v.StageName,
		RestAPIID:           v.RestAPIID,
		DeploymentID:        v.DeploymentID,
		Description:         v.Description,
		ClientCertificateID: v.ClientCertificateID,
		CacheClusterSize:    v.CacheClusterSize,
		CacheClusterStatus:  v.CacheClusterStatus,
		InvokeURL:           v.InvokeURL,
		TracingEnabled:      v.TracingEnabled,
		CacheClusterEnabled: v.CacheClusterEnabled,
	}
}

func fromStageSnapshot(v *stageSnapshot) *Stage {
	return &Stage{
		CanarySettings:      v.CanarySettings,
		AccessLogSettings:   v.AccessLogSettings,
		MethodSettings:      v.MethodSettings,
		Variables:           v.Variables,
		CreatedDate:         v.CreatedDate,
		LastUpdatedDate:     v.LastUpdatedDate,
		StageName:           v.StageName,
		RestAPIID:           v.RestAPIID,
		DeploymentID:        v.DeploymentID,
		Description:         v.Description,
		ClientCertificateID: v.ClientCertificateID,
		CacheClusterSize:    v.CacheClusterSize,
		CacheClusterStatus:  v.CacheClusterStatus,
		InvokeURL:           v.InvokeURL,
		TracingEnabled:      v.TracingEnabled,
		CacheClusterEnabled: v.CacheClusterEnabled,
	}
}

type authorizerSnapshot struct {
	ID                           string   `json:"id"`
	Name                         string   `json:"name"`
	Type                         string   `json:"type"`
	AuthorizerURI                string   `json:"authorizerUri,omitempty"`
	AuthorizerCredentials        string   `json:"authorizerCredentials,omitempty"`
	IdentitySource               string   `json:"identitySource,omitempty"`
	IdentityValidationExpression string   `json:"identityValidationExpression,omitempty"`
	RestAPIID                    string   `json:"restApiId"`
	ProviderARNs                 []string `json:"providerARNs,omitempty"`
	AuthorizerResultTTLInSeconds int      `json:"authorizerResultTtlInSeconds,omitempty"`
}

func authorizerSnapshotKey(v *authorizerSnapshot) string { return authorizerKey(v.RestAPIID, v.ID) }

func toAuthorizerSnapshot(v *Authorizer) *authorizerSnapshot {
	return &authorizerSnapshot{
		ID:                           v.ID,
		Name:                         v.Name,
		Type:                         v.Type,
		AuthorizerURI:                v.AuthorizerURI,
		AuthorizerCredentials:        v.AuthorizerCredentials,
		IdentitySource:               v.IdentitySource,
		IdentityValidationExpression: v.IdentityValidationExpression,
		ProviderARNs:                 v.ProviderARNs,
		RestAPIID:                    v.RestAPIID,
		AuthorizerResultTTLInSeconds: v.AuthorizerResultTTLInSeconds,
	}
}

func fromAuthorizerSnapshot(v *authorizerSnapshot) *Authorizer {
	return &Authorizer{
		ID:                           v.ID,
		Name:                         v.Name,
		Type:                         v.Type,
		AuthorizerURI:                v.AuthorizerURI,
		AuthorizerCredentials:        v.AuthorizerCredentials,
		IdentitySource:               v.IdentitySource,
		IdentityValidationExpression: v.IdentityValidationExpression,
		ProviderARNs:                 v.ProviderARNs,
		RestAPIID:                    v.RestAPIID,
		AuthorizerResultTTLInSeconds: v.AuthorizerResultTTLInSeconds,
	}
}

type requestValidatorSnapshot struct {
	ID                        string `json:"id"`
	Name                      string `json:"name"`
	RestAPIID                 string `json:"restApiId"`
	ValidateRequestBody       bool   `json:"validateRequestBody"`
	ValidateRequestParameters bool   `json:"validateRequestParameters"`
}

func requestValidatorSnapshotKey(v *requestValidatorSnapshot) string {
	return requestValidatorKey(v.RestAPIID, v.ID)
}

func toRequestValidatorSnapshot(v *RequestValidator) *requestValidatorSnapshot {
	return &requestValidatorSnapshot{
		ID:                        v.ID,
		Name:                      v.Name,
		ValidateRequestBody:       v.ValidateRequestBody,
		ValidateRequestParameters: v.ValidateRequestParameters,
		RestAPIID:                 v.RestAPIID,
	}
}

func fromRequestValidatorSnapshot(v *requestValidatorSnapshot) *RequestValidator {
	return &RequestValidator{
		ID:                        v.ID,
		Name:                      v.Name,
		ValidateRequestBody:       v.ValidateRequestBody,
		ValidateRequestParameters: v.ValidateRequestParameters,
		RestAPIID:                 v.RestAPIID,
	}
}

type documentationPartSnapshot struct {
	Location   DocumentationLocation `json:"location"`
	ID         string                `json:"id"`
	RestAPIID  string                `json:"restApiId"`
	Properties string                `json:"properties"`
}

func documentationPartSnapshotKey(v *documentationPartSnapshot) string {
	return documentationPartKey(v.RestAPIID, v.ID)
}

func toDocumentationPartSnapshot(v *DocumentationPart) *documentationPartSnapshot {
	return &documentationPartSnapshot{
		Location:   v.Location,
		ID:         v.ID,
		RestAPIID:  v.RestAPIID,
		Properties: v.Properties,
	}
}

func fromDocumentationPartSnapshot(v *documentationPartSnapshot) *DocumentationPart {
	return &DocumentationPart{
		Location:   v.Location,
		ID:         v.ID,
		RestAPIID:  v.RestAPIID,
		Properties: v.Properties,
	}
}

type documentationVersionSnapshot struct {
	CreatedDate unixEpochTime `json:"createdDate"`
	RestAPIID   string        `json:"restApiId"`
	Version     string        `json:"version"`
	Description string        `json:"description,omitempty"`
}

func documentationVersionSnapshotKey(v *documentationVersionSnapshot) string {
	return documentationVersionKey(v.RestAPIID, v.Version)
}

func toDocumentationVersionSnapshot(v *DocumentationVersion) *documentationVersionSnapshot {
	return &documentationVersionSnapshot{
		CreatedDate: v.CreatedDate,
		RestAPIID:   v.RestAPIID,
		Version:     v.Version,
		Description: v.Description,
	}
}

func fromDocumentationVersionSnapshot(v *documentationVersionSnapshot) *DocumentationVersion {
	return &DocumentationVersion{
		CreatedDate: v.CreatedDate,
		RestAPIID:   v.RestAPIID,
		Version:     v.Version,
		Description: v.Description,
	}
}

type modelSnapshot struct {
	ID          string `json:"id"`
	RestAPIID   string `json:"restApiId"`
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	ContentType string `json:"contentType,omitempty"`
	Schema      string `json:"schema,omitempty"`
}

func modelSnapshotKey(v *modelSnapshot) string { return modelKey(v.RestAPIID, v.ID) }

func toModelSnapshot(v *Model) *modelSnapshot {
	return &modelSnapshot{
		ID:          v.ID,
		RestAPIID:   v.RestAPIID,
		Name:        v.Name,
		Description: v.Description,
		ContentType: v.ContentType,
		Schema:      v.Schema,
	}
}

func fromModelSnapshot(v *modelSnapshot) *Model {
	return &Model{
		ID:          v.ID,
		RestAPIID:   v.RestAPIID,
		Name:        v.Name,
		Description: v.Description,
		ContentType: v.ContentType,
		Schema:      v.Schema,
	}
}

type usagePlanKeySnapshot struct {
	ID          string `json:"id"`
	Type        string `json:"type"`
	Value       string `json:"value,omitempty"`
	Name        string `json:"name,omitempty"`
	UsagePlanID string `json:"usagePlanId"`
}

func usagePlanKeySnapshotKey(v *usagePlanKeySnapshot) string {
	return usagePlanKeyKey(v.UsagePlanID, v.ID)
}

func toUsagePlanKeySnapshot(v *UsagePlanKey) *usagePlanKeySnapshot {
	return &usagePlanKeySnapshot{
		ID:          v.ID,
		Type:        v.Type,
		Value:       v.Value,
		Name:        v.Name,
		UsagePlanID: v.UsagePlanID,
	}
}

func fromUsagePlanKeySnapshot(v *usagePlanKeySnapshot) *UsagePlanKey {
	return &UsagePlanKey{
		ID:          v.ID,
		Type:        v.Type,
		Value:       v.Value,
		Name:        v.Name,
		UsagePlanID: v.UsagePlanID,
	}
}

// dirtyTableNames lists the "dirty" table names shared by Snapshot and
// Restore (see store_setup.go's registerAllTables doc). Both build an
// ephemeral DTO [store.Registry] under these exact names, so a snapshot
// written by one version of Snapshot always lines up with the same version
// of Restore.
//
//nolint:gochecknoglobals // fixed lookup table, mirrors errCodeLookup-style tables elsewhere
var dirtyTableNames = struct {
	resources, deployments, stages, authorizers, requestValidators,
	documentationParts, documentationVersions, models, usagePlanKeys string
}{
	resources:             "resources",
	deployments:           "deployments",
	stages:                "stages",
	authorizers:           "authorizers",
	requestValidators:     "requestValidators",
	documentationParts:    "documentationParts",
	documentationVersions: "documentationVersions",
	models:                "models",
	usagePlanKeys:         "usagePlanKeys",
}

// backendSnapshot is the top-level on-disk shape for the API Gateway backend.
//
// Tables holds one JSON-encoded array per table -- both the "clean" tables
// registered on b.registry (produced by [store.Registry.SnapshotAll]) and the
// "dirty" DTO tables built inline in Snapshot (see store_setup.go's
// registerAllTables doc for the clean/dirty split), merged into one map so a
// single Tables blob round-trips the whole backend.
type backendSnapshot struct {
	Tables         map[string]json.RawMessage  `json:"tables"`
	Account        *Account                    `json:"account,omitempty"`
	UsageOverrides map[string]map[string]int64 `json:"usageOverrides,omitempty"`
	Version        int                         `json:"version"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		// The registered tables are plain JSON-friendly structs, so a marshal
		// failure here would indicate a programming error rather than bad
		// input data. Log and skip the snapshot rather than panic, matching
		// the persistence.Persistable contract (nil is skipped by the Manager).
		logger.Load(ctx).WarnContext(ctx, "apigateway: snapshot table marshal failed", "error", err)

		return nil
	}

	dtoReg := store.NewRegistry()
	resourceDTOs := store.Register(dtoReg, dirtyTableNames.resources, store.New(resourceSnapshotKey))
	deploymentDTOs := store.Register(dtoReg, dirtyTableNames.deployments, store.New(deploymentSnapshotKey))
	stageDTOs := store.Register(dtoReg, dirtyTableNames.stages, store.New(stageSnapshotKey))
	authorizerDTOs := store.Register(dtoReg, dirtyTableNames.authorizers, store.New(authorizerSnapshotKey))
	requestValidatorDTOs := store.Register(
		dtoReg, dirtyTableNames.requestValidators, store.New(requestValidatorSnapshotKey),
	)
	documentationPartDTOs := store.Register(
		dtoReg, dirtyTableNames.documentationParts, store.New(documentationPartSnapshotKey),
	)
	documentationVersionDTOs := store.Register(
		dtoReg, dirtyTableNames.documentationVersions, store.New(documentationVersionSnapshotKey),
	)
	modelDTOs := store.Register(dtoReg, dirtyTableNames.models, store.New(modelSnapshotKey))
	usagePlanKeyDTOs := store.Register(dtoReg, dirtyTableNames.usagePlanKeys, store.New(usagePlanKeySnapshotKey))

	for _, v := range b.resources.Snapshot() {
		resourceDTOs.Put(toResourceSnapshot(v))
	}
	for _, v := range b.deployments.Snapshot() {
		deploymentDTOs.Put(toDeploymentSnapshot(v))
	}
	for _, v := range b.stages.Snapshot() {
		stageDTOs.Put(toStageSnapshot(v))
	}
	for _, v := range b.authorizers.Snapshot() {
		authorizerDTOs.Put(toAuthorizerSnapshot(v))
	}
	for _, v := range b.requestValidators.Snapshot() {
		requestValidatorDTOs.Put(toRequestValidatorSnapshot(v))
	}
	for _, v := range b.documentationParts.Snapshot() {
		documentationPartDTOs.Put(toDocumentationPartSnapshot(v))
	}
	for _, v := range b.documentationVersions.Snapshot() {
		documentationVersionDTOs.Put(toDocumentationVersionSnapshot(v))
	}
	for _, v := range b.models.Snapshot() {
		modelDTOs.Put(toModelSnapshot(v))
	}
	for _, v := range b.usagePlanKeys.Snapshot() {
		usagePlanKeyDTOs.Put(toUsagePlanKeySnapshot(v))
	}

	dirtyTables, err := dtoReg.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "apigateway: snapshot DTO table marshal failed", "error", err)

		return nil
	}

	maps.Copy(tables, dirtyTables)

	snap := backendSnapshot{
		Version:        apigatewaySnapshotVersion,
		Tables:         tables,
		Account:        b.account,
		UsageOverrides: b.usageOverrides,
	}

	return persistence.MarshalSnapshot(ctx, "apigateway", snap)
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "apigateway", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != apigatewaySnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"apigateway: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", apigatewaySnapshotVersion)

		b.registry.ResetAll()
		b.resources.Reset()
		b.deployments.Reset()
		b.stages.Reset()
		b.authorizers.Reset()
		b.requestValidators.Reset()
		b.documentationParts.Reset()
		b.documentationVersions.Reset()
		b.models.Reset()
		b.usagePlanKeys.Reset()

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("apigateway: restore snapshot tables: %w", err)
	}

	dtoReg := store.NewRegistry()
	resourceDTOs := store.Register(dtoReg, dirtyTableNames.resources, store.New(resourceSnapshotKey))
	deploymentDTOs := store.Register(dtoReg, dirtyTableNames.deployments, store.New(deploymentSnapshotKey))
	stageDTOs := store.Register(dtoReg, dirtyTableNames.stages, store.New(stageSnapshotKey))
	authorizerDTOs := store.Register(dtoReg, dirtyTableNames.authorizers, store.New(authorizerSnapshotKey))
	requestValidatorDTOs := store.Register(
		dtoReg, dirtyTableNames.requestValidators, store.New(requestValidatorSnapshotKey),
	)
	documentationPartDTOs := store.Register(
		dtoReg, dirtyTableNames.documentationParts, store.New(documentationPartSnapshotKey),
	)
	documentationVersionDTOs := store.Register(
		dtoReg, dirtyTableNames.documentationVersions, store.New(documentationVersionSnapshotKey),
	)
	modelDTOs := store.Register(dtoReg, dirtyTableNames.models, store.New(modelSnapshotKey))
	usagePlanKeyDTOs := store.Register(dtoReg, dirtyTableNames.usagePlanKeys, store.New(usagePlanKeySnapshotKey))

	if err := dtoReg.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("apigateway: restore snapshot DTO tables: %w", err)
	}

	restoreDirtyTables(b, resourceDTOs, deploymentDTOs, stageDTOs, authorizerDTOs,
		requestValidatorDTOs, documentationPartDTOs, documentationVersionDTOs, modelDTOs, usagePlanKeyDTOs)

	if snap.Account != nil {
		b.account = snap.Account
	} else {
		b.account = &Account{}
	}

	if snap.UsageOverrides != nil {
		b.usageOverrides = snap.UsageOverrides
	} else {
		b.usageOverrides = make(map[string]map[string]int64)
	}

	return nil
}

// restoreDirtyTables converts each DTO table's contents back to its live
// value type and loads the corresponding live b.<table> via Table.Restore,
// split out from Restore to keep its growth in check.
func restoreDirtyTables(
	b *InMemoryBackend,
	resourceDTOs *store.Table[resourceSnapshot],
	deploymentDTOs *store.Table[deploymentSnapshot],
	stageDTOs *store.Table[stageSnapshot],
	authorizerDTOs *store.Table[authorizerSnapshot],
	requestValidatorDTOs *store.Table[requestValidatorSnapshot],
	documentationPartDTOs *store.Table[documentationPartSnapshot],
	documentationVersionDTOs *store.Table[documentationVersionSnapshot],
	modelDTOs *store.Table[modelSnapshot],
	usagePlanKeyDTOs *store.Table[usagePlanKeySnapshot],
) {
	resources := make([]*Resource, 0, resourceDTOs.Len())
	for _, v := range resourceDTOs.All() {
		resources = append(resources, fromResourceSnapshot(v))
	}
	b.resources.Restore(resources)

	deployments := make([]*Deployment, 0, deploymentDTOs.Len())
	for _, v := range deploymentDTOs.All() {
		deployments = append(deployments, fromDeploymentSnapshot(v))
	}
	b.deployments.Restore(deployments)

	stages := make([]*Stage, 0, stageDTOs.Len())
	for _, v := range stageDTOs.All() {
		stages = append(stages, fromStageSnapshot(v))
	}
	b.stages.Restore(stages)

	authorizers := make([]*Authorizer, 0, authorizerDTOs.Len())
	for _, v := range authorizerDTOs.All() {
		authorizers = append(authorizers, fromAuthorizerSnapshot(v))
	}
	b.authorizers.Restore(authorizers)

	requestValidators := make([]*RequestValidator, 0, requestValidatorDTOs.Len())
	for _, v := range requestValidatorDTOs.All() {
		requestValidators = append(requestValidators, fromRequestValidatorSnapshot(v))
	}
	b.requestValidators.Restore(requestValidators)

	documentationParts := make([]*DocumentationPart, 0, documentationPartDTOs.Len())
	for _, v := range documentationPartDTOs.All() {
		documentationParts = append(documentationParts, fromDocumentationPartSnapshot(v))
	}
	b.documentationParts.Restore(documentationParts)

	documentationVersions := make([]*DocumentationVersion, 0, documentationVersionDTOs.Len())
	for _, v := range documentationVersionDTOs.All() {
		documentationVersions = append(documentationVersions, fromDocumentationVersionSnapshot(v))
	}
	b.documentationVersions.Restore(documentationVersions)

	models := make([]*Model, 0, modelDTOs.Len())
	for _, v := range modelDTOs.All() {
		models = append(models, fromModelSnapshot(v))
	}
	b.models.Restore(models)

	usagePlanKeys := make([]*UsagePlanKey, 0, usagePlanKeyDTOs.Len())
	for _, v := range usagePlanKeyDTOs.All() {
		usagePlanKeys = append(usagePlanKeys, fromUsagePlanKeySnapshot(v))
	}
	b.usagePlanKeys.Restore(usagePlanKeys)
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
