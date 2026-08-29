package lambda

import (
	"maps"
	"sort"
	"strconv"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// PublishVersion creates an immutable version snapshot of the current $LATEST function config.
func (b *InMemoryBackend) PublishVersion(name, description string) (*FunctionVersion, error) {
	return b.publishVersion(name, description, "")
}

// PublishVersionWithRevision behaves like PublishVersion but additionally
// enforces AWS's optimistic-concurrency RevisionId precondition: when
// revisionID is non-empty it must match the function's current ($LATEST)
// RevisionId or the publish is rejected with ErrPreconditionFailed and the
// function is left unmodified. The check and the publish happen under a
// single lock acquisition to avoid a check-then-act race against a concurrent
// UpdateFunctionConfiguration/UpdateFunctionCode.
func (b *InMemoryBackend) PublishVersionWithRevision(name, description, revisionID string) (*FunctionVersion, error) {
	return b.publishVersion(name, description, revisionID)
}

func (b *InMemoryBackend) publishVersion(name, description, revisionID string) (*FunctionVersion, error) {
	b.mu.Lock("PublishVersion")
	defer b.mu.Unlock()

	fn, ok := b.functions.Get(name)
	if !ok {
		return nil, ErrFunctionNotFound
	}

	if revisionID != "" && revisionID != fn.RevisionID {
		return nil, ErrPreconditionFailed
	}

	b.versionCounters[name]++
	versionNum := strconv.Itoa(b.versionCounters[name])

	ver := &FunctionVersion{
		FunctionName:        fn.FunctionName,
		FunctionArn:         buildVersionARN(b.region, b.accountID, fn.FunctionName, versionNum),
		Description:         description,
		Version:             versionNum,
		Runtime:             fn.Runtime,
		Handler:             fn.Handler,
		Role:                fn.Role,
		MemorySize:          fn.MemorySize,
		Timeout:             fn.Timeout,
		PackageType:         fn.PackageType,
		ImageURI:            fn.ImageURI,
		ImageConfigResponse: fn.ImageConfigResponse,
		VpcConfig:           fn.VpcConfig,
		TracingConfig:       fn.TracingConfig,
		FileSystemConfigs:   fn.FileSystemConfigs,
		DeadLetterConfig:    fn.DeadLetterConfig,
		Environment:         deepCopyEnvironment(fn.Environment),
		Layers:              deepCopyFunctionLayers(fn.Layers),
		CodeSize:            fn.CodeSize,
		RevisionID:          uuid.New().String(),
		CreatedAt:           fn.LastModified,
		State:               fn.State,
		SnapStart:           copySnapStart(fn.SnapStart),
		DurableConfig:       fn.DurableConfig,
	}

	b.versions[name] = append(b.versions[name], ver)

	if b.versionIndex[name] == nil {
		b.versionIndex[name] = make(map[string]*FunctionVersion)
	}
	b.versionIndex[name][versionNum] = ver

	return ver, nil
}

// GetVersion returns a specific version snapshot of a function.
// Pass "$LATEST" to get the live function config as a FunctionVersion.
func (b *InMemoryBackend) GetVersion(name, version string) (*FunctionVersion, error) {
	b.mu.RLock("GetVersion")
	defer b.mu.RUnlock()

	if version == versionLatest {
		fn, ok := b.functions.Get(name)
		if !ok {
			return nil, ErrFunctionNotFound
		}

		return fnToVersion(fn), nil
	}

	if _, ok := b.functions.Get(name); !ok {
		return nil, ErrFunctionNotFound
	}

	if vMap := b.versionIndex[name]; vMap != nil {
		if v, ok := vMap[version]; ok {
			return v, nil
		}
	}

	return nil, ErrVersionNotFound
}

// ListVersionsByFunction returns a page of published versions for a function (including $LATEST).
func (b *InMemoryBackend) ListVersionsByFunction(
	name, marker string,
	maxItems int,
) (page.Page[*FunctionVersion], error) {
	b.mu.RLock("ListVersionsByFunction")
	defer b.mu.RUnlock()

	fn, ok := b.functions.Get(name)
	if !ok {
		return page.Page[*FunctionVersion]{}, ErrFunctionNotFound
	}

	result := make([]*FunctionVersion, 0, len(b.versions[name])+1)

	// $LATEST is always first.
	result = append(result, fnToVersion(fn))
	result = append(result, b.versions[name]...)

	return page.New(result, marker, maxItems, lambdaDefaultMaxItems), nil
}

// CreateAlias creates a new alias for a Lambda function pointing to a version.
func (b *InMemoryBackend) CreateAlias(
	name string,
	input *CreateAliasInput,
) (*FunctionAlias, error) {
	b.mu.Lock("CreateAlias")
	defer b.mu.Unlock()

	if _, ok := b.functions.Get(name); !ok {
		return nil, ErrFunctionNotFound
	}

	// Validate the target version: must be "$LATEST" or an existing published version.
	if input.FunctionVersion != versionLatest {
		if !versionInList(b.versions[name], input.FunctionVersion) {
			return nil, ErrVersionNotFound
		}
	}

	if _, exists := b.aliases.Get(aliasKey(name, input.Name)); exists {
		return nil, ErrAliasAlreadyExists
	}

	alias := &FunctionAlias{
		Name:            input.Name,
		AliasArn:        buildAliasARN(b.region, b.accountID, name, input.Name),
		FunctionVersion: input.FunctionVersion,
		Description:     input.Description,
		RoutingConfig:   input.RoutingConfig,
		RevisionID:      uuid.New().String(),
	}

	b.aliases.Put(alias)

	return alias, nil
}

// GetAlias returns a named alias for a function.
func (b *InMemoryBackend) GetAlias(name, aliasName string) (*FunctionAlias, error) {
	b.mu.RLock("GetAlias")
	defer b.mu.RUnlock()

	if _, ok := b.functions.Get(name); !ok {
		return nil, ErrFunctionNotFound
	}

	alias, ok := b.aliases.Get(aliasKey(name, aliasName))
	if !ok {
		return nil, ErrAliasNotFound
	}

	return alias, nil
}

// ListAliases returns a page of aliases for a function sorted by name.
// If functionVersion is non-empty, only aliases pointing to that version are returned.
func (b *InMemoryBackend) ListAliases(
	name, functionVersion, marker string,
	maxItems int,
) (page.Page[*FunctionAlias], error) {
	b.mu.RLock("ListAliases")
	defer b.mu.RUnlock()

	if _, ok := b.functions.Get(name); !ok {
		return page.Page[*FunctionAlias]{}, ErrFunctionNotFound
	}

	forFunction := b.aliasesByFunction.Get(name)
	result := make([]*FunctionAlias, 0, len(forFunction))

	for _, a := range forFunction {
		if functionVersion != "" && a.FunctionVersion != functionVersion {
			continue
		}

		result = append(result, a)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})

	return page.New(result, marker, maxItems, lambdaDefaultMaxItems), nil
}

// UpdateAlias updates an existing alias.
func (b *InMemoryBackend) UpdateAlias(
	name, aliasName string,
	input *UpdateAliasInput,
) (*FunctionAlias, error) {
	b.mu.Lock("UpdateAlias")
	defer b.mu.Unlock()

	alias, ok := b.aliases.Get(aliasKey(name, aliasName))
	if !ok {
		return nil, ErrAliasNotFound
	}

	if input.RevisionID != "" && input.RevisionID != alias.RevisionID {
		return nil, ErrPreconditionFailed
	}

	if input.FunctionVersion != "" {
		if input.FunctionVersion != versionLatest && !versionInList(b.versions[name], input.FunctionVersion) {
			return nil, ErrVersionNotFound
		}

		alias.FunctionVersion = input.FunctionVersion
	}

	if input.Description != "" {
		alias.Description = input.Description
	}

	if input.RoutingConfig != nil {
		alias.RoutingConfig = input.RoutingConfig
	}

	alias.RevisionID = uuid.New().String()

	return alias, nil
}

// DeleteAlias removes a named alias from a function.
func (b *InMemoryBackend) DeleteAlias(name, aliasName string) error {
	b.mu.Lock("DeleteAlias")
	defer b.mu.Unlock()

	if !b.aliases.Delete(aliasKey(name, aliasName)) {
		return ErrAliasNotFound
	}

	return nil
}

// deepCopyEnvironment returns a deep copy of an EnvironmentConfig, or nil if src is nil.
func deepCopyEnvironment(src *EnvironmentConfig) *EnvironmentConfig {
	if src == nil {
		return nil
	}

	vars := make(map[string]string, len(src.Variables))
	maps.Copy(vars, src.Variables)

	return &EnvironmentConfig{Variables: vars}
}

// deepCopyFunctionLayers returns a shallow copy of a FunctionLayer slice.
func deepCopyFunctionLayers(src []*FunctionLayer) []*FunctionLayer {
	if len(src) == 0 {
		return nil
	}

	dst := make([]*FunctionLayer, len(src))
	for i, l := range src {
		if l == nil {
			continue
		}

		cp := *l
		dst[i] = &cp
	}

	return dst
}

// fnToVersion converts a live FunctionConfiguration to a $LATEST FunctionVersion.
func fnToVersion(fn *FunctionConfiguration) *FunctionVersion {
	return &FunctionVersion{
		FunctionName:        fn.FunctionName,
		FunctionArn:         fn.FunctionArn,
		Description:         fn.Description,
		Version:             versionLatest,
		Runtime:             fn.Runtime,
		Handler:             fn.Handler,
		Role:                fn.Role,
		MemorySize:          fn.MemorySize,
		Timeout:             fn.Timeout,
		PackageType:         fn.PackageType,
		ImageURI:            fn.ImageURI,
		ImageConfigResponse: fn.ImageConfigResponse,
		Environment:         fn.Environment,
		VpcConfig:           fn.VpcConfig,
		TracingConfig:       fn.TracingConfig,
		FileSystemConfigs:   fn.FileSystemConfigs,
		DeadLetterConfig:    fn.DeadLetterConfig,
		Layers:              fn.Layers,
		CodeSize:            fn.CodeSize,
		RevisionID:          fn.RevisionID,
		CreatedAt:           fn.LastModified,
		State:               fn.State,
		CodeSha256:          fn.CodeSha256,
		SnapStart:           copySnapStart(fn.SnapStart),
		DurableConfig:       fn.DurableConfig,
	}
}

// copySnapStart returns a copy of the SnapStart response so version snapshots do
// not alias the live function's configuration. Returns nil for an unset config
// (field omitted from responses).
func copySnapStart(cfg *SnapStartResponse) *SnapStartResponse {
	if cfg == nil {
		return nil
	}

	dup := *cfg

	return &dup
}

// versionToFn synthesises a FunctionConfiguration from an immutable version snapshot.
// This is used for qualified invocations.
func versionToFn(v *FunctionVersion) *FunctionConfiguration {
	return &FunctionConfiguration{
		FunctionName: v.FunctionName,
		FunctionArn:  v.FunctionArn,
		Description:  v.Description,
		Runtime:      v.Runtime,
		Handler:      v.Handler,
		Role:         v.Role,
		MemorySize:   v.MemorySize,
		Timeout:      v.Timeout,
		PackageType:  v.PackageType,
		ImageURI:     v.ImageURI,
		Environment:  v.Environment,
		CodeSize:     v.CodeSize,
		RevisionID:   v.RevisionID,
		LastModified: v.CreatedAt,
		State:        v.State,
		SnapStart:    v.SnapStart,
		Version:      v.Version,
	}
}

// versionToConfig builds a complete FunctionConfiguration response from an
// immutable version snapshot. Unlike versionToFn (used for the invocation hot
// path, which only needs runtime-critical fields), this preserves every
// control-plane field AWS returns from GetFunction on a published version,
// including Version, Layers, VpcConfig, TracingConfig, and the version ARN.
func versionToConfig(v *FunctionVersion) *FunctionConfiguration {
	return &FunctionConfiguration{
		FunctionName:        v.FunctionName,
		FunctionArn:         v.FunctionArn,
		Description:         v.Description,
		Runtime:             v.Runtime,
		Handler:             v.Handler,
		Role:                v.Role,
		MemorySize:          v.MemorySize,
		Timeout:             v.Timeout,
		PackageType:         v.PackageType,
		ImageURI:            v.ImageURI,
		ImageConfigResponse: v.ImageConfigResponse,
		Environment:         deepCopyEnvironment(v.Environment),
		VpcConfig:           v.VpcConfig,
		TracingConfig:       v.TracingConfig,
		FileSystemConfigs:   v.FileSystemConfigs,
		DeadLetterConfig:    v.DeadLetterConfig,
		Layers:              deepCopyFunctionLayers(v.Layers),
		CodeSize:            v.CodeSize,
		CodeSha256:          v.CodeSha256,
		RevisionID:          v.RevisionID,
		LastModified:        v.CreatedAt,
		State:               v.State,
		Version:             v.Version,
		SnapStart:           copySnapStart(v.SnapStart),
		DurableConfig:       v.DurableConfig,
		// Published versions are immutable: their last-update status is always
		// Successful (AWS never reports Pending/InProgress for a numbered version).
		LastUpdateStatus: LastUpdateStatusSuccessful,
	}
}
