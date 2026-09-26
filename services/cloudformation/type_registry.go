package cloudformation

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/google/uuid"

	awsarn "github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// buildTypeARN returns the real AWS-shaped ARN for a privately-registered
// CloudFormation type: TypeName's "::" separators become "-" and the ARN is
// account/region-scoped (real type ARNs, e.g.
// arn:aws:cloudformation:us-east-1:123456789012:type/resource/MyOrg-Svc-Res,
// per RegisterType/DescribeType's TypeArn/Arn members).
func (b *InMemoryBackend) buildTypeARN(typeName string) string {
	hyphenated := strings.ReplaceAll(typeName, "::", "-")

	return awsarn.Build("cloudformation", b.region, b.accountID, "type/resource/"+hyphenated)
}

// buildTypeVersionARN returns the version-specific ARN for a registered type
// version. Real DescribeTypeRegistrationOutput.TypeVersionArn is distinct
// from TypeArn -- the ARN of this specific version, not the type as a whole
// (cloudformation@v1.76.1 api_op_DescribeTypeRegistration.go) -- and real
// TypeVersionSummary.Arn (ListTypeVersions) carries the same per-version
// suffix.
func buildTypeVersionARN(typeARN, versionID string) string {
	return typeARN + "/" + versionID
}

// typeVersionIDPattern matches the "00000001"-style 8-digit version id
// RegisterType/SetTypeDefaultVersion produce (see RegisterType's
// fmt.Sprintf("%08d", versionNum)).
var typeVersionIDPattern = regexp.MustCompile(`^\d{8}$`)

// splitTypeVersionARN splits a version-suffixed type ARN
// (.../type/resource/Name/00000001) into its base ARN and version id. The
// third return is false when typeARN has no such trailing version segment.
func splitTypeVersionARN(typeARN string) (string, string, bool) {
	idx := strings.LastIndex(typeARN, "/")
	if idx < 0 {
		return "", "", false
	}

	candidate := typeARN[idx+1:]
	if !typeVersionIDPattern.MatchString(candidate) {
		return "", "", false
	}

	return typeARN[:idx], candidate, true
}

func (b *InMemoryBackend) ActivateType(typeName, typeArn string) (string, error) {
	b.mu.Lock("ActivateType")
	defer b.mu.Unlock()
	key := typeArn
	if key == "" {
		key = b.buildTypeARN(typeName)
	}
	if t, ok := b.typeRegistry.Get(key); ok {
		t.IsActivated = true
	} else {
		b.typeRegistry.Put(&RegisteredType{
			TypeArn:     key,
			TypeName:    typeName,
			Type:        typeKindResource,
			VersionID:   "00000001",
			Status:      statusComplete,
			IsActivated: true,
		})
	}

	return key, nil
}

func (b *InMemoryBackend) DeactivateType(typeName, typeArn string) error {
	b.mu.Lock("DeactivateType")
	defer b.mu.Unlock()
	key := typeArn
	if key == "" {
		key = b.buildTypeARN(typeName)
	}
	t, ok := b.typeRegistry.Get(key)
	if !ok || !t.IsActivated {
		return fmt.Errorf("%w: %s", ErrTypeNotFound, key)
	}
	t.IsActivated = false

	return nil
}

func (b *InMemoryBackend) RegisterType(typeName, _ string) (string, error) {
	b.mu.Lock("RegisterType")
	defer b.mu.Unlock()
	token := uuid.New().String()
	typeArn := b.buildTypeARN(typeName)
	// Each call to RegisterType creates a new version.
	existingVersions := b.typeVersions[typeArn]
	versionNum := len(existingVersions) + 1
	versionID := fmt.Sprintf("%08d", versionNum)
	b.typeVersions[typeArn] = append(b.typeVersions[typeArn], &RegisteredTypeVersion{
		TypeArn:   typeArn,
		VersionID: versionID,
		IsDefault: true,
		Status:    statusComplete,
	})
	// Mark prior versions as non-default.
	for i := range b.typeVersions[typeArn][:len(b.typeVersions[typeArn])-1] {
		b.typeVersions[typeArn][i].IsDefault = false
	}
	if t, ok := b.typeRegistry.Get(typeArn); ok {
		t.VersionID = versionID
		t.DefaultVersion = versionID
	} else {
		b.typeRegistry.Put(&RegisteredType{
			TypeArn:        typeArn,
			TypeName:       typeName,
			Type:           "RESOURCE",
			VersionID:      versionID,
			DefaultVersion: versionID,
			Status:         statusComplete,
		})
	}
	b.typeRegistrations.Put(&TypeRegistrationRecord{
		Token:     token,
		TypeName:  typeName,
		TypeArn:   typeArn,
		VersionID: versionID,
		Status:    statusComplete,
	})

	return token, nil
}

// DeregisterType deprecates a type or a single version of it (cloudformation@v1.76.1
// api_op_DeregisterType.go doc comment). With no versionID it deprecates the whole
// type. With a versionID: deregistering the default version while other active
// versions exist is rejected; deregistering the last active version (including the
// only version) deprecates the whole type along with it.
func (b *InMemoryBackend) DeregisterType(typeName, typeArn, versionID string) error {
	b.mu.Lock("DeregisterType")
	defer b.mu.Unlock()

	key := typeArn
	if key == "" {
		key = b.buildTypeARN(typeName)
	}
	t, ok := b.typeRegistry.Get(key)
	if !ok {
		return fmt.Errorf("%w: %s", ErrTypeNotFound, key)
	}

	if versionID == "" {
		t.Status = typeStatusDeprecated
		for _, v := range b.typeVersions[key] {
			v.Status = typeStatusDeprecated
		}

		return nil
	}

	versions := b.typeVersions[key]
	if len(versions) == 0 {
		if versionID != t.VersionID {
			return fmt.Errorf("%w: %s version %s", ErrTypeVersionNotFound, key, versionID)
		}
		t.Status = typeStatusDeprecated

		return nil
	}

	var target *RegisteredTypeVersion
	activeCount := 0
	for _, v := range versions {
		if v.Status != typeStatusDeprecated {
			activeCount++
		}
		if v.VersionID == versionID {
			target = v
		}
	}
	if target == nil || target.Status == typeStatusDeprecated {
		return fmt.Errorf("%w: %s version %s", ErrTypeVersionNotFound, key, versionID)
	}

	isDefault := versionID == t.DefaultVersion
	if isDefault && activeCount > 1 {
		return fmt.Errorf("%w: %s version %s", ErrCannotDeregisterDefaultVersion, key, versionID)
	}

	target.Status = typeStatusDeprecated
	target.IsDefault = false
	if isDefault {
		t.Status = typeStatusDeprecated
	}

	return nil
}

func (b *InMemoryBackend) PublishType(typeName string) (string, error) {
	b.mu.Lock("PublishType")
	defer b.mu.Unlock()
	typeArn := b.buildTypeARN(typeName)
	t, ok := b.typeRegistry.Get(typeArn)
	if !ok {
		return "", fmt.Errorf("%w: %s", ErrTypeNotFound, typeArn)
	}
	t.IsPublished = true

	return typeArn, nil
}

// SetTypeDefaultVersion identifies the type by Arn, or by TypeName when Arn
// is empty (SetTypeDefaultVersionInput allows either -- api_op_SetTypeDefaultVersion.go:
// "Arn" or "TypeName"+"Type"; the aws_cloudformation_type resource always
// sends TypeName, never Arn).
func (b *InMemoryBackend) SetTypeDefaultVersion(typeArn, typeName, version string) error {
	b.mu.Lock("SetTypeDefaultVersion")
	defer b.mu.Unlock()

	key := typeArn
	if key == "" {
		key = b.buildTypeARN(typeName)
	}

	t, ok := b.typeRegistry.Get(key)
	if !ok {
		return fmt.Errorf("%w: %s", ErrTypeNotFound, key)
	}
	t.DefaultVersion = version
	t.VersionID = version
	// Update typeVersions IsDefault flags.
	for _, v := range b.typeVersions[key] {
		v.IsDefault = v.VersionID == version
	}

	return nil
}

func (b *InMemoryBackend) SetTypeConfiguration(typeName, configuration string) (string, error) {
	b.mu.Lock("SetTypeConfiguration")
	defer b.mu.Unlock()
	b.typeConfigs[typeName] = configuration

	return "arn:aws:cloudformation:::type-configuration/resource/" + typeName + "/default", nil
}

func (b *InMemoryBackend) BatchDescribeTypeConfigurations(
	identifiers []TypeConfigurationIdentifier,
) ([]TypeConfigurationDetail, []BatchDescribeTypeConfigurationsError, []TypeConfigurationIdentifier) {
	b.mu.RLock("BatchDescribeTypeConfigurations")
	defer b.mu.RUnlock()

	var (
		details     []TypeConfigurationDetail
		errs        []BatchDescribeTypeConfigurationsError
		unprocessed []TypeConfigurationIdentifier
	)

	for _, ident := range identifiers {
		name := ident.TypeName
		if name == "" {
			name = ident.TypeConfigurationArn
		}
		if name == "" {
			unprocessed = append(unprocessed, ident)

			continue
		}

		typeArn := ident.TypeArn
		if typeArn == "" {
			typeArn = b.buildTypeARN(name)
		}
		cfg, hasCfg := b.typeConfigs[name]
		_, registered := b.typeRegistry.Get(typeArn)
		if !hasCfg && !registered {
			errs = append(errs, BatchDescribeTypeConfigurationsError{
				TypeConfigurationIdentifier: &ident,
				// BatchDescribeTypeConfigurations' own deserializer declares
				// CFNRegistryException/TypeConfigurationNotFoundException, not
				// TypeNotFoundException -- that code belongs to
				// ActivateType/DeactivateType/DeregisterType/DescribeType/
				// PublishType, which operate on types rather than type
				// configurations (confirmed against
				// aws-sdk-go-v2/service/cloudformation@v1.76.1/deserializers.go).
				ErrorCode:    "TypeConfigurationNotFoundException",
				ErrorMessage: fmt.Sprintf("type configuration not found: %s", name),
			})

			continue
		}
		if cfg == "" {
			cfg = "{}"
		}
		const defaultConfigSuffix = "/default"
		configArn := ident.TypeConfigurationArn
		if configArn == "" {
			configArn = "arn:aws:cloudformation:::type-configuration/resource/" + name + defaultConfigSuffix
		}
		details = append(details, TypeConfigurationDetail{
			Arn:                    configArn,
			TypeName:               name,
			TypeArn:                typeArn,
			Alias:                  ident.TypeConfigurationAlias,
			Configuration:          cfg,
			IsDefaultConfiguration: !hasCfg,
		})
	}

	return details, errs, unprocessed
}

// ListTypes returns registered/activated types, paginated by
// MaxResults/NextToken (real query-protocol form fields, ListTypes
// serializers.go:9145-9153). visibilityFilter/provisioningTypeFilter mirror
// ListTypesInput's own Visibility/ProvisioningType fields; an empty filter
// imposes no constraint (the documented default: PRIVATE for Visibility,
// unfiltered for ProvisioningType).
func (b *InMemoryBackend) ListTypes(
	visibilityFilter, provisioningTypeFilter string, maxResults int, nextToken string,
) (page.Page[TypeSummary], error) {
	b.mu.RLock("ListTypes")
	defer b.mu.RUnlock()

	if provisioningTypeFilter != "" && provisioningTypeFilter != provisioningTypeFullyMutable {
		return page.New([]TypeSummary{}, nextToken, maxResults, cfnDefaultPageSize), nil
	}

	result := make([]TypeSummary, 0, b.typeRegistry.Len())
	for _, t := range b.typeRegistry.All() {
		if t.Status == typeStatusDeprecated {
			continue
		}
		if t.Status == statusComplete || t.IsActivated {
			visibility := "PRIVATE"
			if t.IsPublished {
				visibility = typeVisibilityPublic
			}

			if visibilityFilter != "" && visibilityFilter != visibility {
				continue
			}

			result = append(result, TypeSummary{
				TypeName:         t.TypeName,
				TypeArn:          t.TypeArn,
				Type:             t.Type,
				Visibility:       visibility,
				Description:      t.Configuration,
				DefaultVersionID: t.DefaultVersion,
				IsActivated:      t.IsActivated,
			})
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].TypeName < result[j].TypeName })

	return page.New(result, nextToken, maxResults, cfnDefaultPageSize), nil
}

// ListTypeVersions defaults to LIVE versions only, matching ListTypeVersionsInput's
// DeprecatedStatus field ("The default is LIVE", cloudformation@v1.76.1
// api_op_ListTypeVersions.go). Paginated by MaxResults/NextToken (real
// query-protocol form fields, serializers.go's
// awsAwsquery_serializeOpDocumentListTypeVersionsInput).
func (b *InMemoryBackend) ListTypeVersions(
	typeName, deprecatedStatus string, maxResults int, nextToken string,
) (page.Page[string], error) {
	b.mu.RLock("ListTypeVersions")
	defer b.mu.RUnlock()
	typeArn := b.buildTypeARN(typeName)
	wantDeprecated := deprecatedStatus == typeStatusDeprecated

	if versions, ok := b.typeVersions[typeArn]; ok && len(versions) > 0 {
		ids := make([]string, 0, len(versions))
		for _, v := range versions {
			if (v.Status == typeStatusDeprecated) != wantDeprecated {
				continue
			}
			ids = append(ids, v.VersionID)
		}

		return page.New(ids, nextToken, maxResults, cfnDefaultPageSize), nil
	}
	// Fallback: if no version records but type exists, return its current version.
	if t, ok := b.typeRegistry.Get(typeArn); ok {
		if (t.Status == typeStatusDeprecated) != wantDeprecated {
			return page.New([]string{}, nextToken, maxResults, cfnDefaultPageSize), nil
		}

		return page.New([]string{t.VersionID}, nextToken, maxResults, cfnDefaultPageSize), nil
	}

	return page.New([]string{}, nextToken, maxResults, cfnDefaultPageSize), nil
}

// ListTypeRegistrations returns registration tokens, paginated by
// MaxResults/NextToken (real query-protocol form fields,
// serializers.go's awsAwsquery_serializeOpDocumentListTypeRegistrationsInput).
// Snapshot (not All) for a deterministic, sortable-by-Token order --
// required for stable pagination across calls.
// ListTypeRegistrations: registrationStatusFilter mirrors
// ListTypeRegistrationsInput.RegistrationStatusFilter (default IN_PROGRESS,
// api_op_ListTypeRegistrations.go); every registration this backend creates
// completes synchronously (see DescribeTypeRegistration's own doc comment),
// so filtering by COMPLETE matches every registration and IN_PROGRESS/FAILED
// match none, honestly reflecting that this backend never produces those
// states rather than fabricating an in-progress window. The Type
// (RESOURCE/MODULE/HOOK) filter remains unimplemented -- TypeRegistrationRecord
// doesn't track a type kind at all, a separate, pre-existing gap.
func (b *InMemoryBackend) ListTypeRegistrations(
	typeName, _ /* typeKind */, registrationStatusFilter string, maxResults int, nextToken string,
) (page.Page[string], error) {
	b.mu.RLock("ListTypeRegistrations")
	defer b.mu.RUnlock()
	tokens := make([]string, 0, b.typeRegistrations.Len())
	for _, rec := range b.typeRegistrations.Snapshot() {
		if typeName != "" && rec.TypeName != typeName {
			continue
		}

		if registrationStatusFilter != "" && rec.Status != registrationStatusFilter {
			continue
		}

		tokens = append(tokens, rec.Token)
	}

	return page.New(tokens, nextToken, maxResults, cfnDefaultPageSize), nil
}

// DescribeTypeRegistration returns the registration's ProgressStatus and, per
// the real DescribeTypeRegistrationOutput.TypeArn doc comment ("For
// registration requests with a ProgressStatus of other than COMPLETE, this
// will be null"), its TypeArn -- populated here since every registration this
// mock creates is immediately COMPLETE.
func (b *InMemoryBackend) DescribeTypeRegistration(
	registrationToken string,
) (string, string, string, error) {
	b.mu.RLock("DescribeTypeRegistration")
	defer b.mu.RUnlock()
	rec, ok := b.typeRegistrations.Get(registrationToken)
	if !ok {
		return "", "", "", fmt.Errorf("%w: %s", ErrRegistrationTokenNotFound, registrationToken)
	}

	versionArn := rec.TypeArn
	if rec.VersionID != "" {
		versionArn = buildTypeVersionARN(rec.TypeArn, rec.VersionID)
	}

	return rec.Status, rec.TypeArn, versionArn, nil
}

// TestType starts a test run for a registered extension. versionID mirrors
// TestTypeInput.VersionId (api_op_TestType.go: "You can specify the version
// id with either Arn, or with TypeName and Type. If you don't specify a
// version, CloudFormation uses the default version"); when given, it is
// validated as a real registered version of the target type rather than
// silently ignored.
func (b *InMemoryBackend) TestType(typeName, typeArn, versionID string) (string, error) {
	b.mu.Lock("TestType")
	defer b.mu.Unlock()
	token := uuid.New().String()
	key := typeArn
	if key == "" {
		key = b.buildTypeARN(typeName)
	}

	if versionID != "" {
		found := false

		for _, v := range b.typeVersions[key] {
			if v.VersionID == versionID {
				found = true

				break
			}
		}

		if !found {
			return "", fmt.Errorf("%w: %s version %s", ErrTypeVersionNotFound, key, versionID)
		}
	}

	resolvedVersion := versionID
	if resolvedVersion == "" {
		if t, ok := b.typeRegistry.Get(key); ok {
			resolvedVersion = t.DefaultVersion
		}
	}

	b.typeRegistrations.Put(&TypeRegistrationRecord{
		Token:     token,
		TypeName:  typeName,
		TypeArn:   key,
		VersionID: resolvedVersion,
		Status:    statusComplete,
	})

	return token, nil
}

func (b *InMemoryBackend) RegisterPublisher(connectionArn string) (string, error) {
	b.mu.Lock("RegisterPublisher")
	defer b.mu.Unlock()
	publisherID := uuid.New().String()
	b.publishers.Put(&Publisher{
		PublisherID:   publisherID,
		ConnectionArn: connectionArn,
		Status:        "VERIFIED",
	})

	return publisherID, nil
}

func (b *InMemoryBackend) DescribePublisher(publisherID string) (string, error) {
	b.mu.RLock("DescribePublisher")
	defer b.mu.RUnlock()
	p, ok := b.publishers.Get(publisherID)
	if !ok {
		return "", fmt.Errorf("%w: %s", ErrPublisherNotFound, publisherID)
	}

	return p.Status, nil
}

// typeVersionDeprecatedStatus reports LIVE/DEPRECATED for a resolved version,
// falling back to the whole type's status when the version itself isn't
// individually tracked as deprecated.
func (b *InMemoryBackend) typeVersionDeprecatedStatus(reg *RegisteredType, resolvedVersionID string) string {
	if reg.Status == typeStatusDeprecated {
		return typeStatusDeprecated
	}
	for _, v := range b.typeVersions[reg.TypeArn] {
		if v.VersionID == resolvedVersionID && v.Status == typeStatusDeprecated {
			return typeStatusDeprecated
		}
	}

	return "LIVE"
}

// findTypeByARNLocked looks up a registered type by its base ARN, falling
// back to stripping a trailing "/<versionId>" segment (see
// splitTypeVersionARN) when the exact key isn't found: DescribeTypeRegistrationOutput.
// TypeVersionArn (and TypeVersionSummary.Arn) carry that suffix, and the
// aws_cloudformation_type resource stores TypeVersionArn as its id and reads
// it back via DescribeTypeInput.Arn. When the fallback matches, it also
// returns the stripped version id so the caller resolves to that specific
// version. Must be called with at least a read lock held.
func (b *InMemoryBackend) findTypeByARNLocked(typeARN string) (*RegisteredType, string, bool) {
	if r, ok := b.typeRegistry.Get(typeARN); ok {
		return r, "", true
	}

	base, ver, ok := splitTypeVersionARN(typeARN)
	if !ok {
		return nil, "", false
	}

	r, ok := b.typeRegistry.Get(base)
	if !ok {
		return nil, "", false
	}

	return r, ver, true
}

// DescribeType returns detailed information about a registered CloudFormation type.
// Lookup is by typeName, arn, or versionID — at least one must be non-empty.
func (b *InMemoryBackend) DescribeType(typeName, arn, versionID string) (*TypeDetails, error) {
	b.mu.RLock("DescribeType")
	defer b.mu.RUnlock()

	var reg *RegisteredType

	switch {
	case arn != "":
		r, versionFromARN, ok := b.findTypeByARNLocked(arn)
		if !ok {
			return nil, fmt.Errorf("%w: %s", ErrTypeNotFound, arn)
		}

		reg = r
		if versionID == "" {
			versionID = versionFromARN
		}
	case typeName != "":
		key := b.buildTypeARN(typeName)

		r, ok := b.typeRegistry.Get(key)
		if !ok {
			return nil, fmt.Errorf("%w: %s", ErrTypeNotFound, typeName)
		}

		reg = r
	default:
		return nil, fmt.Errorf("%w: TypeName or Arn is required", ErrTypeNotFound)
	}

	// Determine which version to return.
	resolvedVersionID := reg.DefaultVersion
	if versionID != "" {
		// Verify the version exists.
		found := false
		for _, v := range b.typeVersions[reg.TypeArn] {
			if v.VersionID == versionID {
				found = true
				resolvedVersionID = versionID

				break
			}
		}
		if !found && len(b.typeVersions[reg.TypeArn]) > 0 {
			return nil, fmt.Errorf("%w: %s version %s", ErrTypeVersionNotFound, reg.TypeName, versionID)
		}
	}

	isDefaultVersion := resolvedVersionID == reg.DefaultVersion
	visibility := typeVisibilityPrivate
	if reg.IsPublished {
		visibility = typeVisibilityPublic
	}
	deprecatedStatus := b.typeVersionDeprecatedStatus(reg, resolvedVersionID)

	return &TypeDetails{
		TypeName:         reg.TypeName,
		TypeArn:          reg.TypeArn,
		Type:             reg.Type,
		Visibility:       visibility,
		Status:           reg.Status,
		VersionID:        resolvedVersionID,
		DefaultVersionID: reg.DefaultVersion,
		IsActivated:      reg.IsActivated,
		IsDefaultVersion: isDefaultVersion,
		DeprecatedStatus: deprecatedStatus,
	}, nil
}
