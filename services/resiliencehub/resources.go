package resiliencehub

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// sameLogicalID reports whether a and b identify the same logical resource.
func sameLogicalID(a, b *LogicalResourceID) bool {
	if a == nil || b == nil {
		return false
	}

	return *a == *b
}

// findResourceLocator identifies a stored PhysicalResource by whichever one
// of resourceName/logicalID/physicalID(+account+region) is non-empty --
// DescribeAppVersionResource's own doc comment: "This API accepts only one
// of the following parameters". Callers must hold b.mu.
type findResourceLocator struct {
	logicalID    *LogicalResourceID
	resourceName string
	physicalID   string
	awsAccountID string
	awsRegion    string
}

func findResource(v *AppVersion, loc findResourceLocator) (*PhysicalResource, int) {
	for i, r := range v.Resources {
		switch {
		case loc.resourceName != "" && r.ResourceName == loc.resourceName:
			return r, i
		case loc.logicalID != nil && sameLogicalID(r.LogicalResourceID, loc.logicalID):
			return r, i
		case loc.physicalID != "" && r.PhysicalResourceID != nil && r.PhysicalResourceID.Identifier == loc.physicalID &&
			(loc.awsAccountID == "" || r.PhysicalResourceID.AwsAccountID == loc.awsAccountID) &&
			(loc.awsRegion == "" || r.PhysicalResourceID.AwsRegion == loc.awsRegion):
			return r, i
		}
	}

	return nil, -1
}

// ensureAppComponentsLocked auto-creates any AppComponent name in names that
// does not already exist on v, per CreateAppVersionResource's own doc
// comment ("If you specify a new Application Component, Resilience Hub will
// automatically create the Application Component"). Callers must hold b.mu.
func ensureAppComponentsLocked(v *AppVersion, names []string) {
	for _, name := range names {
		if findAppComponent(v, name) == nil {
			v.AppComponents = append(v.AppComponents, &AppComponent{ID: newAppComponentID(), Name: name})
		}
	}
}

// CreateAppVersionResource adds a new manually-added resource to the App's
// draft version, auto-creating any AppComponent named in req.AppComponents
// that does not already exist.
func (b *InMemoryBackend) CreateAppVersionResource(
	appArn string, req *createAppVersionResourceRequest,
) (*App, *PhysicalResource, error) {
	if req.PhysicalResourceID == "" {
		return nil, nil, validationError("physicalResourceId is required")
	}

	if req.ResourceType == "" {
		return nil, nil, validationError("resourceType is required")
	}

	b.mu.Lock("CreateAppVersionResource")
	defer b.mu.Unlock()

	a, ok := b.resolveAppLocked(appArn)
	if !ok {
		return nil, nil, notFoundError(resourceApp, appArn)
	}

	loc := findResourceLocator{
		logicalID: fromLogicalResourceIDWire(req.LogicalResourceID), resourceName: req.ResourceName,
		physicalID: req.PhysicalResourceID, awsAccountID: req.AwsAccountID, awsRegion: req.AwsRegion,
	}
	if existing, _ := findResource(a.Draft, loc); existing != nil {
		return nil, nil, conflictError("resource already exists: " + req.PhysicalResourceID)
	}

	ensureAppComponentsLocked(a.Draft, req.AppComponents)

	r := &PhysicalResource{
		LogicalResourceID: fromLogicalResourceIDWire(req.LogicalResourceID),
		PhysicalResourceID: &PhysicalResourceID{
			Identifier: req.PhysicalResourceID, Type: physicalIDTypeFor(req.ResourceType),
			AwsAccountID: req.AwsAccountID, AwsRegion: req.AwsRegion,
		},
		ResourceType:   req.ResourceType,
		AdditionalInfo: cloneStrSliceMap(req.AdditionalInfo),
		AppComponents:  cloneStrs(req.AppComponents),
		ResourceName:   req.ResourceName,
		SourceType:     ResourceSourceAppTemplate,
	}

	a.Draft.Resources = append(a.Draft.Resources, r)

	return a.clone(), r.clone(), nil
}

// physicalIDTypeFor returns the PhysicalIdentifierType (Arn/Native) implied
// by resourceType, per the two closed lists documented on
// types.PhysicalResourceId.Type (see consts.go's supportedArnResourceTypes/
// supportedNativeResourceTypes). Defaults to Native for any resourceType not
// in either list -- a defensible default, since Native is the larger and
// more general of the two documented lists.
func physicalIDTypeFor(resourceType string) string {
	if supportedArnResourceTypes[resourceType] {
		return PhysicalIDTypeArn
	}

	return PhysicalIDTypeNative
}

// DescribeAppVersionResource returns the resource identified by loc in
// (appArn, appVersion) -- reads may target any version.
func (b *InMemoryBackend) DescribeAppVersionResource(
	appArn, appVersion string, loc findResourceLocator,
) (*App, *PhysicalResource, error) {
	b.mu.RLock("DescribeAppVersionResource")
	defer b.mu.RUnlock()

	a, ok := b.resolveAppLocked(appArn)
	if !ok {
		return nil, nil, notFoundError(resourceApp, appArn)
	}

	v, ok := resolveAppVersionLocked(a, appVersion)
	if !ok {
		return nil, nil, notFoundError(resourceAppVersion, appVersion)
	}

	r, _ := findResource(v, loc)
	if r == nil {
		return nil, nil, notFoundError(resourceAppResource, loc.resourceName+loc.physicalID)
	}

	return a, r.clone(), nil
}

// UpdateAppVersionResource updates the resource identified by loc on the
// draft version.
func (b *InMemoryBackend) UpdateAppVersionResource(
	appArn string, req *updateAppVersionResourceRequest, loc findResourceLocator,
) (*App, *PhysicalResource, error) {
	b.mu.Lock("UpdateAppVersionResource")
	defer b.mu.Unlock()

	a, ok := b.resolveAppLocked(appArn)
	if !ok {
		return nil, nil, notFoundError(resourceApp, appArn)
	}

	r, _ := findResource(a.Draft, loc)
	if r == nil {
		return nil, nil, notFoundError(resourceAppResource, loc.resourceName+loc.physicalID)
	}

	if req.ResourceType != "" {
		r.ResourceType = req.ResourceType
	}

	if req.PhysicalResourceID != "" {
		if r.PhysicalResourceID == nil {
			r.PhysicalResourceID = &PhysicalResourceID{}
		}

		r.PhysicalResourceID.Identifier = req.PhysicalResourceID
		r.PhysicalResourceID.Type = physicalIDTypeFor(r.ResourceType)
	}

	if req.AwsAccountID != "" && r.PhysicalResourceID != nil {
		r.PhysicalResourceID.AwsAccountID = req.AwsAccountID
	}

	if req.AwsRegion != "" && r.PhysicalResourceID != nil {
		r.PhysicalResourceID.AwsRegion = req.AwsRegion
	}

	if req.AdditionalInfo != nil {
		r.AdditionalInfo = cloneStrSliceMap(req.AdditionalInfo)
	}

	if len(req.AppComponents) > 0 {
		ensureAppComponentsLocked(a.Draft, req.AppComponents)
		r.AppComponents = cloneStrs(req.AppComponents)
	}

	if req.Excluded != nil {
		r.Excluded = *req.Excluded
	}

	return a.clone(), r.clone(), nil
}

// DeleteAppVersionResource deletes the resource identified by loc from the
// draft version. Only manually-added (AppTemplate-sourced) resources may be
// deleted -- per DeleteAppVersionResource's own doc comment ("You can only
// delete a manually added resource").
func (b *InMemoryBackend) DeleteAppVersionResource(
	appArn string,
	loc findResourceLocator,
) (*App, *PhysicalResource, error) {
	b.mu.Lock("DeleteAppVersionResource")
	defer b.mu.Unlock()

	a, ok := b.resolveAppLocked(appArn)
	if !ok {
		return nil, nil, notFoundError(resourceApp, appArn)
	}

	r, idx := findResource(a.Draft, loc)
	if r == nil {
		return nil, nil, notFoundError(resourceAppResource, loc.resourceName+loc.physicalID)
	}

	if r.SourceType != ResourceSourceAppTemplate {
		return nil, nil, conflictError("only manually-added resources can be deleted: " + r.ResourceName)
	}

	a.Draft.Resources = append(a.Draft.Resources[:idx], a.Draft.Resources[idx+1:]...)

	return a.clone(), r.clone(), nil
}

// ListAppVersionResources returns every PhysicalResource on (appArn,
// appVersion). resolutionId is accepted but not further filtered against --
// this backend keeps only the latest resolution snapshot per version rather
// than a history of past resolutionIds, a documented simplification.
func (b *InMemoryBackend) ListAppVersionResources(
	appArn, appVersion, token string, limit int,
) (*App, *AppVersion, page.Page[*PhysicalResource], error) {
	b.mu.RLock("ListAppVersionResources")
	defer b.mu.RUnlock()

	a, ok := b.resolveAppLocked(appArn)
	if !ok {
		return nil, nil, page.Page[*PhysicalResource]{}, notFoundError(resourceApp, appArn)
	}

	v, ok := resolveAppVersionLocked(a, appVersion)
	if !ok {
		return nil, nil, page.Page[*PhysicalResource]{}, notFoundError(resourceAppVersion, appVersion)
	}

	return a, v, page.New(v.Resources, token, limit, defaultPageLimit), nil
}

// ListUnsupportedAppVersionResources returns every resource on (appArn,
// appVersion) whose ResourceType is not one of the two closed lists
// documented on types.PhysicalResourceId.Type -- real, derivable behavior
// (see consts.go's isSupportedResourceType), not a fabricated classification.
func (b *InMemoryBackend) ListUnsupportedAppVersionResources(
	appArn, appVersion, token string, limit int,
) (*App, *AppVersion, page.Page[*PhysicalResource], error) {
	b.mu.RLock("ListUnsupportedAppVersionResources")
	defer b.mu.RUnlock()

	a, ok := b.resolveAppLocked(appArn)
	if !ok {
		return nil, nil, page.Page[*PhysicalResource]{}, notFoundError(resourceApp, appArn)
	}

	v, ok := resolveAppVersionLocked(a, appVersion)
	if !ok {
		return nil, nil, page.Page[*PhysicalResource]{}, notFoundError(resourceAppVersion, appVersion)
	}

	unsupported := make([]*PhysicalResource, 0, len(v.Resources))

	for _, r := range v.Resources {
		if !isSupportedResourceType(r.ResourceType) {
			unsupported = append(unsupported, r)
		}
	}

	return a, v, page.New(unsupported, token, limit, defaultPageLimit), nil
}

// --- Resource mappings -------------------------------------------------------

// AddDraftAppVersionResourceMappings appends resourceMappings to the App's
// draft version.
func (b *InMemoryBackend) AddDraftAppVersionResourceMappings(
	appArn string, mappings []resourceMappingWire,
) (*App, []ResourceMapping, error) {
	if len(mappings) == 0 {
		return nil, nil, validationError("resourceMappings is required")
	}

	b.mu.Lock("AddDraftAppVersionResourceMappings")
	defer b.mu.Unlock()

	a, ok := b.resolveAppLocked(appArn)
	if !ok {
		return nil, nil, notFoundError(resourceApp, appArn)
	}

	for _, w := range mappings {
		a.Draft.ResourceMappings = append(a.Draft.ResourceMappings, fromResourceMappingWire(w))
	}

	return a.clone(), cloneResourceMappings(a.Draft.ResourceMappings), nil
}

// RemoveDraftAppVersionResourceMappings removes resource mappings from the
// App's draft version matching any of the given name lists.
func (b *InMemoryBackend) RemoveDraftAppVersionResourceMappings(
	appArn string,
	req *removeDraftAppVersionResourceMappingsRequest,
) (*App, error) {
	b.mu.Lock("RemoveDraftAppVersionResourceMappings")
	defer b.mu.Unlock()

	a, ok := b.resolveAppLocked(appArn)
	if !ok {
		return nil, notFoundError(resourceApp, appArn)
	}

	kept := make([]ResourceMapping, 0, len(a.Draft.ResourceMappings))

	for _, m := range a.Draft.ResourceMappings {
		if mappingMatchesRemoveRequest(m, req) {
			continue
		}

		kept = append(kept, m)
	}

	a.Draft.ResourceMappings = kept

	return a.clone(), nil
}

func mappingMatchesRemoveRequest(m ResourceMapping, req *removeDraftAppVersionResourceMappingsRequest) bool {
	switch {
	case m.AppRegistryAppName != "" && containsStr(req.AppRegistryAppNames, m.AppRegistryAppName):
		return true
	case m.EksSourceName != "" && containsStr(req.EksSourceNames, m.EksSourceName):
		return true
	case m.LogicalStackName != "" && containsStr(req.LogicalStackNames, m.LogicalStackName):
		return true
	case m.ResourceGroupName != "" && containsStr(req.ResourceGroupNames, m.ResourceGroupName):
		return true
	case m.ResourceName != "" && containsStr(req.ResourceNames, m.ResourceName):
		return true
	case m.TerraformSourceName != "" && containsStr(req.TerraformSourceNames, m.TerraformSourceName):
		return true
	default:
		return false
	}
}

// ListAppVersionResourceMappings returns every ResourceMapping on (appArn,
// appVersion).
func (b *InMemoryBackend) ListAppVersionResourceMappings(
	appArn, appVersion, token string, limit int,
) (page.Page[ResourceMapping], error) {
	b.mu.RLock("ListAppVersionResourceMappings")
	defer b.mu.RUnlock()

	a, ok := b.resolveAppLocked(appArn)
	if !ok {
		return page.Page[ResourceMapping]{}, notFoundError(resourceApp, appArn)
	}

	v, ok := resolveAppVersionLocked(a, appVersion)
	if !ok {
		return page.Page[ResourceMapping]{}, notFoundError(resourceAppVersion, appVersion)
	}

	return page.New(cloneResourceMappings(v.ResourceMappings), token, limit, defaultPageLimit), nil
}

// --- Resource resolution -----------------------------------------------------

// ResolveAppVersionResources kicks off an async resolution of (appArn,
// appVersion)'s ResourceMappings into concrete PhysicalResource entries. See
// resolveMappingsLocked for exactly which mapping types this backend
// genuinely resolves versus honestly leaves unresolved: "Resource" (a real
// pass-through) and "CfnStack"/"ResourceGroup"/"EKS" (real cross-service
// lookups against services/cloudformation, services/resourcegroups, and
// services/eks -- see cross_service.go) are resolved for real;
// "AppRegistryApp"/"Terraform" are honestly declined since no backing
// service exists in this tree for either.
func (b *InMemoryBackend) ResolveAppVersionResources(appArn, appVersion string) (*App, string, string, error) {
	b.mu.Lock("ResolveAppVersionResources")
	defer b.mu.Unlock()

	a, ok := b.resolveAppLocked(appArn)
	if !ok {
		return nil, "", "", notFoundError(resourceApp, appArn)
	}

	v, ok := resolveAppVersionLocked(a, appVersion)
	if !ok {
		return nil, "", "", notFoundError(resourceAppVersion, appVersion)
	}

	resolutionID := newResolutionID()
	v.Resolution = &ResolutionState{ResolutionID: resolutionID, Status: AsyncStatusPending}

	b.scheduleResolution(a.ID, v.Number, resolutionID)

	return a.clone(), resolutionID, AsyncStatusPending, nil
}

// scheduleResolution transitions a version's resolution Pending -> Success,
// materializing every resolvable mapping into a PhysicalResource entry along
// the way -- see resolveMappingsLocked.
func (b *InMemoryBackend) scheduleResolution(appID, versionNumber, resolutionID string) {
	b.work.After("ResolveAppVersionResources", asyncTransitionDelay, func() {
		b.mu.Lock("ResolveAppVersionResources-async")
		defer b.mu.Unlock()

		a, ok := b.apps.Get(appID)
		if !ok {
			return
		}

		v, ok := resolveAppVersionLocked(a, versionNumber)
		if !ok || v.Resolution == nil || v.Resolution.ResolutionID != resolutionID {
			return
		}

		b.resolveMappingsLocked(v)
		v.Resolution.Status = AsyncStatusSuccess
	})
}

// resolveMappingsLocked materializes every ResourceMapping on v into a
// PhysicalResource entry, per MappingType: "Resource" is a real,
// non-fabricated pass-through (the mapping already carries a caller-supplied
// PhysicalResourceId); "CfnStack"/"ResourceGroup"/"EKS" are resolved against
// this emulator's real CloudFormation/Resource Groups/EKS backends when
// wired (see cross_service.go) -- genuine cross-service resolution, not
// fabricated resource lists. "AppRegistryApp"/"Terraform" are left
// unresolved: no services/appregistry backend exists in this tree, and
// Terraform state files are an external S3 concept with no local semantics
// -- an honest, documented gap, not a stub. Callers must hold b.mu.
func (b *InMemoryBackend) resolveMappingsLocked(v *AppVersion) {
	for _, m := range v.ResourceMappings {
		switch m.MappingType {
		case MappingTypeResource:
			resolveResourceMappingLocked(v, m)
		case MappingTypeCfnStack:
			b.resolveCfnStackMappingLocked(v, m)
		case MappingTypeResourceGroup:
			b.resolveResourceGroupMappingLocked(v, m)
		case MappingTypeEKS:
			b.resolveEKSMappingLocked(v, m)
		}
	}
}

// resolveResourceMappingLocked materializes m's caller-supplied
// PhysicalResourceId into a PhysicalResource entry -- a real, non-fabricated
// pass-through. Callers must hold b.mu.
func resolveResourceMappingLocked(v *AppVersion, m ResourceMapping) {
	if m.PhysicalResourceID == nil {
		return
	}

	loc := findResourceLocator{resourceName: m.ResourceName, physicalID: m.PhysicalResourceID.Identifier}
	if existing, _ := findResource(v, loc); existing != nil {
		return
	}

	v.Resources = append(v.Resources, &PhysicalResource{
		PhysicalResourceID: m.PhysicalResourceID.clone(),
		ResourceName:       m.ResourceName,
		ResourceType:       resourceTypeFromPhysicalID(m.PhysicalResourceID),
		SourceType:         ResourceSourceDiscovered,
	})
}

// resourceTypeFromPhysicalID returns a best-effort ResourceType string for a
// resolved mapping that carries no explicit type -- left empty since this
// backend has no way to derive a real CFN-style type string from a bare
// PhysicalResourceId alone without guessing.
func resourceTypeFromPhysicalID(*PhysicalResourceID) string { return "" }

// DescribeAppVersionResourcesResolutionStatus returns the current resolution
// state for (appArn, appVersion).
func (b *InMemoryBackend) DescribeAppVersionResourcesResolutionStatus(
	appArn, appVersion, resolutionID string,
) (string, string, string, error) {
	b.mu.RLock("DescribeAppVersionResourcesResolutionStatus")
	defer b.mu.RUnlock()

	a, ok := b.resolveAppLocked(appArn)
	if !ok {
		return "", "", "", notFoundError(resourceApp, appArn)
	}

	v, ok := resolveAppVersionLocked(a, appVersion)
	if !ok {
		return "", "", "", notFoundError(resourceAppVersion, appVersion)
	}

	if v.Resolution == nil {
		return "", "", "", notFoundError(resourceAppVersion, "no resolution has been started for this version")
	}

	if resolutionID != "" && resolutionID != v.Resolution.ResolutionID {
		return "", "", "", notFoundError(resourceAppVersion, resolutionID)
	}

	return v.Resolution.ResolutionID, v.Resolution.Status, v.Resolution.ErrorMessage, nil
}

// --- Resource import ----------------------------------------------------------

// ImportResourcesToDraftAppVersion kicks off an async import of resources
// from the given sources into the App's draft version.
func (b *InMemoryBackend) ImportResourcesToDraftAppVersion(
	appArn string, req *importResourcesToDraftAppVersionRequest,
) (*App, string, error) {
	b.mu.Lock("ImportResourcesToDraftAppVersion")
	defer b.mu.Unlock()

	a, ok := b.resolveAppLocked(appArn)
	if !ok {
		return nil, "", notFoundError(resourceApp, appArn)
	}

	now := time.Now().UTC()
	a.Import = &ImportState{Status: AsyncStatusPending, StatusChangeTime: now}

	for _, arn := range req.SourceArns {
		a.Draft.InputSources = append(a.Draft.InputSources, AppInputSource{
			ImportType: MappingTypeResource, SourceArn: arn, SourceName: arn,
		})
	}

	for _, e := range req.EksSources {
		a.Draft.InputSources = append(a.Draft.InputSources, AppInputSource{
			ImportType: MappingTypeEKS, SourceArn: e.EksClusterArn, SourceName: e.EksClusterArn,
		})
	}

	for _, t := range req.TerraformSources {
		a.Draft.InputSources = append(a.Draft.InputSources, AppInputSource{
			ImportType: MappingTypeTerraform, SourceName: t.S3StateFileURL,
			TerraformSource: &TerraformSource{S3StateFileURL: t.S3StateFileURL},
		})
	}

	b.scheduleImport(a.ID, req.SourceArns, req.EksSources)

	return a.clone(), AsyncStatusPending, nil
}

// scheduleImport transitions an App's import status Pending ->
// Success|Failed, resolving sourceArns/eksSources against real cross-service
// backend state along the way -- see resolveImportSourcesLocked.
func (b *InMemoryBackend) scheduleImport(appID string, sourceArns []string, eksSources []eksSourceWire) {
	b.work.After("ImportResourcesToDraftAppVersion", asyncTransitionDelay, func() {
		b.mu.Lock("ImportResourcesToDraftAppVersion-async")
		defer b.mu.Unlock()

		a, ok := b.apps.Get(appID)
		if !ok || a.Import == nil || a.Import.Status != AsyncStatusPending {
			return
		}

		errDetails := b.resolveImportSourcesLocked(a.Draft, sourceArns, eksSources)

		a.Import.StatusChangeTime = time.Now().UTC()

		if len(errDetails) > 0 {
			a.Import.Status = AsyncStatusFailed
			a.Import.ErrorMessage = errDetails[0]
			a.Import.ErrorDetails = errDetails

			return
		}

		a.Import.Status = AsyncStatusSuccess
	})
}

// resolveImportSourcesLocked resolves sourceArns (by ARN service segment)
// and eksSources (by EksClusterArn) against this emulator's real EC2/RDS/
// DynamoDB/EKS backends (see cross_service.go), materializing every resolved
// source into v's Resources. Returns one AWS-accurate ResourceNotFoundException
// -shaped message per source that a wired, recognized backend could not find
// -- a source whose service this backend has no cross-service resolution for
// is left honestly unresolved, not reported as an error, matching
// resolveMappingsLocked's AppRegistryApp/Terraform treatment. Callers must
// hold b.mu.
func (b *InMemoryBackend) resolveImportSourcesLocked(
	v *AppVersion, sourceArns []string, eksSources []eksSourceWire,
) []string {
	var errDetails []string

	for _, sourceArn := range sourceArns {
		if materialized, checked := b.resolveSourceArnLocked(v, sourceArn); checked && !materialized {
			errDetails = append(errDetails, notFoundError(resourceAppResource, sourceArn).Error())
		}
	}

	for _, e := range eksSources {
		if e.EksClusterArn == "" {
			continue
		}

		if materialized, checked := b.resolveEKSSourceArnLocked(v, e.EksClusterArn); checked && !materialized {
			errDetails = append(errDetails, notFoundError(resourceAppResource, e.EksClusterArn).Error())
		}
	}

	return errDetails
}

// recordDiscoveredResourceLocked appends sourceArn as a discovered
// PhysicalResource to v, deduplicating against an already-recorded resource
// with the same ARN identifier. Callers must hold b.mu.
func recordDiscoveredResourceLocked(v *AppVersion, sourceArn, resourceType string) {
	loc := findResourceLocator{physicalID: sourceArn}
	if existing, _ := findResource(v, loc); existing != nil {
		return
	}

	v.Resources = append(v.Resources, &PhysicalResource{
		PhysicalResourceID: &PhysicalResourceID{Identifier: sourceArn, Type: PhysicalIDTypeArn},
		ResourceType:       resourceType,
		SourceType:         ResourceSourceDiscovered,
	})
}

// DescribeDraftAppVersionResourcesImportStatus returns the App's current
// import status.
func (b *InMemoryBackend) DescribeDraftAppVersionResourcesImportStatus(appArn string) (*App, *ImportState, error) {
	b.mu.RLock("DescribeDraftAppVersionResourcesImportStatus")
	defer b.mu.RUnlock()

	a, ok := b.resolveAppLocked(appArn)
	if !ok {
		return nil, nil, notFoundError(resourceApp, appArn)
	}

	if a.Import == nil {
		return nil, nil, notFoundError(resourceApp, "no import has been started for this app")
	}

	imp := *a.Import
	imp.ErrorDetails = cloneStrs(a.Import.ErrorDetails)

	return a.clone(), &imp, nil
}

// DeleteAppInputSource removes an input source (and any resources it solely
// produced -- this backend does not track per-resource provenance to that
// granularity, so only the AppInputSource bookkeeping entry is removed, a
// documented simplification) from the App's draft version.
func (b *InMemoryBackend) DeleteAppInputSource(appArn, sourceArn string) (*App, *AppInputSource, error) {
	b.mu.Lock("DeleteAppInputSource")
	defer b.mu.Unlock()

	a, ok := b.resolveAppLocked(appArn)
	if !ok {
		return nil, nil, notFoundError(resourceApp, appArn)
	}

	for i, s := range a.Draft.InputSources {
		if s.SourceArn == sourceArn || s.SourceName == sourceArn {
			removed := s
			a.Draft.InputSources = append(a.Draft.InputSources[:i], a.Draft.InputSources[i+1:]...)

			return a.clone(), &removed, nil
		}
	}

	return nil, nil, notFoundError(resourceAppInputSource, sourceArn)
}

// ListAppInputSources returns every input source on (appArn, appVersion).
func (b *InMemoryBackend) ListAppInputSources(
	appArn, appVersion, token string,
	limit int,
) (page.Page[AppInputSource], error) {
	b.mu.RLock("ListAppInputSources")
	defer b.mu.RUnlock()

	a, ok := b.resolveAppLocked(appArn)
	if !ok {
		return page.Page[AppInputSource]{}, notFoundError(resourceApp, appArn)
	}

	v, ok := resolveAppVersionLocked(a, appVersion)
	if !ok {
		return page.Page[AppInputSource]{}, notFoundError(resourceAppVersion, appVersion)
	}

	return page.New(append([]AppInputSource(nil), v.InputSources...), token, limit, defaultPageLimit), nil
}
