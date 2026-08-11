package codeartifact

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// --- Package group methods ---

// PackageGroupOriginRestrictionMode's real SDK enum values.
const (
	restrictionModeAllow             = "ALLOW"
	restrictionModeAllowSpecificRepo = "ALLOW_SPECIFIC_REPOSITORIES"
	restrictionModeBlock             = "BLOCK"
	restrictionModeInherit           = "INHERIT"
)

// PackageGroupOriginRestrictionType's real SDK enum values.
const (
	restrictionTypeExternalUpstream = "EXTERNAL_UPSTREAM"
	restrictionTypeInternalUpstream = "INTERNAL_UPSTREAM"
	restrictionTypePublish          = "PUBLISH"
)

// validRestrictionTypes are PackageGroupOriginRestrictionType's real SDK
// enum values.
//
//nolint:gochecknoglobals // read-only validation set initialized once at startup
var validRestrictionTypes = map[string]bool{
	restrictionTypeExternalUpstream: true,
	restrictionTypeInternalUpstream: true,
	restrictionTypePublish:          true,
}

// validRestrictionModes are PackageGroupOriginRestrictionMode's real SDK
// enum values.
//
//nolint:gochecknoglobals // read-only validation set initialized once at startup
var validRestrictionModes = map[string]bool{
	restrictionModeAllow:             true,
	restrictionModeAllowSpecificRepo: true,
	restrictionModeBlock:             true,
	restrictionModeInherit:           true,
}

// packageGroupKey returns the map key for a package group.
func packageGroupKey(domainName, pattern string) string {
	return domainName + "/" + pattern
}

// CreatePackageGroup creates a new CodeArtifact package group.
func (b *InMemoryBackend) CreatePackageGroup(
	ctx context.Context,
	domainName, pattern, description, contactInfo string,
	kv map[string]string,
) (*PackageGroup, error) {
	region := getRegion(ctx, b.region)

	if _, err := parseGroupPattern(pattern); err != nil {
		return nil, err
	}

	b.mu.Lock("CreatePackageGroup")
	defer b.mu.Unlock()

	if !b.domains.Has(regionKey(region, domainName)) {
		return nil, fmt.Errorf("%w: domain %s not found", ErrNotFound, domainName)
	}

	key := packageGroupKey(domainName, pattern)
	if b.packageGroups.Has(regionKey(region, key)) {
		return nil, fmt.Errorf(
			"%w: package group %s already exists in domain %s",
			ErrAlreadyExists,
			pattern,
			domainName,
		)
	}

	pgARN := arn.Build("codeartifact", region, b.accountID, "package-group/"+domainName+pattern)
	t := tags.New("codeartifact.package-group." + key + ".tags")
	if len(kv) > 0 {
		t.Merge(kv)
	}
	pg := &PackageGroup{
		ARN:         pgARN,
		DomainName:  domainName,
		DomainOwner: b.accountID,
		Pattern:     pattern,
		Description: description,
		ContactInfo: contactInfo,
		CreatedTime: time.Now().UTC(),
		Tags:        t,
		region:      region,
	}
	b.packageGroups.Put(pg)
	cp := *pg

	return &cp, nil
}

// DescribePackageGroup returns a package group by domain and pattern.
func (b *InMemoryBackend) DescribePackageGroup(ctx context.Context, domainName, pattern string) (*PackageGroup, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribePackageGroup")
	defer b.mu.RUnlock()

	pg, ok := b.packageGroups.Get(regionKey(region, packageGroupKey(domainName, pattern)))
	if !ok {
		return nil, fmt.Errorf("%w: package group %s not found in domain %s", ErrNotFound, pattern, domainName)
	}
	cp := *pg

	return &cp, nil
}

// DeletePackageGroup deletes a package group by domain and pattern.
func (b *InMemoryBackend) DeletePackageGroup(ctx context.Context, domainName, pattern string) (*PackageGroup, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeletePackageGroup")
	defer b.mu.Unlock()

	key := packageGroupKey(domainName, pattern)
	pg, ok := b.packageGroups.Get(regionKey(region, key))
	if !ok {
		return nil, fmt.Errorf("%w: package group %s not found in domain %s", ErrNotFound, pattern, domainName)
	}
	cp := *pg
	b.packageGroups.Delete(regionKey(region, key))
	pg.Tags.Close()

	return &cp, nil
}

// GetAssociatedPackageGroup returns the most specific package group whose
// pattern matches the given package coordinate, per AWS's pattern
// specificity algorithm (see package_group_pattern.go), plus its
// associationType ("STRONG" or "WEAK" -- see bestMatchingGroup). Returns
// (nil, "", nil) if no group in the domain matches -- not an error per the
// real API.
//
// Scope note: this backend does not auto-create the implicit root group
// ("/*") that real AWS attaches to every domain, so an empty domain (or one
// whose groups don't cover the package) returns no match here where real
// AWS would always find at least the root group -- see PARITY.md's gaps.
func (b *InMemoryBackend) GetAssociatedPackageGroup(
	ctx context.Context,
	domainName, format, namespace, name string,
) (*PackageGroup, string, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("GetAssociatedPackageGroup")
	defer b.mu.RUnlock()

	if !b.domains.Has(regionKey(region, domainName)) {
		return nil, "", fmt.Errorf("%w: domain %s not found", ErrNotFound, domainName)
	}

	match, assocType := bestMatchingGroup(b.packageGroupsByRegion.Get(region), domainName, format, namespace, name)
	if match == nil {
		return nil, "", nil // AWS returns no error when no group matches
	}
	cp := *match

	return &cp, assocType, nil
}

// bestMatchingGroup returns the most specific group among entries (scoped
// to domainName) whose pattern weak-matches the given coordinate (weak
// match is a superset of strong match, so this also finds strong matches),
// plus its associationType ("STRONG" if the package's literal coordinate
// equals the winning pattern exactly, "WEAK" if it only matches after
// casefold/separator normalization -- see package_group_pattern.go). Returns
// (nil, "") if no group matches at all. Malformed patterns (should not
// occur -- CreatePackageGroup validates on write) are skipped defensively.
func bestMatchingGroup(
	entries []*PackageGroup, domainName, format, namespace, name string,
) (*PackageGroup, string) {
	var best *PackageGroup

	var bestParsed *groupPattern

	var bestRank int

	for _, pg := range entries {
		if pg.DomainName != domainName {
			continue
		}

		parsed, err := parseGroupPattern(pg.Pattern)
		if err != nil || !parsed.matchesWeak(format, namespace, name) {
			continue
		}

		rank := parsed.specificityRank()
		if best == nil || rank > bestRank {
			best = pg
			bestParsed = parsed
			bestRank = rank
		}
	}

	if best == nil {
		return nil, ""
	}

	if bestParsed.matches(format, namespace, name) {
		return best, "STRONG"
	}

	return best, "WEAK"
}

// ListPackageGroups returns all package groups in a domain, optionally filtered by prefix.
func (b *InMemoryBackend) ListPackageGroups(ctx context.Context, domainName, prefix string) ([]*PackageGroup, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListPackageGroups")
	defer b.mu.RUnlock()

	if !b.domains.Has(regionKey(region, domainName)) {
		return nil, fmt.Errorf("%w: domain %s not found", ErrNotFound, domainName)
	}

	entries := b.packageGroupsByRegion.Get(region)
	result := make([]*PackageGroup, 0, len(entries))

	for _, pg := range entries {
		if pg.DomainName != domainName {
			continue
		}

		if prefix != "" && !strings.HasPrefix(pg.Pattern, prefix) {
			continue
		}

		cp := *pg
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Pattern < result[j].Pattern
	})

	return result, nil
}

// ListSubPackageGroups returns the direct children of the given package
// group pattern in the pattern hierarchy (see package_group_pattern.go's
// isProperSubsetPattern): every OTHER group in the domain whose immediate
// parent (the most specific proper-superset pattern) is exactly this one.
// Real AWS returns direct children only, not the full descendant subtree
// (verified against the ListSubPackageGroups API reference).
func (b *InMemoryBackend) ListSubPackageGroups(
	ctx context.Context,
	domainName, pattern string,
) ([]*PackageGroup, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListSubPackageGroups")
	defer b.mu.RUnlock()

	if !b.domains.Has(regionKey(region, domainName)) {
		return nil, fmt.Errorf("%w: domain %s not found", ErrNotFound, domainName)
	}

	self, err := parseGroupPattern(pattern)
	if err != nil {
		return nil, err
	}

	entries := b.packageGroupsByRegion.Get(region)
	domainGroups := make([]*PackageGroup, 0, len(entries))

	for _, pg := range entries {
		if pg.DomainName == domainName {
			domainGroups = append(domainGroups, pg)
		}
	}

	result := make([]*PackageGroup, 0, len(domainGroups))

	for _, candidate := range domainGroups {
		if candidate.Pattern == pattern {
			continue
		}

		candidateParsed, perr := parseGroupPattern(candidate.Pattern)
		if perr != nil || !isProperSubsetPattern(candidateParsed, self) {
			continue
		}

		if immediateParentPattern(domainGroups, candidate, candidateParsed) != pattern {
			continue
		}

		cp := *candidate
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Pattern < result[j].Pattern
	})

	return result, nil
}

// immediateParentPattern returns the pattern string of self's immediate
// parent among candidates (the most specific proper superset of self's
// pattern, excluding self), or "" if self has no parent within candidates.
// Callers must have already parsed self's pattern (selfParsed).
func immediateParentPattern(candidates []*PackageGroup, self *PackageGroup, selfParsed *groupPattern) string {
	parent := immediateParentGroup(candidates, self, selfParsed)
	if parent == nil {
		return ""
	}

	return parent.Pattern
}

// immediateParentGroup returns self's immediate parent group among
// candidates, or nil if none exists. The immediate parent is the most
// specific (highest specificityRank) OTHER group whose match-space is a
// proper superset of self's.
func immediateParentGroup(candidates []*PackageGroup, self *PackageGroup, selfParsed *groupPattern) *PackageGroup {
	var parent *PackageGroup

	var parentParsed *groupPattern

	for _, candidate := range candidates {
		if candidate.Pattern == self.Pattern {
			continue
		}

		candidateParsed, err := parseGroupPattern(candidate.Pattern)
		if err != nil || !isProperSubsetPattern(selfParsed, candidateParsed) {
			continue
		}

		if parent == nil || candidateParsed.specificityRank() > parentParsed.specificityRank() {
			parent = candidate
			parentParsed = candidateParsed
		}
	}

	return parent
}

// UpdatePackageGroup updates description or contact info of a package group.
func (b *InMemoryBackend) UpdatePackageGroup(
	ctx context.Context,
	domainName, pattern, description, contactInfo string,
) (*PackageGroup, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("UpdatePackageGroup")
	defer b.mu.Unlock()

	key := packageGroupKey(domainName, pattern)
	pg, ok := b.packageGroups.Get(regionKey(region, key))

	if !ok {
		return nil, fmt.Errorf("%w: package group %s not found", ErrNotFound, pattern)
	}

	if description != "" {
		pg.Description = description
	}

	if contactInfo != "" {
		pg.ContactInfo = contactInfo
	}

	cp := *pg

	return &cp, nil
}

// AllowedRepoOp is one entry of UpdatePackageGroupOriginConfiguration's
// addAllowedRepositories/removeAllowedRepositories request lists (mirrors
// aws-sdk-go-v2 types.PackageGroupAllowedRepository).
type AllowedRepoOp struct {
	OriginRestrictionType string
	RepositoryName        string
}

// UpdatePackageGroupOriginConfiguration updates a package group's per-type
// restriction mode and/or allowed-repository list. Returns the updated
// group plus a restrictionType -> {"ADDED"|"REMOVED": [repoNames]} map
// describing exactly what changed, mirroring the real
// UpdatePackageGroupOriginConfigurationOutput.AllowedRepositoryUpdates
// shape (verified against aws-sdk-go-v2 deserializers.go).
func (b *InMemoryBackend) UpdatePackageGroupOriginConfiguration(
	ctx context.Context,
	domainName, pattern string,
	restrictions map[string]string,
	addRepos, removeRepos []AllowedRepoOp,
) (*PackageGroup, map[string]map[string][]string, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("UpdatePackageGroupOriginConfiguration")
	defer b.mu.Unlock()

	key := packageGroupKey(domainName, pattern)
	pg, ok := b.packageGroups.Get(regionKey(region, key))
	if !ok {
		return nil, nil, fmt.Errorf("%w: package group %s not found in domain %s", ErrNotFound, pattern, domainName)
	}

	if err := validateOriginConfigInput(restrictions, addRepos, removeRepos); err != nil {
		return nil, nil, err
	}

	for _, op := range slices.Concat(addRepos, removeRepos) {
		if !b.repositories.Has(regionKey(region, repoKey(domainName, op.RepositoryName))) {
			return nil, nil, fmt.Errorf(
				"%w: repository %s not found in domain %s", ErrNotFound, op.RepositoryName, domainName,
			)
		}
	}

	if pg.Restrictions == nil {
		pg.Restrictions = make(map[string]*PackageGroupRestriction, len(validRestrictionTypes))
	}

	for restrictionType, mode := range restrictions {
		restrictionFor(pg, restrictionType).Mode = mode
	}

	updates := make(map[string]map[string][]string)

	for _, op := range addRepos {
		r := restrictionFor(pg, op.OriginRestrictionType)
		if !slices.Contains(r.AllowedRepositories, op.RepositoryName) {
			r.AllowedRepositories = append(r.AllowedRepositories, op.RepositoryName)
			recordAllowedRepoUpdate(updates, op.OriginRestrictionType, "ADDED", op.RepositoryName)
		}
	}

	for _, op := range removeRepos {
		r := restrictionFor(pg, op.OriginRestrictionType)
		if idx := slices.Index(r.AllowedRepositories, op.RepositoryName); idx >= 0 {
			r.AllowedRepositories = slices.Delete(r.AllowedRepositories, idx, idx+1)
			recordAllowedRepoUpdate(updates, op.OriginRestrictionType, "REMOVED", op.RepositoryName)
		}
	}

	cp := *pg

	return &cp, updates, nil
}

// validateOriginConfigInput checks restriction-type/mode enum membership
// across all three UpdatePackageGroupOriginConfiguration input lists.
func validateOriginConfigInput(restrictions map[string]string, addRepos, removeRepos []AllowedRepoOp) error {
	for restrictionType, mode := range restrictions {
		if !validRestrictionTypes[restrictionType] {
			return fmt.Errorf("%w: invalid origin restriction type %s", ErrValidation, restrictionType)
		}

		if !validRestrictionModes[mode] {
			return fmt.Errorf("%w: invalid origin restriction mode %s", ErrValidation, mode)
		}
	}

	for _, op := range slices.Concat(addRepos, removeRepos) {
		if !validRestrictionTypes[op.OriginRestrictionType] {
			return fmt.Errorf("%w: invalid origin restriction type %s", ErrValidation, op.OriginRestrictionType)
		}
	}

	return nil
}

// restrictionFor returns pg's PackageGroupRestriction for restrictionType,
// lazily creating it (defaulting Mode to "INHERIT") if absent. Callers must
// hold b.mu and have already validated restrictionType and initialized
// pg.Restrictions.
func restrictionFor(pg *PackageGroup, restrictionType string) *PackageGroupRestriction {
	r, ok := pg.Restrictions[restrictionType]
	if !ok {
		r = &PackageGroupRestriction{Mode: restrictionModeInherit}
		pg.Restrictions[restrictionType] = r
	}

	return r
}

// recordAllowedRepoUpdate appends repoName to updates[restrictionType][action].
func recordAllowedRepoUpdate(updates map[string]map[string][]string, restrictionType, action, repoName string) {
	if updates[restrictionType] == nil {
		updates[restrictionType] = make(map[string][]string)
	}

	updates[restrictionType][action] = append(updates[restrictionType][action], repoName)
}

// ListAllowedRepositoriesForGroup returns the allowed-repository list for
// the given package group and origin restriction type.
func (b *InMemoryBackend) ListAllowedRepositoriesForGroup(
	ctx context.Context,
	domainName, pattern, restrictionType string,
) ([]string, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListAllowedRepositoriesForGroup")
	defer b.mu.RUnlock()

	if !b.domains.Has(regionKey(region, domainName)) {
		return nil, fmt.Errorf("%w: domain %s not found", ErrNotFound, domainName)
	}

	if !validRestrictionTypes[restrictionType] {
		return nil, fmt.Errorf("%w: invalid origin restriction type %s", ErrValidation, restrictionType)
	}

	key := packageGroupKey(domainName, pattern)
	pg, ok := b.packageGroups.Get(regionKey(region, key))
	if !ok {
		return nil, fmt.Errorf("%w: package group %s not found in domain %s", ErrNotFound, pattern, domainName)
	}

	r, ok := pg.Restrictions[restrictionType]
	if !ok {
		return []string{}, nil
	}

	return slices.Clone(r.AllowedRepositories), nil
}

// AssociatedPackage pairs a Package with its associationType ("STRONG" or
// "WEAK") relative to the package group it was matched against -- see
// bestMatchingGroup. This is a query-result shape, not a stored field on
// Package itself (the same package has a different associationType per
// group it's queried against).
type AssociatedPackage struct {
	Package         *Package
	AssociationType string
}

// ListAssociatedPackages returns the packages in the domain whose
// most-specific matching package group (see bestMatchingGroup) is exactly
// the group identified by pattern. Packages are deduplicated by
// (format, namespace, name) across every repository in the domain, mirroring
// how ListPackages dedupes within a single repository.
func (b *InMemoryBackend) ListAssociatedPackages(
	ctx context.Context, domainName, pattern string,
) ([]AssociatedPackage, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListAssociatedPackages")
	defer b.mu.RUnlock()

	if !b.domains.Has(regionKey(region, domainName)) {
		return nil, fmt.Errorf("%w: domain %s not found", ErrNotFound, domainName)
	}

	if _, err := parseGroupPattern(pattern); err != nil {
		return nil, err
	}

	groups := b.packageGroupsByRegion.Get(region)
	seen := make(map[string]bool)
	result := make([]AssociatedPackage, 0)

	for _, pkg := range b.packagesByRegion.Get(region) {
		if pkg.DomainName != domainName {
			continue
		}

		dedupeKey := pkg.Format + "/" + pkg.Namespace + "/" + pkg.Name
		if seen[dedupeKey] {
			continue
		}

		match, assocType := bestMatchingGroup(groups, domainName, pkg.Format, pkg.Namespace, pkg.Name)
		if match == nil || match.Pattern != pattern {
			continue
		}

		seen[dedupeKey] = true
		cp := *pkg
		result = append(result, AssociatedPackage{Package: &cp, AssociationType: assocType})
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Package.Name < result[j].Package.Name
	})

	return result, nil
}

// EffectiveRestriction is a package group's resolved origin-restriction
// state for one restriction type, mirroring aws-sdk-go-v2
// types.PackageGroupOriginRestriction.
type EffectiveRestriction struct {
	InheritedFrom     *PackageGroup
	Mode              string
	EffectiveMode     string
	RepositoriesCount int
}

// PackageGroupOriginInfo bundles a package group's computed origin-control
// wire data: the resolved EffectiveRestriction for each of the three
// restriction types, plus its immediate parent in the pattern hierarchy.
// Handlers use this to build the real PackageGroupDescription/
// PackageGroupSummary "originConfiguration"/"parent" wire fields.
type PackageGroupOriginInfo struct {
	Parent       *PackageGroup
	Restrictions map[string]EffectiveRestriction
}

// DescribeOriginInfo computes pg's PackageGroupOriginInfo. Callers pass a
// group already fetched from this backend (e.g. via DescribePackageGroup);
// domainName scopes the hierarchy search to sibling groups in the same
// domain.
func (b *InMemoryBackend) DescribeOriginInfo(
	ctx context.Context, domainName string, pg *PackageGroup,
) (*PackageGroupOriginInfo, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeOriginInfo")
	defer b.mu.RUnlock()

	selfParsed, err := parseGroupPattern(pg.Pattern)
	if err != nil {
		return nil, err
	}

	domainGroups := make([]*PackageGroup, 0)

	for _, g := range b.packageGroupsByRegion.Get(region) {
		if g.DomainName == domainName {
			domainGroups = append(domainGroups, g)
		}
	}

	info := &PackageGroupOriginInfo{
		Restrictions: make(map[string]EffectiveRestriction, len(validRestrictionTypes)),
		Parent:       immediateParentGroup(domainGroups, pg, selfParsed),
	}

	for rt := range validRestrictionTypes {
		info.Restrictions[rt] = resolveEffectiveRestriction(domainGroups, pg, rt)
	}

	return info, nil
}

// resolveEffectiveRestriction walks self's INHERIT chain (via
// immediateParentGroup) for restrictionType until it finds an ancestor with
// a non-INHERIT mode, defaulting to ALLOW if the chain runs out --
// mirroring real AWS's root-group default of allowing everything.
func resolveEffectiveRestriction(
	domainGroups []*PackageGroup, self *PackageGroup, restrictionType string,
) EffectiveRestriction {
	ownMode, repoCount := ownRestriction(self, restrictionType)
	if ownMode != restrictionModeInherit {
		return EffectiveRestriction{Mode: ownMode, EffectiveMode: ownMode, RepositoriesCount: repoCount}
	}

	for cur := self; ; {
		curParsed, err := parseGroupPattern(cur.Pattern)
		if err != nil {
			break
		}

		parent := immediateParentGroup(domainGroups, cur, curParsed)
		if parent == nil {
			break
		}

		if parentMode, _ := ownRestriction(parent, restrictionType); parentMode != restrictionModeInherit {
			return EffectiveRestriction{
				Mode: ownMode, EffectiveMode: parentMode, InheritedFrom: parent, RepositoriesCount: repoCount,
			}
		}

		cur = parent
	}

	return EffectiveRestriction{Mode: ownMode, EffectiveMode: restrictionModeAllow, RepositoriesCount: repoCount}
}

// ownRestriction returns pg's own (non-inherited) mode for restrictionType
// and its allowed-repository count, defaulting to "INHERIT"/0 when unset.
func ownRestriction(pg *PackageGroup, restrictionType string) (string, int) {
	r, ok := pg.Restrictions[restrictionType]
	if !ok || r.Mode == "" {
		return restrictionModeInherit, 0
	}

	return r.Mode, len(r.AllowedRepositories)
}
