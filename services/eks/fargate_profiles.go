package eks

import (
	"fmt"
	"slices"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// fargateProfileTransitionDelay is the async delay before a CREATING Fargate profile reaches ACTIVE.
const fargateProfileTransitionDelay = 100 * time.Millisecond

// CreateFargateProfile creates a new Fargate profile in a cluster.
func (b *InMemoryBackend) CreateFargateProfile(
	clusterName, profileName, podExecutionRoleARN string,
	selectors []FargateProfileSelector,
	subnets []string,
	kv map[string]string,
) (*FargateProfile, error) {
	b.mu.Lock("CreateFargateProfile")
	defer b.mu.Unlock()

	if _, ok := b.clusters.Get(clusterName); !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	if _, ok := b.fargateProfiles.Get(fargateProfileKey(clusterName, profileName)); ok {
		return nil, fmt.Errorf(
			"%w: fargate profile %s already exists in cluster %s",
			ErrAlreadyExists,
			profileName,
			clusterName,
		)
	}

	profileARN := arn.Build(
		"eks",
		b.region,
		b.accountID,
		"fargateprofile/"+clusterName+"/"+profileName+"/"+stableID(clusterName+"/"+profileName),
	)

	t := tags.New("eks.fargate." + clusterName + "." + profileName + ".tags")
	if len(kv) > 0 {
		t.Merge(kv)
	}

	sels := make([]FargateProfileSelector, len(selectors))
	copy(sels, selectors)

	profile := &FargateProfile{
		ClusterName:         clusterName,
		FargateProfileName:  profileName,
		ARN:                 profileARN,
		PodExecutionRoleARN: podExecutionRoleARN,
		Status:              statusCreating,
		Selectors:           sels,
		Subnets:             cloneStrings(subnets),
		CreatedAt:           time.Now().UTC(),
		Tags:                t,
		Health:              &FargateProfileHealth{Issues: []FargateProfileIssue{}},
	}
	b.fargateProfiles.Put(profile)

	b.work.After("FargateProfileTransition", fargateProfileTransitionDelay, func() {
		b.mu.Lock("CreateFargateProfile-async")
		defer b.mu.Unlock()

		if fp, ok := b.fargateProfiles.Get(fargateProfileKey(clusterName, profileName)); ok &&
			fp.Status == statusCreating {
			fp.Status = statusActive
		}
	})

	cp := *profile
	cp.Selectors = make([]FargateProfileSelector, len(profile.Selectors))
	copy(cp.Selectors, profile.Selectors)
	cp.Subnets = cloneStrings(profile.Subnets)

	return &cp, nil
}

// deepCopyFargateProfile returns a deep copy of a FargateProfile.
func deepCopyFargateProfile(p *FargateProfile) *FargateProfile {
	cp := *p
	cp.Selectors = make([]FargateProfileSelector, len(p.Selectors))
	copy(cp.Selectors, p.Selectors)
	cp.Subnets = cloneStrings(p.Subnets)

	if p.Health != nil {
		issues := make([]FargateProfileIssue, len(p.Health.Issues))
		copy(issues, p.Health.Issues)
		cp.Health = &FargateProfileHealth{Issues: issues}
	}

	return &cp
}

// DeleteFargateProfile removes a Fargate profile from a cluster.
func (b *InMemoryBackend) DeleteFargateProfile(clusterName, profileName string) (*FargateProfile, error) {
	b.mu.Lock("DeleteFargateProfile")
	defer b.mu.Unlock()

	if _, ok := b.clusters.Get(clusterName); !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	profile, ok := b.fargateProfiles.Get(fargateProfileKey(clusterName, profileName))
	if !ok {
		return nil, fmt.Errorf("%w: fargate profile %s not found in cluster %s", ErrNotFound, profileName, clusterName)
	}

	cp := deepCopyFargateProfile(profile)

	if profile.Tags != nil {
		profile.Tags.Close()
	}

	b.fargateProfiles.Delete(fargateProfileKey(clusterName, profileName))

	cp.Status = statusDeleting

	return cp, nil
}

// DescribeFargateProfile returns a Fargate profile by name.
func (b *InMemoryBackend) DescribeFargateProfile(clusterName, profileName string) (*FargateProfile, error) {
	b.mu.RLock("DescribeFargateProfile")
	defer b.mu.RUnlock()

	if _, ok := b.clusters.Get(clusterName); !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	profile, ok := b.fargateProfiles.Get(fargateProfileKey(clusterName, profileName))
	if !ok {
		return nil, fmt.Errorf("%w: fargate profile %s not found in cluster %s", ErrNotFound, profileName, clusterName)
	}

	return deepCopyFargateProfile(profile), nil
}

// ListFargateProfiles returns all Fargate profile names in a cluster sorted alphabetically.
func (b *InMemoryBackend) ListFargateProfiles(clusterName string) ([]string, error) {
	b.mu.RLock("ListFargateProfiles")
	defer b.mu.RUnlock()

	if _, ok := b.clusters.Get(clusterName); !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	items := b.fargateProfilesByCluster.Get(clusterName)
	names := make([]string, len(items))

	for i, p := range items {
		names[i] = p.FargateProfileName
	}

	slices.Sort(names)

	return names, nil
}

// AddFargateProfileInternal inserts a pre-built Fargate profile into the backend.
// Intended only for test seeding.
func (b *InMemoryBackend) AddFargateProfileInternal(p *FargateProfile) {
	b.mu.Lock("AddFargateProfileInternal")
	defer b.mu.Unlock()

	if p.Tags == nil {
		p.Tags = tags.New("eks.fargate." + p.ClusterName + "." + p.FargateProfileName + ".tags")
	}

	if p.Health == nil {
		p.Health = &FargateProfileHealth{Issues: []FargateProfileIssue{}}
	}

	b.fargateProfiles.Put(p)
}

// ListAllFargateProfiles returns all Fargate profiles across all clusters.
func (b *InMemoryBackend) ListAllFargateProfiles() []*FargateProfile {
	b.mu.RLock("ListAllFargateProfiles")
	defer b.mu.RUnlock()

	items := b.fargateProfiles.All()
	list := make([]*FargateProfile, 0, len(items))

	for _, p := range items {
		cp := *p
		cp.Selectors = make([]FargateProfileSelector, len(p.Selectors))
		copy(cp.Selectors, p.Selectors)
		list = append(list, &cp)
	}

	return list
}
