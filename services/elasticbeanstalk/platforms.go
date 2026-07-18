package elasticbeanstalk

import (
	"context"
	"fmt"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// --- PlatformVersion store.Table/Index helpers. Callers must hold b.mu. ---

func (b *InMemoryBackend) platformVersionGet(region, platformARN string) (*PlatformVersion, bool) {
	return b.platformVersions.Get(regionKey(region, platformARN))
}

func (b *InMemoryBackend) platformVersionPut(v *PlatformVersion) { b.platformVersions.Put(v) }

func (b *InMemoryBackend) platformVersionDelete(region, platformARN string) {
	b.platformVersions.Delete(regionKey(region, platformARN))
}

func (b *InMemoryBackend) platformVersionsInRegion(region string) []*PlatformVersion {
	return b.platformVersionsByRegion.Get(region)
}

// --- PlatformVersion operations ---

// CreatePlatformVersion creates a new custom platform version.
func (b *InMemoryBackend) CreatePlatformVersion(
	ctx context.Context,
	platformName, platformVersion string,
	tags map[string]string,
) (*PlatformVersion, error) {
	b.mu.Lock("CreatePlatformVersion")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	// Custom platforms are account-owned resources, so the ARN carries the
	// caller's account ID (matches the documented "platform/{name}/{version}"
	// resource-path pattern: arn:aws:elasticbeanstalk:{region}:{account-id}:platform/...).
	// An empty account ID here would produce a malformed "::platform/..." ARN
	// that real clients constructing the ARN themselves would never match.
	platformARN := arn.Build("elasticbeanstalk", region, b.accountID, "platform/"+platformName+"/"+platformVersion)

	if _, ok := b.platformVersionGet(region, platformARN); ok {
		return nil, fmt.Errorf(
			"%w: platform version %s/%s already exists",
			ErrAlreadyExists,
			platformName,
			platformVersion,
		)
	}

	pv := &PlatformVersion{
		PlatformArn:     platformARN,
		PlatformName:    platformName,
		PlatformVersion: platformVersion,
		PlatformStatus:  envStatusReady,
		Tags:            copyTags(tags),
		region:          region,
	}
	b.platformVersionPut(pv)

	return clonePlatformVersion(pv), nil
}

// DeletePlatformVersion removes a platform version by ARN and returns the deleted version.
func (b *InMemoryBackend) DeletePlatformVersion(ctx context.Context, platformARN string) (*PlatformVersion, error) {
	b.mu.Lock("DeletePlatformVersion")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	pv, ok := b.platformVersionGet(region, platformARN)
	if !ok {
		return nil, fmt.Errorf("%w: platform version %s not found", ErrNotFound, platformARN)
	}

	out := clonePlatformVersion(pv)
	b.platformVersionDelete(region, platformARN)

	return out, nil
}

// DescribePlatformVersion returns a platform version by ARN.
func (b *InMemoryBackend) DescribePlatformVersion(ctx context.Context, platformARN string) (*PlatformVersion, error) {
	b.mu.RLock("DescribePlatformVersion")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	pv, ok := b.platformVersionGet(region, platformARN)
	if !ok {
		return nil, fmt.Errorf("%w: platform version %s not found", ErrNotFound, platformARN)
	}

	return clonePlatformVersion(pv), nil
}

// ListPlatformVersions returns all stored platform versions sorted by ARN.
func (b *InMemoryBackend) ListPlatformVersions(ctx context.Context) []*PlatformVersion {
	b.mu.RLock("ListPlatformVersions")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	pvs := b.platformVersionsInRegion(region)
	list := make([]*PlatformVersion, 0, len(pvs))

	for _, pv := range pvs {
		list = append(list, clonePlatformVersion(pv))
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].PlatformArn < list[j].PlatformArn
	})

	return list
}

// addPlatformVersionInternal seeds a platform version directly into the backend.
// Caller must hold the write lock.
func (b *InMemoryBackend) addPlatformVersionInternal(region string, pv *PlatformVersion) {
	cp := clonePlatformVersion(pv)
	cp.region = region
	b.platformVersionPut(cp)
}
