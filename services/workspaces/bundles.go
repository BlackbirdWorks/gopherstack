package workspaces

import (
	"context"
	"encoding/base64"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

const ownerAmazon = "AMAZON"

// bundlesPageSize is the AWS default page size for DescribeWorkspaceBundles.
const bundlesPageSize = 25

// Bundle storage capacities in GiB matching real Amazon-owned bundle defaults.
const (
	bundleValueUserGiB       int32 = 10
	bundleStandardUserGiB    int32 = 50
	bundlePerformanceUserGiB int32 = 100
	bundlePowerUserGiB       int32 = 100
	bundlePowerProUserGiB    int32 = 100
	bundleStdRootGiB         int32 = 80
	bundlePowerRootGiB       int32 = 175
)

// amazonBundleList returns the predefined Amazon-owned bundles sorted by BundleID.
func amazonBundleList() []*WorkspaceBundle {
	return []*WorkspaceBundle{
		{
			BundleID:    "wsb-1b5w9hkng",
			Name:        "PowerPro",
			Owner:       ownerAmazon,
			Description: "PowerPro with Windows 10 and Office 2019",
			ComputeType: BundleComputeType{Name: "POWERPRO"},
			UserStorage: BundleStorage{Capacity: bundlePowerProUserGiB},
			RootStorage: BundleStorage{Capacity: bundlePowerRootGiB},
		},
		{
			BundleID:    "wsb-b0s22j3d7",
			Name:        "Performance",
			Owner:       ownerAmazon,
			Description: "Performance with Windows 10 and Office 2019",
			ComputeType: BundleComputeType{Name: "PERFORMANCE"},
			UserStorage: BundleStorage{Capacity: bundlePerformanceUserGiB},
			RootStorage: BundleStorage{Capacity: bundleStdRootGiB},
		},
		{
			BundleID:    "wsb-bh8rsxt14",
			Name:        "Value",
			Owner:       ownerAmazon,
			Description: "Value with Windows 10 and Office 2019",
			ComputeType: BundleComputeType{Name: "VALUE"},
			UserStorage: BundleStorage{Capacity: bundleValueUserGiB},
			RootStorage: BundleStorage{Capacity: bundleStdRootGiB},
		},
		{
			BundleID:    "wsb-clj85qzj1",
			Name:        "Power",
			Owner:       ownerAmazon,
			Description: "Power with Windows 10 and Office 2019",
			ComputeType: BundleComputeType{Name: "POWER"},
			UserStorage: BundleStorage{Capacity: bundlePowerUserGiB},
			RootStorage: BundleStorage{Capacity: bundlePowerRootGiB},
		},
		{
			BundleID:    "wsb-gm4d5tx2v",
			Name:        "Standard",
			Owner:       ownerAmazon,
			Description: "Standard with Windows 10 and Office 2019",
			ComputeType: BundleComputeType{Name: "STANDARD"},
			UserStorage: BundleStorage{Capacity: bundleStandardUserGiB},
			RootStorage: BundleStorage{Capacity: bundleStdRootGiB},
		},
	}
}

// DescribeWorkspaceBundles lists bundles: owner AMAZON gives AWS bundles, anything else the
// account's custom ones, and bundleIDs match either kind. Sorted by BundleID, 25 per page.
func (b *InMemoryBackend) DescribeWorkspaceBundles(
	_ context.Context,
	bundleIDs []string, owner string, nextToken string,
) ([]*WorkspaceBundle, string, error) {
	if err := checkPageToken(nextToken); err != nil {
		return nil, "", err
	}

	b.mu.RLock("DescribeWorkspaceBundles")
	defer b.mu.RUnlock()

	var bundles []*WorkspaceBundle

	// api_op_DescribeWorkspaceBundles.go: AMAZON lists AWS bundles; no value lists the account's own.
	if owner == ownerAmazon || len(bundleIDs) > 0 {
		bundles = append(bundles, amazonBundleList()...)
	}

	// Custom bundles are the account's own; BundleIds can name either kind.
	if owner != ownerAmazon || len(bundleIDs) > 0 {
		for _, bun := range b.customBundles.All() {
			bundles = append(bundles, &WorkspaceBundle{
				BundleID:    bun.BundleID,
				Name:        bun.Name,
				Owner:       b.accountID,
				Description: bun.Description,
				ImageID:     bun.ImageID,
				ComputeType: BundleComputeType{Name: bun.ComputeType},
				UserStorage: BundleStorage{Capacity: bun.UserStorageGiB},
				RootStorage: BundleStorage{Capacity: bun.RootStorageGiB},
				// Bundles are created synchronously and no CreateWorkspaceBundle member selects STANDBY.
				BundleType:      "REGULAR",
				State:           "AVAILABLE",
				CreationTime:    bun.CreatedAt,
				LastUpdatedTime: bun.UpdatedAt,
			})
		}
	}

	sort.Slice(bundles, func(i, j int) bool {
		return bundles[i].BundleID < bundles[j].BundleID
	})

	if len(bundleIDs) > 0 {
		idFilter := buildFilter(bundleIDs)
		filtered := bundles[:0]

		for _, bun := range bundles {
			if matchesFilter(idFilter, bun.BundleID) {
				filtered = append(filtered, bun)
			}
		}

		return filtered, "", nil
	}

	bundles = advanceBundleCursor(bundles, nextToken)

	var newToken string

	if len(bundles) > bundlesPageSize {
		newToken = base64.StdEncoding.EncodeToString([]byte(bundles[bundlesPageSize].BundleID))
		bundles = bundles[:bundlesPageSize]
	}

	return bundles, newToken, nil
}

// advanceBundleCursor removes all bundles that sort before the decoded nextToken cursor.
func advanceBundleCursor(bundles []*WorkspaceBundle, nextToken string) []*WorkspaceBundle {
	if nextToken == "" {
		return bundles
	}

	cursorBytes, err := base64.StdEncoding.DecodeString(nextToken)
	if err != nil {
		return bundles
	}

	cursor := string(cursorBytes)

	for i, bun := range bundles {
		if bun.BundleID >= cursor {
			return bundles[i:]
		}
	}

	return nil
}

// CreateWorkspaceBundle creates a custom bundle. Returns errImageNotFound
// for an ImageId that doesn't reference a real image, matching real AWS
// (ResourceNotFoundException is in this operation's error list; see
// deserializers.go's awsAwsjson11_deserializeOpErrorCreateWorkspaceBundle).
func (b *InMemoryBackend) CreateWorkspaceBundle(
	name, description, imageID, computeType string,
	userStorageGiB, rootStorageGiB int32,
	tags map[string]string,
) (*storedCustomBundle, error) {
	b.mu.Lock("CreateWorkspaceBundle")
	defer b.mu.Unlock()

	if !b.images.Has(imageID) {
		return nil, errImageNotFound
	}

	id := b.nextID("wsb-")
	stored := cloneTags(tags)
	now := time.Now().UTC()
	bun := &storedCustomBundle{
		CreatedAt:      now,
		UpdatedAt:      now,
		BundleID:       id,
		Name:           name,
		Description:    description,
		ImageID:        imageID,
		ComputeType:    computeType,
		UserStorageGiB: userStorageGiB,
		RootStorageGiB: rootStorageGiB,
		Tags:           stored,
	}
	b.customBundles.Put(bun)
	b.tags[id] = stored

	return bun, nil
}

// DeleteWorkspaceBundle removes a custom bundle. Returns errBundleInUse when a
// WorkSpace still references the bundle (ResourceAssociatedException is
// modelled for this operation).
func (b *InMemoryBackend) DeleteWorkspaceBundle(bundleID string) error {
	b.mu.Lock("DeleteWorkspaceBundle")
	defer b.mu.Unlock()

	if !b.customBundles.Has(bundleID) {
		return errBundleNotFound
	}

	for _, w := range b.workspaces.All() {
		if w.BundleID == bundleID {
			return errBundleInUse
		}
	}

	b.customBundles.Delete(bundleID)

	return nil
}

// UpdateWorkspaceBundle updates the image of a custom bundle. Returns
// errImageNotFound for an ImageId that doesn't reference a real image,
// matching real AWS (ResourceNotFoundException is in this operation's error
// list, and UpdateWorkspaceBundleInput.ImageId is optional, so an empty
// value is a no-op rather than a validation failure).
func (b *InMemoryBackend) UpdateWorkspaceBundle(bundleID, imageID string) error {
	b.mu.Lock("UpdateWorkspaceBundle")
	defer b.mu.Unlock()

	bun, ok := b.customBundles.Get(bundleID)
	if !ok {
		return errBundleNotFound
	}

	if imageID == "" {
		return nil
	}

	if !b.images.Has(imageID) {
		return errImageNotFound
	}

	bun.ImageID = imageID
	bun.UpdatedAt = time.Now().UTC()

	return nil
}

// bundleExistsLocked reports whether bundleID names either an Amazon-owned
// bundle or an account-owned custom bundle. Callers must hold b.mu.
func (b *InMemoryBackend) bundleExistsLocked(bundleID string) bool {
	if b.customBundles.Has(bundleID) {
		return true
	}

	for _, bun := range amazonBundleList() {
		if bun.BundleID == bundleID {
			return true
		}
	}

	return false
}

// DescribeBundleAssociations returns application associations for a bundle.
// See DescribeImageAssociations in images.go: real AWS exposes no public API
// to create a bundle<->application association, so a freshly emulated account
// always has an empty list. This still performs the real required-field and
// existence validation a live call would enforce.
func (b *InMemoryBackend) DescribeBundleAssociations(
	bundleID string, resourceTypes []string,
) ([]BundleResourceAssociation, error) {
	b.mu.RLock("DescribeBundleAssociations")
	defer b.mu.RUnlock()

	if bundleID == "" {
		return nil, awserr.New("BundleId is required", awserr.ErrInvalidParameter)
	}

	if !b.bundleExistsLocked(bundleID) {
		return nil, errBundleNotFound
	}

	if err := validateAssociatedResourceTypes(resourceTypes); err != nil {
		return nil, err
	}

	return []BundleResourceAssociation{}, nil
}
