package workspaces

import (
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

func (b *InMemoryBackend) createImageLocked(
	name, description, sourceImageID string,
	tags map[string]string,
) *storedImage {
	id := b.nextID("wsi-")
	stored := cloneTags(tags)
	img := &storedImage{
		ImageID:       id,
		Name:          name,
		Description:   description,
		State:         "AVAILABLE",
		SourceImageID: sourceImageID,
		Created:       time.Now().UTC(),
		Tags:          stored,
	}
	b.images.Put(img)
	b.tags[id] = stored

	return img
}

// CopyWorkspaceImage copies an image.
func (b *InMemoryBackend) CopyWorkspaceImage(
	name, sourceImageID, _ /*sourceRegion*/, description string,
	tags map[string]string,
) (string, error) {
	b.mu.Lock("CopyWorkspaceImage")
	defer b.mu.Unlock()

	img := b.createImageLocked(name, description, sourceImageID, tags)

	return img.ImageID, nil
}

// CreateWorkspaceImage creates an image from a workspace.
func (b *InMemoryBackend) CreateWorkspaceImage(
	name, description, _ /*workspaceId*/ string,
	tags map[string]string,
) (*storedImage, error) {
	b.mu.Lock("CreateWorkspaceImage")
	defer b.mu.Unlock()

	img := b.createImageLocked(name, description, "", tags)

	return img, nil
}

// DeleteWorkspaceImage removes an image.
func (b *InMemoryBackend) DeleteWorkspaceImage(imageID string) error {
	b.mu.Lock("DeleteWorkspaceImage")
	defer b.mu.Unlock()

	if !b.images.Has(imageID) {
		return errImageNotFound
	}

	b.images.Delete(imageID)
	delete(b.imagePermissions, imageID)

	return nil
}

// ImportWorkspaceImage imports an EC2 image as a workspace image.
func (b *InMemoryBackend) ImportWorkspaceImage(
	ec2ImageID, name, description string, tags map[string]string,
) (string, error) {
	b.mu.Lock("ImportWorkspaceImage")
	defer b.mu.Unlock()

	img := b.createImageLocked(name, description, ec2ImageID, tags)

	return img.ImageID, nil
}

// ImportCustomWorkspaceImage imports a custom workspace image.
func (b *InMemoryBackend) ImportCustomWorkspaceImage(
	name, description string,
) (*storedImage, error) {
	b.mu.Lock("ImportCustomWorkspaceImage")
	defer b.mu.Unlock()

	img := b.createImageLocked(name, description, "", nil)

	return img, nil
}

// CreateUpdatedWorkspaceImage creates an updated version of an existing image.
func (b *InMemoryBackend) CreateUpdatedWorkspaceImage(
	sourceImageID, name, description string, tags map[string]string,
) (string, error) {
	b.mu.Lock("CreateUpdatedWorkspaceImage")
	defer b.mu.Unlock()

	img := b.createImageLocked(name, description, sourceImageID, tags)

	return img.ImageID, nil
}

// DescribeWorkspaceImages returns workspace images, optionally filtered by IDs.
func (b *InMemoryBackend) DescribeWorkspaceImages(
	imageIDs []string, _ /*imageType*/ string, _ int32, _ string,
) ([]*storedImage, string, error) {
	b.mu.RLock("DescribeWorkspaceImages")
	defer b.mu.RUnlock()

	filter := buildFilter(imageIDs)
	var result []*storedImage

	for _, img := range b.images.All() {
		if !matchesFilter(filter, img.ImageID) {
			continue
		}

		cp := *img
		result = append(result, &cp)
	}

	if result == nil {
		result = []*storedImage{}
	}

	return result, "", nil
}

// DescribeWorkspaceImagePermissions returns sharing permissions for an image.
func (b *InMemoryBackend) DescribeWorkspaceImagePermissions(
	imageID string,
) (string, map[string]bool, error) {
	b.mu.RLock("DescribeWorkspaceImagePermissions")
	defer b.mu.RUnlock()

	if !b.images.Has(imageID) {
		return "", nil, errImageNotFound
	}

	perms := make(map[string]bool)
	maps.Copy(perms, b.imagePermissions[imageID])

	return imageID, perms, nil
}

// UpdateWorkspaceImagePermission sets the sharing permission for an image.
func (b *InMemoryBackend) UpdateWorkspaceImagePermission(
	imageID, sharedAccountID string, allowCopy bool,
) error {
	b.mu.Lock("UpdateWorkspaceImagePermission")
	defer b.mu.Unlock()

	if !b.images.Has(imageID) {
		return errImageNotFound
	}

	if b.imagePermissions[imageID] == nil {
		b.imagePermissions[imageID] = make(map[string]bool)
	}

	b.imagePermissions[imageID][sharedAccountID] = allowCopy

	return nil
}

// DescribeCustomWorkspaceImageImport returns import state for a custom image.
func (b *InMemoryBackend) DescribeCustomWorkspaceImageImport(imageID string) (*storedImage, error) {
	b.mu.RLock("DescribeCustomWorkspaceImageImport")
	defer b.mu.RUnlock()

	img, ok := b.images.Get(imageID)
	if !ok {
		return nil, errImageNotFound
	}

	cp := *img

	return &cp, nil
}

// DescribeImageAssociations returns application associations for an image.
// Real AWS's WorkSpaces Application Manager exposes no public API to create
// an image<->application association (only AssociateWorkspaceApplication,
// which associates an application directly with a WorkSpace, and
// DeployWorkspaceApplications, neither of which touch an image or bundle) --
// so a freshly emulated account always has an empty association list. This
// still performs the real required-field and existence validation a live
// call would enforce, matching the pattern used by RestoreWorkspace for an
// otherwise-no-op operation.
func (b *InMemoryBackend) DescribeImageAssociations(
	imageID string, resourceTypes []string,
) ([]ImageResourceAssociation, error) {
	b.mu.RLock("DescribeImageAssociations")
	defer b.mu.RUnlock()

	if imageID == "" {
		return nil, awserr.New("ImageId is required", awserr.ErrInvalidParameter)
	}

	if !b.images.Has(imageID) {
		return nil, errImageNotFound
	}

	if err := validateAssociatedResourceTypes(resourceTypes); err != nil {
		return nil, err
	}

	return []ImageResourceAssociation{}, nil
}
