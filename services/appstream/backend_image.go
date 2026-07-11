package appstream

import (
	"fmt"
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

const (
	imageStateAvailable  = "AVAILABLE"
	imagePlatformWindows = "WINDOWS_SERVER_2019"

	imageBuilderStateStopped = "STOPPED"
	imageBuilderStateRunning = "RUNNING"

	exportStatusComplete = "COMPLETE"
)

type storedImage struct {
	CreatedTime  time.Time         `json:"createdTime"`
	Tags         map[string]string `json:"tags"`
	Name         string            `json:"name"`
	Arn          string            `json:"arn"`
	Description  string            `json:"description"`
	Platform     string            `json:"platform"`
	Visibility   string            `json:"visibility"`
	State        string            `json:"state"`
	BaseImageArn string            `json:"baseImageArn"`
}

func (i *storedImage) toImage() *Image {
	tags := make(map[string]string)
	maps.Copy(tags, i.Tags)

	return &Image{
		CreatedTime:  i.CreatedTime,
		Tags:         tags,
		Name:         i.Name,
		Arn:          i.Arn,
		Description:  i.Description,
		Platform:     i.Platform,
		Visibility:   i.Visibility,
		State:        i.State,
		BaseImageArn: i.BaseImageArn,
	}
}

// storedImagePermissions previously had no identity field of its own -- it
// was always looked up by an external imageName key on the plain map it
// lived in. Converting that map to a *store.Table[storedImagePermissions]
// (see store_setup.go) requires a keyFn, so it gained a real ImageName field
// for that purpose. This type is purely internal storage (DescribeImagePermissions
// builds its own SharedImagePermissions response type rather than marshaling
// this one), so a plain visible json tag is fine -- unlike a wire-shape type,
// there is no AWS response shape this could leak into.
type storedImagePermissions struct {
	SharedAccounts map[string]*ImagePermissions `json:"sharedAccounts"`
	ImageName      string                       `json:"imageName"`
}

type storedImageBuilder struct {
	CreatedTime  time.Time         `json:"createdTime"`
	Tags         map[string]string `json:"tags"`
	Name         string            `json:"name"`
	Arn          string            `json:"arn"`
	Description  string            `json:"description"`
	Platform     string            `json:"platform"`
	InstanceType string            `json:"instanceType"`
	State        string            `json:"state"`
	ImageName    string            `json:"imageName"`
}

func (ib *storedImageBuilder) toImageBuilder() *ImageBuilder {
	tags := make(map[string]string)
	maps.Copy(tags, ib.Tags)

	return &ImageBuilder{
		CreatedTime:  ib.CreatedTime,
		Tags:         tags,
		Name:         ib.Name,
		Arn:          ib.Arn,
		Description:  ib.Description,
		Platform:     ib.Platform,
		InstanceType: ib.InstanceType,
		State:        ib.State,
		ImageName:    ib.ImageName,
	}
}

type storedExportImageTask struct {
	CreatedAt time.Time `json:"createdAt"`
	TaskID    string    `json:"taskId"`
	ImageName string    `json:"imageName"`
	S3Bucket  string    `json:"s3Bucket"`
	S3Key     string    `json:"s3Key"`
	Status    string    `json:"status"`
}

func (t *storedExportImageTask) toExportImageTask() *ExportImageTask {
	return &ExportImageTask{
		CreatedAt: t.CreatedAt,
		TaskID:    t.TaskID,
		ImageName: t.ImageName,
		S3Bucket:  t.S3Bucket,
		S3Key:     t.S3Key,
		Status:    t.Status,
	}
}

func (b *InMemoryBackend) imageARN(name string) string {
	return arn.Build("appstream", b.region, b.accountID, fmt.Sprintf("image/%s", name))
}

func (b *InMemoryBackend) imageBuilderARN(name string) string {
	return arn.Build("appstream", b.region, b.accountID, fmt.Sprintf("image-builder/%s", name))
}

// CopyImage duplicates an image with a new name.
func (b *InMemoryBackend) CopyImage(
	sourceName, destName, destRegion, description string, //nolint:revive // existing issue.
) (*Image, error) {
	b.mu.Lock("CopyImage")
	defer b.mu.Unlock()

	src, ok := b.images.Get(sourceName)
	if !ok {
		return nil, ErrNotFound
	}

	if b.images.Has(destName) {
		return nil, ErrAlreadyExists
	}

	arn := b.imageARN(destName)
	desc := description
	if desc == "" {
		desc = src.Description
	}

	img := &storedImage{
		CreatedTime:  time.Now().UTC(),
		Tags:         make(map[string]string),
		Name:         destName,
		Arn:          arn,
		Description:  desc,
		Platform:     src.Platform,
		Visibility:   "PRIVATE", //nolint:goconst // existing issue.
		State:        imageStateAvailable,
		BaseImageArn: src.Arn,
	}
	b.images.Put(img)
	b.tags[arn] = make(map[string]string)

	return img.toImage(), nil
}

// CreateImportedImage creates a new image (e.g. imported from S3).
func (b *InMemoryBackend) CreateImportedImage(name, description string, tags map[string]string) (*Image, error) {
	b.mu.Lock("CreateImportedImage")
	defer b.mu.Unlock()

	if b.images.Has(name) {
		return nil, ErrAlreadyExists
	}

	arn := b.imageARN(name)
	storedTags := make(map[string]string)
	maps.Copy(storedTags, tags)

	img := &storedImage{
		CreatedTime: time.Now().UTC(),
		Tags:        storedTags,
		Name:        name,
		Arn:         arn,
		Description: description,
		Platform:    imagePlatformWindows,
		Visibility:  "PRIVATE",
		State:       imageStateAvailable,
	}
	b.images.Put(img)
	b.tags[arn] = storedTags

	return img.toImage(), nil
}

// CreateUpdatedImage creates a new image based on an existing one with updates applied.
func (b *InMemoryBackend) CreateUpdatedImage(imageName, newImageName, description string) (*Image, error) {
	b.mu.Lock("CreateUpdatedImage")
	defer b.mu.Unlock()

	src, ok := b.images.Get(imageName)
	if !ok {
		return nil, ErrNotFound
	}

	if b.images.Has(newImageName) {
		return nil, ErrAlreadyExists
	}

	arn := b.imageARN(newImageName)
	desc := description
	if desc == "" {
		desc = src.Description
	}

	img := &storedImage{
		CreatedTime:  time.Now().UTC(),
		Tags:         make(map[string]string),
		Name:         newImageName,
		Arn:          arn,
		Description:  desc,
		Platform:     src.Platform,
		Visibility:   "PRIVATE",
		State:        imageStateAvailable,
		BaseImageArn: src.Arn,
	}
	b.images.Put(img)
	b.tags[arn] = make(map[string]string)

	return img.toImage(), nil
}

// DeleteImage removes an image.
func (b *InMemoryBackend) DeleteImage(name string) error {
	b.mu.Lock("DeleteImage")
	defer b.mu.Unlock()

	img, ok := b.images.Get(name)
	if !ok {
		return ErrNotFound
	}

	delete(b.tags, img.Arn)
	b.images.Delete(name)
	b.imagePermissions.Delete(name)

	return nil
}

// DescribeImages returns images, optionally filtered by name.
func (b *InMemoryBackend) DescribeImages(names []string) ([]*Image, error) {
	b.mu.RLock("DescribeImages")
	defer b.mu.RUnlock()

	if len(names) > 0 {
		var result []*Image

		for _, name := range names {
			img, ok := b.images.Get(name)
			if !ok {
				return nil, ErrNotFound
			}

			result = append(result, img.toImage())
		}

		return result, nil
	}

	result := make([]*Image, 0, b.images.Len())
	for _, img := range b.images.All() {
		result = append(result, img.toImage())
	}

	return result, nil
}

// UpdateImagePermissions sets sharing permissions for a specific account.
func (b *InMemoryBackend) UpdateImagePermissions(
	imageName, accountID string,
	allowFleet, allowImageBuilder bool,
) error {
	b.mu.Lock("UpdateImagePermissions")
	defer b.mu.Unlock()

	if !b.images.Has(imageName) {
		return ErrNotFound
	}

	perms, ok := b.imagePermissions.Get(imageName)
	if !ok {
		perms = &storedImagePermissions{
			ImageName:      imageName,
			SharedAccounts: make(map[string]*ImagePermissions),
		}
		b.imagePermissions.Put(perms)
	}

	perms.SharedAccounts[accountID] = &ImagePermissions{
		AllowFleet:        allowFleet,
		AllowImageBuilder: allowImageBuilder,
	}

	return nil
}

// DeleteImagePermissions removes sharing permissions for a specific account.
func (b *InMemoryBackend) DeleteImagePermissions(imageName, accountID string) error {
	b.mu.Lock("DeleteImagePermissions")
	defer b.mu.Unlock()

	if !b.images.Has(imageName) {
		return ErrNotFound
	}

	if perms, ok := b.imagePermissions.Get(imageName); ok {
		delete(perms.SharedAccounts, accountID)
	}

	return nil
}

// DescribeImagePermissions returns sharing permissions for an image.
func (b *InMemoryBackend) DescribeImagePermissions(imageName string) ([]*SharedImagePermissions, error) {
	b.mu.RLock("DescribeImagePermissions")
	defer b.mu.RUnlock()

	if !b.images.Has(imageName) {
		return nil, ErrNotFound
	}

	perms, ok := b.imagePermissions.Get(imageName)
	if !ok {
		return []*SharedImagePermissions{}, nil
	}

	result := make([]*SharedImagePermissions, 0, len(perms.SharedAccounts))
	for accID, p := range perms.SharedAccounts {
		pCopy := *p
		result = append(result, &SharedImagePermissions{
			SharedAccountID:  accID,
			ImagePermissions: &pCopy,
		})
	}

	return result, nil
}

// CreateImageBuilder creates a new image builder.
func (b *InMemoryBackend) CreateImageBuilder(
	name, description, platform, instanceType string,
	tags map[string]string,
) (*ImageBuilder, error) {
	if instanceType == "" {
		return nil, fmt.Errorf("%w: InstanceType is required", awserr.ErrInvalidParameter)
	}

	b.mu.Lock("CreateImageBuilder")
	defer b.mu.Unlock()

	if b.imageBuilders.Has(name) {
		return nil, ErrAlreadyExists
	}

	arn := b.imageBuilderARN(name)
	storedTags := make(map[string]string)
	maps.Copy(storedTags, tags)

	plat := platform
	if plat == "" {
		plat = imagePlatformWindows
	}

	ib := &storedImageBuilder{
		CreatedTime:  time.Now().UTC(),
		Tags:         storedTags,
		Name:         name,
		Arn:          arn,
		Description:  description,
		Platform:     plat,
		InstanceType: instanceType,
		State:        imageBuilderStateStopped,
	}
	b.imageBuilders.Put(ib)
	b.tags[arn] = storedTags

	return ib.toImageBuilder(), nil
}

// DeleteImageBuilder removes an image builder and returns the image name (if any).
func (b *InMemoryBackend) DeleteImageBuilder(name string) (string, error) {
	b.mu.Lock("DeleteImageBuilder")
	defer b.mu.Unlock()

	ib, ok := b.imageBuilders.Get(name)
	if !ok {
		return "", ErrNotFound
	}

	imageName := ib.ImageName
	delete(b.tags, ib.Arn)
	b.imageBuilders.Delete(name)
	delete(b.softwareAssoc, name)

	return imageName, nil
}

// DescribeImageBuilders returns image builders, optionally filtered by name.
func (b *InMemoryBackend) DescribeImageBuilders(names []string) ([]*ImageBuilder, error) {
	b.mu.RLock("DescribeImageBuilders")
	defer b.mu.RUnlock()

	if len(names) > 0 {
		var result []*ImageBuilder

		for _, name := range names {
			ib, ok := b.imageBuilders.Get(name)
			if !ok {
				return nil, ErrNotFound
			}

			result = append(result, ib.toImageBuilder())
		}

		return result, nil
	}

	result := make([]*ImageBuilder, 0, b.imageBuilders.Len())
	for _, ib := range b.imageBuilders.All() {
		result = append(result, ib.toImageBuilder())
	}

	return result, nil
}

// StartImageBuilder transitions an image builder to RUNNING and returns a streaming URL.
func (b *InMemoryBackend) StartImageBuilder(
	name, appstreamAgentVersion string, //nolint:revive // existing issue.
) (string, error) {
	b.mu.Lock("StartImageBuilder")
	defer b.mu.Unlock()

	ib, ok := b.imageBuilders.Get(name)
	if !ok {
		return "", ErrNotFound
	}

	if ib.State == imageBuilderStateRunning {
		return "", ErrFleetNotStopped
	}

	ib.State = imageBuilderStateRunning

	url := fmt.Sprintf(
		"https://appstream2.%s.aws.amazon.com/authenticate?param=imagebuilder-%s", b.region, name,
	)

	return url, nil
}

// StopImageBuilder transitions an image builder to STOPPED.
func (b *InMemoryBackend) StopImageBuilder(name string) (*ImageBuilder, error) {
	b.mu.Lock("StopImageBuilder")
	defer b.mu.Unlock()

	ib, ok := b.imageBuilders.Get(name)
	if !ok {
		return nil, ErrNotFound
	}

	if ib.State == imageBuilderStateStopped {
		return nil, ErrFleetNotStopped
	}

	ib.State = imageBuilderStateStopped

	return ib.toImageBuilder(), nil
}

// CreateImageBuilderStreamingURL returns a streaming URL for an image builder.
func (b *InMemoryBackend) CreateImageBuilderStreamingURL(name string) (string, error) {
	b.mu.RLock("CreateImageBuilderStreamingURL")
	defer b.mu.RUnlock()

	if !b.imageBuilders.Has(name) {
		return "", ErrNotFound
	}

	return fmt.Sprintf(
		"https://appstream2.%s.aws.amazon.com/authenticate?param=imagebuilder-url-%s", b.region, name,
	), nil
}

// AssociateSoftwareToImageBuilder adds software packages to an image builder.
func (b *InMemoryBackend) AssociateSoftwareToImageBuilder(imageBuilderName string, software []string) error {
	b.mu.Lock("AssociateSoftwareToImageBuilder")
	defer b.mu.Unlock()

	if !b.imageBuilders.Has(imageBuilderName) {
		return ErrNotFound
	}

	if b.softwareAssoc[imageBuilderName] == nil {
		b.softwareAssoc[imageBuilderName] = make(map[string]bool)
	}

	for _, sw := range software {
		b.softwareAssoc[imageBuilderName][sw] = true
	}

	return nil
}

// DisassociateSoftwareFromImageBuilder removes software packages from an image builder.
func (b *InMemoryBackend) DisassociateSoftwareFromImageBuilder(imageBuilderName string, software []string) error {
	b.mu.Lock("DisassociateSoftwareFromImageBuilder")
	defer b.mu.Unlock()

	if !b.imageBuilders.Has(imageBuilderName) {
		return ErrNotFound
	}

	for _, sw := range software {
		if b.softwareAssoc[imageBuilderName] != nil {
			delete(b.softwareAssoc[imageBuilderName], sw)
		}
	}

	return nil
}

// DescribeSoftwareAssociations returns software associated with an image builder.
func (b *InMemoryBackend) DescribeSoftwareAssociations(imageBuilderName string) ([]SoftwareAssociation, error) {
	b.mu.RLock("DescribeSoftwareAssociations")
	defer b.mu.RUnlock()

	if !b.imageBuilders.Has(imageBuilderName) {
		return nil, ErrNotFound
	}

	sw := b.softwareAssoc[imageBuilderName]
	result := make([]SoftwareAssociation, 0, len(sw))

	for name := range sw {
		result = append(result, SoftwareAssociation{
			ImageBuilderName: imageBuilderName,
			Software:         name,
		})
	}

	return result, nil
}

// StartSoftwareDeploymentToImageBuilder triggers a deployment (no-op for in-memory).
func (b *InMemoryBackend) StartSoftwareDeploymentToImageBuilder(imageBuilderName string) error {
	b.mu.RLock("StartSoftwareDeploymentToImageBuilder")
	defer b.mu.RUnlock()

	if !b.imageBuilders.Has(imageBuilderName) {
		return ErrNotFound
	}

	return nil
}

func (b *InMemoryBackend) nextExportTaskID() string {
	b.exportTaskSeq++

	return fmt.Sprintf("export-task-%05d", b.exportTaskSeq)
}

// CreateExportImageTask creates an image export task.
func (b *InMemoryBackend) CreateExportImageTask(imageName, s3Bucket, s3Prefix string) (*ExportImageTask, error) {
	b.mu.Lock("CreateExportImageTask")
	defer b.mu.Unlock()

	if !b.images.Has(imageName) {
		return nil, ErrNotFound
	}

	taskID := b.nextExportTaskID()
	task := &storedExportImageTask{
		CreatedAt: time.Now().UTC(),
		TaskID:    taskID,
		ImageName: imageName,
		S3Bucket:  s3Bucket,
		S3Key:     s3Prefix + imageName + ".json",
		Status:    exportStatusComplete,
	}
	b.exportTasks.Put(task)

	return task.toExportImageTask(), nil
}

// GetExportImageTask retrieves an export task by ID.
func (b *InMemoryBackend) GetExportImageTask(taskID string) (*ExportImageTask, error) {
	b.mu.RLock("GetExportImageTask")
	defer b.mu.RUnlock()

	task, ok := b.exportTasks.Get(taskID)
	if !ok {
		return nil, ErrNotFound
	}

	return task.toExportImageTask(), nil
}

// ListExportImageTasks returns export tasks, optionally filtered by image name.
func (b *InMemoryBackend) ListExportImageTasks(imageNames []string) ([]*ExportImageTask, error) {
	b.mu.RLock("ListExportImageTasks")
	defer b.mu.RUnlock()

	nameSet := make(map[string]bool, len(imageNames))
	for _, n := range imageNames {
		nameSet[n] = true
	}

	var result []*ExportImageTask

	for _, task := range b.exportTasks.All() {
		if len(nameSet) > 0 && !nameSet[task.ImageName] {
			continue
		}

		result = append(result, task.toExportImageTask())
	}

	return result, nil
}
