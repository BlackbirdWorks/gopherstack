package appstream

import (
	"fmt"
	"maps"
	"time"

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

type storedImagePermissions struct {
	SharedAccounts map[string]*ImagePermissions `json:"sharedAccounts"`
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
	return fmt.Sprintf("arn:aws:appstream:%s:%s:image/%s", b.region, b.accountID, name)
}

func (b *InMemoryBackend) imageBuilderARN(name string) string {
	return fmt.Sprintf("arn:aws:appstream:%s:%s:image-builder/%s", b.region, b.accountID, name)
}

// CopyImage duplicates an image with a new name.
func (b *InMemoryBackend) CopyImage(
	sourceName, destName, destRegion, description string, //nolint:revive // existing issue.
) (*Image, error) {
	b.mu.Lock("CopyImage")
	defer b.mu.Unlock()

	src, ok := b.images[sourceName]
	if !ok {
		return nil, ErrNotFound
	}

	if _, ok := b.images[destName]; ok { //nolint:govet // existing issue.
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
	b.images[destName] = img
	b.tags[arn] = make(map[string]string)

	return img.toImage(), nil
}

// CreateImportedImage creates a new image (e.g. imported from S3).
func (b *InMemoryBackend) CreateImportedImage(name, description string, tags map[string]string) (*Image, error) {
	b.mu.Lock("CreateImportedImage")
	defer b.mu.Unlock()

	if _, ok := b.images[name]; ok {
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
	b.images[name] = img
	b.tags[arn] = storedTags

	return img.toImage(), nil
}

// CreateUpdatedImage creates a new image based on an existing one with updates applied.
func (b *InMemoryBackend) CreateUpdatedImage(imageName, newImageName, description string) (*Image, error) {
	b.mu.Lock("CreateUpdatedImage")
	defer b.mu.Unlock()

	src, ok := b.images[imageName]
	if !ok {
		return nil, ErrNotFound
	}

	if _, ok := b.images[newImageName]; ok { //nolint:govet // existing issue.
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
	b.images[newImageName] = img
	b.tags[arn] = make(map[string]string)

	return img.toImage(), nil
}

// DeleteImage removes an image.
func (b *InMemoryBackend) DeleteImage(name string) error {
	b.mu.Lock("DeleteImage")
	defer b.mu.Unlock()

	img, ok := b.images[name]
	if !ok {
		return ErrNotFound
	}

	delete(b.tags, img.Arn)
	delete(b.images, name)
	delete(b.imagePermissions, name)

	return nil
}

// DescribeImages returns images, optionally filtered by name.
func (b *InMemoryBackend) DescribeImages(names []string) ([]*Image, error) {
	b.mu.RLock("DescribeImages")
	defer b.mu.RUnlock()

	if len(names) > 0 {
		var result []*Image

		for _, name := range names {
			img, ok := b.images[name]
			if !ok {
				return nil, ErrNotFound
			}

			result = append(result, img.toImage())
		}

		return result, nil
	}

	result := make([]*Image, 0, len(b.images))
	for _, img := range b.images {
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

	if _, ok := b.images[imageName]; !ok {
		return ErrNotFound
	}

	if b.imagePermissions[imageName] == nil {
		b.imagePermissions[imageName] = &storedImagePermissions{
			SharedAccounts: make(map[string]*ImagePermissions),
		}
	}

	b.imagePermissions[imageName].SharedAccounts[accountID] = &ImagePermissions{
		AllowFleet:        allowFleet,
		AllowImageBuilder: allowImageBuilder,
	}

	return nil
}

// DeleteImagePermissions removes sharing permissions for a specific account.
func (b *InMemoryBackend) DeleteImagePermissions(imageName, accountID string) error {
	b.mu.Lock("DeleteImagePermissions")
	defer b.mu.Unlock()

	if _, ok := b.images[imageName]; !ok {
		return ErrNotFound
	}

	if b.imagePermissions[imageName] != nil {
		delete(b.imagePermissions[imageName].SharedAccounts, accountID)
	}

	return nil
}

// DescribeImagePermissions returns sharing permissions for an image.
func (b *InMemoryBackend) DescribeImagePermissions(imageName string) ([]*SharedImagePermissions, error) {
	b.mu.RLock("DescribeImagePermissions")
	defer b.mu.RUnlock()

	if _, ok := b.images[imageName]; !ok {
		return nil, ErrNotFound
	}

	perms := b.imagePermissions[imageName]
	if perms == nil {
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

	if _, ok := b.imageBuilders[name]; ok {
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
	b.imageBuilders[name] = ib
	b.tags[arn] = storedTags

	return ib.toImageBuilder(), nil
}

// DeleteImageBuilder removes an image builder and returns the image name (if any).
func (b *InMemoryBackend) DeleteImageBuilder(name string) (string, error) {
	b.mu.Lock("DeleteImageBuilder")
	defer b.mu.Unlock()

	ib, ok := b.imageBuilders[name]
	if !ok {
		return "", ErrNotFound
	}

	imageName := ib.ImageName
	delete(b.tags, ib.Arn)
	delete(b.imageBuilders, name)
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
			ib, ok := b.imageBuilders[name]
			if !ok {
				return nil, ErrNotFound
			}

			result = append(result, ib.toImageBuilder())
		}

		return result, nil
	}

	result := make([]*ImageBuilder, 0, len(b.imageBuilders))
	for _, ib := range b.imageBuilders {
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

	ib, ok := b.imageBuilders[name]
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

	ib, ok := b.imageBuilders[name]
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

	if _, ok := b.imageBuilders[name]; !ok {
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

	if _, ok := b.imageBuilders[imageBuilderName]; !ok {
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

	if _, ok := b.imageBuilders[imageBuilderName]; !ok {
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

	if _, ok := b.imageBuilders[imageBuilderName]; !ok {
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

	if _, ok := b.imageBuilders[imageBuilderName]; !ok {
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

	if _, ok := b.images[imageName]; !ok {
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
	b.exportTasks[taskID] = task

	return task.toExportImageTask(), nil
}

// GetExportImageTask retrieves an export task by ID.
func (b *InMemoryBackend) GetExportImageTask(taskID string) (*ExportImageTask, error) {
	b.mu.RLock("GetExportImageTask")
	defer b.mu.RUnlock()

	task, ok := b.exportTasks[taskID]
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

	for _, task := range b.exportTasks {
		if len(nameSet) > 0 && !nameSet[task.ImageName] {
			continue
		}

		result = append(result, task.toExportImageTask())
	}

	return result, nil
}
