package fsx

import (
	"fmt"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

type storedS3AccessPoint struct {
	CreationTime time.Time         `json:"creationTime"`
	Tags         map[string]string `json:"tags"`
	Name         string            `json:"name"`
	FileSystemID string            `json:"fileSystemId"`
	VolumeID     string            `json:"volumeId,omitempty"`
	Lifecycle    string            `json:"lifecycle"`
	ResourceARN  string            `json:"resourceArn"`
}

func (a *storedS3AccessPoint) toPublic() *S3AccessPoint {
	return &S3AccessPoint{
		CreationTime: epochTime(a.CreationTime),
		Name:         a.Name,
		FileSystemID: a.FileSystemID,
		VolumeID:     a.VolumeID,
		Lifecycle:    a.Lifecycle,
		ResourceARN:  a.ResourceARN,
		Tags:         tagsMapToSlice(a.Tags),
	}
}

type createAndAttachS3AccessPointInput struct {
	Name         string `json:"Name"`
	FileSystemID string `json:"FileSystemId"`
	VolumeID     string `json:"VolumeId,omitempty"`
	Tags         []Tag  `json:"Tags,omitempty"`
}

// CreateAndAttachS3AccessPoint creates and attaches an S3 access point.
func (b *InMemoryBackend) CreateAndAttachS3AccessPoint(
	input *createAndAttachS3AccessPointInput,
) (*S3AccessPoint, error) {
	if input.Name == "" || input.FileSystemID == "" {
		return nil, ErrValidation
	}

	if err := validateTags(input.Tags); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateAndAttachS3AccessPoint")
	defer b.mu.Unlock()

	if !b.fileSystems.Has(input.FileSystemID) {
		return nil, ErrFileSystemNotFound
	}

	arn := b.s3AccessPointARN(input.Name)
	now := time.Now().UTC()
	tags := tagsSliceToMap(input.Tags)

	ap := &storedS3AccessPoint{
		CreationTime: now,
		Tags:         tags,
		Name:         input.Name,
		FileSystemID: input.FileSystemID,
		VolumeID:     input.VolumeID,
		Lifecycle:    lifecycleAvailable,
		ResourceARN:  arn,
	}

	b.s3AccessPoints.Put(ap)
	b.tags[arn] = tags

	return ap.toPublic(), nil
}

// DetachAndDeleteS3AccessPoint removes an S3 access point.
func (b *InMemoryBackend) DetachAndDeleteS3AccessPoint(name, fileSystemID string) error {
	b.mu.Lock("DetachAndDeleteS3AccessPoint")
	defer b.mu.Unlock()

	ap, ok := b.s3AccessPoints.Get(name)
	if !ok || ap.FileSystemID != fileSystemID {
		return ErrS3AccessPointNotFound
	}

	b.s3AccessPoints.Delete(name)
	delete(b.tags, ap.ResourceARN)

	return nil
}

// DescribeS3AccessPointAttachments returns S3 access points.
func (b *InMemoryBackend) DescribeS3AccessPointAttachments( //nolint:dupl // existing issue.
	names []string,
	maxResults int32,
	nextToken string,
) ([]*S3AccessPoint, string, error) {
	b.mu.RLock("DescribeS3AccessPointAttachments")
	defer b.mu.RUnlock()

	if maxResults <= 0 {
		maxResults = maxResultsDefault
	}

	var all []*storedS3AccessPoint

	if len(names) > 0 {
		for _, name := range names {
			ap, ok := b.s3AccessPoints.Get(name)
			if !ok {
				return nil, "", ErrS3AccessPointNotFound
			}

			all = append(all, ap)
		}
	} else {
		all = b.s3AccessPoints.All()

		sort.Slice(all, func(i, j int) bool { return all[i].Name < all[j].Name })
	}

	start, end, next := paginate(len(all), int(maxResults), nextToken, func(i int) string {
		return all[i].Name
	})

	result := make([]*S3AccessPoint, end-start)
	for i, ap := range all[start:end] {
		result[i] = ap.toPublic()
	}

	return result, next, nil
}

func (b *InMemoryBackend) s3AccessPointARN(name string) string {
	return arn.Build("fsx", b.region, b.accountID, fmt.Sprintf("s3-access-point/%s", name))
}
