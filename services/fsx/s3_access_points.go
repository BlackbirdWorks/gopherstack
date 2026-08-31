package fsx

import (
	"fmt"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

type storedS3AccessPoint struct {
	CreationTime time.Time `json:"creationTime"`
	Name         string    `json:"name"`
	Type         string    `json:"type"`
	VolumeID     string    `json:"volumeId"`
	Lifecycle    string    `json:"lifecycle"`
	ResourceARN  string    `json:"resourceArn"`
	Alias        string    `json:"alias"`
}

func (a *storedS3AccessPoint) toPublic() *S3AccessPointAttachment {
	att := &S3AccessPointAttachment{
		CreationTime: epochTime(a.CreationTime),
		Name:         a.Name,
		Lifecycle:    a.Lifecycle,
		Type:         a.Type,
		S3AccessPoint: &S3AccessPoint{
			Alias:       a.Alias,
			ResourceARN: a.ResourceARN,
		},
	}

	switch a.Type {
	case s3APTypeOntap:
		att.OntapConfiguration = &S3AccessPointOntapConfiguration{VolumeID: a.VolumeID}
	case s3APTypeOpenZFS:
		att.OpenZFSConfiguration = &S3AccessPointOpenZFSConfiguration{VolumeID: a.VolumeID}
	}

	return att
}

type createAndAttachS3AccessPointVolumeConfigInput struct {
	VolumeID string `json:"VolumeId"`
}

type createAndAttachS3AccessPointInput struct {
	OntapConfiguration   *createAndAttachS3AccessPointVolumeConfigInput `json:"OntapConfiguration,omitempty"`
	OpenZFSConfiguration *createAndAttachS3AccessPointVolumeConfigInput `json:"OpenZFSConfiguration,omitempty"`
	Name                 string                                         `json:"Name"`
	Type                 string                                         `json:"Type"`
}

// CreateAndAttachS3AccessPoint creates and attaches an S3 access point to an
// ONTAP or OpenZFS volume. Real AWS input has no top-level FileSystemId or
// Tags -- the attached volume is nested under whichever of
// OntapConfiguration/OpenZFSConfiguration matches Type
// (api_op_CreateAndAttachS3AccessPoint.go:52).
func (b *InMemoryBackend) CreateAndAttachS3AccessPoint(
	input *createAndAttachS3AccessPointInput,
) (*S3AccessPointAttachment, error) {
	if input.Name == "" || input.Type == "" {
		return nil, ErrValidation
	}

	var volumeID string

	switch input.Type {
	case s3APTypeOntap:
		if input.OntapConfiguration == nil || input.OntapConfiguration.VolumeID == "" {
			return nil, fmt.Errorf("%w: OntapConfiguration.VolumeId is required", ErrValidation)
		}

		volumeID = input.OntapConfiguration.VolumeID
	case s3APTypeOpenZFS:
		if input.OpenZFSConfiguration == nil || input.OpenZFSConfiguration.VolumeID == "" {
			return nil, fmt.Errorf("%w: OpenZFSConfiguration.VolumeId is required", ErrValidation)
		}

		volumeID = input.OpenZFSConfiguration.VolumeID
	default:
		return nil, fmt.Errorf("%w: Type must be ONTAP or OPENZFS", ErrValidation)
	}

	b.mu.Lock("CreateAndAttachS3AccessPoint")
	defer b.mu.Unlock()

	if !b.volumes.Has(volumeID) {
		return nil, ErrVolumeNotFound
	}

	arnStr := b.s3AccessPointARN(input.Name)
	now := time.Now().UTC()

	ap := &storedS3AccessPoint{
		CreationTime: now,
		Name:         input.Name,
		Type:         input.Type,
		VolumeID:     volumeID,
		Lifecycle:    lifecycleAvailable,
		ResourceARN:  arnStr,
		Alias:        generateS3AccessPointAlias(input.Name, b.region),
	}

	b.s3AccessPoints.Put(ap)
	b.tags[arnStr] = map[string]string{}

	return ap.toPublic(), nil
}

// DetachAndDeleteS3AccessPoint removes an S3 access point. Real AWS's
// DetachAndDeleteS3AccessPointInput has no FileSystemId member at all
// (api_op_DetachAndDeleteS3AccessPoint.go:36) -- Name alone identifies it.
func (b *InMemoryBackend) DetachAndDeleteS3AccessPoint(name string) error {
	b.mu.Lock("DetachAndDeleteS3AccessPoint")
	defer b.mu.Unlock()

	ap, ok := b.s3AccessPoints.Get(name)
	if !ok {
		return ErrS3AccessPointAttachmentNotFound
	}

	b.s3AccessPoints.Delete(name)
	delete(b.tags, ap.ResourceARN)

	return nil
}

// DescribeS3AccessPointAttachments returns S3 access point attachments,
// optionally filtered by Name or Filters. Real
// S3AccessPointAttachmentsFilterName (aws-sdk-go-v2/service/fsx@v1.68.4
// types/enums.go) has 3 values: file-system-id, volume-id, type.
// file-system-id requires resolving the attachment's owning volume --
// storedS3AccessPoint only tracks VolumeID directly.
func (b *InMemoryBackend) DescribeS3AccessPointAttachments(
	names []string,
	filters []wireFilter,
	maxResults int32,
	nextToken string,
) ([]*S3AccessPointAttachment, string, error) {
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
				return nil, "", ErrS3AccessPointAttachmentNotFound
			}

			all = append(all, ap)
		}
	} else {
		for _, ap := range b.s3AccessPoints.All() {
			if matchesFilters(filters, func(name string) (string, bool) {
				switch name {
				case "volume-id":
					return ap.VolumeID, true
				case "type":
					return ap.Type, true
				case filterNameFileSystemID:
					if vol, ok := b.volumes.Get(ap.VolumeID); ok {
						return vol.FileSystemID, true
					}

					return "", true
				default:
					return "", false
				}
			}) {
				all = append(all, ap)
			}
		}

		sort.Slice(all, func(i, j int) bool { return all[i].Name < all[j].Name })
	}

	start, end, next := paginate(len(all), int(maxResults), nextToken, func(i int) string {
		return all[i].Name
	})

	result := make([]*S3AccessPointAttachment, end-start)
	for i, ap := range all[start:end] {
		result[i] = ap.toPublic()
	}

	return result, next, nil
}

func (b *InMemoryBackend) s3AccessPointARN(name string) string {
	return arn.Build("fsx", b.region, b.accountID, fmt.Sprintf("s3-access-point/%s", name))
}

// generateS3AccessPointAlias returns a synthetic alias in the style AWS
// assigns to S3 access points (a globally-unique DNS-style hostname); the
// real hash algorithm AWS uses is undocumented, so this is a plausible
// stand-in, not a byte-exact reproduction.
func generateS3AccessPointAlias(name, region string) string {
	return fmt.Sprintf("%s-%s.s3-accesspoint.%s.amazonaws.com", name, newFSXHexUUID(s3AccessPointAliasHexLen), region)
}
