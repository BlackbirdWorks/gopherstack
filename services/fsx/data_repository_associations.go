package fsx

import (
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// defaultImportedFileChunkSize is the real API's documented omission
// default for ImportedFileChunkSize on CreateDataRepositoryAssociation:
// "The default chunk size is 1,024 MiB (1 GiB)"
// (api_op_CreateDataRepositoryAssociation.go).
const defaultImportedFileChunkSize = 1024

type storedDataRepositoryAssoc struct {
	CreationTime          time.Time                      `json:"creationTime"`
	Tags                  map[string]string              `json:"tags"`
	AssociationID         string                         `json:"associationId"`
	FileSystemID          string                         `json:"fileSystemId"`
	FileSystemPath        string                         `json:"fileSystemPath"`
	DataRepositoryPath    string                         `json:"dataRepositoryPath"`
	Lifecycle             string                         `json:"lifecycle"`
	ResourceARN           string                         `json:"resourceArn"`
	S3                    *S3DataRepositoryConfiguration `json:"s3,omitempty"`
	NFS                   *FileCacheNFSConfiguration     `json:"nfs,omitempty"`
	FileCacheID           string                         `json:"fileCacheId,omitempty"`
	FileCachePath         string                         `json:"fileCachePath,omitempty"`
	Subdirectories        []string                       `json:"subdirectories,omitempty"`
	ImportedFileChunkSize int32                          `json:"importedFileChunkSize"`
}

func (a *storedDataRepositoryAssoc) toPublic() *DataRepositoryAssociation {
	return &DataRepositoryAssociation{
		CreationTime:                 epochTime(a.CreationTime),
		AssociationID:                a.AssociationID,
		FileSystemID:                 a.FileSystemID,
		FileSystemPath:               a.FileSystemPath,
		DataRepositoryPath:           a.DataRepositoryPath,
		Lifecycle:                    a.Lifecycle,
		ResourceARN:                  a.ResourceARN,
		Tags:                         tagsMapToSlice(a.Tags),
		S3:                           cloneS3Config(a.S3),
		NFS:                          cloneNFSConfig(a.NFS),
		FileCacheID:                  a.FileCacheID,
		FileCachePath:                a.FileCachePath,
		DataRepositorySubdirectories: slices.Clone(a.Subdirectories),
		ImportedFileChunkSize:        a.ImportedFileChunkSize,
	}
}

type createDataRepositoryAssociationInput struct {
	ImportedFileChunkSize       *int32                         `json:"ImportedFileChunkSize,omitempty"`
	BatchImportMetaDataOnCreate *bool                          `json:"BatchImportMetaDataOnCreate,omitempty"`
	ClientRequestToken          string                         `json:"ClientRequestToken,omitempty"`
	FileSystemID                string                         `json:"FileSystemId"`
	FileSystemPath              string                         `json:"FileSystemPath"`
	DataRepositoryPath          string                         `json:"DataRepositoryPath"`
	S3                          *S3DataRepositoryConfiguration `json:"S3,omitempty"`
	Tags                        []Tag                          `json:"Tags,omitempty"`
}

// CreateDataRepositoryAssociation creates a data repository association.
func (b *InMemoryBackend) CreateDataRepositoryAssociation(
	input *createDataRepositoryAssociationInput,
) (*DataRepositoryAssociation, error) {
	if err := validateCreateTags(input.Tags); err != nil {
		return nil, err
	}

	if err := validateS3Config(input.S3); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateDataRepositoryAssociation")
	defer b.mu.Unlock()

	fp, replayID, err := b.replayTokenLocked("CreateDataRepositoryAssociation", input.ClientRequestToken, input)
	if err != nil {
		return nil, err
	}

	if existing, ok := b.dataRepositoryAssocs.Get(replayID); ok {
		return existing.toPublic(), nil
	}

	if !b.fileSystems.Has(input.FileSystemID) {
		return nil, ErrFileSystemNotFound
	}

	id := newDataRepositoryAssociationID()
	arn := b.draARN(id)
	now := time.Now().UTC()
	tags := tagsSliceToMap(input.Tags)

	chunkSize := int32(defaultImportedFileChunkSize)
	if input.ImportedFileChunkSize != nil {
		chunkSize = *input.ImportedFileChunkSize
	}

	a := &storedDataRepositoryAssoc{
		CreationTime:          now,
		Tags:                  tags,
		AssociationID:         id,
		FileSystemID:          input.FileSystemID,
		FileSystemPath:        input.FileSystemPath,
		DataRepositoryPath:    input.DataRepositoryPath,
		Lifecycle:             lifecycleAvailable,
		ResourceARN:           arn,
		ImportedFileChunkSize: chunkSize,
		S3:                    cloneS3Config(input.S3),
	}

	b.dataRepositoryAssocs.Put(a)
	b.tags[arn] = tags
	b.recordTokenLocked("CreateDataRepositoryAssociation", input.ClientRequestToken, fp, id)

	if input.BatchImportMetaDataOnCreate != nil && *input.BatchImportMetaDataOnCreate {
		b.startImportTaskLocked(a, now)
	}

	return a.toPublic(), nil
}

// DeleteDataRepositoryAssociation removes a data repository association.
func (b *InMemoryBackend) DeleteDataRepositoryAssociation(associationID string) error {
	b.mu.Lock("DeleteDataRepositoryAssociation")
	defer b.mu.Unlock()

	a, ok := b.dataRepositoryAssocs.Get(associationID)
	if !ok {
		return ErrDataRepositoryAssociationNotFound
	}

	b.dataRepositoryAssocs.Delete(associationID)
	delete(b.tags, a.ResourceARN)

	return nil
}

// DescribeDataRepositoryAssociations returns DRAs, optionally filtered by ID
// or Filters. Real DescribeDataRepositoryAssociationsInput.Filters
// (aws-sdk-go-v2/service/fsx@v1.68.4 api_op_DescribeDataRepositoryAssociations.go)
// reuses the same types.Filter/FilterName as DescribeBackups, but a DRA has
// no backup-type/volume-id/file-cache-* concept of its own -- only
// file-system-id is recognized here; the other enum values are honored as
// documented-but-unsupported (matches everything, same as an unset filter).
func (b *InMemoryBackend) DescribeDataRepositoryAssociations( //nolint:dupl // existing issue.
	ids []string,
	filters []wireFilter,
	maxResults int32,
	nextToken string,
) ([]*DataRepositoryAssociation, string, error) {
	b.mu.RLock("DescribeDataRepositoryAssociations")
	defer b.mu.RUnlock()

	if maxResults <= 0 {
		maxResults = maxResultsDefault
	}

	var all []*storedDataRepositoryAssoc

	if len(ids) > 0 {
		for _, id := range ids {
			a, ok := b.dataRepositoryAssocs.Get(id)
			if !ok {
				return nil, "", ErrDataRepositoryAssociationNotFound
			}

			all = append(all, a)
		}
	} else {
		for _, a := range b.dataRepositoryAssocs.All() {
			if matchesFilters(filters, func(name string) (string, bool) {
				switch name {
				case filterNameFileSystemID:
					return a.FileSystemID, true
				case "file-cache-id":
					return a.FileCacheID, true
				default:
					return "", false
				}
			}) {
				all = append(all, a)
			}
		}

		sort.Slice(all, func(i, j int) bool { return all[i].AssociationID < all[j].AssociationID })
	}

	start, end, next := paginate(len(all), int(maxResults), nextToken, func(i int) string {
		return all[i].AssociationID
	})

	result := make([]*DataRepositoryAssociation, end-start)
	for i, a := range all[start:end] {
		result[i] = a.toPublic()
	}

	return result, next, nil
}

type updateDataRepositoryAssociationInput struct {
	ImportedFileChunkSize *int32                         `json:"ImportedFileChunkSize,omitempty"`
	S3                    *S3DataRepositoryConfiguration `json:"S3,omitempty"`
	AssociationID         string                         `json:"AssociationId"`
	FileSystemPath        string                         `json:"FileSystemPath,omitempty"`
	DataRepositoryPath    string                         `json:"DataRepositoryPath,omitempty"`
}

// UpdateDataRepositoryAssociation updates a DRA's paths.
func (b *InMemoryBackend) UpdateDataRepositoryAssociation(
	input *updateDataRepositoryAssociationInput,
) (*DataRepositoryAssociation, error) {
	if err := validateS3Config(input.S3); err != nil {
		return nil, err
	}

	b.mu.Lock("UpdateDataRepositoryAssociation")
	defer b.mu.Unlock()

	a, ok := b.dataRepositoryAssocs.Get(input.AssociationID)
	if !ok {
		return nil, ErrDataRepositoryAssociationNotFound
	}

	if input.FileSystemPath != "" {
		a.FileSystemPath = input.FileSystemPath
	}

	if input.DataRepositoryPath != "" {
		a.DataRepositoryPath = input.DataRepositoryPath
	}

	if input.ImportedFileChunkSize != nil {
		a.ImportedFileChunkSize = *input.ImportedFileChunkSize
	}

	a.S3 = mergeS3Config(a.S3, input.S3)

	return a.toPublic(), nil
}

func (b *InMemoryBackend) draARN(id string) string {
	return arn.Build("fsx", b.region, b.accountID, fmt.Sprintf("association/%s", id))
}

// startImportTaskLocked runs the metadata import a DRA created with
// BatchImportMetaDataOnCreate triggers. Caller holds the write lock.
func (b *InMemoryBackend) startImportTaskLocked(a *storedDataRepositoryAssoc, now time.Time) {
	id := newDataRepositoryTaskID()
	taskARN := b.drtARN(id)
	disabled := false

	b.dataRepositoryTasks.Put(&storedDataRepositoryTask{
		CreationTime:  now,
		DeadlineAt:    now.Add(dataRepositoryTaskCompletionDelay),
		Report:        &CompletionReport{Enabled: &disabled},
		Tags:          map[string]string{},
		TaskID:        id,
		FileSystemID:  a.FileSystemID,
		AssociationID: a.AssociationID,
		Type:          "IMPORT_METADATA_FROM_REPOSITORY",
		Lifecycle:     drtLifecycleExecuting,
		ResourceARN:   taskARN,
		Paths:         []string{a.FileSystemPath},
	})
	b.tags[taskARN] = map[string]string{}
}

func validateEventPolicy(name string, p *EventPolicy) error {
	if p == nil {
		return nil
	}

	for _, e := range p.Events {
		switch e {
		case "NEW", "CHANGED", "DELETED":
		default:
			return fmt.Errorf("%w: S3.%s.Events value %q must be NEW, CHANGED or DELETED", ErrValidation, name, e)
		}
	}

	return nil
}

func validateS3Config(c *S3DataRepositoryConfiguration) error {
	if c == nil {
		return nil
	}

	if err := validateEventPolicy("AutoExportPolicy", c.AutoExportPolicy); err != nil {
		return err
	}

	return validateEventPolicy("AutoImportPolicy", c.AutoImportPolicy)
}

func cloneEventPolicy(p *EventPolicy) *EventPolicy {
	if p == nil {
		return nil
	}

	return &EventPolicy{Events: slices.Clone(p.Events)}
}

func cloneS3Config(c *S3DataRepositoryConfiguration) *S3DataRepositoryConfiguration {
	if c == nil {
		return nil
	}

	return &S3DataRepositoryConfiguration{
		AutoExportPolicy: cloneEventPolicy(c.AutoExportPolicy),
		AutoImportPolicy: cloneEventPolicy(c.AutoImportPolicy),
	}
}

// mergeS3Config applies an update: each policy present in upd replaces the
// stored one, absent policies are kept.
func mergeS3Config(cur, upd *S3DataRepositoryConfiguration) *S3DataRepositoryConfiguration {
	if upd == nil {
		return cur
	}

	out := cloneS3Config(cur)
	if out == nil {
		out = &S3DataRepositoryConfiguration{}
	}

	if upd.AutoExportPolicy != nil {
		out.AutoExportPolicy = cloneEventPolicy(upd.AutoExportPolicy)
	}

	if upd.AutoImportPolicy != nil {
		out.AutoImportPolicy = cloneEventPolicy(upd.AutoImportPolicy)
	}

	return out
}

func cloneNFSConfig(c *FileCacheNFSConfiguration) *FileCacheNFSConfiguration {
	if c == nil {
		return nil
	}

	return &FileCacheNFSConfiguration{Version: c.Version, DNSIPs: slices.Clone(c.DNSIPs)}
}
