package fsx

import (
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

type storedStorageVirtualMachine struct {
	CreationTime            time.Time         `json:"creationTime"`
	Tags                    map[string]string `json:"tags"`
	StorageVirtualMachineID string            `json:"storageVirtualMachineId"`
	FileSystemID            string            `json:"fileSystemId"`
	Name                    string            `json:"name"`
	Lifecycle               string            `json:"lifecycle"`
	ResourceARN             string            `json:"resourceArn"`
	Subtype                 string            `json:"subtype,omitempty"`
	RootVolumeSecurityStyle string            `json:"rootVolumeSecurityStyle,omitempty"`
}

func (s *storedStorageVirtualMachine) toPublic() *StorageVirtualMachine {
	return &StorageVirtualMachine{
		CreationTime:            epochTime(s.CreationTime),
		StorageVirtualMachineID: s.StorageVirtualMachineID,
		FileSystemID:            s.FileSystemID,
		Name:                    s.Name,
		Lifecycle:               s.Lifecycle,
		ResourceARN:             s.ResourceARN,
		Subtype:                 s.Subtype,
		RootVolumeSecurityStyle: s.RootVolumeSecurityStyle,
		Tags:                    tagsMapToSlice(s.Tags),
	}
}

type createStorageVirtualMachineInput struct {
	FileSystemID            string `json:"FileSystemId"`
	Name                    string `json:"Name"`
	Subtype                 string `json:"Subtype,omitempty"`
	RootVolumeSecurityStyle string `json:"RootVolumeSecurityStyle,omitempty"`
	Tags                    []Tag  `json:"Tags,omitempty"`
}

// CreateStorageVirtualMachine creates an SVM on an ONTAP file system.
func (b *InMemoryBackend) CreateStorageVirtualMachine(
	input *createStorageVirtualMachineInput,
) (*StorageVirtualMachine, error) {
	if input.FileSystemID == "" {
		return nil, ErrValidation
	}

	if err := validateTags(input.Tags); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateStorageVirtualMachine")
	defer b.mu.Unlock()

	if !b.fileSystems.Has(input.FileSystemID) {
		return nil, ErrFileSystemNotFound
	}

	id := "svm-" + uuid.New().String()[:17]
	arn := b.svmARN(id)
	now := time.Now().UTC()
	tags := tagsSliceToMap(input.Tags)

	svm := &storedStorageVirtualMachine{
		CreationTime:            now,
		Tags:                    tags,
		StorageVirtualMachineID: id,
		FileSystemID:            input.FileSystemID,
		Name:                    input.Name,
		Lifecycle:               lifecycleAvailable,
		ResourceARN:             arn,
		Subtype:                 input.Subtype,
		RootVolumeSecurityStyle: input.RootVolumeSecurityStyle,
	}

	b.storageVirtualMachines.Put(svm)
	b.tags[arn] = tags

	return svm.toPublic(), nil
}

// DeleteStorageVirtualMachine removes an SVM.
func (b *InMemoryBackend) DeleteStorageVirtualMachine(svmID string) error {
	b.mu.Lock("DeleteStorageVirtualMachine")
	defer b.mu.Unlock()

	svm, ok := b.storageVirtualMachines.Get(svmID)
	if !ok {
		return ErrStorageVirtualMachineNotFound
	}

	b.storageVirtualMachines.Delete(svmID)
	delete(b.tags, svm.ResourceARN)

	return nil
}

// DescribeStorageVirtualMachines returns SVMs, optionally filtered by ID.
func (b *InMemoryBackend) DescribeStorageVirtualMachines( //nolint:dupl // existing issue.
	ids []string,
	maxResults int32,
	nextToken string,
) ([]*StorageVirtualMachine, string, error) {
	b.mu.RLock("DescribeStorageVirtualMachines")
	defer b.mu.RUnlock()

	if maxResults <= 0 {
		maxResults = maxResultsDefault
	}

	var all []*storedStorageVirtualMachine

	if len(ids) > 0 {
		for _, id := range ids {
			svm, ok := b.storageVirtualMachines.Get(id)
			if !ok {
				return nil, "", ErrStorageVirtualMachineNotFound
			}

			all = append(all, svm)
		}
	} else {
		all = b.storageVirtualMachines.All()

		sort.Slice(all, func(i, j int) bool {
			return all[i].StorageVirtualMachineID < all[j].StorageVirtualMachineID
		})
	}

	start, end, next := paginate(len(all), int(maxResults), nextToken, func(i int) string {
		return all[i].StorageVirtualMachineID
	})

	result := make([]*StorageVirtualMachine, end-start)
	for i, svm := range all[start:end] {
		result[i] = svm.toPublic()
	}

	return result, next, nil
}

type updateStorageVirtualMachineInput struct {
	StorageVirtualMachineID string `json:"StorageVirtualMachineId"`
	Subtype                 string `json:"Subtype,omitempty"`
}

// UpdateStorageVirtualMachine updates an SVM.
func (b *InMemoryBackend) UpdateStorageVirtualMachine(
	input *updateStorageVirtualMachineInput,
) (*StorageVirtualMachine, error) {
	b.mu.Lock("UpdateStorageVirtualMachine")
	defer b.mu.Unlock()

	svm, ok := b.storageVirtualMachines.Get(input.StorageVirtualMachineID)
	if !ok {
		return nil, ErrStorageVirtualMachineNotFound
	}

	if input.Subtype != "" {
		svm.Subtype = input.Subtype
	}

	return svm.toPublic(), nil
}

func (b *InMemoryBackend) svmARN(id string) string {
	return arn.Build("fsx", b.region, b.accountID, fmt.Sprintf("storage-virtual-machine/%s", id))
}
