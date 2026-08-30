package bedrock

import (
	"fmt"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// newProvisionedID generates a unique provisioned model throughput ID.
func (b *InMemoryBackend) newProvisionedID() string {
	b.provisionedCounter++

	return fmt.Sprintf("pmt-%07d", b.provisionedCounter)
}

// CreateProvisionedModelThroughput creates a new provisioned model throughput.
func (b *InMemoryBackend) CreateProvisionedModelThroughput(
	name, modelID string,
	modelUnits int32,
	commitmentDuration string,
	tags []Tag,
) (*ProvisionedModelThroughput, error) {
	b.mu.Lock("CreateProvisionedModelThroughput")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: provisionedModelName is required", ErrValidation)
	}

	if modelUnits <= 0 {
		return nil, fmt.Errorf("%w: modelUnits must be greater than 0", ErrValidation)
	}

	if modelUnits > maxProvisionedModelUnits {
		return nil, fmt.Errorf(
			"%w: modelUnits must be at most %d",
			ErrValidation, maxProvisionedModelUnits,
		)
	}

	if _, exists := b.pmtsByName[name]; exists {
		return nil, fmt.Errorf(
			"%w: provisioned model throughput %s already exists",
			ErrAlreadyExists,
			name,
		)
	}

	id := b.newProvisionedID()
	pmtARN := arn.Build("bedrock", b.region, b.accountID, "provisioned-model/"+id)
	modelARN := foundationModelARN(b.region, modelID)
	now := time.Now().UTC()

	tagsCopy := make([]Tag, len(tags))
	copy(tagsCopy, tags)

	pmt := &ProvisionedModelThroughput{
		ProvisionedModelArn:  pmtARN,
		ProvisionedModelName: name,
		ModelArn:             modelARN,
		DesiredModelArn:      modelARN,
		FoundationModelArn:   modelARN,
		Status:               statusCreating,
		ModelUnits:           modelUnits,
		DesiredModelUnits:    modelUnits,
		CommitmentDuration:   commitmentDuration,
		CreationTime:         now,
		LastModifiedTime:     now,
		Tags:                 tagsCopy,
	}
	b.provisionedModelThroughputs.Put(pmt)
	b.pmtsByName[name] = pmtARN
	cp := *pmt

	return &cp, nil
}

func (b *InMemoryBackend) GetProvisionedModelThroughput(
	idOrARN string,
) (*ProvisionedModelThroughput, error) {
	b.mu.RLock("GetProvisionedModelThroughput")
	defer b.mu.RUnlock()

	pmt, ok := b.findPMTByIDOrARN(idOrARN)
	if !ok {
		return nil, fmt.Errorf(
			"%w: provisioned model throughput %s not found",
			ErrNotFound,
			idOrARN,
		)
	}

	cp := *pmt

	return &cp, nil
}

// ListProvisionedModelThroughputs returns provisioned model throughputs
// matching in's filters, sorted and paginated. in may be nil, matching an
// unfiltered call. Structurally similar to ListModelCopyJobs/
// ListModelImportJobs/ListCustomModelDeployments (same filter/sort/paginate
// shape) but over a distinct resource type and filter set; see
// matchesProvisionedModelThroughputFilter.
func (b *InMemoryBackend) ListProvisionedModelThroughputs(
	in *ListProvisionedModelThroughputsInput,
) ([]*ProvisionedModelThroughput, string) {
	b.mu.RLock("ListProvisionedModelThroughputs")
	defer b.mu.RUnlock()

	list := make([]*ProvisionedModelThroughput, 0, b.provisionedModelThroughputs.Len())

	for _, pmt := range b.provisionedModelThroughputs.All() {
		if !matchesProvisionedModelThroughputFilter(pmt, in) {
			continue
		}

		cp := *pmt
		list = append(list, &cp)
	}

	descending := in != nil && in.SortOrder == sortOrderDescending
	sort.Slice(list, func(i, k int) bool {
		if !list[i].CreationTime.Equal(list[k].CreationTime) {
			if descending {
				return list[i].CreationTime.After(list[k].CreationTime)
			}

			return list[i].CreationTime.Before(list[k].CreationTime)
		}

		return list[i].ProvisionedModelArn < list[k].ProvisionedModelArn
	})

	if in == nil {
		list, _ = paginate(list, 0, "")

		return list, ""
	}

	return paginate(list, int(in.MaxResults), in.NextToken)
}

// matchesProvisionedModelThroughputFilter reports whether a provisioned
// model throughput satisfies the list filters (statusEquals, modelArnEquals,
// nameContains, creationTimeAfter/Before).
func matchesProvisionedModelThroughputFilter(
	pmt *ProvisionedModelThroughput, in *ListProvisionedModelThroughputsInput,
) bool {
	if in == nil {
		return true
	}
	if in.StatusEquals != "" && pmt.Status != in.StatusEquals {
		return false
	}
	if in.ModelArnEquals != "" && pmt.ModelArn != in.ModelArnEquals {
		return false
	}
	if in.NameContains != "" && !containsIgnoreCase(pmt.ProvisionedModelName, in.NameContains) {
		return false
	}
	if in.CreationTimeAfter != nil && !pmt.CreationTime.After(*in.CreationTimeAfter) {
		return false
	}
	if in.CreationTimeBefore != nil && !pmt.CreationTime.Before(*in.CreationTimeBefore) {
		return false
	}

	return true
}

// UpdateProvisionedModelThroughput updates a provisioned model throughput's desired
// model association and/or name. AWS does not allow changing modelUnits via Update —
// the unit count is fixed at creation (UpdateProvisionedModelThroughputInput only has
// desiredModelId and desiredProvisionedModelName).
func (b *InMemoryBackend) UpdateProvisionedModelThroughput(
	idOrARN, desiredModelID, newName string,
) (*ProvisionedModelThroughput, error) {
	b.mu.Lock("UpdateProvisionedModelThroughput")
	defer b.mu.Unlock()

	pmt, ok := b.findPMTByIDOrARN(idOrARN)
	if !ok {
		return nil, fmt.Errorf(
			"%w: provisioned model throughput %s not found",
			ErrNotFound,
			idOrARN,
		)
	}

	if desiredModelID != "" {
		pmt.DesiredModelArn = foundationModelARN(b.region, desiredModelID)
	}

	if newName != "" && newName != pmt.ProvisionedModelName {
		if _, exists := b.pmtsByName[newName]; exists {
			return nil, fmt.Errorf(
				"%w: provisioned model throughput %s already exists",
				ErrAlreadyExists,
				newName,
			)
		}

		delete(b.pmtsByName, pmt.ProvisionedModelName)
		b.pmtsByName[newName] = pmt.ProvisionedModelArn
		pmt.ProvisionedModelName = newName
	}

	pmt.LastModifiedTime = time.Now().UTC()
	cp := *pmt

	return &cp, nil
}

// DeleteProvisionedModelThroughput removes a provisioned model throughput by ID or ARN.
func (b *InMemoryBackend) DeleteProvisionedModelThroughput(idOrARN string) error {
	b.mu.Lock("DeleteProvisionedModelThroughput")
	defer b.mu.Unlock()

	pmt, ok := b.findPMTByIDOrARN(idOrARN)
	if !ok {
		return fmt.Errorf("%w: provisioned model throughput %s not found", ErrNotFound, idOrARN)
	}

	b.provisionedModelThroughputs.Delete(pmt.ProvisionedModelArn)
	delete(b.pmtsByName, pmt.ProvisionedModelName)

	return nil
}

// AdvanceProvisionedModelThroughputStatuses transitions PMTs from Creating → InService.
// Called by the janitor after the creation delay has elapsed.
func (b *InMemoryBackend) AdvanceProvisionedModelThroughputStatuses() int {
	b.mu.Lock("AdvanceProvisionedModelThroughputStatuses")
	defer b.mu.Unlock()

	now := time.Now().UTC()
	advanced := 0

	for _, pmt := range b.provisionedModelThroughputs.All() {
		if pmt.Status != statusCreating {
			continue
		}
		pmt.Status = statusInService
		pmt.ModelArn = pmt.DesiredModelArn
		pmt.ModelUnits = pmt.DesiredModelUnits
		pmt.LastModifiedTime = now
		advanced++
	}

	return advanced
}

// findPMTByIDOrARN finds a provisioned model throughput by ID or ARN.
// Caller must hold at least a read lock.
func (b *InMemoryBackend) findPMTByIDOrARN(idOrARN string) (*ProvisionedModelThroughput, bool) {
	if pmt, ok := b.provisionedModelThroughputs.Get(idOrARN); ok {
		return pmt, true
	}

	if pmtARN, ok := b.pmtsByName[idOrARN]; ok {
		return b.provisionedModelThroughputs.Get(pmtARN)
	}

	return nil, false
}
