package glacier

import (
	"fmt"
	"sort"
	"time"
)

const (
	// jobStatusInProgress and jobStatusSucceeded are the retrieval-job status codes.
	jobStatusInProgress = "InProgress"
	jobStatusSucceeded  = "Succeeded"

	// jobIDLength is the length of the random job ID.
	jobIDLength = 60
	// jobTypeArchiveRetrieval is the Glacier job action name for archive retrieval (response).
	jobTypeArchiveRetrieval = "ArchiveRetrieval"
	// jobTypeInventoryRetrieval is the Glacier job action name for inventory retrieval (response).
	jobTypeInventoryRetrieval = "InventoryRetrieval"
	// jobInputArchiveRetrieval is the type value sent by SDK/clients for archive retrieval (request).
	jobInputArchiveRetrieval = "archive-retrieval"
	// jobInputInventoryRetrieval is the type value sent by SDK/clients for inventory retrieval (request).
	jobInputInventoryRetrieval = "inventory-retrieval"
)

// cloneJob returns a shallow copy of a Job.
func cloneJob(j *Job) *Job {
	cp := *j

	return &cp
}

// normalizeJobType converts the SDK request type (kebab-case) to the canonical
// action name (PascalCase) used in job responses. Returns empty string if unknown.
func normalizeJobType(t string) string {
	switch t {
	case jobTypeArchiveRetrieval, jobInputArchiveRetrieval:
		return jobTypeArchiveRetrieval
	case jobTypeInventoryRetrieval, jobInputInventoryRetrieval:
		return jobTypeInventoryRetrieval
	default:
		return ""
	}
}

// isValidTier reports whether tier is one of the allowed retrieval tier values.
func isValidTier(tier string) bool {
	return tier == "Bulk" || tier == "Standard" || tier == "Expedited"
}

// InitiateJob creates a new retrieval or inventory job.
func (b *InMemoryBackend) InitiateJob(accountID, region, vaultName string, req *initiateJobRequest) (*Job, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Normalize the job type: the SDK sends kebab-case ("archive-retrieval",
	// "inventory-retrieval") but the job action is stored in PascalCase
	// ("ArchiveRetrieval", "InventoryRetrieval") per the AWS API spec.
	action := normalizeJobType(req.Type)
	if action == "" {
		return nil, ErrValidation
	}

	v, vaultExists := b.vaults.Get(vaultARN(accountID, region, vaultName))
	if !vaultExists {
		return nil, ErrVaultNotFound
	}

	if action == jobTypeArchiveRetrieval {
		if req.ArchiveID == "" {
			return nil, ErrValidation
		}

		if _, archiveExists := v.Archives[req.ArchiveID]; !archiveExists {
			return nil, ErrArchiveNotFound
		}
	}

	tier := req.Tier
	if tier == "" {
		tier = "Standard"
	}

	if !isValidTier(tier) {
		return nil, fmt.Errorf("%w: tier must be Bulk, Standard, or Expedited", ErrValidation)
	}

	inventoryFormat := req.InventoryFormat
	if inventoryFormat == "" {
		inventoryFormat = "JSON"
	}

	now := time.Now()

	// AWS retrievals are asynchronous: a freshly initiated job starts InProgress and
	// only completes after a retrieval window elapses. We simulate that window with
	// retrievalDelay; reads promote the job to Succeeded once it has passed. A zero
	// delay means jobs complete immediately.
	readyAt := now.Add(b.retrievalDelay)
	ready := !now.Before(readyAt)

	statusCode := jobStatusInProgress
	if ready {
		statusCode = jobStatusSucceeded
	}

	j := &Job{
		JobID:              generateID(jobIDLength),
		VaultARN:           v.VaultARN,
		VaultName:          vaultName,
		Action:             action,
		ArchiveID:          req.ArchiveID,
		InventoryFormat:    inventoryFormat,
		JobDescription:     req.Description,
		StatusCode:         statusCode,
		StatusMessage:      statusCode,
		CreationDate:       formatDate(now),
		Completed:          ready,
		Tier:               tier,
		SNSTopic:           req.SNSTopic,
		RetrievalByteRange: req.RetrievalByteRange,
		readyAt:            readyAt,
	}

	if ready {
		j.CompletionDate = formatDate(now)
	}

	if action == jobTypeArchiveRetrieval {
		if a, archiveFound := v.Archives[req.ArchiveID]; archiveFound {
			j.ArchiveSizeInBytes = a.Size
			j.ArchiveDescription = a.Description
			// ArchiveSHA256TreeHash is archive metadata: available immediately,
			// regardless of job completion (matches AWS). SHA256TreeHash is the
			// retrieved-range hash and must stay unset until the job completes --
			// promoteJobIfReady sets it when the job transitions to Succeeded.
			j.ArchiveSHA256TreeHash = a.SHA256TreeHash
			if ready {
				j.SHA256TreeHash = a.SHA256TreeHash
			}
		}
	}

	if action == jobTypeInventoryRetrieval {
		v.LastInventoryDate = formatDate(now)
	}

	b.jobs.Put(j)

	return j, nil
}

// DescribeJob returns metadata for a job.
func (b *InMemoryBackend) DescribeJob(accountID, region, vaultName, jobID string) (*Job, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	vArn := vaultARN(accountID, region, vaultName)

	if !b.vaults.Has(vArn) {
		return nil, ErrVaultNotFound
	}

	j, ok := b.jobs.Get(jobKey(vArn, jobID))
	if !ok {
		return nil, ErrJobNotFound
	}

	promoteJobIfReady(j)

	return cloneJob(j), nil
}

// promoteJobIfReady transitions an InProgress retrieval job to Succeeded once its
// simulated retrieval window (readyAt) has elapsed. Caller must hold b.mu for writing.
func promoteJobIfReady(j *Job) {
	if j.Completed {
		return
	}

	if j.readyAt.IsZero() || time.Now().Before(j.readyAt) {
		return
	}

	j.Completed = true
	j.StatusCode = jobStatusSucceeded
	j.StatusMessage = jobStatusSucceeded
	j.CompletionDate = formatDate(time.Now())

	// SHA256TreeHash (the retrieved-range hash) only becomes available once the
	// job completes; ArchiveSHA256TreeHash (the whole-archive hash) was already
	// set at InitiateJob time. For whole-archive retrievals they are equal.
	if j.Action == jobTypeArchiveRetrieval {
		j.SHA256TreeHash = j.ArchiveSHA256TreeHash
	}
}

// ListJobs returns all jobs for the given vault.
// Returns ErrVaultNotFound if the vault does not exist.
func (b *InMemoryBackend) ListJobs(accountID, region, vaultName string) ([]*Job, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	vArn := vaultARN(accountID, region, vaultName)

	if !b.vaults.Has(vArn) {
		return nil, ErrVaultNotFound
	}

	jobs := b.jobsByVault.Get(vArn)
	result := make([]*Job, 0, len(jobs))

	for _, j := range jobs {
		cj := cloneJob(j)
		promoteJobIfReady(cj)
		result = append(result, cj)
	}

	sort.Slice(result, func(i, j int) bool { return result[i].JobID < result[j].JobID })

	return result, nil
}

// SetJobInventorySize stores the computed inventory size on the job.
// No-op if the job does not exist.
func (b *InMemoryBackend) SetJobInventorySize(accountID, region, vaultName, jobID string, size int64) {
	b.mu.Lock()
	defer b.mu.Unlock()

	vArn := vaultARN(accountID, region, vaultName)

	if j, ok := b.jobs.Get(jobKey(vArn, jobID)); ok {
		j.InventorySizeInBytes = size
	}
}

// AddJobInternal adds a job directly to the backend for testing. VaultARN is
// always recomputed from the accountID/region/vaultName parameters -- see
// the AddVaultInternal doc comment for why.
func (b *InMemoryBackend) AddJobInternal(accountID, region, vaultName string, j *Job) {
	b.mu.Lock()
	defer b.mu.Unlock()

	cp := *j
	cp.VaultARN = vaultARN(accountID, region, vaultName)
	b.jobs.Put(&cp)
}
