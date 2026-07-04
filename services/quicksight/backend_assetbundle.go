package quicksight

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

const (
	assetBundleJobStatusQueued     = "QUEUED_FOR_IMMEDIATE_EXECUTION"
	assetBundleJobStatusSuccessful = "SUCCESSFUL"

	defaultAssetBundleExportFormat = "QUICKSIGHT_JSON"
)

// storedAssetBundleExportJob is the persisted representation of one asset-bundle
// export job, keyed by JobId.
type storedAssetBundleExportJob struct {
	CreatedTime            time.Time `json:"createdTime"`
	JobID                  string    `json:"jobId"`
	Arn                    string    `json:"arn"`
	Status                 string    `json:"status"`
	ExportFormat           string    `json:"exportFormat"`
	DownloadURL            string    `json:"downloadUrl,omitempty"`
	ResourceArns           []string  `json:"resourceArns"`
	IncludeAllDependencies bool      `json:"includeAllDependencies"`
}

func (j *storedAssetBundleExportJob) toAssetBundleExportJob() *AssetBundleExportJob {
	arns := make([]string, len(j.ResourceArns))
	copy(arns, j.ResourceArns)

	return &AssetBundleExportJob{
		CreatedTime:            j.CreatedTime,
		ResourceArns:           arns,
		JobID:                  j.JobID,
		Arn:                    j.Arn,
		Status:                 j.Status,
		ExportFormat:           j.ExportFormat,
		DownloadURL:            j.DownloadURL,
		IncludeAllDependencies: j.IncludeAllDependencies,
	}
}

// storedAssetBundleImportJob is the persisted representation of one asset-bundle
// import job, keyed by JobId.
type storedAssetBundleImportJob struct {
	CreatedTime   time.Time `json:"createdTime"`
	JobID         string    `json:"jobId"`
	Arn           string    `json:"arn"`
	Status        string    `json:"status"`
	FailureAction string    `json:"failureAction"`
}

func (j *storedAssetBundleImportJob) toAssetBundleImportJob() *AssetBundleImportJob {
	return &AssetBundleImportJob{
		CreatedTime:   j.CreatedTime,
		JobID:         j.JobID,
		Arn:           j.Arn,
		Status:        j.Status,
		FailureAction: j.FailureAction,
	}
}

// ---- Asset bundle export jobs ----

func (b *InMemoryBackend) StartAssetBundleExportJob(
	accountID, jobID, exportFormat string,
	resourceArns []string,
	includeAllDependencies bool,
) (*AssetBundleExportJob, error) {
	if jobID == "" || len(resourceArns) == 0 {
		return nil, ErrValidation
	}

	b.mu.Lock("StartAssetBundleExportJob")
	defer b.mu.Unlock()

	if exportFormat == "" {
		exportFormat = defaultAssetBundleExportFormat
	}

	arns := make([]string, len(resourceArns))
	copy(arns, resourceArns)

	job := &storedAssetBundleExportJob{
		CreatedTime:            time.Now().UTC(),
		ResourceArns:           arns,
		JobID:                  jobID,
		Arn:                    b.buildARN("asset-bundle-export-job", jobID),
		Status:                 assetBundleJobStatusQueued,
		ExportFormat:           exportFormat,
		IncludeAllDependencies: includeAllDependencies,
	}
	b.assetBundleExportJobs[assetBundleJobKey(accountID, jobID)] = job

	return job.toAssetBundleExportJob(), nil
}

func (b *InMemoryBackend) DescribeAssetBundleExportJob(accountID, jobID string) (*AssetBundleExportJob, error) {
	b.mu.Lock("DescribeAssetBundleExportJob")
	defer b.mu.Unlock()

	job, ok := b.assetBundleExportJobs[assetBundleJobKey(accountID, jobID)]
	if !ok {
		return nil, ErrAssetBundleExportJobNotFound
	}

	// Settle the job deterministically on first poll, mirroring how a real export
	// job finishes shortly after being queued.
	if job.Status == assetBundleJobStatusQueued {
		job.Status = assetBundleJobStatusSuccessful
		job.DownloadURL = fmt.Sprintf(
			"https://quicksight-asset-bundles.s3.%s.amazonaws.com/%s/assetbundle-%s.qs",
			b.region, accountID, jobID,
		)
	}

	return job.toAssetBundleExportJob(), nil
}

//nolint:dupl // list functions share structure but operate on different stored types
func (b *InMemoryBackend) ListAssetBundleExportJobs(
	accountID string,
	maxResults int32,
	nextToken string,
) ([]*AssetBundleExportJob, string, error) {
	b.mu.RLock("ListAssetBundleExportJobs")
	defer b.mu.RUnlock()

	prefix := accountID + "/"
	all := make([]*storedAssetBundleExportJob, 0, len(b.assetBundleExportJobs))
	for k, job := range b.assetBundleExportJobs {
		if strings.HasPrefix(k, prefix) {
			all = append(all, job)
		}
	}
	sort.Slice(all, func(i, j int) bool { return all[i].JobID < all[j].JobID })

	if maxResults <= 0 || maxResults > defaultMaxResults {
		maxResults = defaultMaxResults
	}

	start := 0
	if nextToken != "" {
		for i, job := range all {
			if job.JobID == nextToken {
				start = i

				break
			}
		}
	}

	end := start + int(maxResults)
	var next string
	if end < len(all) {
		next = all[end].JobID
	} else {
		end = len(all)
	}

	result := make([]*AssetBundleExportJob, 0, end-start)
	for _, job := range all[start:end] {
		result = append(result, job.toAssetBundleExportJob())
	}

	return result, next, nil
}

// ---- Asset bundle import jobs ----

func (b *InMemoryBackend) StartAssetBundleImportJob(
	accountID, jobID, failureAction string,
) (*AssetBundleImportJob, error) {
	if jobID == "" {
		return nil, ErrValidation
	}

	b.mu.Lock("StartAssetBundleImportJob")
	defer b.mu.Unlock()

	if failureAction == "" {
		failureAction = "ROLLBACK"
	}

	job := &storedAssetBundleImportJob{
		CreatedTime:   time.Now().UTC(),
		JobID:         jobID,
		Arn:           b.buildARN("asset-bundle-import-job", jobID),
		Status:        assetBundleJobStatusQueued,
		FailureAction: failureAction,
	}
	b.assetBundleImportJobs[assetBundleJobKey(accountID, jobID)] = job

	return job.toAssetBundleImportJob(), nil
}

func (b *InMemoryBackend) DescribeAssetBundleImportJob(accountID, jobID string) (*AssetBundleImportJob, error) {
	b.mu.Lock("DescribeAssetBundleImportJob")
	defer b.mu.Unlock()

	job, ok := b.assetBundleImportJobs[assetBundleJobKey(accountID, jobID)]
	if !ok {
		return nil, ErrAssetBundleImportJobNotFound
	}

	if job.Status == assetBundleJobStatusQueued {
		job.Status = assetBundleJobStatusSuccessful
	}

	return job.toAssetBundleImportJob(), nil
}

//nolint:dupl // list functions share structure but operate on different stored types
func (b *InMemoryBackend) ListAssetBundleImportJobs(
	accountID string,
	maxResults int32,
	nextToken string,
) ([]*AssetBundleImportJob, string, error) {
	b.mu.RLock("ListAssetBundleImportJobs")
	defer b.mu.RUnlock()

	prefix := accountID + "/"
	all := make([]*storedAssetBundleImportJob, 0, len(b.assetBundleImportJobs))
	for k, job := range b.assetBundleImportJobs {
		if strings.HasPrefix(k, prefix) {
			all = append(all, job)
		}
	}
	sort.Slice(all, func(i, j int) bool { return all[i].JobID < all[j].JobID })

	if maxResults <= 0 || maxResults > defaultMaxResults {
		maxResults = defaultMaxResults
	}

	start := 0
	if nextToken != "" {
		for i, job := range all {
			if job.JobID == nextToken {
				start = i

				break
			}
		}
	}

	end := start + int(maxResults)
	var next string
	if end < len(all) {
		next = all[end].JobID
	} else {
		end = len(all)
	}

	result := make([]*AssetBundleImportJob, 0, end-start)
	for _, job := range all[start:end] {
		result = append(result, job.toAssetBundleImportJob())
	}

	return result, next, nil
}
