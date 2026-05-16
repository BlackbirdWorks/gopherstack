package glacier

import "time"

// Vault stores all metadata and state for a single Glacier vault.
type Vault struct {
	Tags                 map[string]string `json:"tags,omitempty"`
	AccessPolicy         string            `json:"accessPolicy,omitempty"`
	NotificationSNSTopic string            `json:"notificationSNSTopic,omitempty"`
	VaultARN             string            `json:"vaultARN"`
	VaultName            string            `json:"vaultName"`
	CreationDate         string            `json:"creationDate"`
	LastInventoryDate    string            `json:"lastInventoryDate,omitempty"`
	NotificationEvents   []string          `json:"notificationEvents,omitempty"`
	NumberOfArchives     int64             `json:"numberOfArchives"`
	SizeInBytes          int64             `json:"sizeInBytes"`
}

// Archive stores metadata for a single archive uploaded to a vault.
type Archive struct {
	ArchiveID      string `json:"archiveID"`
	Description    string `json:"description,omitempty"`
	CreationDate   string `json:"creationDate"`
	SHA256TreeHash string `json:"sha256TreeHash,omitempty"`
	Size           int64  `json:"size"`
}

// Job stores state for a single Glacier retrieval or inventory job.
type Job struct {
	VaultARN             string `json:"vaultARN"`
	VaultName            string `json:"vaultName"`
	JobID                string `json:"jobID"`
	JobDescription       string `json:"jobDescription,omitempty"`
	Action               string `json:"action"`
	ArchiveID            string `json:"archiveID,omitempty"`
	InventoryFormat      string `json:"inventoryFormat,omitempty"`
	StatusCode           string `json:"statusCode"`
	StatusMessage        string `json:"statusMessage,omitempty"`
	CreationDate         string `json:"creationDate"`
	CompletionDate       string `json:"completionDate,omitempty"`
	Tier                 string `json:"tier,omitempty"`
	SHA256TreeHash       string `json:"sha256TreeHash,omitempty"`
	SNSTopic             string `json:"snsTopic,omitempty"`
	RetrievalByteRange   string `json:"retrievalByteRange,omitempty"`
	ArchiveSizeInBytes   int64  `json:"archiveSizeInBytes,omitempty"`
	InventorySizeInBytes int64  `json:"inventorySizeInBytes,omitempty"`
	Completed            bool   `json:"completed"`
}

// vaultLockPolicyRequest is the request body for InitiateVaultLock.
type vaultLockPolicyRequest struct {
	Policy string `json:"Policy"`
}

// vaultNotificationConfig holds the SNS configuration for a vault.
type vaultNotificationConfig struct {
	SNSTopic string   `json:"SNSTopic"`
	Events   []string `json:"Events"`
}

// vaultAccessPolicy wraps the vault access policy document.
type vaultAccessPolicy struct {
	Policy string `json:"Policy"`
}

// createVaultResponse is the response body for CreateVault.
type createVaultResponse struct {
	Location string `json:"Location"`
}

// describeVaultResponse is the response body for DescribeVault / ListVaults item.
type describeVaultResponse struct {
	VaultARN          string `json:"VaultARN"`
	VaultName         string `json:"VaultName"`
	CreationDate      string `json:"CreationDate"`
	LastInventoryDate string `json:"LastInventoryDate,omitempty"`
	NumberOfArchives  int64  `json:"NumberOfArchives"`
	SizeInBytes       int64  `json:"SizeInBytes"`
}

// listVaultsResponse is the response body for ListVaults.
type listVaultsResponse struct {
	Marker    *string                 `json:"Marker,omitempty"`
	VaultList []describeVaultResponse `json:"VaultList"`
}

// uploadArchiveResponse is the response header/body for UploadArchive.
type uploadArchiveResponse struct {
	ArchiveID string `json:"archiveId"`
	Checksum  string `json:"checksum"`
	Location  string `json:"location"`
}

// initiateJobRequest is the request body for InitiateJob.
type initiateJobRequest struct {
	Type               string `json:"Type"`
	ArchiveID          string `json:"ArchiveId,omitempty"`
	Description        string `json:"Description,omitempty"`
	Tier               string `json:"Tier,omitempty"`
	SNSTopic           string `json:"SNSTopic,omitempty"`
	InventoryFormat    string `json:"Format,omitempty"`
	RetrievalByteRange string `json:"RetrievalByteRange,omitempty"`
}

// initiateJobResponse is the response for InitiateJob.
type initiateJobResponse struct {
	JobID    string `json:"jobId"`
	Location string `json:"location"`
}

// describeJobResponse is the response body for DescribeJob.
type describeJobResponse struct {
	ArchiveSizeInBytes   *int64 `json:"ArchiveSizeInBytes,omitempty"`
	InventorySizeInBytes *int64 `json:"InventorySizeInBytes,omitempty"`
	CompletionDate       string `json:"CompletionDate,omitempty"`
	ArchiveID            string `json:"ArchiveId,omitempty"`
	VaultARN             string `json:"VaultARN"`
	CreationDate         string `json:"CreationDate"`
	StatusCode           string `json:"StatusCode"`
	StatusMessage        string `json:"StatusMessage,omitempty"`
	JobID                string `json:"JobId"`
	Action               string `json:"Action"`
	JobDescription       string `json:"JobDescription,omitempty"`
	InventoryFormat      string `json:"Format,omitempty"`
	Tier                 string `json:"Tier,omitempty"`
	SHA256TreeHash       string `json:"SHA256TreeHash,omitempty"`
	Completed            bool   `json:"Completed"`
}

// listJobsResponse is the response body for ListJobs.
type listJobsResponse struct {
	Marker  *string               `json:"Marker,omitempty"`
	JobList []describeJobResponse `json:"JobList,omitempty"`
}

// addTagsRequest is the request body for AddTagsToVault.
type addTagsRequest struct {
	Tags map[string]string `json:"Tags"`
}

// removeTagsRequest is the request body for RemoveTagsFromVault.
type removeTagsRequest struct {
	TagKeys []string `json:"TagKeys"`
}

// listTagsResponse is the response body for ListTagsForVault.
type listTagsResponse struct {
	Tags map[string]string `json:"Tags"`
}

// errorResponse is the standard Glacier error response.
// __type is included because many AWS SDK versions key on it rather than "code".
type errorResponse struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Type      string `json:"type"`
	TypeAlias string `json:"__type"`
}

// MultipartUpload holds metadata for an in-progress multipart upload.
type MultipartUpload struct {
	MultipartUploadID  string `json:"MultipartUploadId"`
	VaultARN           string `json:"VaultARN"`
	ArchiveDescription string `json:"ArchiveDescription,omitempty"`
	CreationDate       string `json:"CreationDate"`
	PartSizeInBytes    int64  `json:"PartSizeInBytes"`
}

// MultipartPart holds metadata for a single uploaded part.
type MultipartPart struct {
	RangeInBytes   string `json:"RangeInBytes"`
	SHA256TreeHash string `json:"SHA256TreeHash,omitempty"`
}

// VaultLock holds the state of a vault lock policy.
type VaultLock struct {
	Policy         string `json:"Policy"`
	LockID         string `json:"LockId,omitempty"`
	State          string `json:"State"`
	CreationDate   string `json:"CreationDate,omitempty"`
	ExpirationDate string `json:"ExpirationDate,omitempty"`
}

// ProvisionedCapacity holds a single provisioned capacity unit.
type ProvisionedCapacity struct {
	CapacityID     string `json:"CapacityId"`
	StartDate      string `json:"StartDate"`
	ExpirationDate string `json:"ExpirationDate"`
}

// initiateMultipartUploadResponse is the response for InitiateMultipartUpload.
type initiateMultipartUploadResponse struct {
	Location          string `json:"location"`
	MultipartUploadID string `json:"uploadId"`
}

// completeMultipartUploadResponse is the response for CompleteMultipartUpload.
type completeMultipartUploadResponse struct {
	ArchiveID string `json:"archiveId"`
	Checksum  string `json:"checksum"`
	Location  string `json:"location"`
}

// listMultipartUploadsResponse is the response for ListMultipartUploads.
type listMultipartUploadsResponse struct {
	Marker      *string           `json:"Marker,omitempty"`
	UploadsList []MultipartUpload `json:"UploadsList"`
}

// ListPartsOutput is the response for ListParts.
type ListPartsOutput struct {
	Marker             *string         `json:"Marker,omitempty"`
	MultipartUploadID  string          `json:"MultipartUploadId"`
	VaultARN           string          `json:"VaultARN"`
	ArchiveDescription string          `json:"ArchiveDescription,omitempty"`
	CreationDate       string          `json:"CreationDate"`
	Parts              []MultipartPart `json:"Parts"`
	PartSizeInBytes    int64           `json:"PartSizeInBytes"`
}

// getVaultLockResponse is the response for GetVaultLock.
type getVaultLockResponse struct {
	CreationDate   string `json:"CreationDate,omitempty"`
	ExpirationDate string `json:"ExpirationDate,omitempty"`
	Policy         string `json:"Policy,omitempty"`
	State          string `json:"State"`
}

// listProvisionedCapacityResponse is the response for ListProvisionedCapacity.
type listProvisionedCapacityResponse struct {
	ProvisionedCapacityList []ProvisionedCapacity `json:"ProvisionedCapacityList"`
}

// purchaseProvisionedCapacityResponse is the response for PurchaseProvisionedCapacity.
type purchaseProvisionedCapacityResponse struct {
	CapacityID string `json:"capacityId"`
}

// formatDate formats a [time.Time] as an ISO 8601 timestamp.
func formatDate(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.000Z")
}
