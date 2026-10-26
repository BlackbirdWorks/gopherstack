package support

import "time"

const (
	defaultUploadPartSize = 5 * 1024 * 1024
	maxUploadPartSize     = 100 * 1024 * 1024
	maxUploadFileSize     = 150 * 1024 * 1024
	maxUploadLinksPerCall = 10
	maxPendingUploads     = 1000
	uploadLinkLifetime    = time.Hour
	pendingUploadLifetime = 24 * time.Hour
	maxUploadIDLength     = 2048

	linkKindUpload   = "upload"
	linkKindDownload = "download"

	uploadStatusReady    = "attachment-ready"
	uploadStatusNotReady = "attachment-not-ready"
)

// UploadPart is one received part of a multipart attachment upload.
type UploadPart struct {
	ETag     string `json:"eTag"`
	Data     []byte `json:"data,omitempty"`
	Reported bool   `json:"reported"`
}

// AttachmentUpload is a multipart attachment upload started by GetAttachmentUploadLinks.
type AttachmentUpload struct {
	Created       time.Time          `json:"created"`
	CompletedAt   time.Time          `json:"completedAt,omitzero"`
	Parts         map[int]UploadPart `json:"parts"`
	UploadID      string             `json:"uploadId"`
	FileName      string             `json:"fileName"`
	AttachmentID  string             `json:"attachmentId,omitempty"`
	FileSizeBytes int64              `json:"fileSizeBytes"`
	PartSizeBytes int64              `json:"partSizeBytes"`
	TotalParts    int                `json:"totalParts"`
}

// AttachmentLink is an unguessable, expiring token behind an upload-part or download URL.
type AttachmentLink struct {
	Expiry       time.Time `json:"expiry"`
	Token        string    `json:"token"`
	Kind         string    `json:"kind"`
	UploadID     string    `json:"uploadId,omitempty"`
	AttachmentID string    `json:"attachmentId,omitempty"`
	PartIndex    int       `json:"partIndex,omitempty"`
}

func attachmentUploadKeyFn(v *AttachmentUpload) string { return v.UploadID }

func attachmentLinkKeyFn(v *AttachmentLink) string { return v.Token }

// SetClock replaces the backend clock used for link expiry and upload processing.
func (b *InMemoryBackend) SetClock(now func() time.Time) {
	b.mu.Lock("SetClock")
	defer b.mu.Unlock()

	b.clock = now
}

// SetUploadPartSize sets the part size handed out for new multipart uploads.
func (b *InMemoryBackend) SetUploadPartSize(size int64) {
	b.mu.Lock("SetUploadPartSize")
	defer b.mu.Unlock()

	b.uploadPartSize = size
}

// SetUploadProcessingDelay sets how long a fully reported upload stays attachment-not-ready.
func (b *InMemoryBackend) SetUploadProcessingDelay(d time.Duration) {
	b.mu.Lock("SetUploadProcessingDelay")
	defer b.mu.Unlock()

	b.uploadProcessingDelay = d
}
