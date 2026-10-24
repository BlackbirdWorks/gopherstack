package support

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// ErrLinkDenied is returned for an unknown, expired, wrong-kind or finalized upload/download link.
var ErrLinkDenied = errors.New("link is not valid")

// UploadRangeInput is the half-open [Start, End) part range of GetAttachmentUploadLinks.
type UploadRangeInput struct {
	End   *int
	Start int
}

// UploadLinksInput is the request for GetAttachmentUploadLinks.
type UploadLinksInput struct {
	FileSizeBytes *int64
	Range         *UploadRangeInput
	FileName      string
	UploadID      string
	DryRun        bool
}

// IssuedLink is one token handed out for a part or an attachment.
type IssuedLink struct {
	Expiry    time.Time
	Token     string
	PartIndex int
}

// UploadLinksResult is the response of GetAttachmentUploadLinks.
type UploadLinksResult struct {
	UploadID      string
	Links         []IssuedLink
	PartSizeBytes int64
	TotalParts    int
	NextIndex     int
}

// CompletedPart reports one uploaded part to CompleteAttachmentUpload.
type CompletedPart struct {
	ETag      string
	PartIndex int
}

// CompleteUploadInput is the request for CompleteAttachmentUpload.
type CompleteUploadInput struct {
	UploadID string
	Parts    []CompletedPart
	DryRun   bool
}

// UploadStatusResult is the response of DescribeAttachmentUploadStatus.
type UploadStatusResult struct {
	FileName       string
	Status         string
	CompletedParts int
	TotalParts     int
}

// DownloadLinkResult is the response of GetAttachmentDownloadLink.
type DownloadLinkResult struct {
	Expiry   time.Time
	FileName string
	Token    string
}

func newLinkToken() string {
	return strings.ReplaceAll(uuid.NewString()+uuid.NewString(), "-", "")
}

func partETag(data []byte) string {
	sum := sha256.Sum256(data)

	return `"` + hex.EncodeToString(sum[:16]) + `"`
}

func normalizeETag(etag string) string { return strings.Trim(etag, `"`) }

func (u *AttachmentUpload) expectedPartSize(partIndex int) int64 {
	if partIndex < u.TotalParts {
		return u.PartSizeBytes
	}

	return u.FileSizeBytes - int64(u.TotalParts-1)*u.PartSizeBytes
}

func (b *InMemoryBackend) uploadStatusLocked(u *AttachmentUpload) string {
	if u.AttachmentID == "" {
		return uploadStatusNotReady
	}

	if b.uploadProcessingDelay > 0 && b.clock().Before(u.CompletedAt.Add(b.uploadProcessingDelay)) {
		return uploadStatusNotReady
	}

	return uploadStatusReady
}

func (b *InMemoryBackend) uploadLocked(id string) (*AttachmentUpload, error) {
	if id == "" || len(id) > maxUploadIDLength {
		return nil, fmt.Errorf("%w: uploadId must be 1 to %d characters", ErrValidation, maxUploadIDLength)
	}

	u, ok := b.attachmentUploads.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: upload %s not found", ErrUploadIDNotFound, id)
	}

	return u, nil
}

func (b *InMemoryBackend) issueLinkLocked(link AttachmentLink) IssuedLink {
	link.Token = newLinkToken()
	link.Expiry = b.clock().Add(uploadLinkLifetime)
	b.attachmentLinks.Put(&link)

	return IssuedLink{Token: link.Token, Expiry: link.Expiry, PartIndex: link.PartIndex}
}

func (b *InMemoryBackend) newUploadLocked(in UploadLinksInput) (*AttachmentUpload, error) {
	switch {
	case in.FileName == "":
		return nil, fmt.Errorf("%w: fileName is required", ErrValidation)
	case in.FileSizeBytes == nil:
		return nil, fmt.Errorf("%w: fileSizeBytes is required when uploadId is not provided", ErrValidation)
	case *in.FileSizeBytes < 1 || *in.FileSizeBytes > maxUploadFileSize:
		return nil, fmt.Errorf("%w: fileSizeBytes must be between 1 and %d", ErrValidation, maxUploadFileSize)
	}

	if in.DryRun {
		return nil, ErrDryRun
	}

	if b.attachmentUploads.Len() >= maxPendingUploads {
		return nil, fmt.Errorf("%w: too many pending attachment uploads", ErrAttachmentLimitExceeded)
	}

	partSize := min(max(b.uploadPartSize, 1), maxUploadPartSize)
	size := *in.FileSizeBytes
	u := &AttachmentUpload{
		UploadID:      uuid.NewString(),
		FileName:      in.FileName,
		FileSizeBytes: size,
		PartSizeBytes: partSize,
		TotalParts:    int((size + partSize - 1) / partSize),
		Created:       b.clock(),
		Parts:         map[int]UploadPart{},
	}
	b.attachmentUploads.Put(u)

	return u, nil
}

func uploadRange(r *UploadRangeInput, total int) (int, int, error) {
	start := 1
	if r != nil {
		start = r.Start
	}

	if start < 1 || start > total {
		return 0, 0, fmt.Errorf("%w: uploadRange.startIndex must be between 1 and %d", ErrValidation, total)
	}

	end := start + maxUploadLinksPerCall
	if r != nil && r.End != nil {
		end = *r.End
		if end <= start || end-start > maxUploadLinksPerCall {
			return 0, 0, fmt.Errorf(
				"%w: uploadRange must span 1 to %d parts", ErrValidation, maxUploadLinksPerCall)
		}
	}

	return start, min(end, total+1), nil
}

// GetAttachmentUploadLinks starts (or continues) a multipart upload and issues part URLs.
func (b *InMemoryBackend) GetAttachmentUploadLinks(in UploadLinksInput) (*UploadLinksResult, error) {
	b.mu.Lock("GetAttachmentUploadLinks")
	defer b.mu.Unlock()

	var u *AttachmentUpload

	var err error

	if in.UploadID != "" {
		if u, err = b.uploadLocked(in.UploadID); err != nil {
			return nil, err
		}
	} else if u, err = b.newUploadLocked(in); err != nil {
		return nil, err
	}

	start, end, err := uploadRange(in.Range, u.TotalParts)
	if err != nil {
		return nil, err
	}

	if in.DryRun {
		return nil, ErrDryRun
	}

	res := &UploadLinksResult{
		UploadID: u.UploadID, PartSizeBytes: u.PartSizeBytes, TotalParts: u.TotalParts,
		Links: make([]IssuedLink, 0, end-start),
	}

	for i := start; i < end; i++ {
		res.Links = append(res.Links, b.issueLinkLocked(AttachmentLink{
			Kind: linkKindUpload, UploadID: u.UploadID, PartIndex: i,
		}))
	}

	if end <= u.TotalParts {
		res.NextIndex = end
	}

	return res, nil
}

func (u *AttachmentUpload) validateReportedParts(parts []CompletedPart) error {
	if len(parts) == 0 {
		return fmt.Errorf("%w: completedUploads is required", ErrValidation)
	}

	for _, p := range parts {
		if p.PartIndex < 1 || p.PartIndex > u.TotalParts {
			return fmt.Errorf("%w: partIndex %d is out of range 1 to %d", ErrValidation, p.PartIndex, u.TotalParts)
		}

		got, ok := u.Parts[p.PartIndex]
		if !ok {
			return fmt.Errorf("%w: part %d has not been uploaded", ErrValidation, p.PartIndex)
		}

		if p.ETag == "" || normalizeETag(p.ETag) != normalizeETag(got.ETag) {
			return fmt.Errorf("%w: eTag for part %d does not match the uploaded part", ErrValidation, p.PartIndex)
		}
	}

	return nil
}

func (u *AttachmentUpload) allReported() bool {
	for i := 1; i <= u.TotalParts; i++ {
		if !u.Parts[i].Reported {
			return false
		}
	}

	return true
}

func (b *InMemoryBackend) assembleLocked(u *AttachmentUpload) {
	data := make([]byte, 0, u.FileSizeBytes)

	for i := 1; i <= u.TotalParts; i++ {
		part := u.Parts[i]
		data = append(data, part.Data...)
		part.Data = nil
		u.Parts[i] = part
	}

	u.AttachmentID = "att-" + uuid.NewString()
	u.CompletedAt = b.clock()
	b.attachments.Put(&Attachment{AttachmentID: u.AttachmentID, FileName: u.FileName, Data: data})
}

// CompleteAttachmentUpload records uploaded parts and finalizes the upload once all are reported.
func (b *InMemoryBackend) CompleteAttachmentUpload(in CompleteUploadInput) (string, error) {
	b.mu.Lock("CompleteAttachmentUpload")
	defer b.mu.Unlock()

	u, err := b.uploadLocked(in.UploadID)
	if err != nil {
		return "", err
	}

	if err = u.validateReportedParts(in.Parts); err != nil {
		return "", err
	}

	if in.DryRun {
		return "", ErrDryRun
	}

	for _, p := range in.Parts {
		part := u.Parts[p.PartIndex]
		part.Reported = true
		u.Parts[p.PartIndex] = part
	}

	if u.AttachmentID == "" && u.allReported() {
		b.assembleLocked(u)
	}

	return b.uploadStatusLocked(u), nil
}

// DescribeAttachmentUploadStatus reports a multipart upload's status and progress.
func (b *InMemoryBackend) DescribeAttachmentUploadStatus(uploadID string, dryRun bool) (*UploadStatusResult, error) {
	b.mu.RLock("DescribeAttachmentUploadStatus")
	defer b.mu.RUnlock()

	u, err := b.uploadLocked(uploadID)
	if err != nil {
		return nil, err
	}

	if dryRun {
		return nil, ErrDryRun
	}

	reported := 0

	for _, p := range u.Parts {
		if p.Reported {
			reported++
		}
	}

	return &UploadStatusResult{
		FileName: u.FileName, Status: b.uploadStatusLocked(u), CompletedParts: reported, TotalParts: u.TotalParts,
	}, nil
}

// GetAttachmentDownloadLink issues a time-limited download token for an attachment.
func (b *InMemoryBackend) GetAttachmentDownloadLink(attachmentID string, dryRun bool) (*DownloadLinkResult, error) {
	b.mu.Lock("GetAttachmentDownloadLink")
	defer b.mu.Unlock()

	if attachmentID == "" {
		return nil, fmt.Errorf("%w: attachmentId is required", ErrValidation)
	}

	a, ok := b.attachments.Get(attachmentID)
	if !ok {
		return nil, fmt.Errorf("%w: attachment %s not found", ErrAttachmentNotFound, attachmentID)
	}

	if dryRun {
		return nil, ErrDryRun
	}

	link := b.issueLinkLocked(AttachmentLink{Kind: linkKindDownload, AttachmentID: attachmentID})

	return &DownloadLinkResult{FileName: a.FileName, Token: link.Token, Expiry: link.Expiry}, nil
}

func (b *InMemoryBackend) liveLinkLocked(token, kind string) (*AttachmentLink, error) {
	l, ok := b.attachmentLinks.Get(token)
	if !ok || l.Kind != kind || !b.clock().Before(l.Expiry) {
		return nil, ErrLinkDenied
	}

	return l, nil
}

func (b *InMemoryBackend) openUploadLocked(token string) (*AttachmentLink, *AttachmentUpload, error) {
	l, err := b.liveLinkLocked(token, linkKindUpload)
	if err != nil {
		return nil, nil, err
	}

	u, ok := b.attachmentUploads.Get(l.UploadID)
	if !ok || u.AttachmentID != "" {
		return nil, nil, ErrLinkDenied
	}

	return l, u, nil
}

// PrepareUploadPart validates an upload token and returns the exact byte length its part must have.
func (b *InMemoryBackend) PrepareUploadPart(token string) (int64, error) {
	b.mu.RLock("PrepareUploadPart")
	defer b.mu.RUnlock()

	l, u, err := b.openUploadLocked(token)
	if err != nil {
		return 0, err
	}

	return u.expectedPartSize(l.PartIndex), nil
}

// PutUploadPart stores the body of one part and returns its ETag.
func (b *InMemoryBackend) PutUploadPart(token string, data []byte) (string, error) {
	b.mu.Lock("PutUploadPart")
	defer b.mu.Unlock()

	l, u, err := b.openUploadLocked(token)
	if err != nil {
		return "", err
	}

	if int64(len(data)) != u.expectedPartSize(l.PartIndex) {
		return "", fmt.Errorf(
			"%w: part %d must be %d bytes",
			ErrValidation,
			l.PartIndex,
			u.expectedPartSize(l.PartIndex),
		)
	}

	etag := partETag(data)
	u.Parts[l.PartIndex] = UploadPart{ETag: etag, Data: append([]byte(nil), data...)}

	return etag, nil
}

// OpenDownload resolves a download token to the attachment's file name and bytes.
func (b *InMemoryBackend) OpenDownload(token string) (string, []byte, error) {
	b.mu.RLock("OpenDownload")
	defer b.mu.RUnlock()

	l, err := b.liveLinkLocked(token, linkKindDownload)
	if err != nil {
		return "", nil, err
	}

	a, ok := b.attachments.Get(l.AttachmentID)
	if !ok {
		return "", nil, ErrLinkDenied
	}

	return a.FileName, append([]byte(nil), a.Data...), nil
}

// resolveUploadsLocked validates that every upload exists and is attachment-ready; it mutates nothing.
func (b *InMemoryBackend) resolveUploadsLocked(ids []string) ([]*AttachmentUpload, error) {
	ups := make([]*AttachmentUpload, 0, len(ids))
	seen := make(map[string]struct{}, len(ids))

	for _, id := range ids {
		if _, dup := seen[id]; dup {
			continue
		}

		seen[id] = struct{}{}

		u, err := b.uploadLocked(id)
		if errors.Is(err, ErrUploadIDNotFound) {
			// UploadIdNotFound is not modeled on CreateCase/AddCommunicationToCase.
			return nil, fmt.Errorf("%w: upload %s not found", ErrAttachmentSetNotFound, id)
		}

		if err != nil {
			return nil, err
		}

		if b.uploadStatusLocked(u) != uploadStatusReady {
			return nil, fmt.Errorf("%w: upload %s is not attachment-ready", ErrValidation, id)
		}

		ups = append(ups, u)
	}

	return ups, nil
}

// consumeUploadsLocked attaches resolved uploads (single use) and returns every ref plus the <=5 MB subset.
func (b *InMemoryBackend) consumeUploadsLocked(ups []*AttachmentUpload) ([]AttachmentRef, []AttachmentRef) {
	all := make([]AttachmentRef, 0, len(ups))
	var small []AttachmentRef

	for _, u := range ups {
		ref := AttachmentRef{AttachmentID: u.AttachmentID, FileName: u.FileName}
		all = append(all, ref)

		if u.FileSizeBytes <= maxAttachmentSize {
			small = append(small, ref)
		}

		b.deleteLinksLocked(func(l *AttachmentLink) bool { return l.UploadID == u.UploadID })
		b.attachmentUploads.Delete(u.UploadID)
	}

	return all, small
}

func (b *InMemoryBackend) deleteLinksLocked(match func(*AttachmentLink) bool) {
	var tokens []string

	b.attachmentLinks.Range(func(l *AttachmentLink) bool {
		if match(l) {
			tokens = append(tokens, l.Token)
		}

		return true
	})

	for _, t := range tokens {
		b.attachmentLinks.Delete(t)
	}
}

// sweepUploadsLocked drops expired links and abandoned uploads (with their unattached attachment).
func (b *InMemoryBackend) sweepUploadsLocked(now time.Time) int {
	removed := 0

	var stale []*AttachmentUpload

	b.attachmentUploads.Range(func(u *AttachmentUpload) bool {
		if now.Sub(u.Created) > pendingUploadLifetime {
			stale = append(stale, u)
		}

		return true
	})

	for _, u := range stale {
		if u.AttachmentID != "" {
			b.attachments.Delete(u.AttachmentID)
		}

		b.deleteLinksLocked(func(l *AttachmentLink) bool { return l.UploadID == u.UploadID })
		b.attachmentUploads.Delete(u.UploadID)

		removed++
	}

	b.deleteLinksLocked(func(l *AttachmentLink) bool {
		if now.Before(l.Expiry) {
			return false
		}

		removed++

		return true
	})

	return removed
}
