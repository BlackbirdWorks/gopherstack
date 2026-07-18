package support

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

const (
	// maxAttachmentSets caps the number of in-flight staged attachment sets to prevent unbounded growth.
	maxAttachmentSets = 1000

	maxAttachmentsPerSet = 3
	maxAttachmentSize    = 5 * 1024 * 1024
)

// AddAttachmentsToSet creates a new attachment set and returns its ID.
func (b *InMemoryBackend) AddAttachmentsToSet(attachmentSetID string) (string, time.Time, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if attachmentSetID == "" {
		attachmentSetID = uuid.New().String()
	}

	expiry := time.Now().Add(time.Hour)
	b.attachmentSets.Put(&AttachmentSet{ID: attachmentSetID, Expiry: expiry})

	return attachmentSetID, expiry, nil
}

// DescribeAttachment returns the attachment with the given ID.
func (b *InMemoryBackend) DescribeAttachment(attachmentID string) (*Attachment, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	a, ok := b.attachments.Get(attachmentID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrAttachmentNotFound, attachmentID)
	}

	cp := *a

	return &cp, nil
}

// AddAttachmentInternal seeds an attachment directly into the backend (for testing).
func (b *InMemoryBackend) AddAttachmentInternal(a *Attachment) {
	b.mu.Lock()
	defer b.mu.Unlock()

	cp := *a
	if a.Data != nil {
		cp.Data = make([]byte, len(a.Data))
		copy(cp.Data, a.Data)
	}

	b.attachments.Put(&cp)
}

// AddAttachmentsToSetWithAttachments stages uploaded files for single-use consumption.
func (b *InMemoryBackend) AddAttachmentsToSetWithAttachments(
	attachmentSetID string,
	attachments []Attachment,
) (string, time.Time, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(attachments) == 0 {
		return "", time.Time{}, fmt.Errorf("%w: attachments is required", ErrValidation)
	}
	if err := validateAttachments(attachments); err != nil {
		return "", time.Time{}, err
	}
	set, setID, err := b.attachmentSetForAppendLocked(attachmentSetID)
	if err != nil {
		return "", time.Time{}, err
	}
	if len(set.AttachmentIDs)+len(attachments) > maxAttachmentsPerSet {
		return "", time.Time{}, fmt.Errorf(
			"%w: maximum of %d attachments exceeded",
			ErrValidation,
			maxAttachmentsPerSet,
		)
	}
	for _, attachment := range attachments {
		attachment.AttachmentID = "att-" + uuid.NewString()
		cp := attachment
		cp.Data = append([]byte(nil), attachment.Data...)
		b.attachments.Put(&cp)
		set.AttachmentIDs = append(set.AttachmentIDs, cp.AttachmentID)
	}
	set.Expiry = time.Now().Add(time.Hour)

	return setID, set.Expiry, nil
}

func validateAttachments(attachments []Attachment) error {
	for _, attachment := range attachments {
		if attachment.FileName == "" || len(attachment.Data) == 0 || len(attachment.Data) > maxAttachmentSize {
			return fmt.Errorf("%w: invalid attachment", ErrValidation)
		}
	}

	return nil
}

func (b *InMemoryBackend) attachmentSetForAppendLocked(id string) (*AttachmentSet, string, error) {
	if id == "" {
		if b.attachmentSets.Len() >= maxAttachmentSets {
			// Evict the set with the earliest (soonest-expired) expiry.
			var oldestID string
			var oldestExpiry time.Time

			found := false

			b.attachmentSets.Range(func(s *AttachmentSet) bool {
				if !found || s.Expiry.Before(oldestExpiry) {
					oldestID = s.ID
					oldestExpiry = s.Expiry
					found = true
				}

				return true
			})

			b.attachmentSets.Delete(oldestID)
		}

		id = uuid.NewString()
		set := &AttachmentSet{ID: id}
		b.attachmentSets.Put(set)

		return set, id, nil
	}

	set, ok := b.attachmentSets.Get(id)
	if !ok {
		return nil, "", fmt.Errorf("%w: %s", ErrAttachmentSetNotFound, id)
	}

	if time.Now().After(set.Expiry) {
		return nil, "", fmt.Errorf("%w: %s", ErrAttachmentSetExpired, id)
	}

	return set, id, nil
}

func (b *InMemoryBackend) consumeAttachmentSetLocked(id string) ([]AttachmentRef, error) {
	if id == "" {
		return nil, nil
	}
	set, ok := b.attachmentSets.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrAttachmentSetNotFound, id)
	}
	if time.Now().After(set.Expiry) {
		return nil, fmt.Errorf("%w: %s", ErrAttachmentSetExpired, id)
	}
	refs := make([]AttachmentRef, 0, len(set.AttachmentIDs))
	for _, attachmentID := range set.AttachmentIDs {
		attachment, _ := b.attachments.Get(attachmentID)
		refs = append(refs, AttachmentRef{AttachmentID: attachmentID, FileName: attachment.FileName})
	}
	b.attachmentSets.Delete(id)

	return refs, nil
}
