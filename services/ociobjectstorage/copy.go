package ociobjectstorage

import (
	"time"

	"github.com/google/uuid"
)

// asyncLag is how long replication and cross-region copies take on the backend clock.
const (
	asyncLag     = 30 * time.Second
	inProgressAt = asyncLag / 2
)

// CopyInput is the CopyObjectDetails payload.
type CopyInput struct {
	DestinationMetadata map[string]string `json:"destinationObjectMetadata"`
	SourceObjectName    string            `json:"sourceObjectName"`
	DestinationRegion   string            `json:"destinationRegion"`
	DestinationNS       string            `json:"destinationNamespace"`
	DestinationBucket   string            `json:"destinationBucket"`
	DestinationObject   string            `json:"destinationObjectName"`
	SourceIfMatch       string            `json:"sourceObjectIfMatchETag"`
	SourceVersionID     string            `json:"sourceVersionId"`
	DestIfMatch         string            `json:"destinationObjectIfMatchETag"`
	DestIfNoneMatch     string            `json:"destinationObjectIfNoneMatchETag"`
	DestinationTier     string            `json:"destinationObjectStorageTier"`
}

// CopyOptions are the opc-source-sse-customer-* and destination encryption headers of a copy.
type CopyOptions struct {
	Dest   SSEReq
	SrcKey SSEKey
}

func (in CopyInput) validate() error {
	if in.SourceObjectName == "" || in.DestinationObject == "" || in.DestinationBucket == "" ||
		in.DestinationNS == "" || in.DestinationRegion == "" {
		return errMissing("sourceObjectName, destinationRegion, destinationNamespace, " +
			"destinationBucket and destinationObjectName are required")
	}

	return nil
}

// copyMeta derives the destination metadata of a copy.
func copyMeta(o *Object, dst *Bucket, in CopyInput) ObjectMeta {
	meta := o.Meta
	meta.Metadata = copyMap(o.Meta.Metadata)

	if in.DestinationMetadata != nil {
		meta.Metadata = in.DestinationMetadata
	}

	if in.DestinationTier != "" {
		meta.StorageTier = in.DestinationTier
	} else if meta.StorageTier == tierArchive {
		meta.StorageTier = dst.StorageTier
	}

	return meta
}

func specOf(o *Object) writeSpec {
	return writeSpec{
		MD5: o.MD5, MultipartMD5: o.MultipartMD5, Checksum: o.Checksum, SseHash: o.SseKeySha256, Kms: o.KmsKeyID,
	}
}

func newWorkRequestID() string {
	return "ocid1.coreservicesworkrequest.oc1." + Region + "." + uuid.NewString()
}

// CopyObject copies an object and returns a work request id.
func (b *InMemoryBackend) CopyObject(ns, bucket string, in CopyInput) (string, error) {
	return b.CopyObjectWith(ns, bucket, in, CopyOptions{})
}

// CopyObjectWith copies an object, honouring SSE-C source/destination keys. A copy to a region other than
// Region is accepted as a pending work request that completes once the clock passes asyncLag.
func (b *InMemoryBackend) CopyObjectWith(ns, bucket string, in CopyInput, opt CopyOptions) (string, error) {
	if err := in.validate(); err != nil {
		return "", err
	}

	defer b.flushEvents()

	b.mu.Lock("CopyObject")
	defer b.mu.Unlock()

	src, err := b.bucket(ns, bucket)
	if err != nil {
		return "", err
	}

	dst, err := b.bucket(in.DestinationNS, in.DestinationBucket)
	if err != nil {
		return "", err
	}

	if err = writable(dst); err != nil {
		return "", err
	}

	o := pick(src, in.SourceObjectName, in.SourceVersionID)
	if o == nil {
		return "", errObject(in.SourceObjectName)
	}

	if err = (Preconds{IfMatch: in.SourceIfMatch}).check(o.Etag, true, false); err != nil {
		return "", err
	}

	if st := archivalAt(o, b.nowUTC()); st == archivalArchived || st == archivalRestoring {
		return "", errConflict(
			"NotRestored",
			"The source object is archived and must be restored before it can be copied",
		)
	}

	if err = requireKey(o.SseKeySha256, opt.SrcKey); err != nil {
		return "", err
	}

	hash, err := opt.Dest.resolve(b.checkKMSKey)
	if err != nil {
		return "", err
	}

	if err = b.checkDestPreconds(dst, in); err != nil {
		return "", err
	}

	now := b.nowUTC()
	wr := &WorkRequest{
		ID: newWorkRequestID(), OperationType: "COPY_OBJECT", CompartmentID: dst.CompartmentID, TimeAccepted: now,
		EntityURI: "/n/" + in.DestinationNS + "/b/" + in.DestinationBucket + "/o/" + in.DestinationObject,
	}
	b.workRequests[wr.ID] = wr

	if in.DestinationRegion != Region {
		wr.Status = wrAccepted
		wr.Pending = &PendingCopy{
			SourceNS: ns, SourceBucket: bucket, VersionID: o.VersionID, DueAt: now.Add(asyncLag), Input: in,
			SseKeySha256: hash, KmsKeyID: opt.Dest.KmsKeyID,
		}

		return wr.ID, nil
	}

	if err = b.copyLocked(dst, o, in, hash, opt.Dest.KmsKeyID); err != nil {
		delete(b.workRequests, wr.ID)

		return "", err
	}

	wr.Status, wr.TimeFinished = wrCompleted, now

	return wr.ID, nil
}

func (b *InMemoryBackend) checkDestPreconds(dst *Bucket, in CopyInput) error {
	cur := live(dst, in.DestinationObject)
	etag := ""

	if cur != nil {
		etag = cur.Etag
	}

	return (Preconds{IfMatch: in.DestIfMatch, IfNoneMatch: in.DestIfNoneMatch}).check(etag, cur != nil, false)
}

func (b *InMemoryBackend) copyLocked(dst *Bucket, o *Object, in CopyInput, hash, kms string) error {
	replaced := live(dst, in.DestinationObject) != nil
	spec := specOf(o)
	spec.SseHash, spec.Kms = hash, kms

	stored, err := b.storeLocked(dst, in.DestinationObject, append([]byte(nil), o.Data...), copyMeta(o, dst, in), spec)
	if err != nil {
		return err
	}

	b.recordEvent(dst, writeEventType(replaced), in.DestinationObject, stored.Etag)
	b.replicate(dst, in.DestinationObject, false)

	return nil
}

// runPendingCopies completes cross-region copies whose due time has passed.
func (b *InMemoryBackend) runPendingCopies(now time.Time) int {
	done := 0

	for _, wr := range b.workRequests {
		p := wr.Pending
		if p == nil || now.Before(p.DueAt) {
			continue
		}

		wr.Pending, wr.TimeFinished = nil, now
		done++

		if err := b.finishCopy(p); err != nil {
			wr.Status, wr.Message = wrFailed, err.Error()

			continue
		}

		wr.Status = wrCompleted
	}

	return done
}

func (b *InMemoryBackend) finishCopy(p *PendingCopy) error {
	src, err := b.bucket(p.SourceNS, p.SourceBucket)
	if err != nil {
		return err
	}

	dst, err := b.bucket(p.Input.DestinationNS, p.Input.DestinationBucket)
	if err != nil {
		return err
	}

	if err = writable(dst); err != nil {
		return err
	}

	var o *Object

	for _, v := range src.Objects[p.Input.SourceObjectName] {
		if v.VersionID == p.VersionID && !v.IsDeleteMarker {
			o = v
		}
	}

	if o == nil {
		return errObject(p.Input.SourceObjectName)
	}

	if err = b.checkDestPreconds(dst, p.Input); err != nil {
		return err
	}

	return b.copyLocked(dst, o, p.Input, p.SseKeySha256, p.KmsKeyID)
}

// statusAt is the wire status of a work request at now.
func (w *WorkRequest) statusAt(now time.Time) string {
	if w.Pending != nil && w.Status == wrAccepted && !now.Before(w.TimeAccepted.Add(inProgressAt)) {
		return wrInProgress
	}

	return w.Status
}

func (b *InMemoryBackend) cancelLocked(w *WorkRequest) error {
	if w.Pending == nil {
		return errConflict("Conflict", "The work request has already finished and cannot be canceled")
	}

	w.Pending, w.Status, w.TimeFinished = nil, wrCanceled, b.nowUTC()

	return nil
}
