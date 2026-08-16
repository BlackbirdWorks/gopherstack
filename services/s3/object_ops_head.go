package s3

import (
	"context"
	"encoding/xml"
	"errors"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

func (h *S3Handler) headObject(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucketName, key string,
) {
	h.setOperation(ctx, "HeadObject")

	if err := validateExpectedBucketOwner(r); err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	if err := h.authorizeObjectAccess(ctx, r, bucketName, key, actionGetObject); err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	newCtx, ok := h.attachSSEInfoToCtx(ctx, w, r)
	if !ok {
		return
	}
	ctx = newCtx

	versionID := r.URL.Query().Get("versionId")
	var vid *string

	if versionID != "" {
		vid = aws.String(versionID)
	}

	out, err := h.Backend.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(key),
		VersionId: vid,
	})
	var nsb *types.NoSuchBucket
	var nsk *types.NoSuchKey
	if errors.Is(err, ErrLatestDeleteMarker) {
		// HEAD of a key whose latest version is a delete marker: 404 + header.
		w.Header().Set("X-Amz-Delete-Marker", "true")
		w.WriteHeader(http.StatusNotFound)

		return
	}

	if errors.As(err, &nsb) || errors.As(err, &nsk) ||
		errors.Is(err, ErrNoSuchBucket) || errors.Is(err, ErrNoSuchKey) {
		w.WriteHeader(http.StatusNotFound)

		return
	}

	if errors.Is(err, ErrDeleteMarker) {
		w.Header().Set("X-Amz-Delete-Marker", "true")
		w.Header().Set("Allow", "DELETE")
		w.WriteHeader(http.StatusMethodNotAllowed)

		return
	}

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)

		return
	}

	if validateErr := validateSSECOnRead(
		r, aws.ToString(out.SSECustomerAlgorithm), aws.ToString(out.SSECustomerKeyMD5),
	); validateErr != nil {
		WriteError(ctx, w, r, validateErr)

		return
	}

	if status, condOK := checkConditionalHeaders(r, aws.ToString(out.ETag), aws.ToTime(out.LastModified)); !condOK {
		w.WriteHeader(status)

		return
	}

	h.writeHeadObjectResponse(ctx, w, r, bucketName, key, out)
}

// writeHeadObjectResponse builds the HEAD response headers (common, SSE,
// content-encoding/disposition, expiration) and dispatches an access-log
// record. Extracted so headObject stays under the funlen cap.
func (h *S3Handler) writeHeadObjectResponse(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucketName, key string,
	out *s3.HeadObjectOutput,
) {
	details := objectCommonDetails{
		Metadata:          out.Metadata,
		ETag:              out.ETag,
		ContentType:       out.ContentType,
		ContentLength:     out.ContentLength,
		LastModified:      out.LastModified,
		VersionID:         out.VersionId,
		StorageClass:      string(out.StorageClass),
		ChecksumCRC32:     out.ChecksumCRC32,
		ChecksumCRC32C:    out.ChecksumCRC32C,
		ChecksumSHA1:      out.ChecksumSHA1,
		ChecksumSHA256:    out.ChecksumSHA256,
		ChecksumCRC64NVME: out.ChecksumCRC64NVME,
		SSEAlgorithm:      string(out.ServerSideEncryption),
		SSEKMSKeyID:       aws.ToString(out.SSEKMSKeyId),
		SSECAlgorithm:     aws.ToString(out.SSECustomerAlgorithm),
		SSECKeyMD5:        aws.ToString(out.SSECustomerKeyMD5),
		TagCount:          out.TagCount,
	}

	h.setCommonHeaders(w, details)
	setSSEHeaders(w, details)
	h.setExpirationHeader(ctx, w, bucketName, key, out.LastModified)

	if ce := aws.ToString(out.ContentEncoding); ce != "" {
		w.Header().Set("Content-Encoding", ce)
	}
	if cd := aws.ToString(out.ContentDisposition); cd != "" {
		w.Header().Set("Content-Disposition", cd)
	}

	applyResponseOverrideHeaders(w, r)

	h.dispatchAccessLog(ctx, r, bucketName, "REST.HEAD.OBJECT", key, http.StatusOK, 0)

	w.WriteHeader(http.StatusOK)
}

// objectAttributesResult is the populated XML body for GetObjectAttributes.
// ObjectSize carries no omitempty: a legitimate 0-byte object must still emit
// <ObjectSize>0</ObjectSize> so the SDK populates its *int64 field.
type objectAttributesResult struct {
	XMLName      xml.Name               `xml:"GetObjectAttributesResult"`
	Xmlns        string                 `xml:"xmlns,attr"`
	ETag         string                 `xml:"ETag,omitempty"`
	Checksum     *attrsChecksumElem     `xml:"Checksum,omitempty"`
	ObjectParts  *objectPartsResultElem `xml:"ObjectParts,omitempty"`
	StorageClass string                 `xml:"StorageClass,omitempty"`
	ObjectSize   int64                  `xml:"ObjectSize"`
}

type objectPartsResultElem struct {
	Parts                []objectPartElem `xml:"Part,omitempty"`
	TotalPartsCount      int32            `xml:"TotalPartsCount,omitempty"`
	PartNumberMarker     int32            `xml:"PartNumberMarker,omitempty"`
	NextPartNumberMarker int32            `xml:"NextPartNumberMarker,omitempty"`
	MaxParts             int32            `xml:"MaxParts,omitempty"`
	IsTruncated          bool             `xml:"IsTruncated,omitempty"`
}

type objectPartElem struct {
	ChecksumCRC32     string `xml:"ChecksumCRC32,omitempty"`
	ChecksumCRC32C    string `xml:"ChecksumCRC32C,omitempty"`
	ChecksumCRC64NVME string `xml:"ChecksumCRC64NVME,omitempty"`
	ChecksumSHA1      string `xml:"ChecksumSHA1,omitempty"`
	ChecksumSHA256    string `xml:"ChecksumSHA256,omitempty"`
	PartNumber        int32  `xml:"PartNumber"`
	Size              int64  `xml:"Size"`
}

type attrsChecksumElem struct {
	ChecksumCRC32     string `xml:"ChecksumCRC32,omitempty"`
	ChecksumCRC32C    string `xml:"ChecksumCRC32C,omitempty"`
	ChecksumCRC64NVME string `xml:"ChecksumCRC64NVME,omitempty"`
	ChecksumSHA1      string `xml:"ChecksumSHA1,omitempty"`
	ChecksumSHA256    string `xml:"ChecksumSHA256,omitempty"`
}

func buildAttrsChecksumElem(checksum map[string]string) *attrsChecksumElem {
	if len(checksum) == 0 {
		return nil
	}

	return &attrsChecksumElem{
		ChecksumCRC32:     checksum["ChecksumCRC32"],
		ChecksumCRC32C:    checksum["ChecksumCRC32C"],
		ChecksumCRC64NVME: checksum["ChecksumCRC64NVME"],
		ChecksumSHA1:      checksum["ChecksumSHA1"],
		ChecksumSHA256:    checksum["ChecksumSHA256"],
	}
}

func buildObjectPartsResultElem(parts *ObjectPartsAttributes) *objectPartsResultElem {
	if parts == nil {
		return nil
	}

	elem := &objectPartsResultElem{
		Parts:                make([]objectPartElem, len(parts.Parts)),
		TotalPartsCount:      parts.TotalPartsCount,
		PartNumberMarker:     parts.PartNumberMarker,
		NextPartNumberMarker: parts.NextPartNumberMarker,
		MaxParts:             parts.MaxParts,
		IsTruncated:          parts.IsTruncated,
	}

	for i, p := range parts.Parts {
		elem.Parts[i] = buildObjectPartElem(p)
	}

	return elem
}

func buildObjectPartElem(p StoredObjectPart) objectPartElem {
	pe := objectPartElem{
		PartNumber: p.PartNumber,
		Size:       p.Size,
	}
	if p.ChecksumCRC32 != nil {
		pe.ChecksumCRC32 = *p.ChecksumCRC32
	}
	if p.ChecksumCRC32C != nil {
		pe.ChecksumCRC32C = *p.ChecksumCRC32C
	}
	if p.ChecksumCRC64NVME != nil {
		pe.ChecksumCRC64NVME = *p.ChecksumCRC64NVME
	}
	if p.ChecksumSHA1 != nil {
		pe.ChecksumSHA1 = *p.ChecksumSHA1
	}
	if p.ChecksumSHA256 != nil {
		pe.ChecksumSHA256 = *p.ChecksumSHA256
	}

	return pe
}

// handleGetObjectAttributes handles GET /{bucket}/{key}?attributes.
// The X-Amz-Object-Attributes header lists which attributes the caller wants;
// we always return the full set we support (ETag, ObjectSize, StorageClass, Checksum, ObjectParts).
func (h *S3Handler) handleGetObjectAttributes(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
) {
	h.setOperation(ctx, "GetObjectAttributes")

	bucket, key, ok := h.resolveBucketAndKey(ctx, w, r)
	if !ok {
		return
	}
	if bucket == "" || key == "" {
		WriteError(ctx, w, r, ErrNoSuchKey)

		return
	}

	versionID := r.URL.Query().Get("versionId")

	attrs, err := h.Backend.GetObjectAttributes(ctx, bucket, key, versionID)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	out := objectAttributesResult{
		Xmlns:        xmlNamespaceS3,
		ETag:         attrs.ETag,
		ObjectSize:   attrs.ObjectSize,
		StorageClass: attrs.StorageClass,
		Checksum:     buildAttrsChecksumElem(attrs.Checksum),
		ObjectParts:  buildObjectPartsResultElem(attrs.Parts),
	}

	if versionID != "" {
		w.Header().Set("X-Amz-Version-Id", versionID)
	}

	// AWS returns the object's Last-Modified as an HTTP header (RFC1123), not a
	// body element, on GetObjectAttributes.
	if !attrs.LastModified.IsZero() {
		w.Header().Set("Last-Modified", attrs.LastModified.UTC().Format(http.TimeFormat))
	}

	httputils.WriteXML(ctx, w, http.StatusOK, out)
}
