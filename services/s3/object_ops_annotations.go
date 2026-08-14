package s3

import (
	"context"
	"encoding/xml"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/ptrconv"
)

// listObjectAnnotationsResult is the XML body of a ListObjectAnnotations
// response. Element names are load-bearing (s3@v1.106.5 deserializers.go:
// awsRestxml_deserializeOpDocumentListObjectAnnotationsOutput matches
// AnnotationCount/AnnotationPrefix/Annotations/Bucket/ContinuationToken/Key/
// MaxAnnotationResults/NextContinuationToken by name; the wrapped list uses
// awsRestxml_deserializeDocumentAnnotationList, which matches AnnotationEntry
// children of the Annotations element) -- the root element name itself is not
// matched by the client.
type listObjectAnnotationsResult struct {
	XMLName               xml.Name             `xml:"ListObjectAnnotationsResult"`
	Xmlns                 string               `xml:"xmlns,attr"`
	Bucket                string               `xml:"Bucket"`
	Key                   string               `xml:"Key"`
	AnnotationPrefix      string               `xml:"AnnotationPrefix,omitempty"`
	ContinuationToken     string               `xml:"ContinuationToken,omitempty"`
	NextContinuationToken string               `xml:"NextContinuationToken,omitempty"`
	Annotations           []annotationEntryXML `xml:"Annotations>AnnotationEntry"`
	MaxAnnotationResults  int32                `xml:"MaxAnnotationResults,omitempty"`
	AnnotationCount       int32                `xml:"AnnotationCount"`
}

// annotationEntryXML mirrors types.AnnotationEntry's field names (s3@v1.106.5
// deserializers.go: awsRestxml_deserializeDocumentAnnotationEntry). ETag,
// LastModified and Size are declared required on the SDK type; ChecksumAlgorithm
// and ReplicationStatus are optional and gopherstack only ever populates the
// former.
type annotationEntryXML struct {
	AnnotationName    string `xml:"AnnotationName"`
	LastModified      string `xml:"LastModified"`
	ETag              string `xml:"ETag,omitempty"`
	ChecksumAlgorithm string `xml:"ChecksumAlgorithm,omitempty"`
	Size              int64  `xml:"Size"`
}

// putObjectAnnotationResult is the XML body of a PutObjectAnnotation response
// (s3@v1.106.5 deserializers.go: awsRestxml_deserializeOpDocumentPutObjectAnnotationOutput
// decodes AnnotationName and Key from body children; everything else on
// PutObjectAnnotationOutput is header-bound).
type putObjectAnnotationResult struct {
	XMLName        xml.Name `xml:"PutObjectAnnotationResult"`
	Xmlns          string   `xml:"xmlns,attr"`
	AnnotationName string   `xml:"AnnotationName"`
	Key            string   `xml:"Key"`
}

func (h *S3Handler) putObjectAnnotation(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucketName, key string,
) {
	h.setOperation(ctx, "PutObjectAnnotation")

	r = maybeDecodeChunkedBody(r)

	q := r.URL.Query()
	algo, crc32p, crc32cp, sha1p, sha256p := extractAlgoAndChecksums(r)
	crc64nvmeP := extractCRC64NVMEChecksum(r)

	out, err := h.Backend.PutObjectAnnotation(ctx, &s3.PutObjectAnnotationInput{
		Bucket:            aws.String(bucketName),
		Key:               aws.String(key),
		AnnotationName:    aws.String(q.Get("annotationName")),
		AnnotationPayload: r.Body,
		VersionId:         ptrconv.NilIfEmpty(q.Get("versionId")),
		ChecksumAlgorithm: types.ChecksumAlgorithm(algo),
		ChecksumCRC32:     crc32p,
		ChecksumCRC32C:    crc32cp,
		ChecksumSHA1:      sha1p,
		ChecksumSHA256:    sha256p,
		ChecksumCRC64NVME: crc64nvmeP,
		ObjectIfMatch:     ptrconv.NilIfEmpty(r.Header.Get("X-Amz-Object-If-Match")),
	})
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	w.Header().Set("ETag", aws.ToString(out.ETag))
	h.setChecksumHeaders(w, objectCommonDetails{
		ChecksumCRC32:     out.ChecksumCRC32,
		ChecksumCRC32C:    out.ChecksumCRC32C,
		ChecksumSHA1:      out.ChecksumSHA1,
		ChecksumSHA256:    out.ChecksumSHA256,
		ChecksumCRC64NVME: out.ChecksumCRC64NVME,
	})
	setObjectVersionHeader(w, out.ObjectVersionId)

	httputils.WriteXML(ctx, w, http.StatusOK, putObjectAnnotationResult{
		Xmlns:          xmlNamespaceS3,
		AnnotationName: aws.ToString(out.AnnotationName),
		Key:            aws.ToString(out.Key),
	})
}

func (h *S3Handler) getObjectAnnotation(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucketName, key string,
) {
	h.setOperation(ctx, "GetObjectAnnotation")

	q := r.URL.Query()

	out, err := h.Backend.GetObjectAnnotation(ctx, &s3.GetObjectAnnotationInput{
		Bucket:         aws.String(bucketName),
		Key:            aws.String(key),
		AnnotationName: aws.String(q.Get("annotationName")),
		VersionId:      ptrconv.NilIfEmpty(q.Get("versionId")),
	})
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	defer out.AnnotationPayload.Close()

	w.Header().Set("ETag", aws.ToString(out.ETag))
	w.Header().Set("Content-Length", strconv.FormatInt(aws.ToInt64(out.ContentLength), 10))
	if out.LastModified != nil {
		w.Header().Set("Last-Modified", out.LastModified.UTC().Format(http.TimeFormat))
	}
	h.setChecksumHeaders(w, objectCommonDetails{
		ChecksumCRC32:     out.ChecksumCRC32,
		ChecksumCRC32C:    out.ChecksumCRC32C,
		ChecksumSHA1:      out.ChecksumSHA1,
		ChecksumSHA256:    out.ChecksumSHA256,
		ChecksumCRC64NVME: out.ChecksumCRC64NVME,
	})
	setObjectVersionHeader(w, out.ObjectVersionId)

	w.WriteHeader(http.StatusOK)
	if _, copyErr := io.Copy(w, out.AnnotationPayload); copyErr != nil {
		logger.Load(ctx).ErrorContext(ctx, "failed to write annotation payload", "error", copyErr)
	}
}

func (h *S3Handler) deleteObjectAnnotation(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucketName, key string,
) {
	h.setOperation(ctx, "DeleteObjectAnnotation")

	q := r.URL.Query()

	out, err := h.Backend.DeleteObjectAnnotation(ctx, &s3.DeleteObjectAnnotationInput{
		Bucket:         aws.String(bucketName),
		Key:            aws.String(key),
		AnnotationName: aws.String(q.Get("annotationName")),
		VersionId:      ptrconv.NilIfEmpty(q.Get("versionId")),
		ObjectIfMatch:  ptrconv.NilIfEmpty(r.Header.Get("X-Amz-Object-If-Match")),
	})
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	setObjectVersionHeader(w, out.ObjectVersionId)
	w.WriteHeader(http.StatusNoContent)
}

func (h *S3Handler) listObjectAnnotations(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucketName, key string,
) {
	h.setOperation(ctx, "ListObjectAnnotations")

	q := r.URL.Query()

	input := &s3.ListObjectAnnotationsInput{
		Bucket:            aws.String(bucketName),
		Key:               aws.String(key),
		AnnotationPrefix:  ptrconv.NilIfEmpty(q.Get("annotation-prefix")),
		ContinuationToken: ptrconv.NilIfEmpty(q.Get("continuation-token")),
		VersionId:         ptrconv.NilIfEmpty(q.Get("versionId")),
	}
	if raw := q.Get("max-annotation-results"); raw != "" {
		if v, convErr := strconv.Atoi(raw); convErr == nil && v > 0 && v <= maxAnnotationsPerObject {
			//nolint:gosec // G115: bounded by the [1, maxAnnotationsPerObject] check above
			input.MaxAnnotationResults = aws.Int32(int32(v))
		}
	}

	out, err := h.Backend.ListObjectAnnotations(ctx, input)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	setObjectVersionHeader(w, out.ObjectVersionId)
	httputils.WriteXML(ctx, w, http.StatusOK, buildListObjectAnnotationsResult(bucketName, key, out))
}

func buildListObjectAnnotationsResult(
	bucketName, key string,
	out *s3.ListObjectAnnotationsOutput,
) listObjectAnnotationsResult {
	entries := make([]annotationEntryXML, 0, len(out.Annotations))
	for _, a := range out.Annotations {
		entries = append(entries, annotationEntryXML{
			AnnotationName:    aws.ToString(a.AnnotationName),
			LastModified:      formatAnnotationTime(a.LastModified),
			ETag:              aws.ToString(a.ETag),
			ChecksumAlgorithm: firstChecksumAlgorithm(a.ChecksumAlgorithm),
			Size:              aws.ToInt64(a.Size),
		})
	}

	return listObjectAnnotationsResult{
		Xmlns:                 xmlNamespaceS3,
		Bucket:                bucketName,
		Key:                   key,
		AnnotationPrefix:      aws.ToString(out.AnnotationPrefix),
		ContinuationToken:     aws.ToString(out.ContinuationToken),
		NextContinuationToken: aws.ToString(out.NextContinuationToken),
		MaxAnnotationResults:  aws.ToInt32(out.MaxAnnotationResults),
		AnnotationCount:       aws.ToInt32(out.AnnotationCount),
		Annotations:           entries,
	}
}

func formatAnnotationTime(t *time.Time) string {
	if t == nil {
		return ""
	}

	return t.UTC().Format(time.RFC3339)
}

func firstChecksumAlgorithm(algos []types.ChecksumAlgorithm) string {
	if len(algos) == 0 {
		return ""
	}

	return string(algos[0])
}

// setObjectVersionHeader writes x-amz-object-version-id, omitting it for the
// unversioned NullVersion sentinel (real S3 does not send this header for
// buckets without versioning enabled -- matches setPutObjectResponseHeaders).
func setObjectVersionHeader(w http.ResponseWriter, versionID *string) {
	if versionID != nil && *versionID != NullVersion {
		w.Header().Set("X-Amz-Object-Version-Id", *versionID)
	}
}
