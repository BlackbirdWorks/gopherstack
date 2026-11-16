package gcpstorage

import (
	"encoding/base64"
	"encoding/hex"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
)

// XML API multipart uploads: docs.cloud.google.com/storage/docs/xml-api/{post-object-multipart,
// put-object-multipart,post-object-complete,delete-multipart}.
const (
	maxPartNumber       = 10000
	minNonFinalPartSize = 5 << 20
	maxPartSize         = 5 << 30
	maxMultipartBytes   = 2 << 30
	maxStagedBytes      = 4 << 30
	maxMultipartUploads = 1000
	actionAbortMPU      = "AbortIncompleteMultipartUpload"
)

type xmlPart struct {
	ETag string
	Data []byte
}

type xmlUpload struct {
	Created time.Time
	Meta    Patch
	Parts   map[int]*xmlPart
	Bucket  string
	Name    string
	Bytes   int64
}

// PartRef names one part in a complete request.
type PartRef struct {
	ETag   string
	Number int
}

func errMPU(status int, code, msg string) *APIError {
	return &APIError{Status: status, Reason: "invalid", XMLCode: code, Message: msg}
}

func errNoSuchUpload() *APIError {
	return errMPU(
		http.StatusNotFound,
		"NoSuchUpload",
		"The specified upload does not exist: the upload ID is invalid, or the upload was aborted or completed.",
	)
}

func (b *InMemoryBackend) uploadLocked(bucket, name, id string) (*xmlUpload, *APIError) {
	u, ok := b.mpus[id]
	if !ok || u.Bucket != bucket || u.Name != name {
		return nil, errNoSuchUpload()
	}

	return u, nil
}

// InitiateMultipart opens an XML API multipart upload and returns its upload id.
func (b *InMemoryBackend) InitiateMultipart(bucket, name string, meta Patch) (string, error) {
	b.mu.Lock("InitiateMultipart")
	defer b.mu.Unlock()

	if _, err := b.bucketLocked(bucket); err != nil {
		return "", err
	}

	if !validObjectName(name) {
		return "", errInvalid("Invalid object name: '" + name + "'")
	}

	now := b.now().UTC()

	if len(b.mpus) >= maxMultipartUploads {
		return "", errMPU(http.StatusServiceUnavailable, "SlowDown", "Too many multipart uploads are in progress.")
	}

	id := awsid.NewN("", uploadIDLen)
	b.mpus[id] = &xmlUpload{Created: now, Bucket: bucket, Name: name, Meta: meta, Parts: map[int]*xmlPart{}}

	return id, nil
}

// UploadPart stores one part and returns its ETag; keySHA must equal the key the upload was initiated with.
func (b *InMemoryBackend) UploadPart(bucket, name, id string, number int, data []byte, keySHA string) (string, error) {
	if number < 1 || number > maxPartNumber {
		return "", errMPU(http.StatusBadRequest, "InvalidArgument", "partNumber must be between 1 and 10000.")
	}

	if len(data) > maxPartSize {
		return "", errMPU(http.StatusBadRequest, "EntityTooLarge", "The part exceeds the maximum size of 5 GiB.")
	}

	b.mu.Lock("UploadPart")
	defer b.mu.Unlock()

	u, apiErr := b.uploadLocked(bucket, name, id)
	if apiErr != nil {
		return "", apiErr
	}

	if apiErr = u.checkKey(keySHA); apiErr != nil {
		return "", apiErr
	}

	var replaced int64
	if old, ok := u.Parts[number]; ok {
		replaced = int64(len(old.Data))
	}

	if b.stagedBytesLocked()-replaced+int64(len(data)) > b.stagedLimit() {
		return "", errMPU(http.StatusServiceUnavailable, "SlowDown", "Too many bytes are staged by multipart uploads.")
	}

	if u.Bytes-replaced+int64(len(data)) > maxMultipartBytes {
		return "", errMPU(
			http.StatusBadRequest,
			"EntityTooLarge",
			"The multipart upload exceeds the emulator limit of 2 GiB.",
		)
	}

	raw, _ := base64.StdEncoding.DecodeString(md5Of(data))
	etag := `"` + hex.EncodeToString(raw) + `"`
	u.Parts[number] = &xmlPart{ETag: etag, Data: slices.Clone(data)}
	u.Bytes += int64(len(data)) - replaced

	return etag, nil
}

func (b *InMemoryBackend) stagedBytesLocked() int64 {
	var total int64

	for _, u := range b.mpus {
		total += u.Bytes
	}

	return total
}

func (b *InMemoryBackend) stagedLimit() int64 {
	if b.stagedCap > 0 {
		return b.stagedCap
	}

	return maxStagedBytes
}

// dropUploadsLocked discards the open multipart uploads of a bucket.
func (b *InMemoryBackend) dropUploadsLocked(bucket string) {
	for id, u := range b.mpus {
		if u.Bucket == bucket {
			delete(b.mpus, id)
		}
	}
}

// multipartETag is the ETag of a completed upload: the MD5 of the concatenated part MD5s, a dash and the part
// count (sample in docs.cloud.google.com/storage/docs/xml-api/post-object-complete).
func multipartETag(sums []byte, parts int) string {
	raw, _ := base64.StdEncoding.DecodeString(md5Of(sums))

	return `"` + hex.EncodeToString(raw) + "-" + strconv.Itoa(parts) + `"`
}

func (u *xmlUpload) checkKey(keySHA string) *APIError {
	if u.Meta.str(keyCustomerKeySHA) != keySHA {
		return errInvalid("The customer-supplied encryption headers must match those of the upload initiation.")
	}

	return nil
}

func (u *xmlUpload) assemble(parts []PartRef) ([]byte, string, *APIError) {
	if len(parts) == 0 {
		return nil, "", errMPU(http.StatusBadRequest, "MalformedXML", "The request must list at least one part.")
	}

	for i := 1; i < len(parts); i++ {
		if parts[i].Number <= parts[i-1].Number {
			return nil, "", errMPU(
				http.StatusBadRequest,
				"InvalidPartOrder",
				"The list of parts was not in ascending order.",
			)
		}
	}

	var data, sums []byte

	for i, ref := range parts {
		p, ok := u.Parts[ref.Number]
		if !ok || strings.Trim(ref.ETag, `"`) != strings.Trim(p.ETag, `"`) {
			return nil, "", errMPU(http.StatusBadRequest, "InvalidPart",
				"One or more of the specified parts could not be found or the ETag did not match.")
		}

		if i < len(parts)-1 && len(p.Data) < minNonFinalPartSize {
			return nil, "", errMPU(
				http.StatusBadRequest,
				"InvalidArgument",
				"Your proposed upload is smaller than the minimum "+
					"allowed object size: every part except the last must be at least 5 MiB.",
			)
		}

		sum, _ := hex.DecodeString(strings.Trim(p.ETag, `"`))
		sums = append(sums, sum...)
		data = append(data, p.Data...)
	}

	return data, multipartETag(sums, len(parts)), nil
}

// CompleteMultipart assembles the listed parts into the object and discards the upload.
func (b *InMemoryBackend) CompleteMultipart(
	bucket, name, id string, parts []PartRef, pre Preconds, keySHA string,
) (*Object, error) {
	defer b.flushEvents()

	b.mu.Lock("CompleteMultipart")
	defer b.mu.Unlock()

	u, apiErr := b.uploadLocked(bucket, name, id)
	if apiErr != nil {
		return nil, apiErr
	}

	if apiErr = u.checkKey(keySHA); apiErr != nil {
		return nil, apiErr
	}

	data, etag, apiErr := u.assemble(parts)
	if apiErr != nil {
		return nil, apiErr
	}

	bk, err := b.bucketLocked(bucket)
	if err != nil {
		return nil, err
	}

	if apiErr = pre.checkObject(bk.live(name), false); apiErr != nil {
		return nil, apiErr
	}

	if apiErr = b.guardMutation(bk, bk.live(name)); apiErr != nil {
		return nil, apiErr
	}

	o, apiErr := b.newObject(bk, name, data, u.Meta, b.nextGen())
	if apiErr != nil {
		return nil, apiErr
	}

	o.MD5, o.PartsETag = "", etag
	b.storeLocked(bk, o)
	delete(b.mpus, id)

	return bk.out(o), nil
}

// AbortMultipart discards an upload and its parts.
func (b *InMemoryBackend) AbortMultipart(bucket, name, id string) error {
	b.mu.Lock("AbortMultipart")
	defer b.mu.Unlock()

	if _, apiErr := b.uploadLocked(bucket, name, id); apiErr != nil {
		return apiErr
	}

	delete(b.mpus, id)

	return nil
}

// sweepMultipartLocked applies AbortIncompleteMultipartUpload rules to the bucket's open uploads.
func (b *InMemoryBackend) sweepMultipartLocked(bk *Bucket, rules []lifecycleRule, now time.Time) {
	for id, u := range b.mpus {
		if u.Bucket != bk.Name {
			continue
		}

		for _, r := range rules {
			if r.Action.Type == actionAbortMPU && r.Condition.abortsUpload(u, now) {
				delete(b.mpus, id)

				break
			}
		}
	}
}

func (c lifecycleCondition) abortsUpload(u *xmlUpload, now time.Time) bool {
	if c.Age == nil || now.Sub(u.Created) < time.Duration(*c.Age)*day {
		return false
	}

	if len(c.MatchesPrefix) > 0 && !matchesAny(u.Name, c.MatchesPrefix, strings.HasPrefix) {
		return false
	}

	return len(c.MatchesSuffix) == 0 || matchesAny(u.Name, c.MatchesSuffix, strings.HasSuffix)
}

// validateAbortRules rejects AbortIncompleteMultipartUpload rules that use conditions other than age,
// matchesPrefix and matchesSuffix (docs.cloud.google.com/storage/docs/lifecycle).
func validateAbortRules(bk *Bucket) *APIError {
	for _, r := range lifecycleRules(bk) {
		if r.Action.Type != actionAbortMPU {
			continue
		}

		c := r.Condition
		other := c.NumNewerVersions != nil || c.SizeAboveBytes != nil || c.SizeBelowBytes != nil || c.IsLive != nil ||
			c.DaysSinceCustomTime != nil || c.DaysSinceNoncurrentTime != nil || c.CustomTimeBefore != "" ||
			c.NoncurrentTimeBefore != "" || c.CreatedBefore != "" || len(c.MatchesStorageClass) > 0

		if other || c.Age == nil {
			return errInvalid("AbortIncompleteMultipartUpload rules take an age condition and only matchesPrefix " +
				"and matchesSuffix besides it.")
		}
	}

	return nil
}
