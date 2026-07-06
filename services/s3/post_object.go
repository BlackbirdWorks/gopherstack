package s3

import (
	"bytes"
	"context"
	"encoding/xml"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/blackbirdworks/gopherstack/pkgs/ptrconv"
)

// handlePostObject implements browser-style POST form-data uploads to S3.
//
// AWS S3 supports POST /bucket with Content-Type: multipart/form-data so that
// browsers (or any HTML form) can upload directly to S3 using a presigned
// policy without ever sending the file through the application server. This
// is the mechanism behind the canonical "presigned POST" flow used by uppy,
// aws-amplify Storage.put, etc.
//
// Wire format:
//   - The form may include any number of field-name/value pairs followed by
//     a 'file' part containing the body. AWS requires 'file' to be last.
//   - 'key' picks the destination key. AWS supports the literal '${filename}'
//     placeholder which expands to the uploaded filename.
//   - 'Content-Type', 'Cache-Control', 'Content-Disposition' etc. flow into
//     the stored object metadata.
//   - 'x-amz-meta-*' fields become user-metadata.
//   - 'success_action_status' overrides the default 204 response code.
//   - 'success_action_redirect' returns 303 with Location when set.
//
// We do NOT verify the signature/policy fields — same posture as the rest of
// the mock — matching LocalStack's behaviour so presigned-POST tests pass
// end-to-end without real AWS credentials.
func (h *S3Handler) handlePostObject(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucketName string,
) {
	h.setOperation(ctx, "PostObject")

	mediaType, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || !strings.HasPrefix(mediaType, "multipart/") {
		WriteError(ctx, w, r, ErrInvalidArgument)

		return
	}

	boundary := params["boundary"]
	if boundary == "" {
		WriteError(ctx, w, r, ErrInvalidArgument)

		return
	}

	fields, fileName, fileBody, parseErr := parsePostFormUpload(r.Body, boundary)
	if parseErr != nil {
		WriteError(ctx, w, r, parseErr)

		return
	}

	key := fields["key"]
	if key == "" {
		WriteError(ctx, w, r, ErrInvalidArgument)

		return
	}
	// AWS expands ${filename} to the uploaded part's filename.
	if fileName != "" {
		key = strings.ReplaceAll(key, "${filename}", fileName)
	}

	put, buildErr := buildPostPutInput(bucketName, key, fileBody, fields)
	if buildErr != nil {
		WriteError(ctx, w, r, buildErr)

		return
	}

	ver, putErr := h.Backend.PutObject(ctx, put)
	if putErr != nil {
		WriteError(ctx, w, r, putErr)

		return
	}

	h.dispatchPostObjectNotification(ctx, bucketName, key, aws.ToString(ver.ETag), len(fileBody))

	writePostObjectResponse(w, r, bucketName, key, ver.ETag, fields)
}

// buildPostPutInput constructs the PutObjectInput for a POST form upload from
// the parsed form fields, validating any x-amz-tagging value.
func buildPostPutInput(
	bucketName, key string,
	fileBody []byte,
	fields map[string]string,
) (*s3.PutObjectInput, error) {
	objContentType := fields["Content-Type"]
	if objContentType == "" {
		objContentType = "binary/octet-stream"
	}

	userMeta := map[string]string{}
	for k, v := range fields {
		if lk := strings.ToLower(k); strings.HasPrefix(lk, "x-amz-meta-") {
			userMeta[strings.TrimPrefix(lk, "x-amz-meta-")] = v
		}
	}

	put := &s3.PutObjectInput{
		Bucket:             aws.String(bucketName),
		Key:                aws.String(key),
		Body:               bytes.NewReader(fileBody),
		ContentType:        aws.String(objContentType),
		ContentDisposition: ptrconv.NilIfEmpty(fields["Content-Disposition"]),
		ContentEncoding:    ptrconv.NilIfEmpty(fields["Content-Encoding"]),
		CacheControl:       ptrconv.NilIfEmpty(fields["Cache-Control"]),
		Metadata:           userMeta,
	}

	if v := fields["x-amz-tagging"]; v != "" {
		if tagErr := validateTaggingHeader(v); tagErr != nil {
			return nil, tagErr
		}
		put.Tagging = aws.String(v)
	}

	return put, nil
}

// dispatchPostObjectNotification fires an ObjectCreated event for a POST upload
// when the bucket has a notification configuration.
func (h *S3Handler) dispatchPostObjectNotification(
	ctx context.Context,
	bucketName, key, etag string,
	size int,
) {
	if h.notifier == nil {
		return
	}

	notifXML, ncErr := h.Backend.GetBucketNotificationConfiguration(ctx, bucketName)
	if ncErr != nil || notifXML == "" {
		return
	}

	go h.notifier.DispatchObjectCreated(
		h.notificationDispatchContext(), bucketName, key, etag, int64(size), notifXML,
	)
}

// parsePostFormUpload reads a multipart/form-data body and returns the
// non-file form fields, the uploaded filename (or "" if absent), and the file
// bytes. AWS requires 'file' to be last in the form; we tolerate any order.
func parsePostFormUpload(
	body io.ReadCloser, boundary string,
) (map[string]string, string, []byte, error) {
	defer body.Close()

	fields := map[string]string{}
	var fileName string
	var fileBody []byte

	mr := multipart.NewReader(body, boundary)
	for {
		part, err := mr.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, "", nil, ErrInvalidArgument
		}

		if part.FormName() == "file" {
			fileName = part.FileName()
			content, readErr := io.ReadAll(part)
			_ = part.Close()
			if readErr != nil {
				return nil, "", nil, ErrInvalidArgument
			}
			fileBody = content

			continue
		}

		content, readErr := io.ReadAll(part)
		_ = part.Close()
		if readErr != nil {
			return nil, "", nil, ErrInvalidArgument
		}
		fields[part.FormName()] = string(content)
	}

	if fileBody == nil {
		return nil, "", nil, ErrInvalidArgument
	}

	return fields, fileName, fileBody, nil
}

// writePostObjectResponse honours the success_action_redirect /
// success_action_status form fields exactly as AWS does. Default is 204.
func writePostObjectResponse(
	w http.ResponseWriter, r *http.Request,
	bucketName, key string, etag *string, fields map[string]string,
) {
	if etag != nil {
		w.Header().Set("ETag", *etag)
	}

	if redirect := fields["success_action_redirect"]; redirect != "" {
		if u, err := url.Parse(redirect); err == nil {
			q := u.Query()
			q.Set("bucket", bucketName)
			q.Set("key", key)
			if etag != nil {
				q.Set("etag", strings.Trim(*etag, `"`))
			}
			u.RawQuery = q.Encode()
			w.Header().Set("Location", u.String())
			w.WriteHeader(http.StatusSeeOther)

			return
		}
	}

	status := http.StatusNoContent
	if s := fields["success_action_status"]; s != "" {
		if n, convErr := strconv.Atoi(s); convErr == nil {
			switch n {
			case http.StatusOK, http.StatusCreated, http.StatusNoContent:
				status = n
			}
		}
	}

	if status == http.StatusCreated {
		loc := "/" + bucketName + "/" + key
		w.Header().Set("Location", loc)
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusCreated)
		body := postObjectResponseXML(bucketName, key, etag, loc, r)
		_, _ = w.Write(body) //nolint:gosec // XML-escaped by encoding/xml, served as application/xml

		return
	}

	w.WriteHeader(status)
}

// postResponseXML is the <PostResponse> body S3 returns for a successful
// browser POST upload when success_action_status=201. Marshalling through
// encoding/xml (rather than string concatenation) XML-escapes bucket/key
// values, which may legally contain '&', '<' or '>'.
type postResponseXML struct {
	XMLName  xml.Name `xml:"PostResponse"`
	Location string   `xml:"Location"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	ETag     string   `xml:"ETag,omitempty"`
}

func postObjectResponseXML(
	bucketName, key string, etag *string, location string, r *http.Request,
) []byte {
	resp := postResponseXML{
		Location: "http://" + r.Host + location,
		Bucket:   bucketName,
		Key:      key,
		ETag:     aws.ToString(etag),
	}

	out, err := xml.Marshal(resp)
	if err != nil {
		return nil
	}

	return append([]byte(xml.Header), out...)
}
