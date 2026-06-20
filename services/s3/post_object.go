package s3

import (
	"bytes"
	"context"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
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
		ContentDisposition: nilStringIfEmpty(fields["Content-Disposition"]),
		ContentEncoding:    nilStringIfEmpty(fields["Content-Encoding"]),
		CacheControl:       nilStringIfEmpty(fields["Cache-Control"]),
		Metadata:           userMeta,
	}
	if v := fields["x-amz-tagging"]; v != "" {
		if err := validateTaggingHeader(v); err != nil {
			WriteError(ctx, w, r, err)

			return
		}
		put.Tagging = aws.String(v)
	}

	ver, putErr := h.Backend.PutObject(ctx, put)
	if putErr != nil {
		WriteError(ctx, w, r, putErr)

		return
	}

	if h.notifier != nil {
		if notifXML, ncErr := h.Backend.GetBucketNotificationConfiguration(
			ctx, bucketName,
		); ncErr == nil && notifXML != "" {
			etag := aws.ToString(ver.ETag)
			size := int64(len(fileBody))
			go h.notifier.DispatchObjectCreated(
				h.notificationDispatchContext(), bucketName, key, etag, size, notifXML,
			)
		}
	}

	writePostObjectResponse(w, r, bucketName, key, ver.ETag, fields)
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
		_, _ = w.Write(postObjectResponseXML(bucketName, key, etag, loc, r))

		return
	}

	w.WriteHeader(status)
}

func postObjectResponseXML(
	bucketName, key string, etag *string, location string, r *http.Request,
) []byte {
	var sb strings.Builder
	sb.WriteString(`<?xml version="1.0" encoding="UTF-8"?>`)
	sb.WriteString(`<PostResponse>`)
	sb.WriteString(`<Location>http://`)
	sb.WriteString(r.Host)
	sb.WriteString(location)
	sb.WriteString(`</Location>`)
	sb.WriteString(`<Bucket>`)
	sb.WriteString(bucketName)
	sb.WriteString(`</Bucket>`)
	sb.WriteString(`<Key>`)
	sb.WriteString(key)
	sb.WriteString(`</Key>`)
	if etag != nil {
		sb.WriteString(`<ETag>`)
		sb.WriteString(*etag)
		sb.WriteString(`</ETag>`)
	}
	sb.WriteString(`</PostResponse>`)

	return []byte(sb.String())
}
