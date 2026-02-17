package dashboard

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"

	"Gopherstack/pkgs/logger"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const (
	maxKeys            = 1000
	maxMultipartMemory = 32 << 20 // 32 MB
)

// BucketInfo represents bucket information for display.
type BucketInfo struct {
	PageData

	Name              string
	CreationDate      string
	VersioningEnabled bool
	ObjectCount       int
}

// FileTreeItem represents a file or folder in the tree.
type FileTreeItem struct {
	Name         string
	FullPath     string
	Size         string
	LastModified string
	BucketName   string
	IsFolder     bool
}

// s3Index renders the S3 index page.
func (h *Handler) s3Index(w http.ResponseWriter, _ *http.Request) {
	data := PageData{
		Title:     "S3 Buckets",
		ActiveTab: "s3",
	}
	h.renderTemplate(w, "s3/s3_index.html", data)
}

// s3BucketList returns the list of buckets as HTML fragment.
func (h *Handler) s3BucketList(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	log := logger.Load(ctx)
	output, err := h.S3.ListBuckets(ctx, &s3.ListBucketsInput{})
	if err != nil {
		log.ErrorContext(ctx, "Failed to list buckets", "error", err)
		http.Error(w, "Failed to list buckets", http.StatusInternalServerError)

		return
	}

	search := strings.ToLower(r.URL.Query().Get("search"))
	var bucketInfos []BucketInfo
	for _, bucket := range output.Buckets {
		if search != "" && !strings.Contains(strings.ToLower(*bucket.Name), search) {
			continue
		}
		// Get versioning status
		versioning, _ := h.S3.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{
			Bucket: bucket.Name,
		})

		// Count objects (simplified)
		objects, _ := h.S3.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:  bucket.Name,
			MaxKeys: aws.Int32(maxKeys),
		})

		info := BucketInfo{
			Name:              *bucket.Name,
			CreationDate:      bucket.CreationDate.Format("2006-01-02 15:04:05"),
			VersioningEnabled: versioning != nil && versioning.Status == types.BucketVersioningStatusEnabled,
			ObjectCount:       int(*objects.KeyCount),
		}
		bucketInfos = append(bucketInfos, info)
	}

	// Render bucket cards
	for _, bucketInfo := range bucketInfos {
		h.renderFragment(w, "bucket-card", bucketInfo)
	}
}

// s3BucketDetail renders the bucket detail page.
func (h *Handler) s3BucketDetail(w http.ResponseWriter, r *http.Request, bucketName string) {
	ctx := r.Context()

	// Check if bucket exists
	_, err := h.S3.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: &bucketName})
	if err != nil {
		http.NotFound(w, r)

		return
	}

	prefix := r.URL.Query().Get("prefix")
	var pathParts []string
	if prefix != "" {
		pathParts = strings.Split(strings.TrimSuffix(prefix, "/"), "/")
	}

	data := struct {
		PageData

		BucketName string
		Prefix     string
		PathParts  []string
	}{
		PageData: PageData{
			Title:     bucketName,
			ActiveTab: "s3",
		},
		BucketName: bucketName,
		Prefix:     prefix,
		PathParts:  pathParts,
	}

	h.renderTemplate(w, "s3/bucket_detail.html", data)
}

// s3FileTree returns the file tree as HTML fragment.
func (h *Handler) s3FileTree(w http.ResponseWriter, r *http.Request, bucketName string) {
	ctx := r.Context()
	log := logger.Load(ctx)
	prefix := r.URL.Query().Get("prefix")

	input := &s3.ListObjectsV2Input{
		Bucket:  &bucketName,
		MaxKeys: aws.Int32(maxKeys),
	}
	if prefix != "" {
		input.Prefix = &prefix
	}

	output, err := h.S3.ListObjectsV2(ctx, input)
	if err != nil {
		log.ErrorContext(ctx, "Failed to list objects", "error", err)
		http.Error(w, "Failed to list objects", http.StatusInternalServerError)

		return
	}

	// Group objects into folders and files
	folders := make(map[string]bool)
	var items []FileTreeItem

	for _, obj := range output.Contents {
		key := *obj.Key
		if prefix != "" {
			key = strings.TrimPrefix(key, prefix)
			key = strings.TrimPrefix(key, "/")
		}

		// Check if this is a folder (contains /)
		if before, _, ok := strings.Cut(key, "/"); ok {
			folderName := before
			if !folders[folderName] {
				folders[folderName] = true
				items = append(items, FileTreeItem{
					Name:       folderName,
					FullPath:   prefix + folderName + "/",
					IsFolder:   true,
					BucketName: bucketName,
				})
			}
		} else {
			// It's a file
			var size string
			if obj.Size != nil {
				size = formatBytes(*obj.Size)
			}
			items = append(items, FileTreeItem{
				Name:         key,
				FullPath:     *obj.Key,
				IsFolder:     false,
				Size:         size,
				LastModified: obj.LastModified.Format("2006-01-02 15:04"),
				BucketName:   bucketName,
			})
		}
	}

	// Render file tree items
	for _, item := range items {
		h.renderFragment(w, "file-tree-item", item)
	}
}

// s3FileDetail renders the file detail page.
func (h *Handler) s3FileDetail(w http.ResponseWriter, r *http.Request, bucketName, key string) {
	ctx := r.Context()

	// Get object metadata
	obj, err := h.S3.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: &bucketName,
		Key:    &key,
	})
	if err != nil {
		http.NotFound(w, r)

		return
	}

	// Get versioning status
	versioning, _ := h.S3.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{
		Bucket: &bucketName,
	})
	versioningEnabled := versioning != nil && versioning.Status == types.BucketVersioningStatusEnabled

	var size string
	if obj.ContentLength != nil {
		size = formatBytes(*obj.ContentLength)
	}

	var contentType, etag string
	if obj.ContentType != nil {
		contentType = *obj.ContentType
	}
	if obj.ETag != nil {
		etag = *obj.ETag
	}

	// Get Tags
	tags := make(map[string]string)
	tagging, err := h.S3.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{
		Bucket: &bucketName,
		Key:    &key,
	})
	if err == nil {
		for _, t := range tagging.TagSet {
			tags[*t.Key] = *t.Value
		}
	}

	// List versions
	var versionList []struct {
		VersionID    string
		LastModified string
	}
	var verOutput *s3.ListObjectVersionsOutput
	if versioningEnabled {
		verOutput, err = h.S3.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
			Bucket: &bucketName,
			Prefix: &key,
		})
		if err == nil {
			for _, v := range verOutput.Versions {
				if *v.Key == key {
					versionList = append(versionList, struct {
						VersionID    string
						LastModified string
					}{
						VersionID:    *v.VersionId,
						LastModified: v.LastModified.Format("2006-01-02 15:04:05"),
					})
				}
			}
		}
	}

	data := struct {
		PageData

		Tags         map[string]string
		BucketName   string
		Key          string
		Size         string
		LastModified string
		ContentType  string
		ETag         string
		Versions     []struct {
			VersionID    string
			LastModified string
		}
		VersioningEnabled bool
	}{
		PageData: PageData{
			Title:     key,
			ActiveTab: "s3",
		},
		BucketName:        bucketName,
		Key:               key,
		Size:              size,
		LastModified:      obj.LastModified.Format("2006-01-02 15:04:05"),
		ContentType:       contentType,
		ETag:              etag,
		VersioningEnabled: versioningEnabled,
		Versions:          versionList,
		Tags:              tags,
	}

	h.renderTemplate(w, "s3/file_detail.html", data)
}

// s3Download downloads a file.
func (h *Handler) s3Download(w http.ResponseWriter, r *http.Request, bucketName, key string) {
	ctx := r.Context()

	output, err := h.S3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &bucketName,
		Key:    &key,
	})
	if err != nil {
		http.NotFound(w, r)

		return
	}
	defer output.Body.Close()

	// Set headers for download
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", key))
	if output.ContentType != nil {
		w.Header().Set("Content-Type", *output.ContentType)
	}

	// Copy object data to response
	_, _ = io.Copy(w, output.Body)
}

// s3Upload handles file upload.
func (h *Handler) s3Upload(w http.ResponseWriter, r *http.Request, bucketName string) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)

		return
	}

	ctx := r.Context()
	log := logger.Load(ctx)

	// Parse multipart form
	if err := r.ParseMultipartForm(maxMultipartMemory); err != nil {
		http.Error(w, "Failed to parse form", http.StatusBadRequest)

		return
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "Failed to get file", http.StatusBadRequest)

		return
	}
	defer file.Close()

	// Get key from form or use filename
	key := r.FormValue("key")
	if key == "" {
		key = header.Filename
	}

	// Add prefix if provided
	prefix := r.URL.Query().Get("prefix")
	if prefix != "" {
		key = prefix + "/" + key
	}

	// Upload object
	input := &s3.PutObjectInput{
		Bucket: &bucketName,
		Key:    &key,
		Body:   file,
	}

	contentType := header.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	input.ContentType = aws.String(contentType)

	// Process Tags
	tagsInput := r.FormValue("tags")
	if tagsInput != "" {
		// Convert "Key=Value,Key2=Value2" to "Key=Value&Key2=Value2" (URL encoded)
		// The SDK expects URL encoded query string for Tagging field in PutObject?
		// Wait, PutObjectInput.Tagging is a string.
		// "The tag-set for the object. The tag-set must be encoded as URL Query parameters."
		var tagParts []string
		pairs := strings.SplitSeq(tagsInput, ",")
		for p := range pairs {
			kv := strings.TrimSpace(p)
			// Simple URL encoding of key and value if needed, but for now assuming simple text
			tagParts = append(tagParts, kv)
		}
		input.Tagging = aws.String(strings.Join(tagParts, "&"))
	}

	_, err = h.S3.PutObject(ctx, input)
	if err != nil {
		log.ErrorContext(ctx, "Failed to upload object", "bucket", bucketName, "key", key, "error", err)
		toastMessage := fmt.Sprintf(`{"showToast": {"message": "Failed to upload file: %s", "type": "error"}}`,
			strings.ReplaceAll(err.Error(), `"`, `'`))
		w.Header().Set("Hx-Trigger", toastMessage)
		w.WriteHeader(http.StatusUnprocessableEntity)

		return
	}

	// Return success and refresh file tree
	w.Header().Set("Hx-Trigger", "fileUploaded")
	// Instead of alert, return the updated file tree!
	// This refreshes the list in place (target is #file-tree)
	h.s3FileTree(w, r, bucketName)
}

// deleteAllVersions deletes all versions of an object including delete markers.
func (h *Handler) deleteAllVersions(ctx context.Context, bucketName, key string) error {
	output, err := h.S3.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
		Bucket: &bucketName,
		Prefix: &key,
	})
	if err != nil {
		h.Logger.ErrorContext(ctx, "Failed to list versions for deletion", "error", err)

		return err
	}

	for _, v := range output.Versions {
		if *v.Key == key {
			_, err = h.S3.DeleteObject(ctx, &s3.DeleteObjectInput{
				Bucket:    &bucketName,
				Key:       &key,
				VersionId: v.VersionId,
			})
			if err != nil {
				h.Logger.ErrorContext(ctx, "Failed to delete version",
					"key", key, "versionId", *v.VersionId, "error", err)
			}
		}
	}

	for _, dm := range output.DeleteMarkers {
		if *dm.Key == key {
			_, err = h.S3.DeleteObject(ctx, &s3.DeleteObjectInput{
				Bucket:    &bucketName,
				Key:       &key,
				VersionId: dm.VersionId,
			})
			if err != nil {
				h.Logger.ErrorContext(ctx, "Failed to delete delete marker",
					"key", key, "versionId", *dm.VersionId, "error", err)
			}
		}
	}

	return nil
}

// s3Versioning handles bucket versioning configuration.
func (h *Handler) s3Versioning(w http.ResponseWriter, r *http.Request, bucketName string) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)

		return
	}

	ctx := r.Context()
	log := logger.Load(ctx)

	// Parse form to get enabled status
	enabled := r.FormValue("enabled") == "true"

	status := types.BucketVersioningStatusSuspended
	if enabled {
		status = types.BucketVersioningStatusEnabled
	}

	_, err := h.S3.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
		Bucket: &bucketName,
		VersioningConfiguration: &types.VersioningConfiguration{
			Status: status,
		},
	})
	if err != nil {
		log.ErrorContext(ctx, "Failed to update versioning", "error", err)
		http.Error(w, "Failed to update versioning", http.StatusInternalServerError)

		return
	}

	// Return updated bucket list
	h.s3BucketList(w, r)
}

// formatBytes formats bytes into human-readable format.
func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}

	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

// s3CreateBucket handles bucket creation requests.
func (h *Handler) s3CreateBucket(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	log := logger.Load(ctx)
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)

		return
	}

	if err := r.ParseForm(); err != nil {
		log.ErrorContext(ctx, "Failed to parse form", "error", err)
		http.Error(w, "Bad request", http.StatusBadRequest)

		return
	}

	bucketName := r.FormValue("bucketName")
	versioningEnabled := r.FormValue("versioning") == "on" // Checkbox sends "on" if checked

	// Create bucket
	_, err := h.S3.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: &bucketName,
	})
	if err != nil {
		log.ErrorContext(ctx, "Failed to create bucket", "bucket", bucketName, "error", err)
		toastMessage := fmt.Sprintf(`{"showToast": {"message": "Failed to create bucket: %s", "type": "error"}}`,
			strings.ReplaceAll(err.Error(), `"`, `'`))
		w.Header().Set("Hx-Trigger", toastMessage)
		w.WriteHeader(http.StatusUnprocessableEntity)

		return
	}

	// Enable versioning if requested
	if versioningEnabled {
		_, err = h.S3.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
			Bucket: &bucketName,
			VersioningConfiguration: &types.VersioningConfiguration{
				Status: types.BucketVersioningStatusEnabled,
			},
		})
		if err != nil {
			log.ErrorContext(ctx, "Failed to enable versioning", "bucket", bucketName, "error", err)
			// Non-critical error, just log it. Or could show warning.
		}
	}

	// On success, return the updated bucket list
	h.s3BucketList(w, r)
}

// s3DeleteFile handles file deletion.
func (h *Handler) s3DeleteFile(w http.ResponseWriter, r *http.Request, bucketName, key string) {
	ctx := r.Context()
	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3DeleteFile request", "bucket", bucketName, "key", key, "method", r.Method, "url", r.URL.String())

	if r.Method != http.MethodDelete {
		log.WarnContext(ctx, "Method not allowed", "method", r.Method)
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)

		return
	}

	deleteAll := r.URL.Query().Get("deleteAll") == "true"
	versionID := r.URL.Query().Get("versionId")

	log.InfoContext(ctx, "Deletion parameters", "deleteAll", deleteAll, "versionID", versionID)

	if deleteAll {
		if err := h.deleteAllVersions(ctx, bucketName, key); err != nil {
			http.Error(w, "Failed to delete all versions", http.StatusInternalServerError)

			return
		}
	} else {
		// Standard delete (creates delete marker or deletes specific version)
		input := &s3.DeleteObjectInput{
			Bucket: &bucketName,
			Key:    &key,
		}
		if versionID != "" {
			input.VersionId = &versionID
		}

		_, err := h.S3.DeleteObject(ctx, input)
		if err != nil {
			log.ErrorContext(ctx, "Failed to delete object", "error", err)
			http.Error(w, "Failed to delete object", http.StatusInternalServerError)

			return
		}
	}

	// Read HX-Trigger to determine source context (list vs detail)
	// If HX-Target is body, likely from detail page, redirect to bucket
	if r.Header.Get("Hx-Target") == "body" {
		w.Header().Set("Hx-Redirect", fmt.Sprintf("/dashboard/s3/bucket/%s", bucketName))

		return
	}

	// Otherwise from list, just remove the element
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(" ")) // Write something to ensure HTMX triggers swap
}

// s3DeleteBucket handles bucket deletion.
func (h *Handler) s3DeleteBucket(w http.ResponseWriter, r *http.Request, bucketName string) {
	ctx := r.Context()
	log := logger.Load(ctx)
	if r.Method != http.MethodDelete {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)

		return
	}

	_, err := h.S3.DeleteBucket(ctx, &s3.DeleteBucketInput{
		Bucket: &bucketName,
	})
	if err != nil {
		log.ErrorContext(ctx, "Failed to delete bucket", "bucket", bucketName, "error", err)
		// Set a trigger for the client-side toast
		toastMessage := fmt.Sprintf(`{"showToast": {"message": "Failed to delete bucket: %s", "type": "error"}}`,
			strings.ReplaceAll(err.Error(), `"`, `'`))
		w.Header().Set("Hx-Trigger", toastMessage)
		w.WriteHeader(http.StatusUnprocessableEntity)

		return
	}

	// Success: return nothing to remove the card (target is .card with outerHTML)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(" "))
}
