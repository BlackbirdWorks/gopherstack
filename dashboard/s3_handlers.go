package dashboard

import (
	"context"
	"encoding/json"
	"fmt"
	"html"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"

	"Gopherstack/pkgs/logger"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const (
	maxKeys            = 1000
	maxMultipartMemory = 32 << 20 // 32 MB
)

// S3FileVersion represents a version of an S3 object.
type S3FileVersion struct {
	VersionID    string
	LastModified string
}

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

// s3FileDetailData holds data for the s3/file_detail.html template.
type s3FileDetailData struct {
	PageData

	BucketName        string
	Key               string
	Size              string
	LastModified      string
	ContentType       string
	ETag              string
	Tags              map[string]string
	Versions          []S3FileVersion
	IsImage           bool
	IsPDF             bool
	IsText            bool
	VersioningEnabled bool
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
	offsetStr := r.URL.Query().Get("offset")
	offset, _ := strconv.Atoi(offsetStr)

	var filteredBuckets []types.Bucket
	for _, b := range output.Buckets {
		if search == "" || strings.Contains(strings.ToLower(*b.Name), search) {
			filteredBuckets = append(filteredBuckets, b)
		}
	}

	h.handleS3BucketListing(ctx, w, filteredBuckets, offset)
}

func (h *Handler) handleS3BucketListing(
	ctx context.Context,
	w http.ResponseWriter,
	buckets []types.Bucket,
	offset int,
) {
	const displayLimit = 100
	totalFiltered := len(buckets)
	endIdx := min(offset+displayLimit, totalFiltered)

	pageBuckets := buckets[offset:endIdx]
	bucketInfos := make([]BucketInfo, len(pageBuckets))
	const maxConcurrent = 8
	semaphore := make(chan struct{}, maxConcurrent)
	var wg sync.WaitGroup
	var mu sync.Mutex

	for i, bucket := range pageBuckets {
		wg.Add(1)
		go func(idx int, b types.Bucket) {
			defer wg.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			versioning, _ := h.S3.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{
				Bucket: b.Name,
			})
			objects, _ := h.S3.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
				Bucket:  b.Name,
				MaxKeys: aws.Int32(maxKeys),
			})

			mu.Lock()
			bucketInfos[idx] = BucketInfo{
				Name: *b.Name,
				CreationDate: b.CreationDate.Format(
					"2006-01-02 15:04:05",
				),
				VersioningEnabled: versioning != nil &&
					versioning.Status == types.BucketVersioningStatusEnabled,
				ObjectCount: int(*objects.KeyCount),
			}
			mu.Unlock()
		}(i, bucket)
	}

	wg.Wait()

	for _, info := range bucketInfos {
		if info.Name != "" {
			h.renderFragment(w, "bucket-card", info)
		}
	}

	if endIdx < totalFiltered {
		// #nosec G705
		fmt.Fprintf(w, `
            <button class="btn btn-outline col-span-full mt-4" 
                hx-get="/dashboard/s3/buckets?offset=%d" 
                hx-target="this" 
                hx-swap="outerHTML"
                hx-indicator=".htmx-indicator">
                Load More
            </button>`, endIdx)
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
	token := r.URL.Query().Get("token")

	input := &s3.ListObjectsV2Input{
		Bucket:  &bucketName,
		MaxKeys: aws.Int32(maxS3ObjectSearch),
	}
	if prefix != "" {
		input.Prefix = &prefix
	}
	if token != "" {
		input.ContinuationToken = &token
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

	if output.NextContinuationToken != nil {
		// #nosec G705
		fmt.Fprintf(w, `
            <div id="load-more-objects" class="col-span-full py-4 flex justify-center">
                <button class="btn btn-outline" 
                    hx-get="/dashboard/s3/bucket/%s/files?token=%s&prefix=%s" 
                    hx-target="this" 
                    hx-swap="outerHTML"
                    hx-indicator=".htmx-indicator">
                    Load More
                </button>
            </div>`, url.PathEscape(bucketName),
			url.QueryEscape(*output.NextContinuationToken),
			url.QueryEscape(prefix))
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

	h.handleS3FileDetail(ctx, w, bucketName, key, obj)
}

func (h *Handler) handleS3FileDetail(
	ctx context.Context,
	w http.ResponseWriter,
	bucketName, key string,
	obj *s3.HeadObjectOutput,
) {
	versioningEnabled := h.isS3BucketVersioningEnabled(ctx, bucketName)
	tags := h.fetchS3ObjectTags(ctx, bucketName, key)
	versionList := h.fetchS3ObjectVersions(ctx, bucketName, key, versioningEnabled)

	data := h.prepareS3FileDetailData(bucketName, key, obj, versioningEnabled, tags, versionList)

	h.renderTemplate(w, "s3/file_detail.html", data)
}

func (h *Handler) isS3BucketVersioningEnabled(ctx context.Context, bucketName string) bool {
	versioning, _ := h.S3.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{
		Bucket: &bucketName,
	})

	return versioning != nil && versioning.Status == types.BucketVersioningStatusEnabled
}

func (h *Handler) fetchS3ObjectTags(ctx context.Context, bucketName, key string) map[string]string {
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

	return tags
}

func (h *Handler) fetchS3ObjectVersions(
	ctx context.Context,
	bucketName, key string,
	enabled bool,
) []S3FileVersion {
	var versionList []S3FileVersion
	if !enabled {
		return versionList
	}
	verOutput, err := h.S3.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
		Bucket: &bucketName,
		Prefix: &key,
	})
	if err == nil {
		for _, v := range verOutput.Versions {
			if *v.Key == key {
				versionList = append(versionList, S3FileVersion{
					VersionID:    *v.VersionId,
					LastModified: v.LastModified.Format("2006-01-02 15:04:05"),
				})
			}
		}
	}

	return versionList
}

func (h *Handler) prepareS3FileDetailData(
	bucketName, key string,
	obj *s3.HeadObjectOutput,
	versioningEnabled bool,
	tags map[string]string,
	versionList []S3FileVersion,
) s3FileDetailData {
	contentType := ""
	if obj.ContentType != nil {
		contentType = *obj.ContentType
	}
	etag := ""
	if obj.ETag != nil {
		etag = *obj.ETag
	}

	return s3FileDetailData{
		PageData: PageData{
			Title:     key,
			ActiveTab: "s3",
		},
		Versions:          versionList,
		Tags:              tags,
		BucketName:        bucketName,
		Key:               key,
		Size:              formatBytes(*obj.ContentLength),
		LastModified:      obj.LastModified.Format("2006-01-02 15:04:05"),
		ContentType:       contentType,
		ETag:              etag,
		VersioningEnabled: versioningEnabled,
		IsImage:           strings.HasPrefix(contentType, "image/"),
		IsPDF:             contentType == "application/pdf",
		IsText: strings.HasPrefix(contentType, "text/") ||
			contentType == "application/json" ||
			contentType == "application/javascript" ||
			contentType == "application/xml",
	}
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
		var tagParts []string
		//nolint:modernize // Avoid SplitSeq for Go 1.22 compatibility
		pairs := strings.Split(tagsInput, ",")
		for _, p := range pairs {
			kv := strings.TrimSpace(p)
			tagParts = append(tagParts, kv)
		}
		input.Tagging = aws.String(strings.Join(tagParts, "&"))
	}

	_, err = h.S3.PutObject(ctx, input)
	if err != nil {
		log.ErrorContext(
			ctx,
			"Failed to upload object",
			"bucket",
			bucketName,
			"key",
			key,
			"error",
			err,
		)
		toastMessage := fmt.Sprintf(
			`{"showToast": {"message": "Failed to upload file: %s", "type": "error"}}`,
			strings.ReplaceAll(err.Error(), `"`, `'`),
		)
		w.Header().Set("Hx-Trigger", toastMessage)
		w.WriteHeader(http.StatusUnprocessableEntity)

		return
	}

	// Return success and refresh file tree
	w.Header().Set("Hx-Trigger", "fileUploaded")
	h.s3FileTree(w, r, bucketName)
}

// deleteAllVersions deletes all versions of an object including delete markers.
func (h *Handler) deleteAllVersions(ctx context.Context, bucketName, key string) error {
	log := logger.Load(ctx)
	output, err := h.S3.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
		Bucket: &bucketName,
		Prefix: &key,
	})
	if err != nil {
		log.ErrorContext(ctx, "Failed to list versions for deletion", "error", err)

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
				log.ErrorContext(ctx, "Failed to delete version",
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
				log.ErrorContext(ctx, "Failed to delete delete marker",
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
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)

		return
	}

	ctx := r.Context()
	log := logger.Load(ctx)

	if err := r.ParseForm(); err != nil {
		log.ErrorContext(ctx, "Failed to parse form", "error", err)
		http.Error(w, "Bad request", http.StatusBadRequest)

		return
	}

	bucketName := r.FormValue("bucketName")
	versioningEnabled := r.FormValue("versioning") == "on"

	// Create bucket
	_, err := h.S3.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: &bucketName,
	})
	if err != nil {
		log.ErrorContext(ctx, "Failed to create bucket", "bucket", bucketName, "error", err)
		toastMessage := fmt.Sprintf(
			`{"showToast": {"message": "Failed to create bucket: %s", "type": "error"}}`,
			strings.ReplaceAll(err.Error(), `"`, `'`),
		)
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
		}
	}

	// On success, return the updated bucket list
	h.s3BucketList(w, r)
}

// s3DeleteFile handles file deletion.
func (h *Handler) s3DeleteFile(w http.ResponseWriter, r *http.Request, bucketName, key string) {
	ctx := r.Context()
	log := logger.Load(ctx)

	if r.Method != http.MethodDelete {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)

		return
	}

	deleteAll := r.URL.Query().Get("deleteAll") == "true"
	versionID := r.URL.Query().Get("versionId")

	if deleteAll {
		if err := h.deleteAllVersions(ctx, bucketName, key); err != nil {
			http.Error(w, "Failed to delete all versions", http.StatusInternalServerError)

			return
		}
	} else {
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

	if r.Header.Get("Hx-Target") == "body" {
		w.Header().Set("Hx-Redirect", fmt.Sprintf("/dashboard/s3/bucket/%s", bucketName))

		return
	}

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(" "))
}

// s3DeleteBucket handles bucket deletion.
func (h *Handler) s3DeleteBucket(w http.ResponseWriter, r *http.Request, bucketName string) {
	if r.Method != http.MethodDelete {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)

		return
	}

	ctx := r.Context()
	log := logger.Load(ctx)

	_, err := h.S3.DeleteBucket(ctx, &s3.DeleteBucketInput{
		Bucket: &bucketName,
	})
	if err != nil {
		log.ErrorContext(ctx, "Failed to delete bucket", "bucket", bucketName, "error", err)
		toastMessage := fmt.Sprintf(
			`{"showToast": {"message": "Failed to delete bucket: %s", "type": "error"}}`,
			strings.ReplaceAll(err.Error(), `"`, `'`),
		)
		w.Header().Set("Hx-Trigger", toastMessage)
		w.WriteHeader(http.StatusUnprocessableEntity)

		return
	}

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(" "))
}

// s3Purge deletes all buckets and their contents.
func (h *Handler) s3Purge(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)

		return
	}

	ctx := r.Context()
	log := logger.Load(ctx)

	output, err := h.S3.ListBuckets(ctx, &s3.ListBucketsInput{})
	if err != nil {
		log.ErrorContext(ctx, "Failed to list buckets for purge", "error", err)
		http.Error(w, "Failed to list buckets", http.StatusInternalServerError)

		return
	}

	for _, bucket := range output.Buckets {
		bucketName := *bucket.Name
		h.purgeBucket(ctx, bucketName)
	}

	w.Header().Set("Hx-Trigger", "bucketsPurged")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(
		[]byte(
			`<div class="alert alert-success col-span-full"><span>All buckets purged successfully.</span></div>`,
		),
	)
}

func (h *Handler) purgeBucket(ctx context.Context, bucketName string) {
	log := logger.Load(ctx)
	versions, err := h.S3.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
		Bucket: &bucketName,
	})
	if err == nil {
		h.deleteObjectsInBucket(ctx, bucketName, versions)
	}

	_, err = h.S3.DeleteBucket(ctx, &s3.DeleteBucketInput{
		Bucket: &bucketName,
	})
	if err != nil {
		log.ErrorContext(
			ctx,
			"Failed to delete bucket during purge",
			"bucket",
			bucketName,
			"error",
			err,
		)
	}
}

func (h *Handler) deleteObjectsInBucket(
	ctx context.Context,
	bucketName string,
	versions *s3.ListObjectVersionsOutput,
) {
	log := logger.Load(ctx)
	objectsToDelete := make(
		[]types.ObjectIdentifier,
		0,
		len(versions.Versions)+len(versions.DeleteMarkers),
	)
	for _, v := range versions.Versions {
		objectsToDelete = append(objectsToDelete, types.ObjectIdentifier{
			Key:       v.Key,
			VersionId: v.VersionId,
		})
	}
	for _, dm := range versions.DeleteMarkers {
		objectsToDelete = append(objectsToDelete, types.ObjectIdentifier{
			Key:       dm.Key,
			VersionId: dm.VersionId,
		})
	}

	if len(objectsToDelete) > 0 {
		_, err := h.S3.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: &bucketName,
			Delete: &types.Delete{Objects: objectsToDelete},
		})
		if err != nil {
			log.ErrorContext(
				ctx,
				"Failed to delete objects during purge",
				"bucket",
				bucketName,
				"error",
				err,
			)
		}
	}
}

// s3ExportJSON exports object metadata as JSON.
func (h *Handler) s3ExportJSON(w http.ResponseWriter, r *http.Request, bucketName, key string) {
	ctx := r.Context()

	obj, err := h.S3.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: &bucketName,
		Key:    &key,
	})
	if err != nil {
		http.NotFound(w, r)

		return
	}

	tagging, _ := h.S3.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{
		Bucket: &bucketName,
		Key:    &key,
	})

	tags := make(map[string]string)
	if tagging != nil {
		for _, t := range tagging.TagSet {
			tags[*t.Key] = *t.Value
		}
	}

	export := struct {
		Tags        map[string]string `json:"tags"`
		Metadata    map[string]string `json:"metadata"`
		Bucket      string            `json:"bucket"`
		Key         string            `json:"key"`
		ContentType string            `json:"content_type"`
		ETag        string            `json:"etag"`
		Size        int64             `json:"size"`
	}{
		Bucket:      bucketName,
		Key:         key,
		ContentType: *obj.ContentType,
		Size:        *obj.ContentLength,
		ETag:        *obj.ETag,
		Tags:        tags,
		Metadata:    obj.Metadata,
	}

	w.Header().
		Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", key+"-metadata.json"))
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(export)
}

// s3Preview returns a text preview of an object.
func (h *Handler) s3Preview(w http.ResponseWriter, r *http.Request, bucketName, key string) {
	ctx := r.Context()
	log := logger.Load(ctx)

	const maxPreviewSize = 100 * 1024

	output, err := h.S3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &bucketName,
		Key:    &key,
	})
	if err != nil {
		http.Error(w, "Failed to get preview", http.StatusInternalServerError)

		return
	}
	defer output.Body.Close()

	lr := io.LimitReader(output.Body, maxPreviewSize)
	content, err := io.ReadAll(lr)
	if err != nil {
		log.ErrorContext(ctx, "Failed to read preview content", "error", err)
	}

	fmt.Fprintf(
		w,
		`<pre class="text-xs bg-base-300 p-2 rounded overflow-auto max-h-[600px] w-full">%s</pre>`,
		html.EscapeString(string(content)),
	)
}

// s3UpdateMetadata updates object Content-Type.
func (h *Handler) s3UpdateMetadata(w http.ResponseWriter, r *http.Request, bucketName, key string) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)

		return
	}

	ctx := r.Context()
	contentType := r.FormValue("contentType")

	_, err := h.S3.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:            &bucketName,
		Key:               &key,
		CopySource:        aws.String(url.PathEscape(bucketName + "/" + key)),
		ContentType:       &contentType,
		MetadataDirective: types.MetadataDirectiveReplace,
	})
	if err != nil {
		http.Error(w, "Failed to update metadata: "+err.Error(), http.StatusInternalServerError)

		return
	}

	w.Header().Set("Hx-Refresh", "true")
	w.WriteHeader(http.StatusOK)
}

// s3UpdateTag adds/updates an object tag.
func (h *Handler) s3UpdateTag(w http.ResponseWriter, r *http.Request, bucketName, key string) {
	ctx := r.Context()
	tagKey := r.FormValue("tagKey")
	tagValue := r.FormValue("tagValue")

	current, err := h.S3.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{
		Bucket: &bucketName,
		Key:    &key,
	})

	var tags []types.Tag
	if err == nil {
		tags = current.TagSet
	}

	found := false
	for i := range tags {
		if *tags[i].Key == tagKey {
			tags[i].Value = &tagValue
			found = true

			break
		}
	}
	if !found {
		tags = append(tags, types.Tag{Key: &tagKey, Value: &tagValue})
	}

	_, err = h.S3.PutObjectTagging(ctx, &s3.PutObjectTaggingInput{
		Bucket:  &bucketName,
		Key:     &key,
		Tagging: &types.Tagging{TagSet: tags},
	})
	if err != nil {
		http.Error(w, "Failed to update tags", http.StatusInternalServerError)

		return
	}

	h.renderTagsList(w, bucketName, key, tags)
}

// s3DeleteTag removes a tag.
func (h *Handler) s3DeleteTag(w http.ResponseWriter, r *http.Request, bucketName, key string) {
	ctx := r.Context()
	tagKey := r.URL.Query().Get("key")

	current, err := h.S3.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{
		Bucket: &bucketName,
		Key:    &key,
	})
	if err != nil {
		http.Error(w, "Failed to get tags", http.StatusInternalServerError)

		return
	}

	var newTags []types.Tag
	for _, t := range current.TagSet {
		if *t.Key != tagKey {
			newTags = append(newTags, t)
		}
	}

	_, err = h.S3.PutObjectTagging(ctx, &s3.PutObjectTaggingInput{
		Bucket:  &bucketName,
		Key:     &key,
		Tagging: &types.Tagging{TagSet: newTags},
	})
	if err != nil {
		http.Error(w, "Failed to delete tag", http.StatusInternalServerError)

		return
	}

	h.renderTagsList(w, bucketName, key, newTags)
}

func (h *Handler) renderTagsList(w http.ResponseWriter, bucketName, key string, tags []types.Tag) {
	for _, t := range tags {
		h.renderTagItem(w, bucketName, key, *t.Key, *t.Value)
	}
}

func (h *Handler) renderTagItem(w http.ResponseWriter, bucketName, key, tagKey, tagValue string) {
	// #nosec G705
	fmt.Fprintf(w, `
            <div class="flex justify-between items-center bg-base-200 px-3 py-1 rounded">
                <span class="text-xs font-mono"><b>%s</b>: %s</span>
                <button hx-delete="/dashboard/s3/bucket/%s/file/%s/tag?key=%s"
                    hx-target="#tags-list" class="btn btn-ghost btn-xs text-error">×</button>
            </div>`,
		html.EscapeString(tagKey),
		html.EscapeString(tagValue),
		url.PathEscape(bucketName),
		url.PathEscape(key),
		url.QueryEscape(tagKey))
}
