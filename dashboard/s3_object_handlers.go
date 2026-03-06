package dashboard

import (
	"context"
	"encoding/json"
	"fmt"
	"html"
	"io"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// s3ObjectExportData is the JSON export structure for S3 object metadata.
type s3ObjectExportData struct {
	Tags        map[string]string `json:"tags"`
	Metadata    map[string]string `json:"metadata"`
	Bucket      string            `json:"bucket"`
	Key         string            `json:"key"`
	ContentType string            `json:"content_type"`
	ETag        string            `json:"etag"`
	Size        int64             `json:"size"`
}

// s3FileTree returns the file tree as HTML fragment.
func (h *DashboardHandler) s3FileTree(w http.ResponseWriter, r *http.Request, bucketName string) {
	ctx := r.Context()
	prefix := r.URL.Query().Get("prefix")
	input := &s3.ListObjectsV2Input{
		Bucket:    &bucketName,
		Prefix:    &prefix,
		Delimiter: aws.String("/"),
		MaxKeys:   aws.Int32(maxKeys),
	}

	output, err := h.S3.ListObjectsV2(ctx, input)
	if err != nil {
		http.Error(w, "Failed to list objects", http.StatusInternalServerError)

		return
	}

	var items []FileTreeItem

	// Folders (CommonPrefixes)
	for _, cp := range output.CommonPrefixes {
		p := *cp.Prefix
		items = append(items, FileTreeItem{
			Name:       path.Base(p),
			FullPath:   p,
			BucketName: bucketName,
			IsFolder:   true,
		})
	}

	// Files
	for _, obj := range output.Contents {
		if *obj.Key == prefix {
			continue // Skip the directory itself if it appears
		}
		items = append(items, FileTreeItem{
			Name:         path.Base(*obj.Key),
			FullPath:     *obj.Key,
			Size:         formatBytes(*obj.Size),
			LastModified: obj.LastModified.Format("2006-01-02 15:04:05"),
			BucketName:   bucketName,
			IsFolder:     false,
		})
	}

	// Sort: folders first, then files, both alphabetically
	sort.Slice(items, func(i, j int) bool {
		if items[i].IsFolder != items[j].IsFolder {
			return items[i].IsFolder
		}

		return items[i].Name < items[j].Name
	})

	if len(items) == 0 {
		h.renderFragment(w, "s3-empty", bucketName)
	}

	for _, item := range items {
		h.renderFragment(w, "file-tree-item", item)
	}
}

// s3FileDetail renders the file detail page.
func (h *DashboardHandler) s3FileDetail(w http.ResponseWriter, r *http.Request, bucketName, key string) {
	ctx := r.Context()
	log := logger.Load(ctx)

	obj, err := h.S3.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: &bucketName,
		Key:    &key,
	})
	if err != nil {
		log.ErrorContext(ctx, "Failed to get object info", "bucket", bucketName, "key", key, "error", err)
		http.NotFound(w, r)

		return
	}

	h.handleS3FileDetail(ctx, w, bucketName, key, obj)
}

func (h *DashboardHandler) handleS3FileDetail(
	ctx context.Context,
	w http.ResponseWriter,
	bucketName, key string,
	obj *s3.HeadObjectOutput,
) {
	versioning := h.isS3BucketVersioningEnabled(ctx, bucketName)
	tags := h.fetchS3ObjectTags(ctx, bucketName, key)
	versions := h.fetchS3ObjectVersions(ctx, bucketName, key, versioning)

	data := h.prepareS3FileDetailData(bucketName, key, obj, versioning, tags, versions)
	h.renderTemplate(w, "s3/file_detail.html", data)
}

func (h *DashboardHandler) isS3BucketVersioningEnabled(ctx context.Context, bucketName string) bool {
	v, err := h.S3.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{Bucket: &bucketName})
	if err != nil {
		return false
	}

	return v.Status == types.BucketVersioningStatusEnabled
}

func (h *DashboardHandler) fetchS3ObjectTags(ctx context.Context, bucketName, key string) map[string]string {
	tags := make(map[string]string)
	tagging, err := h.S3.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{
		Bucket: &bucketName,
		Key:    &key,
	})
	if err == nil && tagging != nil {
		for _, t := range tagging.TagSet {
			tags[*t.Key] = *t.Value
		}
	}

	return tags
}

func (h *DashboardHandler) fetchS3ObjectVersions(
	ctx context.Context,
	bucketName, key string,
	enabled bool,
) []S3FileVersion {
	if !enabled {
		return nil
	}
	versions := make([]S3FileVersion, 0)
	vOutput, err := h.S3.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
		Bucket: &bucketName,
		Prefix: &key,
	})
	if err == nil {
		for _, v := range vOutput.Versions {
			if *v.Key == key {
				versions = append(versions, S3FileVersion{
					VersionID:    aws.ToString(v.VersionId),
					LastModified: v.LastModified.Format("2006-01-02 15:04:05"),
				})
			}
		}
	}

	return versions
}

func (h *DashboardHandler) prepareS3FileDetailData(
	bucketName, key string,
	obj *s3.HeadObjectOutput,
	versioningEnabled bool,
	tags map[string]string,
	versionList []S3FileVersion,
) s3FileDetailData {
	contentType := aws.ToString(obj.ContentType)

	return s3FileDetailData{
		PageData: PageData{
			Title:     "File: " + path.Base(key),
			ActiveTab: "s3",
			Snippet: &SnippetData{
				ID:    "s3-operations",
				Title: "Using S3",
				Cli:   `aws s3 help --endpoint-url http://localhost:8000`,
				Go: `// Initialize AWS SDK v2 for Using S3
cfg, err := config.LoadDefaultConfig(context.TODO(),
    config.WithEndpointResolverWithOptions(
        aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
            return aws.Endpoint{URL: "http://localhost:8000"}, nil
        }),
    ),
)
if err != nil {
    log.Fatal(err)
}
client := s3.NewFromConfig(cfg)`,
				Python: `# Initialize boto3 client for Using S3
import boto3

client = boto3.client('s3', endpoint_url='http://localhost:8000')`,
			},
		},
		BucketName:        bucketName,
		Key:               key,
		Size:              formatBytes(*obj.ContentLength),
		LastModified:      obj.LastModified.Format("2006-01-02 15:04:05"),
		ContentType:       contentType,
		ETag:              aws.ToString(obj.ETag),
		Tags:              tags,
		Versions:          versionList,
		VersioningEnabled: versioningEnabled,
		IsImage:           strings.HasPrefix(contentType, "image/"),
		IsPDF:             contentType == "application/pdf",
		IsText:            strings.HasPrefix(contentType, "text/") || contentType == "application/json",
	}
}

// s3Download downloads a file.
func (h *DashboardHandler) s3Download(w http.ResponseWriter, r *http.Request, bucketName, key string) {
	ctx := r.Context()
	versionID := r.URL.Query().Get("versionId")

	input := &s3.GetObjectInput{
		Bucket: &bucketName,
		Key:    &key,
	}
	if versionID != "" {
		input.VersionId = &versionID
	}

	output, err := h.S3.GetObject(ctx, input)
	if err != nil {
		http.NotFound(w, r)

		return
	}
	defer output.Body.Close()

	// Set download headers
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", key))
	w.Header().Set("Content-Type", aws.ToString(output.ContentType))
	w.Header().Set("Content-Length", strconv.FormatInt(*output.ContentLength, 10))

	_, _ = io.Copy(w, output.Body)
}

// s3Upload handles file upload.
func (h *DashboardHandler) s3Upload(w http.ResponseWriter, r *http.Request, bucketName string) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)

		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxMultipartMemory)

	ctx := r.Context()
	log := logger.Load(ctx)

	if err := r.ParseMultipartForm(maxMultipartMemory); err != nil {
		log.ErrorContext(ctx, "Failed to parse multipart form", "error", err)
		http.Error(w, "Failed to parse form", http.StatusBadRequest)

		return
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "No file uploaded", http.StatusBadRequest)

		return
	}
	defer file.Close()

	// Determine key: use "key" field if present (from tests), or prefix + filename (from UI)
	key := r.FormValue("key")
	if key == "" {
		key = r.FormValue("prefix") + header.Filename
	}
	contentType := header.Header.Get("Content-Type")

	_, err = h.S3.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      &bucketName,
		Key:         &key,
		Body:        file,
		ContentType: &contentType,
	})

	if err != nil {
		log.ErrorContext(ctx, "Failed to upload file", "bucket", bucketName, "key", key, "error", err)
		toastMessage := fmt.Sprintf(
			`{"showToast": {"message": "Failed to upload file: %s", "type": "error"}}`,
			strings.ReplaceAll(err.Error(), `"`, `'`),
		)
		w.Header().Set("Hx-Trigger", toastMessage)
		w.WriteHeader(http.StatusUnprocessableEntity)

		return
	}

	// Refresh the tree
	h.s3FileTree(w, r, bucketName)
}

// deleteAllVersions deletes all versions of an object including delete markers.
func (h *DashboardHandler) deleteAllVersions(ctx context.Context, bucketName, key string) error {
	vOutput, err := h.S3.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
		Bucket: &bucketName,
		Prefix: &key,
	})
	if err != nil {
		return err
	}

	var objects []types.ObjectIdentifier
	for _, v := range vOutput.Versions {
		if *v.Key == key {
			objects = append(objects, types.ObjectIdentifier{
				Key:       v.Key,
				VersionId: v.VersionId,
			})
		}
	}
	for _, dm := range vOutput.DeleteMarkers {
		if *dm.Key == key {
			objects = append(objects, types.ObjectIdentifier{
				Key:       dm.Key,
				VersionId: dm.VersionId,
			})
		}
	}

	if len(objects) == 0 {
		return nil
	}

	_, err = h.S3.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: &bucketName,
		Delete: &types.Delete{Objects: objects},
	})

	return err
}

// s3DeleteFile handles file deletion.
func (h *DashboardHandler) s3DeleteFile(w http.ResponseWriter, r *http.Request, bucketName, key string) {
	ctx := r.Context()
	log := logger.Load(ctx)

	if r.Method != http.MethodDelete {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)

		return
	}

	deleteAll := r.URL.Query().Get("deleteAll") == constStrTrue
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

// s3ExportJSON exports object metadata as JSON.
func (h *DashboardHandler) s3ExportJSON(w http.ResponseWriter, r *http.Request, bucketName, key string) {
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

	export := s3ObjectExportData{
		Bucket:      bucketName,
		Key:         key,
		ContentType: aws.ToString(obj.ContentType),
		Size:        aws.ToInt64(obj.ContentLength),
		ETag:        aws.ToString(obj.ETag),
		Tags:        tags,
		Metadata:    obj.Metadata,
	}

	w.Header().
		Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", path.Base(key)+"-metadata.json"))
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(export)
}

// s3Preview returns a text preview of an object.
func (h *DashboardHandler) s3Preview(w http.ResponseWriter, r *http.Request, bucketName, key string) {
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
func (h *DashboardHandler) s3UpdateMetadata(w http.ResponseWriter, r *http.Request, bucketName, key string) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)

		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxFormBodySize)

	ctx := r.Context()
	contentType := r.FormValue("contentType")
	log := logger.Load(ctx)
	log.DebugContext(ctx, "s3UpdateMetadata", "bucket", bucketName, "key", key, "newContentType", contentType)

	_, err := h.S3.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:            &bucketName,
		Key:               &key,
		CopySource:        aws.String(bucketName + "/" + url.PathEscape(key)),
		ContentType:       &contentType,
		MetadataDirective: types.MetadataDirectiveReplace,
	})
	if err != nil {
		log.ErrorContext(ctx, "Failed to update metadata", "error", err)
		http.Error(w, "Failed to update metadata: "+err.Error(), http.StatusInternalServerError)

		return
	}

	w.Header().Set("Hx-Refresh", "true")
	w.WriteHeader(http.StatusOK)
}

// s3UpdateTag adds/updates an object tag.
func (h *DashboardHandler) s3UpdateTag(w http.ResponseWriter, r *http.Request, bucketName, key string) {
	r.Body = http.MaxBytesReader(w, r.Body, maxFormBodySize)

	ctx := r.Context()
	tagKey := r.FormValue("key")
	tagValue := r.FormValue("value")

	current, err := h.S3.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{
		Bucket: &bucketName,
		Key:    &key,
	})

	var tags []types.Tag
	if err == nil {
		tags = current.TagSet
	}

	tags = upsertS3Tag(tags, tagKey, tagValue)

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

// upsertS3Tag updates the value of an existing tag matching key, or appends a new tag.
func upsertS3Tag(tags []types.Tag, key, value string) []types.Tag {
	for i := range tags {
		if *tags[i].Key == key {
			tags[i].Value = &value

			return tags
		}
	}

	return append(tags, types.Tag{Key: &key, Value: &value})
}

// s3DeleteTag removes a tag.
func (h *DashboardHandler) s3DeleteTag(w http.ResponseWriter, r *http.Request, bucketName, key string) {
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

func (h *DashboardHandler) renderTagsList(w http.ResponseWriter, bucketName, key string, tags []types.Tag) {
	for _, t := range tags {
		h.renderTagItem(w, bucketName, key, *t.Key, *t.Value)
	}
}

func (h *DashboardHandler) renderTagItem(w http.ResponseWriter, bucketName, key, tagKey, tagValue string) {
	// #nosec G705
	fmt.Fprintf(w, `
            <div class="flex justify-between items-center bg-base-200 px-3 py-1 rounded">
                <span class="text-xs font-mono"><b>%s</b>: %s</span>
                <button hx-delete="/dashboard/s3/bucket/%s/file/%s/tag?key=%s"
                    hx-target="#tags-list" class="btn btn-ghost btn-xs text-error">×</button>
            </div>`,
		html.EscapeString(tagKey),
		html.EscapeString(tagValue),
		html.EscapeString(url.PathEscape(bucketName)),
		html.EscapeString(url.PathEscape(key)),
		html.EscapeString(url.QueryEscape(tagKey)))
}
