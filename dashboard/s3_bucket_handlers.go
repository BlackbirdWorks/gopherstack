package dashboard

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"Gopherstack/pkgs/logger"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// s3Index renders the S3 index page.
func (h *DashboardHandler) s3Index(w http.ResponseWriter, _ *http.Request) {
	data := PageData{
		Title:     "S3 Buckets",
		ActiveTab: "s3",
	}
	h.renderTemplate(w, "s3/s3_index.html", data)
}

// s3BucketList returns the list of buckets as HTML fragment.
func (h *DashboardHandler) s3BucketList(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	output, err := h.S3.ListBuckets(ctx, &s3.ListBucketsInput{})
	if err != nil {
		h.Logger.Error("Failed to list buckets", "error", err)
		http.Error(w, "Failed to list buckets", http.StatusInternalServerError)

		return
	}

	buckets := output.Buckets
	search := strings.ToLower(r.URL.Query().Get("search"))
	if search != "" {
		filtered := make([]types.Bucket, 0)
		for _, b := range buckets {
			if strings.Contains(strings.ToLower(aws.ToString(b.Name)), search) {
				filtered = append(filtered, b)
			}
		}
		buckets = filtered
	}

	offset := 0
	if offStr := r.URL.Query().Get("offset"); offStr != "" {
		offset, _ = strconv.Atoi(offStr)
	}

	h.handleS3BucketListing(ctx, w, buckets, offset)
}

func (h *DashboardHandler) handleS3BucketListing(
	_ context.Context,
	w http.ResponseWriter,
	buckets []types.Bucket,
	offset int,
) {
	const limit = 20
	end := min(offset+limit, len(buckets))

	subset := buckets[offset:end]
	for _, b := range subset {
		info := BucketInfo{
			Name:         aws.ToString(b.Name),
			CreationDate: b.CreationDate.Format("2006-01-02 15:04:05"),
		}
		h.renderFragment(w, "bucket-card", info)
	}

	if end < len(buckets) {
		// #nosec G705
		fmt.Fprintf(w, `
            <button class="btn btn-outline col-span-full mt-4" 
                hx-get="/dashboard/s3/buckets?offset=%d" 
                hx-target="this" 
                hx-swap="outerHTML"
                hx-indicator=".htmx-indicator">
                Load More
            </button>`, end)
	}
}

// s3BucketDetail renders the bucket detail page.
func (h *DashboardHandler) s3BucketDetail(w http.ResponseWriter, r *http.Request, bucketName string) {
	ctx := r.Context()
	log := logger.Load(ctx)

	// Verify bucket exists
	_, err := h.S3.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: &bucketName})
	if err != nil {
		log.ErrorContext(ctx, "Bucket not found", "bucket", bucketName, "error", err)

		var nsb *types.NoSuchBucket
		if errors.As(err, &nsb) || strings.Contains(err.Error(), "StatusCode: 404") {
			http.NotFound(w, r)

			return
		}

		http.Error(w, "Failed to access bucket", http.StatusInternalServerError)

		return
	}

	versioning := h.isS3BucketVersioningEnabled(ctx, bucketName)

	data := BucketInfo{
		PageData: PageData{
			Title:     "Bucket: " + bucketName,
			ActiveTab: "s3",
		},
		Name:              bucketName,
		VersioningEnabled: versioning,
	}

	h.renderTemplate(w, "s3/bucket_detail.html", data)
}

// s3Versioning handles bucket versioning configuration.
func (h *DashboardHandler) s3Versioning(w http.ResponseWriter, r *http.Request, bucketName string) {
	if r.Method != http.MethodPost && r.Method != http.MethodPut {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)

		return
	}

	ctx := r.Context()
	enabled := r.FormValue("versioning") == "on" || r.FormValue("enabled") == constStrTrue

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
		http.Error(w, "Failed to update versioning", http.StatusInternalServerError)

		return
	}

	w.Header().Set("Hx-Refresh", "true")
	w.WriteHeader(http.StatusOK)
}

// s3CreateBucket handles bucket creation requests.
func (h *DashboardHandler) s3CreateBucket(w http.ResponseWriter, r *http.Request) {
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
	versioningEnabled := r.FormValue("versioning") == "on" || r.FormValue("enabled") == constStrTrue

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

// s3DeleteBucket handles bucket deletion.
func (h *DashboardHandler) s3DeleteBucket(w http.ResponseWriter, r *http.Request, bucketName string) {
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
func (h *DashboardHandler) s3Purge(w http.ResponseWriter, r *http.Request) {
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

func (h *DashboardHandler) purgeBucket(ctx context.Context, bucketName string) {
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

func (h *DashboardHandler) deleteObjectsInBucket(
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
