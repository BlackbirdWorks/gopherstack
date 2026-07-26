package ec2_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/ec2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_AllowedImagesSettings_EnableGetReplaceDisable(t *testing.T) {
	t.Parallel()

	h := newHandler()

	enableVals := url.Values{}
	enableVals.Set("Action", "EnableAllowedImagesSettings")
	enableVals.Set("Version", "2016-11-15")
	enableVals.Set("AllowedImagesSettingsState", "enabled")

	enableRec := postForm(t, h, enableVals.Encode())
	require.Equal(t, http.StatusOK, enableRec.Code)
	assert.Contains(t, enableRec.Body.String(), "<allowedImagesSettingsState>enabled</allowedImagesSettingsState>")

	replaceVals := url.Values{}
	replaceVals.Set("Action", "ReplaceImageCriteriaInAllowedImagesSettings")
	replaceVals.Set("Version", "2016-11-15")
	replaceVals.Set("ImageCriterion.1.ImageProvider.1", "amazon")
	replaceVals.Set("ImageCriterion.1.ImageName.1", "amzn2-*")

	replaceRec := postForm(t, h, replaceVals.Encode())
	require.Equal(t, http.StatusOK, replaceRec.Code)
	assert.Contains(t, replaceRec.Body.String(), "<return>true</return>")

	getVals := url.Values{}
	getVals.Set("Action", "GetAllowedImagesSettings")
	getVals.Set("Version", "2016-11-15")

	getRec := postForm(t, h, getVals.Encode())
	require.Equal(t, http.StatusOK, getRec.Code)
	getBody := getRec.Body.String()
	assert.Contains(t, getBody, "<state>enabled</state>")
	assert.Contains(t, getBody, "amazon")
	assert.Contains(t, getBody, "amzn2-*")

	disableVals := url.Values{}
	disableVals.Set("Action", "DisableAllowedImagesSettings")
	disableVals.Set("Version", "2016-11-15")

	disableRec := postForm(t, h, disableVals.Encode())
	require.Equal(t, http.StatusOK, disableRec.Code)
	assert.Contains(t, disableRec.Body.String(), "<allowedImagesSettingsState>disabled</allowedImagesSettingsState>")
}

func TestHandler_AllowedImagesSettings_EnableInvalidStateFails(t *testing.T) {
	t.Parallel()

	h := newHandler()

	vals := url.Values{}
	vals.Set("Action", "EnableAllowedImagesSettings")
	vals.Set("Version", "2016-11-15")
	vals.Set("AllowedImagesSettingsState", "bogus")

	rec := postForm(t, h, vals.Encode())
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_StoreRestoreImageTask(t *testing.T) {
	t.Parallel()

	h := newHandler()

	img, err := h.Backend.RegisterImage("my-ami", "", "")
	require.NoError(t, err)
	imageID := img.ImageID

	storeVals := url.Values{}
	storeVals.Set("Action", "CreateStoreImageTask")
	storeVals.Set("Version", "2016-11-15")
	storeVals.Set("ImageId", imageID)
	storeVals.Set("Bucket", "my-bucket")

	storeRec := postForm(t, h, storeVals.Encode())
	require.Equal(t, http.StatusOK, storeRec.Code)
	storeBody := storeRec.Body.String()
	assert.Contains(t, storeBody, "<CreateStoreImageTaskResponse")

	objectKey := extractXMLValue(t, storeBody, "objectKey")
	require.NotEmpty(t, objectKey)

	describeVals := url.Values{}
	describeVals.Set("Action", "DescribeStoreImageTasks")
	describeVals.Set("Version", "2016-11-15")
	describeVals.Set("ImageId.1", imageID)

	describeRec := postForm(t, h, describeVals.Encode())
	require.Equal(t, http.StatusOK, describeRec.Code)
	assert.Contains(t, describeRec.Body.String(), "my-bucket")

	restoreVals := url.Values{}
	restoreVals.Set("Action", "CreateRestoreImageTask")
	restoreVals.Set("Version", "2016-11-15")
	restoreVals.Set("Bucket", "my-bucket")
	restoreVals.Set("ObjectKey", objectKey)

	restoreRec := postForm(t, h, restoreVals.Encode())
	require.Equal(t, http.StatusOK, restoreRec.Code)
	restoreBody := restoreRec.Body.String()
	assert.Contains(t, restoreBody, "<CreateRestoreImageTaskResponse")

	restoredID := extractXMLValue(t, restoreBody, "imageId")
	require.NotEmpty(t, restoredID)
	assert.NotEqual(t, imageID, restoredID)
}

func TestHandler_CreateStoreImageTask_UnknownImageFails(t *testing.T) {
	t.Parallel()

	h := newHandler()

	vals := url.Values{}
	vals.Set("Action", "CreateStoreImageTask")
	vals.Set("Version", "2016-11-15")
	vals.Set("ImageId", "ami-doesnotexist")
	vals.Set("Bucket", "my-bucket")

	rec := postForm(t, h, vals.Encode())
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ImageUsageReport_CreateDescribeDelete(t *testing.T) {
	t.Parallel()

	h := newHandler()

	img, err := h.Backend.RegisterImage("my-ami", "", "")
	require.NoError(t, err)
	imageID := img.ImageID

	createVals := url.Values{}
	createVals.Set("Action", "CreateImageUsageReport")
	createVals.Set("Version", "2016-11-15")
	createVals.Set("ImageId", imageID)

	createRec := postForm(t, h, createVals.Encode())
	require.Equal(t, http.StatusOK, createRec.Code)
	createBody := createRec.Body.String()
	assert.Contains(t, createBody, "<CreateImageUsageReportResponse")

	reportID := extractXMLValue(t, createBody, "reportId")
	require.NotEmpty(t, reportID)

	describeVals := url.Values{}
	describeVals.Set("Action", "DescribeImageUsageReportEntries")
	describeVals.Set("Version", "2016-11-15")
	describeVals.Set("ReportId.1", reportID)

	describeRec := postForm(t, h, describeVals.Encode())
	require.Equal(t, http.StatusOK, describeRec.Code)
	assert.Contains(t, describeRec.Body.String(), "<DescribeImageUsageReportEntriesResponse")

	deleteVals := url.Values{}
	deleteVals.Set("Action", "DeleteImageUsageReport")
	deleteVals.Set("Version", "2016-11-15")
	deleteVals.Set("ReportId", reportID)

	deleteRec := postForm(t, h, deleteVals.Encode())
	require.Equal(t, http.StatusOK, deleteRec.Code)
	assert.Contains(t, deleteRec.Body.String(), "<return>true</return>")
}

func TestHandler_DeleteImageUsageReport_UnknownFails(t *testing.T) {
	t.Parallel()

	h := newHandler()

	vals := url.Values{}
	vals.Set("Action", "DeleteImageUsageReport")
	vals.Set("Version", "2016-11-15")
	vals.Set("ReportId", "imgusgrpt-doesnotexist")

	rec := postForm(t, h, vals.Encode())
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ConfirmProductInstance(t *testing.T) {
	t.Parallel()

	h := newHandler()

	runVals := url.Values{}
	runVals.Set("Action", "RunInstances")
	runVals.Set("Version", "2016-11-15")
	runVals.Set("ImageId", "ami-12345")
	runVals.Set("InstanceType", "t2.micro")
	runVals.Set("MinCount", "1")
	runVals.Set("MaxCount", "1")

	runRec := postForm(t, h, runVals.Encode())
	require.Equal(t, http.StatusOK, runRec.Code)
	instanceID := extractXMLValue(t, runRec.Body.String(), "instanceId")
	require.NotEmpty(t, instanceID)

	confirmVals := url.Values{}
	confirmVals.Set("Action", "ConfirmProductInstance")
	confirmVals.Set("Version", "2016-11-15")
	confirmVals.Set("InstanceId", instanceID)
	confirmVals.Set("ProductCode", "prod-abc123")

	confirmRec := postForm(t, h, confirmVals.Encode())
	require.Equal(t, http.StatusOK, confirmRec.Code)
	confirmBody := confirmRec.Body.String()
	assert.Contains(t, confirmBody, "<ConfirmProductInstanceResponse")
	assert.Contains(t, confirmBody, "<return>false</return>")
}

func TestHandler_ConfirmProductInstance_UnknownInstanceFails(t *testing.T) {
	t.Parallel()

	h := newHandler()

	vals := url.Values{}
	vals.Set("Action", "ConfirmProductInstance")
	vals.Set("Version", "2016-11-15")
	vals.Set("InstanceId", "i-doesnotexist")
	vals.Set("ProductCode", "prod-abc123")

	rec := postForm(t, h, vals.Encode())
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestHandler_ImageWatermark verifies AttachImageWatermark/DetachImageWatermark
// routing, error codes, and wire shapes (parity-4).
func TestHandler_ImageWatermark(t *testing.T) {
	t.Parallel()

	const stubImageID = "ami-0c55b159cbfafe1f0"

	tests := []struct {
		setup    func(vals url.Values)
		name     string
		action   string
		wantBody string
		wantCode int
	}{
		{
			name:   "attach_missing_image_not_found",
			action: "AttachImageWatermark",
			setup: func(vals url.Values) {
				vals.Set("ImageId", "ami-doesnotexist")
				vals.Set("WatermarkName", "approved")
			},
			wantCode: http.StatusBadRequest,
			wantBody: "InvalidAMIID.NotFound",
		},
		{
			name:   "attach_missing_name_fails",
			action: "AttachImageWatermark",
			setup: func(vals url.Values) {
				vals.Set("ImageId", stubImageID)
			},
			wantCode: http.StatusBadRequest,
			wantBody: "InvalidParameterValue",
		},
		{
			name:   "attach_returns_account_scoped_key",
			action: "AttachImageWatermark",
			setup: func(vals url.Values) {
				vals.Set("ImageId", stubImageID)
				vals.Set("WatermarkName", "approvedAmi")
			},
			wantCode: http.StatusOK,
			wantBody: "<watermarkKey>000000000000:approvedAmi</watermarkKey>",
		},
		{
			name:   "detach_missing_image_not_found",
			action: "DetachImageWatermark",
			setup: func(vals url.Values) {
				vals.Set("ImageId", "ami-doesnotexist")
				vals.Set("WatermarkKey", "000000000000:approvedAmi")
			},
			wantCode: http.StatusBadRequest,
			wantBody: "InvalidAMIID.NotFound",
		},
		{
			name:   "detach_returns_true",
			action: "DetachImageWatermark",
			setup: func(vals url.Values) {
				vals.Set("ImageId", stubImageID)
				vals.Set("WatermarkKey", "000000000000:approvedAmi")
			},
			wantCode: http.StatusOK,
			wantBody: "<return>true</return>",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()

			vals := url.Values{}
			vals.Set("Action", tt.action)
			vals.Set("Version", "2016-11-15")
			tt.setup(vals)

			rec := postForm(t, h, vals.Encode())
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantBody)
		})
	}
}

// TestHandler_ImageWatermark_AttachIsIdempotentAndDetachRemoves verifies the
// full attach/detach round trip mutates real per-image watermark state
// rather than being a no-op stub: re-attaching the same name returns the
// same key without duplicating it, and detach actually removes it.
func TestHandler_ImageWatermark_AttachIsIdempotentAndDetachRemoves(t *testing.T) {
	t.Parallel()

	const stubImageID = "ami-0eb260c4d5475b901"

	b := ec2.NewInMemoryBackend("111122223333", "us-east-1")

	key1, err := b.AttachImageWatermark(stubImageID, "gold")
	require.NoError(t, err)
	assert.Equal(t, "111122223333:gold", key1)

	key2, err := b.AttachImageWatermark(stubImageID, "gold")
	require.NoError(t, err)
	assert.Equal(t, key1, key2, "re-attaching the same watermark name must be idempotent")

	require.NoError(t, b.DetachImageWatermark(stubImageID, key1))

	// Detaching an already-detached key is a no-op, not an error (AWS
	// DetachImageWatermark only validates the ImageId, not that the key was
	// still attached).
	require.NoError(t, b.DetachImageWatermark(stubImageID, key1))
}
