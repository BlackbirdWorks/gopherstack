package ec2_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_BundleTask_CreateDescribeCancel(t *testing.T) {
	t.Parallel()

	h := newHandler()

	instances, err := h.Backend.RunInstances("ami-test", "t3.micro", "", 1)
	require.NoError(t, err)
	instanceID := instances[0].ID

	createVals := url.Values{}
	createVals.Set("Action", "BundleInstance")
	createVals.Set("Version", "2016-11-15")
	createVals.Set("InstanceId", instanceID)
	createVals.Set("Storage.S3.Bucket", "my-bucket")
	createVals.Set("Storage.S3.Prefix", "windows/")

	createRec := postForm(t, h, createVals.Encode())
	require.Equal(t, http.StatusOK, createRec.Code)
	createBody := createRec.Body.String()
	assert.Contains(t, createBody, "<BundleInstanceResponse")
	assert.Contains(t, createBody, "<state>pending</state>")

	bundleID := extractXMLValue(t, createBody, "bundleId")
	require.NotEmpty(t, bundleID)

	describeVals := url.Values{}
	describeVals.Set("Action", "DescribeBundleTasks")
	describeVals.Set("Version", "2016-11-15")
	describeVals.Set("BundleId.1", bundleID)

	describeRec := postForm(t, h, describeVals.Encode())
	require.Equal(t, http.StatusOK, describeRec.Code)
	assert.Contains(t, describeRec.Body.String(), "<state>complete</state>")

	cancelVals := url.Values{}
	cancelVals.Set("Action", "CancelBundleTask")
	cancelVals.Set("Version", "2016-11-15")
	cancelVals.Set("BundleId", bundleID)

	cancelRec := postForm(t, h, cancelVals.Encode())
	require.Equal(t, http.StatusBadRequest, cancelRec.Code)
	assert.Contains(t, cancelRec.Body.String(), "<Code>IncorrectState</Code>")
}

func TestHandler_ConversionTask_ImportInstanceDescribeCancel(t *testing.T) {
	t.Parallel()

	h := newHandler()

	importVals := url.Values{}
	importVals.Set("Action", "ImportInstance")
	importVals.Set("Version", "2016-11-15")
	importVals.Set("Description", "test import")
	importVals.Set("Platform", "Linux")
	importVals.Set("DiskImage.1.Image.Format", "VMDK")
	importVals.Set("DiskImage.1.Image.Bytes", "1024")
	importVals.Set("DiskImage.1.Volume.Size", "20")

	importRec := postForm(t, h, importVals.Encode())
	require.Equal(t, http.StatusOK, importRec.Code)
	importBody := importRec.Body.String()
	assert.Contains(t, importBody, "<ImportInstanceResponse")
	assert.Contains(t, importBody, "<state>active</state>")

	taskID := extractXMLValue(t, importBody, "conversionTaskId")
	require.NotEmpty(t, taskID)

	describeVals := url.Values{}
	describeVals.Set("Action", "DescribeConversionTasks")
	describeVals.Set("Version", "2016-11-15")
	describeVals.Set("ConversionTaskId.1", taskID)

	describeRec := postForm(t, h, describeVals.Encode())
	require.Equal(t, http.StatusOK, describeRec.Code)
	assert.Contains(t, describeRec.Body.String(), "<state>completed</state>")

	cancelVals := url.Values{}
	cancelVals.Set("Action", "CancelConversionTask")
	cancelVals.Set("Version", "2016-11-15")
	cancelVals.Set("ConversionTaskId", taskID)

	cancelRec := postForm(t, h, cancelVals.Encode())
	require.Equal(t, http.StatusBadRequest, cancelRec.Code)
	assert.Contains(t, cancelRec.Body.String(), "<Code>IncorrectState</Code>")
}

func TestHandler_ConversionTask_ImportVolume(t *testing.T) {
	t.Parallel()

	h := newHandler()

	vals := url.Values{}
	vals.Set("Action", "ImportVolume")
	vals.Set("Version", "2016-11-15")
	vals.Set("AvailabilityZone", "us-east-1a")
	vals.Set("Image.Format", "VMDK")
	vals.Set("Image.Bytes", "2048")
	vals.Set("Volume.Size", "40")

	rec := postForm(t, h, vals.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "<ImportVolumeResponse")
	assert.Contains(t, body, "<importVolume>")
}

func TestHandler_ExportTask_CreateDescribeCancel(t *testing.T) {
	t.Parallel()

	h := newHandler()

	instances, err := h.Backend.RunInstances("ami-test", "t3.micro", "", 1)
	require.NoError(t, err)
	instanceID := instances[0].ID

	createVals := url.Values{}
	createVals.Set("Action", "CreateInstanceExportTask")
	createVals.Set("Version", "2016-11-15")
	createVals.Set("InstanceId", instanceID)
	createVals.Set("Description", "export test")
	createVals.Set("TargetEnvironment", "citrix")
	createVals.Set("ExportToS3Task.DiskImageFormat", "VMDK")
	createVals.Set("ExportToS3Task.ContainerFormat", "OVA")
	createVals.Set("ExportToS3Task.S3Bucket", "my-bucket")
	createVals.Set("ExportToS3Task.S3Prefix", "exports/")

	createRec := postForm(t, h, createVals.Encode())
	require.Equal(t, http.StatusOK, createRec.Code)
	createBody := createRec.Body.String()
	assert.Contains(t, createBody, "<CreateInstanceExportTaskResponse")

	taskID := extractXMLValue(t, createBody, "exportTaskId")
	require.NotEmpty(t, taskID)

	describeVals := url.Values{}
	describeVals.Set("Action", "DescribeExportTasks")
	describeVals.Set("Version", "2016-11-15")
	describeVals.Set("ExportTaskId.1", taskID)

	describeRec := postForm(t, h, describeVals.Encode())
	require.Equal(t, http.StatusOK, describeRec.Code)
	assert.Contains(t, describeRec.Body.String(), "<state>completed</state>")

	cancelVals := url.Values{}
	cancelVals.Set("Action", "CancelExportTask")
	cancelVals.Set("Version", "2016-11-15")
	cancelVals.Set("ExportTaskId", taskID)

	cancelRec := postForm(t, h, cancelVals.Encode())
	require.Equal(t, http.StatusBadRequest, cancelRec.Code)
	assert.Contains(t, cancelRec.Body.String(), "<Code>IncorrectState</Code>")
}

func TestHandler_ExportImageTask_DescribeSettles(t *testing.T) {
	t.Parallel()

	h := newHandler()

	exportVals := url.Values{}
	exportVals.Set("Action", "ExportImage")
	exportVals.Set("Version", "2016-11-15")
	exportVals.Set("ImageId", "ami-test")
	exportVals.Set("DiskImageFormat", "raw")
	exportVals.Set("S3ExportLocation.S3Bucket", "my-export-bucket")
	exportVals.Set("S3ExportLocation.S3Prefix", "exports/")

	exportRec := postForm(t, h, exportVals.Encode())
	require.Equal(t, http.StatusOK, exportRec.Code)
	exportBody := exportRec.Body.String()
	assert.Contains(t, exportBody, "<ExportImageResponse")

	taskID := extractXMLValue(t, exportBody, "exportImageTaskId")
	require.NotEmpty(t, taskID)

	describeVals := url.Values{}
	describeVals.Set("Action", "DescribeExportImageTasks")
	describeVals.Set("Version", "2016-11-15")
	describeVals.Set("ExportImageTaskId.1", taskID)

	describeRec := postForm(t, h, describeVals.Encode())
	require.Equal(t, http.StatusOK, describeRec.Code)
	assert.Contains(t, describeRec.Body.String(), "<status>completed</status>")
}

func TestHandler_CancelImportTask_NotFound(t *testing.T) {
	t.Parallel()

	h := newHandler()

	vals := url.Values{}
	vals.Set("Action", "CancelImportTask")
	vals.Set("Version", "2016-11-15")
	vals.Set("ImportTaskId", "import-ami-doesnotexist")

	rec := postForm(t, h, vals.Encode())
	require.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "<Code>InvalidParameterValue</Code>")
}
