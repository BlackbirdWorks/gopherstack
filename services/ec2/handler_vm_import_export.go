package ec2

import (
	"encoding/xml"
	"net/url"
	"strconv"
	"time"
)

// handler_vm_import_export.go implements the HTTP handlers for the Bundle Task, Conversion
// Task (ImportInstance/ImportVolume), and Instance Export Task families, backed by
// vm_import_export.go. ExportImage/DescribeExportImageTasks are handled in
// handler_batch3.go alongside the rest of the image lifecycle operations.

// ---- Registration ----

func registerVMImportExportOps(h *Handler, ops map[string]ec2ActionFn) {
	ops["BundleInstance"] = h.handleBundleInstance
	ops["CancelBundleTask"] = h.handleCancelBundleTask
	ops["DescribeBundleTasks"] = h.handleDescribeBundleTasks

	ops["ImportInstance"] = h.handleImportInstance
	ops["ImportVolume"] = h.handleImportVolume
	ops["DescribeConversionTasks"] = h.handleDescribeConversionTasks
	ops["CancelConversionTask"] = h.handleCancelConversionTask

	ops["CreateInstanceExportTask"] = h.handleCreateInstanceExportTask
	ops["CancelExportTask"] = h.handleCancelExportTask
	ops["DescribeExportTasks"] = h.handleDescribeExportTasks

	ops["CancelImportTask"] = h.handleCancelImportTask
}

// vmImportExportSupportedOperations lists the operation names registered by
// registerVMImportExportOps, for GetSupportedOperations().
func vmImportExportSupportedOperations() []string {
	return []string{
		"BundleInstance",
		"CancelBundleTask",
		"DescribeBundleTasks",
		"ImportInstance",
		"ImportVolume",
		"DescribeConversionTasks",
		"CancelConversionTask",
		"CreateInstanceExportTask",
		"CancelExportTask",
		"DescribeExportTasks",
		"CancelImportTask",
	}
}

// ---- Response shapes: Bundle tasks ----

type s3StorageItem struct {
	Bucket string `xml:"bucket,omitempty"`
	Prefix string `xml:"prefix,omitempty"`
}

type bundleStorageItem struct {
	S3 s3StorageItem `xml:"S3"`
}

type bundleTaskErrorItem struct {
	Code    string `xml:"code,omitempty"`
	Message string `xml:"message,omitempty"`
}

type bundleTaskItem struct {
	BundleID   string              `xml:"bundleId,omitempty"`
	InstanceID string              `xml:"instanceId,omitempty"`
	Progress   string              `xml:"progress,omitempty"`
	StartTime  string              `xml:"startTime,omitempty"`
	State      string              `xml:"state,omitempty"`
	Storage    bundleStorageItem   `xml:"storage"`
	UpdateTime string              `xml:"updateTime,omitempty"`
	Error      bundleTaskErrorItem `xml:"error,omitempty"`
}

func toBundleTaskItem(t *BundleTask) bundleTaskItem {
	item := bundleTaskItem{
		BundleID:   t.BundleID,
		InstanceID: t.InstanceID,
		Progress:   t.Progress,
		StartTime:  t.StartTime.UTC().Format(time.RFC3339),
		State:      t.State,
		Storage:    bundleStorageItem{S3: s3StorageItem{Bucket: t.S3Bucket, Prefix: t.S3Prefix}},
		UpdateTime: t.UpdateTime.UTC().Format(time.RFC3339),
	}
	if t.ErrorCode != "" {
		item.Error = bundleTaskErrorItem{Code: t.ErrorCode, Message: t.ErrorMessage}
	}

	return item
}

type bundleInstanceResponse struct {
	XMLName            xml.Name       `xml:"BundleInstanceResponse"`
	Xmlns              string         `xml:"xmlns,attr"`
	RequestID          string         `xml:"requestId"`
	BundleInstanceTask bundleTaskItem `xml:"bundleInstanceTask"`
}

type cancelBundleTaskResponse struct {
	XMLName            xml.Name       `xml:"CancelBundleTaskResponse"`
	Xmlns              string         `xml:"xmlns,attr"`
	RequestID          string         `xml:"requestId"`
	BundleInstanceTask bundleTaskItem `xml:"bundleInstanceTask"`
}

type describeBundleTasksResponse struct {
	XMLName                xml.Name `xml:"DescribeBundleTasksResponse"`
	Xmlns                  string   `xml:"xmlns,attr"`
	RequestID              string   `xml:"requestId"`
	BundleInstanceTasksSet struct {
		Items []bundleTaskItem `xml:"item"`
	} `xml:"bundleInstanceTasksSet"`
}

// ---- Response shapes: Conversion tasks ----

type diskImageDescriptionItem struct {
	Format string `xml:"format,omitempty"`
	Size   int64  `xml:"size,omitempty"`
}

type volumeDetailItem struct {
	ID   string `xml:"id,omitempty"`
	Size int64  `xml:"size,omitempty"`
}

type importInstanceVolumeDetailItem struct {
	AvailabilityZone string                   `xml:"availabilityZone,omitempty"`
	Description      string                   `xml:"description,omitempty"`
	Status           string                   `xml:"status,omitempty"`
	Image            diskImageDescriptionItem `xml:"image"`
	Volume           volumeDetailItem         `xml:"volume"`
	BytesConverted   int64                    `xml:"bytesConverted"`
}

type importInstanceTaskDetailsItem struct {
	Description string `xml:"description,omitempty"`
	InstanceID  string `xml:"instanceId,omitempty"`
	Platform    string `xml:"platform,omitempty"`
	Volumes     struct {
		Items []importInstanceVolumeDetailItem `xml:"item"`
	} `xml:"volumes"`
}

type importVolumeTaskDetailsItem struct {
	AvailabilityZone string                   `xml:"availabilityZone,omitempty"`
	Description      string                   `xml:"description,omitempty"`
	Image            diskImageDescriptionItem `xml:"image"`
	Volume           volumeDetailItem         `xml:"volume"`
	BytesConverted   int64                    `xml:"bytesConverted"`
}

type conversionTaskItem struct {
	ConversionTaskID string                         `xml:"conversionTaskId,omitempty"`
	ExpirationTime   string                         `xml:"expirationTime,omitempty"`
	ImportInstance   *importInstanceTaskDetailsItem `xml:"importInstance,omitempty"`
	ImportVolume     *importVolumeTaskDetailsItem   `xml:"importVolume,omitempty"`
	State            string                         `xml:"state,omitempty"`
	StatusMessage    string                         `xml:"statusMessage,omitempty"`
}

func toConversionTaskItem(t *ConversionTask) conversionTaskItem {
	item := conversionTaskItem{
		ConversionTaskID: t.ConversionTaskID,
		ExpirationTime:   t.ExpirationTime.UTC().Format(time.RFC3339),
		State:            t.State,
		StatusMessage:    t.StatusMessage,
	}

	image := diskImageDescriptionItem{Format: t.ImageFormat, Size: t.ImageBytes}
	volume := volumeDetailItem{Size: t.VolumeSize}

	switch t.Kind {
	case conversionKindInstance:
		details := &importInstanceTaskDetailsItem{
			Description: t.Description,
			InstanceID:  t.InstanceID,
			Platform:    t.Platform,
		}
		volume.ID = t.InstanceID
		details.Volumes.Items = []importInstanceVolumeDetailItem{
			{
				AvailabilityZone: t.AvailabilityZone,
				Description:      t.Description,
				Image:            image,
				Volume:           volume,
				Status:           t.State,
			},
		}
		item.ImportInstance = details
	case conversionKindVolume:
		volume.ID = t.VolumeID
		item.ImportVolume = &importVolumeTaskDetailsItem{
			AvailabilityZone: t.AvailabilityZone,
			Description:      t.Description,
			Image:            image,
			Volume:           volume,
		}
	}

	return item
}

type importInstanceResponse struct {
	XMLName        xml.Name           `xml:"ImportInstanceResponse"`
	Xmlns          string             `xml:"xmlns,attr"`
	RequestID      string             `xml:"requestId"`
	ConversionTask conversionTaskItem `xml:"conversionTask"`
}

type importVolumeResponse struct {
	XMLName        xml.Name           `xml:"ImportVolumeResponse"`
	Xmlns          string             `xml:"xmlns,attr"`
	RequestID      string             `xml:"requestId"`
	ConversionTask conversionTaskItem `xml:"conversionTask"`
}

type describeConversionTasksResponse struct {
	XMLName         xml.Name `xml:"DescribeConversionTasksResponse"`
	Xmlns           string   `xml:"xmlns,attr"`
	RequestID       string   `xml:"requestId"`
	ConversionTasks struct {
		Items []conversionTaskItem `xml:"item"`
	} `xml:"conversionTasks"`
}

type cancelConversionTaskResponse struct {
	XMLName   xml.Name `xml:"CancelConversionTaskResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
}

// ---- Response shapes: Instance export tasks ----

type exportToS3TaskItem struct {
	ContainerFormat string `xml:"containerFormat,omitempty"`
	DiskImageFormat string `xml:"diskImageFormat,omitempty"`
	S3Bucket        string `xml:"s3Bucket,omitempty"`
	S3Key           string `xml:"s3Key,omitempty"`
}

type instanceExportDetailsItem struct {
	InstanceID        string `xml:"instanceId,omitempty"`
	TargetEnvironment string `xml:"targetEnvironment,omitempty"`
}

type exportTaskItem struct {
	Description           string                    `xml:"description,omitempty"`
	ExportTaskID          string                    `xml:"exportTaskId,omitempty"`
	State                 string                    `xml:"state,omitempty"`
	StatusMessage         string                    `xml:"statusMessage,omitempty"`
	InstanceExportDetails instanceExportDetailsItem `xml:"instanceExportDetails"`
	ExportToS3Task        exportToS3TaskItem        `xml:"exportToS3"`
}

func toExportTaskItem(t *ExportTask) exportTaskItem {
	return exportTaskItem{
		Description:   t.Description,
		ExportTaskID:  t.ExportTaskID,
		State:         t.State,
		StatusMessage: t.StatusMessage,
		InstanceExportDetails: instanceExportDetailsItem{
			InstanceID:        t.InstanceID,
			TargetEnvironment: t.TargetEnvironment,
		},
		ExportToS3Task: exportToS3TaskItem{
			ContainerFormat: t.ContainerFormat,
			DiskImageFormat: t.DiskImageFormat,
			S3Bucket:        t.S3Bucket,
			S3Key:           t.S3Key,
		},
	}
}

type createInstanceExportTaskResponse struct {
	XMLName    xml.Name       `xml:"CreateInstanceExportTaskResponse"`
	Xmlns      string         `xml:"xmlns,attr"`
	RequestID  string         `xml:"requestId"`
	ExportTask exportTaskItem `xml:"exportTask"`
}

type describeExportTasksResponse struct {
	XMLName       xml.Name `xml:"DescribeExportTasksResponse"`
	Xmlns         string   `xml:"xmlns,attr"`
	RequestID     string   `xml:"requestId"`
	ExportTaskSet struct {
		Items []exportTaskItem `xml:"item"`
	} `xml:"exportTaskSet"`
}

type cancelExportTaskResponse struct {
	XMLName   xml.Name `xml:"CancelExportTaskResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
}

// ---- Response shapes: CancelImportTask ----

type cancelImportTaskResponse struct {
	XMLName       xml.Name `xml:"CancelImportTaskResponse"`
	Xmlns         string   `xml:"xmlns,attr"`
	RequestID     string   `xml:"requestId"`
	ImportTaskID  string   `xml:"importTaskId,omitempty"`
	PreviousState string   `xml:"previousState,omitempty"`
	State         string   `xml:"state,omitempty"`
}

// ---- Handlers: Bundle tasks ----

func (h *Handler) handleBundleInstance(vals url.Values, reqID string) (any, error) {
	task, err := h.Backend.BundleInstance(
		vals.Get("InstanceId"), vals.Get("Storage.S3.Bucket"), vals.Get("Storage.S3.Prefix"),
	)
	if err != nil {
		return nil, err
	}

	return &bundleInstanceResponse{Xmlns: ec2XMLNS, RequestID: reqID, BundleInstanceTask: toBundleTaskItem(task)}, nil
}

func (h *Handler) handleCancelBundleTask(vals url.Values, reqID string) (any, error) {
	task, err := h.Backend.CancelBundleTask(vals.Get("BundleId"))
	if err != nil {
		return nil, err
	}

	return &cancelBundleTaskResponse{Xmlns: ec2XMLNS, RequestID: reqID, BundleInstanceTask: toBundleTaskItem(task)}, nil
}

func (h *Handler) handleDescribeBundleTasks(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "BundleId")
	tasks := h.Backend.DescribeBundleTasks(ids)

	resp := &describeBundleTasksResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, t := range tasks {
		resp.BundleInstanceTasksSet.Items = append(resp.BundleInstanceTasksSet.Items, toBundleTaskItem(t))
	}

	return resp, nil
}

// ---- Handlers: Conversion tasks ----

func (h *Handler) handleImportInstance(vals url.Values, reqID string) (any, error) {
	diskBytes, _ := strconv.ParseInt(vals.Get("DiskImage.1.Image.Bytes"), 10, 64)
	volumeSize, _ := strconv.ParseInt(vals.Get("DiskImage.1.Volume.Size"), 10, 64)

	task, err := h.Backend.ImportInstance(
		vals.Get("Description"),
		vals.Get("Platform"),
		vals.Get("LaunchSpecification.Placement.AvailabilityZone"),
		vals.Get("DiskImage.1.Image.Format"),
		diskBytes,
		volumeSize,
	)
	if err != nil {
		return nil, err
	}

	return &importInstanceResponse{Xmlns: ec2XMLNS, RequestID: reqID, ConversionTask: toConversionTaskItem(task)}, nil
}

func (h *Handler) handleImportVolume(vals url.Values, reqID string) (any, error) {
	diskBytes, _ := strconv.ParseInt(vals.Get("Image.Bytes"), 10, 64)
	volumeSize, _ := strconv.ParseInt(vals.Get("Volume.Size"), 10, 64)

	task, err := h.Backend.ImportVolume(
		vals.Get("Description"), vals.Get("AvailabilityZone"), vals.Get("Image.Format"), diskBytes, volumeSize,
	)
	if err != nil {
		return nil, err
	}

	return &importVolumeResponse{Xmlns: ec2XMLNS, RequestID: reqID, ConversionTask: toConversionTaskItem(task)}, nil
}

func (h *Handler) handleDescribeConversionTasks(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "ConversionTaskId")
	tasks := h.Backend.DescribeConversionTasks(ids)

	resp := &describeConversionTasksResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, t := range tasks {
		resp.ConversionTasks.Items = append(resp.ConversionTasks.Items, toConversionTaskItem(t))
	}

	return resp, nil
}

func (h *Handler) handleCancelConversionTask(vals url.Values, reqID string) (any, error) {
	if _, err := h.Backend.CancelConversionTask(vals.Get("ConversionTaskId")); err != nil {
		return nil, err
	}

	return &cancelConversionTaskResponse{Xmlns: ec2XMLNS, RequestID: reqID}, nil
}

// ---- Handlers: Instance export tasks ----

func (h *Handler) handleCreateInstanceExportTask(vals url.Values, reqID string) (any, error) {
	task, err := h.Backend.CreateInstanceExportTask(
		vals.Get("InstanceId"),
		vals.Get("Description"),
		vals.Get("TargetEnvironment"),
		vals.Get("ExportToS3.DiskImageFormat"),
		vals.Get("ExportToS3.ContainerFormat"),
		vals.Get("ExportToS3.S3Bucket"),
		vals.Get("ExportToS3.S3Prefix"),
	)
	if err != nil {
		return nil, err
	}

	return &createInstanceExportTaskResponse{
		Xmlns: ec2XMLNS, RequestID: reqID, ExportTask: toExportTaskItem(task),
	}, nil
}

func (h *Handler) handleCancelExportTask(vals url.Values, reqID string) (any, error) {
	if err := h.Backend.CancelExportTask(vals.Get("ExportTaskId")); err != nil {
		return nil, err
	}

	return &cancelExportTaskResponse{Xmlns: ec2XMLNS, RequestID: reqID}, nil
}

func (h *Handler) handleDescribeExportTasks(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "ExportTaskId")
	tasks := h.Backend.DescribeExportTasks(ids)

	resp := &describeExportTasksResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, t := range tasks {
		resp.ExportTaskSet.Items = append(resp.ExportTaskSet.Items, toExportTaskItem(t))
	}

	return resp, nil
}

// ---- Handlers: CancelImportTask ----

func (h *Handler) handleCancelImportTask(vals url.Values, reqID string) (any, error) {
	importTaskID := vals.Get("ImportTaskId")

	previousState, state, err := h.Backend.CancelImportTask(importTaskID)
	if err != nil {
		return nil, err
	}

	return &cancelImportTaskResponse{
		Xmlns: ec2XMLNS, RequestID: reqID, ImportTaskID: importTaskID, PreviousState: previousState, State: state,
	}, nil
}
