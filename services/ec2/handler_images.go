package ec2

import (
	"encoding/xml"
	"net/url"
)

type imageBlockPublicAccessStateResponse struct {
	XMLName                     xml.Name `xml:"GetImageBlockPublicAccessStateResponse"`
	RequestID                   string   `xml:"requestId"`
	ImageBlockPublicAccessState struct {
		State string `xml:"state"`
	} `xml:"imageBlockPublicAccessState"`
}

type enableImageBlockPublicAccessResponse struct {
	XMLName                     xml.Name `xml:"EnableImageBlockPublicAccessResponse"`
	RequestID                   string   `xml:"requestId"`
	ImageBlockPublicAccessState struct {
		State string `xml:"state"`
	} `xml:"imageBlockPublicAccessState"`
}

type disableImageBlockPublicAccessResponse struct {
	XMLName                     xml.Name `xml:"DisableImageBlockPublicAccessResponse"`
	RequestID                   string   `xml:"requestId"`
	ImageBlockPublicAccessState struct {
		State string `xml:"state"`
	} `xml:"imageBlockPublicAccessState"`
}

type describeInstanceImageMetadataResponse struct {
	XMLName                  xml.Name `xml:"DescribeInstanceImageMetadataResponse"`
	RequestID                string   `xml:"requestId"`
	InstanceImageMetadataSet struct {
		Items []instanceImageMetadataItem `xml:"item"`
	} `xml:"instanceImageMetadataSet"`
}

// ---- Handler implementations ----

func (h *Handler) handleDisableImage(vals url.Values, reqID string) (any, error) {
	imageID := vals.Get("ImageId")
	if err := h.Backend.DisableImage(imageID); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "DisableImageResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleEnableImage(vals url.Values, reqID string) (any, error) {
	imageID := vals.Get("ImageId")
	if err := h.Backend.EnableImage(imageID); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "EnableImageResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleEnableImageBlockPublicAccess(vals url.Values, reqID string) (any, error) {
	state := vals.Get("ImageBlockPublicAccessState")
	if state == "" {
		state = stateImageBlockNew
	}
	if err := h.Backend.EnableImageBlockPublicAccess(state); err != nil {
		return nil, err
	}

	resp := &enableImageBlockPublicAccessResponse{RequestID: reqID}
	resp.ImageBlockPublicAccessState.State = state

	return resp, nil
}

func (h *Handler) handleDisableImageBlockPublicAccess(_ url.Values, reqID string) (any, error) {
	h.Backend.DisableImageBlockPublicAccess()

	resp := &disableImageBlockPublicAccessResponse{RequestID: reqID}
	resp.ImageBlockPublicAccessState.State = stateImageUnblocked

	return resp, nil
}

func (h *Handler) handleGetImageBlockPublicAccessState(_ url.Values, reqID string) (any, error) {
	resp := &imageBlockPublicAccessStateResponse{RequestID: reqID}
	resp.ImageBlockPublicAccessState.State = h.Backend.GetImageBlockPublicAccessState()

	return resp, nil
}

func (h *Handler) handleEnableImageDeprecation(vals url.Values, reqID string) (any, error) {
	imageID := vals.Get("ImageId")
	deprecateAt := vals.Get("DeprecateAt")
	if err := h.Backend.EnableImageDeprecation(imageID, deprecateAt); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "EnableImageDeprecationResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDisableImageDeprecation(vals url.Values, reqID string) (any, error) {
	imageID := vals.Get("ImageId")
	if err := h.Backend.DisableImageDeprecation(imageID); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "DisableImageDeprecationResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleEnableImageDeregistrationProtection(
	vals url.Values,
	reqID string,
) (any, error) {
	imageID := vals.Get("ImageId")
	if err := h.Backend.EnableImageDeregistrationProtection(imageID); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "EnableImageDeregistrationProtectionResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDisableImageDeregistrationProtection(
	vals url.Values,
	reqID string,
) (any, error) {
	imageID := vals.Get("ImageId")
	if err := h.Backend.DisableImageDeregistrationProtection(imageID); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "DisableImageDeregistrationProtectionResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleModifyImageAttribute(vals url.Values, reqID string) (any, error) {
	imageID := vals.Get("ImageId")
	attribute := vals.Get("Attribute")
	value := vals.Get("Value")
	if err := h.Backend.ModifyImageAttribute(imageID, attribute, value); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "ModifyImageAttributeResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleResetImageAttribute(vals url.Values, reqID string) (any, error) {
	imageID := vals.Get("ImageId")
	attribute := vals.Get("Attribute")
	if err := h.Backend.ResetImageAttribute(imageID, attribute); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "ResetImageAttributeResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDescribeInstanceImageMetadata(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "InstanceId")
	items := h.Backend.DescribeInstanceImageMetadata(ids)

	resp := &describeInstanceImageMetadataResponse{RequestID: reqID}
	for _, item := range items {
		resp.InstanceImageMetadataSet.Items = append(
			resp.InstanceImageMetadataSet.Items,
			instanceImageMetadataItem(item),
		)
	}

	return resp, nil
}

type importImageResponse struct {
	XMLName      xml.Name `xml:"ImportImageResponse"`
	RequestID    string   `xml:"requestId"`
	ImportTaskID string   `xml:"importTaskId"`
	Status       string   `xml:"status"`
}

type describeImportImageTasksResponse struct {
	XMLName            xml.Name `xml:"DescribeImportImageTasksResponse"`
	RequestID          string   `xml:"requestId"`
	ImportImageTaskSet struct {
		Items []importImageTaskItem `xml:"item"`
	} `xml:"importImageTaskSet"`
}

type exportTaskS3LocationItem struct {
	S3Bucket string `xml:"s3Bucket,omitempty"`
	S3Prefix string `xml:"s3Prefix,omitempty"`
}

type exportImageResponse struct {
	XMLName           xml.Name                 `xml:"ExportImageResponse"`
	RequestID         string                   `xml:"requestId"`
	Description       string                   `xml:"description,omitempty"`
	DiskImageFormat   string                   `xml:"diskImageFormat,omitempty"`
	ExportImageTaskID string                   `xml:"exportImageTaskId,omitempty"`
	ImageID           string                   `xml:"imageId,omitempty"`
	Progress          string                   `xml:"progress,omitempty"`
	S3ExportLocation  exportTaskS3LocationItem `xml:"s3ExportLocation"`
	Status            string                   `xml:"status,omitempty"`
	StatusMessage     string                   `xml:"statusMessage,omitempty"`
	RoleName          string                   `xml:"roleName,omitempty"`
}

type exportImageTaskItem struct {
	Description       string                   `xml:"description,omitempty"`
	ExportImageTaskID string                   `xml:"exportImageTaskId,omitempty"`
	ImageID           string                   `xml:"imageId,omitempty"`
	Progress          string                   `xml:"progress,omitempty"`
	S3ExportLocation  exportTaskS3LocationItem `xml:"s3ExportLocation"`
	Status            string                   `xml:"status,omitempty"`
	StatusMessage     string                   `xml:"statusMessage,omitempty"`
}

func toExportImageTaskItem(t *ExportImageTaskRec) exportImageTaskItem {
	return exportImageTaskItem{
		Description:       t.Description,
		ExportImageTaskID: t.ExportImageTaskID,
		ImageID:           t.ImageID,
		Progress:          t.Progress,
		S3ExportLocation:  exportTaskS3LocationItem{S3Bucket: t.S3Bucket, S3Prefix: t.S3Prefix},
		Status:            t.Status,
		StatusMessage:     t.StatusMessage,
	}
}

type describeExportImageTasksResponse struct {
	XMLName            xml.Name `xml:"DescribeExportImageTasksResponse"`
	RequestID          string   `xml:"requestId"`
	ExportImageTaskSet struct {
		Items []exportImageTaskItem `xml:"item"`
	} `xml:"exportImageTaskSet"`
}

type recycleBinImageItem struct {
	ImageID string `xml:"imageId"`
	Name    string `xml:"name"`
}

type listImagesInRecycleBinResponse struct {
	XMLName   xml.Name `xml:"ListImagesInRecycleBinResponse"`
	RequestID string   `xml:"requestId"`
	ImageSet  struct {
		Items []recycleBinImageItem `xml:"item"`
	} `xml:"imageSet"`
}

type recycleBinSnapshotItem struct {
	SnapshotID string `xml:"snapshotId"`
}

type describeFastLaunchImagesResponse struct {
	XMLName            xml.Name `xml:"DescribeFastLaunchImagesResponse"`
	RequestID          string   `xml:"requestId"`
	FastLaunchImageSet struct {
		Items []fastLaunchImageItem `xml:"item"`
	} `xml:"fastLaunchImageSet"`
}

type fastSnapshotRestoreItem struct {
	SnapshotID       string `xml:"snapshotId"`
	AvailabilityZone string `xml:"availabilityZone"`
	State            string `xml:"state"`
}

type registerImageResponse struct {
	XMLName   xml.Name `xml:"RegisterImageResponse"`
	RequestID string   `xml:"requestId"`
	ImageID   string   `xml:"imageId"`
}

func (h *Handler) handleRegisterImage(vals url.Values, reqID string) (any, error) {
	name := vals.Get("Name")
	description := vals.Get("Description")
	arch := vals.Get("Architecture")

	img, err := h.Backend.RegisterImage(name, description, arch)
	if err != nil {
		return nil, err
	}

	return &registerImageResponse{
		RequestID: reqID,
		ImageID:   img.ImageID,
	}, nil
}

func (h *Handler) handleImportImage(vals url.Values, reqID string) (any, error) {
	description := vals.Get("Description")
	arch := vals.Get("Architecture")
	platform := vals.Get("Platform")

	task, err := h.Backend.ImportImage(description, arch, platform)
	if err != nil {
		return nil, err
	}

	return &importImageResponse{
		RequestID:    reqID,
		ImportTaskID: task.ImportTaskID,
		Status:       task.Status,
	}, nil
}

func (h *Handler) handleDescribeImportImageTasks(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "ImportTaskId")
	tasks := h.Backend.DescribeImportImageTasks(ids)

	resp := &describeImportImageTasksResponse{RequestID: reqID}
	for _, t := range tasks {
		resp.ImportImageTaskSet.Items = append(resp.ImportImageTaskSet.Items, importImageTaskItem{
			ImportTaskID: t.ImportTaskID,
			Description:  t.Description,
			Architecture: t.Architecture,
			Platform:     t.Platform,
			Status:       t.Status,
		})
	}

	return resp, nil
}

func (h *Handler) handleExportImage(vals url.Values, reqID string) (any, error) {
	imageID := vals.Get("ImageId")
	description := vals.Get("Description")
	diskImageFormat := vals.Get("DiskImageFormat")
	s3Bucket := vals.Get("S3ExportLocation.S3Bucket")
	s3Prefix := vals.Get("S3ExportLocation.S3Prefix")
	roleName := vals.Get("RoleName")

	task, err := h.Backend.ExportImage(imageID, description, diskImageFormat, s3Bucket, s3Prefix, roleName)
	if err != nil {
		return nil, err
	}

	return &exportImageResponse{
		RequestID:         reqID,
		Description:       task.Description,
		DiskImageFormat:   task.DiskImageFormat,
		ExportImageTaskID: task.ExportImageTaskID,
		ImageID:           task.ImageID,
		Progress:          task.Progress,
		S3ExportLocation:  exportTaskS3LocationItem{S3Bucket: task.S3Bucket, S3Prefix: task.S3Prefix},
		Status:            task.Status,
		StatusMessage:     task.StatusMessage,
		RoleName:          task.RoleName,
	}, nil
}

func (h *Handler) handleDescribeExportImageTasks(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "ExportImageTaskId")
	tasks := h.Backend.DescribeExportImageTasks(ids)

	resp := &describeExportImageTasksResponse{RequestID: reqID}
	for _, t := range tasks {
		resp.ExportImageTaskSet.Items = append(resp.ExportImageTaskSet.Items, toExportImageTaskItem(t))
	}

	return resp, nil
}

func (h *Handler) handleListImagesInRecycleBin(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "ImageId")
	images := h.Backend.ListImagesInRecycleBin(ids)

	resp := &listImagesInRecycleBinResponse{RequestID: reqID}
	for _, img := range images {
		resp.ImageSet.Items = append(resp.ImageSet.Items, recycleBinImageItem{
			ImageID: img.ImageID,
			Name:    img.Name,
		})
	}

	return resp, nil
}

func (h *Handler) handleRestoreImageFromRecycleBin(vals url.Values, reqID string) (any, error) {
	imageID := vals.Get("ImageId")
	if err := h.Backend.RestoreImageFromRecycleBin(imageID); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "RestoreImageFromRecycleBinResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleEnableFastLaunch(vals url.Values, reqID string) (any, error) {
	imageID := vals.Get("ImageId")
	if err := h.Backend.EnableFastLaunch(imageID); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "EnableFastLaunchResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDisableFastLaunch(vals url.Values, reqID string) (any, error) {
	imageID := vals.Get("ImageId")
	if err := h.Backend.DisableFastLaunch(imageID); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "DisableFastLaunchResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDescribeFastLaunchImages(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "ImageId")
	items := h.Backend.DescribeFastLaunchImages(ids)

	resp := &describeFastLaunchImagesResponse{RequestID: reqID}
	for _, item := range items {
		resp.FastLaunchImageSet.Items = append(
			resp.FastLaunchImageSet.Items,
			fastLaunchImageItem(item),
		)
	}

	return resp, nil
}

func (h *Handler) handleCopyImage(vals url.Values, reqID string) (any, error) {
	image, err := h.Backend.CopyImage(
		vals.Get("SourceImageId"),
		vals.Get("Name"),
		vals.Get("Description"),
	)
	if err != nil {
		return nil, err
	}

	return &copyImageResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		ImageID:   image.ImageID,
	}, nil
}

func (h *Handler) handleDeregisterImage(vals url.Values, _ string) (any, error) {
	return nil, h.Backend.DeregisterImage(vals.Get("ImageId"))
}

// ---- VPC / Subnet attribute handlers ----

type copyImageResponse struct {
	XMLName   xml.Name `xml:"CopyImageResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	ImageID   string   `xml:"imageId"`
}

type genericReturnResponse struct {
	XMLName   xml.Name `xml:"Response"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Return    string   `xml:"return"`
}

func (h *Handler) handleCancelImageLaunchPermission(vals url.Values, reqID string) (any, error) {
	imageID := vals.Get("ImageId")

	if err := h.Backend.CancelImageLaunchPermission(imageID); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "CancelImageLaunchPermissionResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

type imageReferenceItem struct {
	ImageID      string `xml:"imageId,omitempty"`
	ResourceType string `xml:"resourceType,omitempty"`
	Arn          string `xml:"arn,omitempty"`
}

type describeImageReferencesResponse struct {
	XMLName           xml.Name `xml:"DescribeImageReferencesResponse"`
	RequestID         string   `xml:"requestId"`
	ImageReferenceSet struct {
		Items []imageReferenceItem `xml:"item"`
	} `xml:"imageReferenceSet"`
}

func (h *Handler) handleDescribeImageReferences(vals url.Values, reqID string) (any, error) {
	imageIDs := parseMemberList(vals, "ImageId")

	refs := h.Backend.DescribeImageReferences(imageIDs)

	resp := &describeImageReferencesResponse{RequestID: reqID}
	for _, r := range refs {
		resp.ImageReferenceSet.Items = append(resp.ImageReferenceSet.Items, imageReferenceItem{
			ImageID:      r.ImageID,
			ResourceType: r.ResourceType,
			Arn:          r.Arn,
		})
	}

	return resp, nil
}

type imageAncestryEntryItem struct {
	ImageID       string `xml:"imageId,omitempty"`
	SourceImageID string `xml:"sourceImageId,omitempty"`
}

type getImageAncestryResponse struct {
	XMLName               xml.Name `xml:"GetImageAncestryResponse"`
	RequestID             string   `xml:"requestId"`
	ImageAncestryEntrySet struct {
		Items []imageAncestryEntryItem `xml:"item"`
	} `xml:"imageAncestryEntrySet"`
}

func (h *Handler) handleGetImageAncestry(vals url.Values, reqID string) (any, error) {
	imageID := vals.Get("ImageId")

	entries, err := h.Backend.GetImageAncestry(imageID)
	if err != nil {
		return nil, err
	}

	resp := &getImageAncestryResponse{RequestID: reqID}
	for _, e := range entries {
		resp.ImageAncestryEntrySet.Items = append(resp.ImageAncestryEntrySet.Items, imageAncestryEntryItem{
			ImageID:       e.ImageID,
			SourceImageID: e.SourceImageID,
		})
	}

	return resp, nil
}

// registerImagesOps registers the Images operation handlers.
func registerImagesOps(h *Handler, ops map[string]ec2ActionFn) {
	ops["DisableImage"] = h.handleDisableImage
	ops["EnableImage"] = h.handleEnableImage
	ops["EnableImageBlockPublicAccess"] = h.handleEnableImageBlockPublicAccess
	ops["DisableImageBlockPublicAccess"] = h.handleDisableImageBlockPublicAccess
	ops["GetImageBlockPublicAccessState"] = h.handleGetImageBlockPublicAccessState
	ops["EnableImageDeprecation"] = h.handleEnableImageDeprecation
	ops["DisableImageDeprecation"] = h.handleDisableImageDeprecation
	ops["EnableImageDeregistrationProtection"] = h.handleEnableImageDeregistrationProtection
	ops["DisableImageDeregistrationProtection"] = h.handleDisableImageDeregistrationProtection
	ops["ModifyImageAttribute"] = h.handleModifyImageAttribute
	ops["ResetImageAttribute"] = h.handleResetImageAttribute
	ops["DescribeInstanceImageMetadata"] = h.handleDescribeInstanceImageMetadata
	ops["RegisterImage"] = h.handleRegisterImage
	ops["ImportImage"] = h.handleImportImage
	ops["DescribeImportImageTasks"] = h.handleDescribeImportImageTasks
	ops["ExportImage"] = h.handleExportImage
	ops["DescribeExportImageTasks"] = h.handleDescribeExportImageTasks
	ops["ListImagesInRecycleBin"] = h.handleListImagesInRecycleBin
	ops["RestoreImageFromRecycleBin"] = h.handleRestoreImageFromRecycleBin
	ops["EnableFastLaunch"] = h.handleEnableFastLaunch
	ops["DisableFastLaunch"] = h.handleDisableFastLaunch
	ops["DescribeFastLaunchImages"] = h.handleDescribeFastLaunchImages
	ops["CopyImage"] = h.handleCopyImage
	ops["DeregisterImage"] = h.handleDeregisterImage
	ops["CancelImageLaunchPermission"] = h.handleCancelImageLaunchPermission
	ops["DescribeImageReferences"] = h.handleDescribeImageReferences
	ops["GetImageAncestry"] = h.handleGetImageAncestry
}

// imagesSupportedOperations lists the operation names registered by
// registerImagesOps, for GetSupportedOperations().
func imagesSupportedOperations() []string {
	return []string{
		"DisableImage",
		"EnableImage",
		"EnableImageBlockPublicAccess",
		"DisableImageBlockPublicAccess",
		"GetImageBlockPublicAccessState",
		"EnableImageDeprecation",
		"DisableImageDeprecation",
		"EnableImageDeregistrationProtection",
		"DisableImageDeregistrationProtection",
		"ModifyImageAttribute",
		"ResetImageAttribute",
		"DescribeInstanceImageMetadata",
		"RegisterImage",
		"ImportImage",
		"DescribeImportImageTasks",
		"ExportImage",
		"DescribeExportImageTasks",
		"ListImagesInRecycleBin",
		"RestoreImageFromRecycleBin",
		"EnableFastLaunch",
		"DisableFastLaunch",
		"DescribeFastLaunchImages",
		"CopyImage",
		"DeregisterImage",
		"CancelImageLaunchPermission",
		"DescribeImageReferences",
		"GetImageAncestry",
	}
}
