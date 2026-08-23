package ec2

import (
	"encoding/xml"
	"fmt"
	"net/url"
	"strconv"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// ImageBlockPublicAccessState is a flat scalar in the real shape (ec2@v1.319.1
// deserializers.go, awsEc2query_deserializeOpDocumentGetImageBlockPublicAccessStateOutput):
// <imageBlockPublicAccessState> holds the state text directly, no nested
// <state> child. A nested struct here makes the real decoder's Value() call
// hard-error (smithy-go xml_decoder.go's Value: "got StartElement instead"),
// not just silently drop the field.
type imageBlockPublicAccessStateResponse struct {
	XMLName                     xml.Name `xml:"GetImageBlockPublicAccessStateResponse"`
	RequestID                   string   `xml:"requestId"`
	ImageBlockPublicAccessState string   `xml:"imageBlockPublicAccessState"`
}

type enableImageBlockPublicAccessResponse struct {
	XMLName                     xml.Name `xml:"EnableImageBlockPublicAccessResponse"`
	RequestID                   string   `xml:"requestId"`
	ImageBlockPublicAccessState string   `xml:"imageBlockPublicAccessState"`
}

type disableImageBlockPublicAccessResponse struct {
	XMLName                     xml.Name `xml:"DisableImageBlockPublicAccessResponse"`
	RequestID                   string   `xml:"requestId"`
	ImageBlockPublicAccessState string   `xml:"imageBlockPublicAccessState"`
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

	resp := &enableImageBlockPublicAccessResponse{RequestID: reqID, ImageBlockPublicAccessState: state}

	return resp, nil
}

func (h *Handler) handleDisableImageBlockPublicAccess(_ url.Values, reqID string) (any, error) {
	h.Backend.DisableImageBlockPublicAccess()

	resp := &disableImageBlockPublicAccessResponse{
		RequestID: reqID, ImageBlockPublicAccessState: stateImageUnblocked,
	}

	return resp, nil
}

func (h *Handler) handleGetImageBlockPublicAccessState(_ url.Values, reqID string) (any, error) {
	resp := &imageBlockPublicAccessStateResponse{
		RequestID: reqID, ImageBlockPublicAccessState: h.Backend.GetImageBlockPublicAccessState(),
	}

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

// Real ImageAttributeName values (ec2@v1.319.1 types/enums.go) that this
// backend can round-trip through the generic imageAttributes string store.
const (
	imageAttrDescription = "description"
	imageAttrImdsSupport = "imdsSupport"
)

func (h *Handler) handleModifyImageAttribute(vals url.Values, reqID string) (any, error) {
	imageID := vals.Get("ImageId")
	attribute := vals.Get("Attribute")
	value := vals.Get("Value")

	// A real client typically sends the structured Description/ImdsSupport
	// AttributeValue form (Description.Value=X) rather than the generic
	// Attribute=description&Value=X pair; awsEc2query_serializeDocumentAttributeValue
	// only ever emits a "Value" child, so this is unambiguous.
	switch {
	case vals.Get("Description.Value") != "":
		attribute = imageAttrDescription
		value = vals.Get("Description.Value")
	case vals.Get("ImdsSupport.Value") != "":
		attribute = imageAttrImdsSupport
		value = vals.Get("ImdsSupport.Value")
	}

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

func (h *Handler) handleDeregisterImage(vals url.Values, reqID string) (any, error) {
	if err := h.Backend.DeregisterImage(vals.Get("ImageId")); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "DeregisterImageResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
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

type amiItem struct {
	ImageID        string `xml:"imageId"`
	Name           string `xml:"name"`
	Description    string `xml:"description,omitempty"`
	Architecture   string `xml:"architecture"`
	Platform       string `xml:"platform,omitempty"`
	State          string `xml:"imageState"`
	RootDeviceName string `xml:"rootDeviceName,omitempty"`
}

type amiItemSet struct {
	Items []amiItem `xml:"item"`
}

type describeImagesResponse struct {
	XMLName   xml.Name   `xml:"DescribeImagesResponse"`
	Xmlns     string     `xml:"xmlns,attr"`
	RequestID string     `xml:"requestId"`
	NextToken string     `xml:"nextToken,omitempty"`
	ImagesSet amiItemSet `xml:"imagesSet"`
}

type regionItem struct {
	RegionName string `xml:"regionName"`
	Endpoint   string `xml:"regionEndpoint"`
}

type regionItemSet struct {
	Items []regionItem `xml:"item"`
}

type describeRegionsResponse struct {
	XMLName    xml.Name      `xml:"DescribeRegionsResponse"`
	Xmlns      string        `xml:"xmlns,attr"`
	RequestID  string        `xml:"requestId"`
	RegionInfo regionItemSet `xml:"regionInfo"`
}

type azItem struct {
	ZoneName   string `xml:"zoneName"`
	RegionName string `xml:"regionName"`
	State      string `xml:"zoneState"`
}

type azItemSet struct {
	Items []azItem `xml:"item"`
}

type describeAvailabilityZonesResponse struct {
	XMLName              xml.Name  `xml:"DescribeAvailabilityZonesResponse"`
	Xmlns                string    `xml:"xmlns,attr"`
	RequestID            string    `xml:"requestId"`
	AvailabilityZoneInfo azItemSet `xml:"availabilityZoneInfo"`
}

func parseImagesPagination(vals url.Values) (int, int, error) {
	maxResults := describeImagesDefaultResults
	if v := vals.Get("MaxResults"); v != "" {
		n, parseErr := strconv.Atoi(v)
		if parseErr != nil || n < describeImagesMinResults || n > describeImagesMaxResults {
			return 0, 0, fmt.Errorf(
				"%w: MaxResults must be between %d and %d",
				ErrInvalidParameter, describeImagesMinResults, describeImagesMaxResults,
			)
		}
		maxResults = n
	}

	offset := 0
	if tok := vals.Get("NextToken"); tok != "" {
		n := page.DecodeHMACToken(tok, ec2PaginationSalt)
		if n == 0 {
			return 0, 0, fmt.Errorf("%w: the pagination token is not valid", ErrInvalidPaginationToken)
		}
		offset = n
	}

	return maxResults, offset, nil
}

func (h *Handler) handleDescribeImages(vals url.Values, reqID string) (any, error) {
	amis := h.Backend.DescribeImages()

	// Collect requested image IDs from ImageId.1, ImageId.2, ... query params.
	requested := make(map[string]struct{})

	for i := 1; ; i++ {
		id := vals.Get(fmt.Sprintf("ImageId.%d", i))
		if id == "" {
			break
		}

		requested[id] = struct{}{}
	}

	// Pre-filter by ID, then apply named EC2 filters (name, architecture, state, etc.).
	idFiltered := make([]*AMIStub, 0, len(amis))
	for i := range amis {
		if len(requested) > 0 {
			if _, ok := requested[amis[i].ImageID]; !ok {
				continue
			}
		}
		idFiltered = append(idFiltered, &amis[i])
	}

	filters := parseEC2Filters(vals)
	idFiltered = applyImageFilters(idFiltered, filters, h.Backend)

	filtered := make([]amiItem, 0, len(idFiltered))
	for _, a := range idFiltered {
		st := a.State
		if st == "" {
			st = stateAvailable
		}

		filtered = append(filtered, amiItem{
			ImageID:        a.ImageID,
			Name:           a.Name,
			Description:    a.Description,
			Architecture:   a.Architecture,
			Platform:       a.Platform,
			State:          st,
			RootDeviceName: a.RootDeviceName,
		})
	}

	maxResults, offset, err := parseImagesPagination(vals)
	if err != nil {
		return nil, err
	}

	if offset > len(filtered) {
		offset = len(filtered)
	}
	filtered = filtered[offset:]

	var nextToken string
	if len(filtered) > maxResults {
		nextToken = page.EncodeHMACToken(offset+maxResults, ec2PaginationSalt)
		filtered = filtered[:maxResults]
	}

	return &describeImagesResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		NextToken: nextToken,
		ImagesSet: amiItemSet{Items: filtered},
	}, nil
}

func (h *Handler) handleDescribeRegions(_ url.Values, reqID string) (any, error) {
	regions := h.Backend.DescribeRegions()

	items := make([]regionItem, 0, len(regions))
	for _, r := range regions {
		items = append(items, regionItem{
			RegionName: r,
			Endpoint:   fmt.Sprintf("ec2.%s.amazonaws.com", r),
		})
	}

	return &describeRegionsResponse{
		Xmlns:      ec2XMLNS,
		RequestID:  reqID,
		RegionInfo: regionItemSet{Items: items},
	}, nil
}

func (h *Handler) handleDescribeAvailabilityZones(vals url.Values, reqID string) (any, error) {
	region := vals.Get("RegionName")
	azs := h.Backend.DescribeAvailabilityZones(region)

	effectiveRegion := region
	if effectiveRegion == "" {
		effectiveRegion = h.Region
	}

	items := make([]azItem, 0, len(azs))
	for _, az := range azs {
		items = append(items, azItem{
			ZoneName:   az,
			RegionName: effectiveRegion,
			State:      stateAvailable,
		})
	}

	return &describeAvailabilityZonesResponse{
		Xmlns:                ec2XMLNS,
		RequestID:            reqID,
		AvailabilityZoneInfo: azItemSet{Items: items},
	}, nil
}

// ---- DescribeImageAttribute ----

type imageAttributeValueItem struct {
	Value string `xml:"value,omitempty"`
}

type describeImageAttributeResponse struct {
	Description      *imageAttributeValueItem `xml:"description,omitempty"`
	ImdsSupport      *imageAttributeValueItem `xml:"imdsSupport,omitempty"`
	XMLName          xml.Name                 `xml:"DescribeImageAttributeResponse"`
	Xmlns            string                   `xml:"xmlns,attr"`
	RequestID        string                   `xml:"requestId"`
	ImageID          string                   `xml:"imageId"`
	LaunchPermission launchPermissionList     `xml:"launchPermission"`
}

type launchPermissionList struct {
	Items []launchPermissionItem `xml:"item"`
}

type launchPermissionItem struct {
	Group  string `xml:"group,omitempty"`
	UserID string `xml:"userId,omitempty"`
}

// handleDescribeImageAttribute returns stub launch-permission attributes for
// the specified image. AWS requires the Attribute parameter; if it is absent
// we return an error matching real-AWS behaviour.
func (h *Handler) handleDescribeImageAttribute(vals url.Values, reqID string) (any, error) {
	imageID := vals.Get("ImageId")
	if imageID == "" {
		return nil, fmt.Errorf("%w: ImageId is required", ErrInvalidParameter)
	}

	attribute := vals.Get("Attribute")
	if attribute == "" {
		return nil, fmt.Errorf("%w: Attribute is required", ErrInvalidParameter)
	}

	resp := &describeImageAttributeResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		ImageID:   imageID,
	}

	switch attribute {
	case "launchPermission":
		// launchPermission grants aren't tracked per-grantee by this backend;
		// stub a single "all" (public) grant rather than an empty list.
		resp.LaunchPermission = launchPermissionList{
			Items: []launchPermissionItem{{Group: "all"}},
		}
	case imageAttrDescription:
		if v := h.Backend.GetImageAttribute(imageID, imageAttrDescription); v != "" {
			resp.Description = &imageAttributeValueItem{Value: v}
		}
	case imageAttrImdsSupport:
		if v := h.Backend.GetImageAttribute(imageID, imageAttrImdsSupport); v != "" {
			resp.ImdsSupport = &imageAttributeValueItem{Value: v}
		}
	}

	return resp, nil
}
