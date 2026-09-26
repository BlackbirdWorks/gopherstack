package ec2

import (
	"encoding/xml"
	"fmt"
	"net/url"
	"slices"
	"strconv"
	"strings"

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
	NextToken                string   `xml:"nextToken,omitempty"`
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

// permissionGroupAll is the real AWS "all" (public) grantee value shared by
// AMI LaunchPermission and snapshot CreateVolumePermission grants.
const permissionGroupAll = "all"

// fastLaunchDefaultMaxParallelLaunches is real AWS's documented default for
// EnableFastLaunchInput.MaxParallelLaunches when the request omits it.
const fastLaunchDefaultMaxParallelLaunches = 6

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
	case hasLaunchPermissionModification(vals):
		addIDs, addPublic := parseLaunchPermissionList(vals, "LaunchPermission.Add")
		removeIDs, removePublic := parseLaunchPermissionList(vals, "LaunchPermission.Remove")

		if err := h.Backend.ModifyImageLaunchPermission(
			imageID, addIDs, addPublic, removeIDs, removePublic,
		); err != nil {
			return nil, err
		}

		return &stubResponse{
			XMLName:   xml.Name{Local: "ModifyImageAttributeResponse"},
			RequestID: reqID,
			Return:    true,
		}, nil
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

// hasLaunchPermissionModification reports whether vals carries a
// LaunchPermission.Add/Remove modification (api_op_ModifyImageAttribute.go).
func hasLaunchPermissionModification(vals url.Values) bool {
	_, hasAdd := vals["LaunchPermission.Add.1.UserId"]
	_, hasAddGroup := vals["LaunchPermission.Add.1.Group"]
	_, hasRemove := vals["LaunchPermission.Remove.1.UserId"]
	_, hasRemoveGroup := vals["LaunchPermission.Remove.1.Group"]

	return hasAdd || hasAddGroup || hasRemove || hasRemoveGroup
}

// parseLaunchPermissionList parses a LaunchPermission.Add/Remove.N.{UserId,Group}
// list, returning the account IDs and whether the "all" (public) group was
// present. Org/OU ARNs are documented but not modelled -- this backend has no
// AWS Organizations membership graph to resolve them against.
func parseLaunchPermissionList(vals url.Values, prefix string) ([]string, bool) {
	var ids []string

	public := false

	for i := 1; ; i++ {
		itemPrefix := fmt.Sprintf("%s.%d.", prefix, i)
		userID := vals.Get(itemPrefix + "UserId")
		group := vals.Get(itemPrefix + "Group")

		if userID == "" && group == "" {
			break
		}

		if userID != "" {
			ids = append(ids, userID)
		}

		if group == permissionGroupAll {
			public = true
		}
	}

	return ids, public
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

	maxResults, offset, err := parseEC2Pagination(vals, ec2PageMinDefault, ec2PageMaxDefault, ec2PageMaxDefault)
	if err != nil {
		return nil, err
	}

	var nextToken string
	items, nextToken = pageSlice(items, offset, maxResults)

	resp := &describeInstanceImageMetadataResponse{RequestID: reqID, NextToken: nextToken}
	for _, item := range items {
		resp.InstanceImageMetadataSet.Items = append(
			resp.InstanceImageMetadataSet.Items,
			toInstanceImageMetadataItem(item, h.Backend.TagsForResource(item.InstanceID)),
		)
	}

	return resp, nil
}

type importImageResponse struct {
	XMLName      xml.Name `xml:"ImportImageResponse"`
	RequestID    string   `xml:"requestId"`
	ImportTaskID string   `xml:"importTaskId"`
	Status       string   `xml:"status"`
	KmsKeyID     string   `xml:"kmsKeyId,omitempty"`
	Encrypted    bool     `xml:"encrypted"`
}

type describeImportImageTasksResponse struct {
	XMLName            xml.Name `xml:"DescribeImportImageTasksResponse"`
	RequestID          string   `xml:"requestId"`
	NextToken          string   `xml:"nextToken,omitempty"`
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
	NextToken          string   `xml:"nextToken,omitempty"`
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
	NextToken string   `xml:"nextToken,omitempty"`
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
	NextToken          string   `xml:"nextToken,omitempty"`
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

	virtType := vals.Get("VirtualizationType")
	if virtType == "" {
		virtType = "paravirtual" // api_op_RegisterImage.go: "Default: paravirtual"
	}
	h.Backend.SetImageMetadata(img.ImageID, vals.Get("ImdsSupport"), virtType)
	h.Backend.SetImageRootDeviceName(img.ImageID, vals.Get("RootDeviceName"))
	h.Backend.SetImageBlockDeviceMappings(img.ImageID, parseImageBlockDeviceMappings(vals))

	_, hasEnaSupport := vals["EnaSupport"]
	h.Backend.SetImageEnhancedNetworking(
		img.ImageID, hasEnaSupport, vals.Get("EnaSupport") == ec2BooleanTrue, vals.Get("SriovNetSupport"),
	)

	if tags := parseTagSpecification(vals, "image"); len(tags) > 0 {
		if err = h.Backend.CreateTags([]string{img.ImageID}, tags); err != nil {
			return nil, err
		}
	}

	return &registerImageResponse{
		RequestID: reqID,
		ImageID:   img.ImageID,
	}, nil
}

// parseImageBlockDeviceMappings parses RegisterImage's BlockDeviceMapping.N.*
// members (api_op_RegisterImage.go), stopping at the first index with neither
// a DeviceName nor a NoDevice marker.
func parseImageBlockDeviceMappings(vals url.Values) []ImageBlockDeviceMapping {
	var mappings []ImageBlockDeviceMapping

	for i := 1; ; i++ {
		prefix := fmt.Sprintf("BlockDeviceMapping.%d.", i)
		deviceName := vals.Get(prefix + "DeviceName")
		virtualName := vals.Get(prefix + "VirtualName")
		_, hasNoDevice := vals[prefix+"NoDevice"]

		if deviceName == "" && virtualName == "" && !hasNoDevice {
			break
		}

		mappings = append(mappings, ImageBlockDeviceMapping{
			DeviceName:          deviceName,
			VirtualName:         virtualName,
			NoDevice:            hasNoDevice,
			SnapshotID:          vals.Get(prefix + "Ebs.SnapshotId"),
			VolumeType:          vals.Get(prefix + "Ebs.VolumeType"),
			VolumeSize:          parseInt32Value(vals.Get(prefix + "Ebs.VolumeSize")),
			Iops:                parseInt32Value(vals.Get(prefix + "Ebs.Iops")),
			Throughput:          parseInt32Value(vals.Get(prefix + "Ebs.Throughput")),
			DeleteOnTermination: vals.Get(prefix+"Ebs.DeleteOnTermination") == ec2BooleanTrue,
			Encrypted:           vals.Get(prefix+"Ebs.Encrypted") == ec2BooleanTrue,
		})
	}

	return mappings
}

func (h *Handler) handleImportImage(vals url.Values, reqID string) (any, error) {
	description := vals.Get("Description")
	arch := vals.Get("Architecture")
	platform := vals.Get("Platform")
	encrypted := vals.Get("Encrypted") == ec2BooleanTrue
	kmsKeyID := vals.Get("KmsKeyId")

	task, err := h.Backend.ImportImage(description, arch, platform, encrypted, kmsKeyID)
	if err != nil {
		return nil, err
	}

	return &importImageResponse{
		RequestID:    reqID,
		ImportTaskID: task.ImportTaskID,
		Status:       task.Status,
		Encrypted:    task.Encrypted,
		KmsKeyID:     task.KmsKeyID,
	}, nil
}

func (h *Handler) handleDescribeImportImageTasks(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "ImportTaskId")
	tasks := h.Backend.DescribeImportImageTasks(ids)
	// DescribeImportImageTasksInput flattens its filter list under "Filters",
	// not "Filter" (api_op_DescribeImportImageTasks.go serializer FlatKey) --
	// parseEC2Filters would silently read nothing.
	tasks = applyImportImageTaskFilters(tasks, parseEC2FilterListKeyed(vals, "Filters"))

	maxResults, offset, err := parseEC2Pagination(vals, ec2PageMinDefault, ec2PageMaxDefault, ec2PageMaxDefault)
	if err != nil {
		return nil, err
	}

	var nextToken string
	tasks, nextToken = pageSlice(tasks, offset, maxResults)

	resp := &describeImportImageTasksResponse{RequestID: reqID, NextToken: nextToken}
	for _, t := range tasks {
		resp.ImportImageTaskSet.Items = append(resp.ImportImageTaskSet.Items, importImageTaskItem{
			ImportTaskID: t.ImportTaskID,
			Description:  t.Description,
			Architecture: t.Architecture,
			Platform:     t.Platform,
			Status:       t.Status,
			Encrypted:    t.Encrypted,
			KmsKeyID:     t.KmsKeyID,
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

	maxResults, offset, err := parseEC2Pagination(vals, ec2PageMinDefault, ec2PageMaxDefault, ec2PageMaxDefault)
	if err != nil {
		return nil, err
	}

	var nextToken string
	tasks, nextToken = pageSlice(tasks, offset, maxResults)

	resp := &describeExportImageTasksResponse{RequestID: reqID, NextToken: nextToken}
	for _, t := range tasks {
		resp.ExportImageTaskSet.Items = append(resp.ExportImageTaskSet.Items, toExportImageTaskItem(t))
	}

	return resp, nil
}

func (h *Handler) handleListImagesInRecycleBin(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "ImageId")
	images := h.Backend.ListImagesInRecycleBin(ids)

	maxResults, offset, err := parseEC2Pagination(vals, ec2PageMinDefault, ec2PageMaxDefault, ec2PageMaxDefault)
	if err != nil {
		return nil, err
	}

	var nextToken string
	images, nextToken = pageSlice(images, offset, maxResults)

	resp := &listImagesInRecycleBinResponse{RequestID: reqID, NextToken: nextToken}
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

// fastLaunchLaunchTemplateItem and fastLaunchSnapshotConfigItem match the
// nested shapes of FastLaunchLaunchTemplateSpecificationResponse and
// FastLaunchSnapshotConfigurationResponse (ec2@v1.319.1 types/types.go).
type fastLaunchLaunchTemplateItem struct {
	LaunchTemplateID   string `xml:"launchTemplateId,omitempty"`
	LaunchTemplateName string `xml:"launchTemplateName,omitempty"`
	Version            string `xml:"version,omitempty"`
}

type fastLaunchSnapshotConfigItem struct {
	TargetResourceCount int `xml:"targetResourceCount,omitempty"`
}

// enableFastLaunchResponse matches EnableFastLaunchOutput (ec2@v1.319.1
// api_op_EnableFastLaunch.go): there is no Return member at all -- the real
// deserializer has no case for it, only imageId/launchTemplate/
// maxParallelLaunches/ownerId/resourceType/snapshotConfiguration/state/
// stateTransitionReason/stateTransitionTime.
type enableFastLaunchResponse struct {
	LaunchTemplate        *fastLaunchLaunchTemplateItem `xml:"launchTemplate,omitempty"`
	SnapshotConfiguration *fastLaunchSnapshotConfigItem `xml:"snapshotConfiguration,omitempty"`
	XMLName               xml.Name                      `xml:"EnableFastLaunchResponse"`
	RequestID             string                        `xml:"requestId"`
	ImageID               string                        `xml:"imageId,omitempty"`
	ResourceType          string                        `xml:"resourceType,omitempty"`
	OwnerID               string                        `xml:"ownerId,omitempty"`
	State                 string                        `xml:"state,omitempty"`
	MaxParallelLaunches   int                           `xml:"maxParallelLaunches,omitempty"`
}

// disableFastLaunchResponse matches DisableFastLaunchOutput (same shape as
// EnableFastLaunchOutput): LaunchTemplate/MaxParallelLaunches/ResourceType/
// SnapshotConfiguration are the parameters fast launch had before being
// disabled.
type disableFastLaunchResponse struct {
	LaunchTemplate        *fastLaunchLaunchTemplateItem `xml:"launchTemplate,omitempty"`
	SnapshotConfiguration *fastLaunchSnapshotConfigItem `xml:"snapshotConfiguration,omitempty"`
	XMLName               xml.Name                      `xml:"DisableFastLaunchResponse"`
	RequestID             string                        `xml:"requestId"`
	ImageID               string                        `xml:"imageId,omitempty"`
	ResourceType          string                        `xml:"resourceType,omitempty"`
	OwnerID               string                        `xml:"ownerId,omitempty"`
	State                 string                        `xml:"state,omitempty"`
	MaxParallelLaunches   int                           `xml:"maxParallelLaunches,omitempty"`
}

// parseFastLaunchLaunchTemplate reads LaunchTemplate.{LaunchTemplateId,
// LaunchTemplateName,Version} from the EnableFastLaunch request (ec2@v1.319.1
// serializers.go, awsEc2query_serializeDocumentFastLaunchLaunchTemplateSpecificationRequest).
func parseFastLaunchLaunchTemplate(vals url.Values) *fastLaunchLaunchTemplateItem {
	id := vals.Get("LaunchTemplate.LaunchTemplateId")
	name := vals.Get("LaunchTemplate.LaunchTemplateName")
	version := vals.Get("LaunchTemplate.Version")

	if id == "" && name == "" && version == "" {
		return nil
	}

	return &fastLaunchLaunchTemplateItem{LaunchTemplateID: id, LaunchTemplateName: name, Version: version}
}

func (h *Handler) handleEnableFastLaunch(vals url.Values, reqID string) (any, error) {
	imageID := vals.Get("ImageId")

	resourceType := vals.Get("ResourceType")
	if resourceType == "" {
		resourceType = "snapshot"
	}

	maxParallelLaunches := fastLaunchDefaultMaxParallelLaunches
	if v := vals.Get("MaxParallelLaunches"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			maxParallelLaunches = n
		}
	}

	var snapshotConfig *fastLaunchSnapshotConfigItem
	if v := vals.Get("SnapshotConfiguration.TargetResourceCount"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			snapshotConfig = &fastLaunchSnapshotConfigItem{TargetResourceCount: n}
		}
	}

	launchTemplate := parseFastLaunchLaunchTemplate(vals)

	cfg := FastLaunchConfig{
		ResourceType:        resourceType,
		MaxParallelLaunches: maxParallelLaunches,
	}
	if launchTemplate != nil {
		cfg.HasLaunchTemplate = true
		cfg.LaunchTemplateID = launchTemplate.LaunchTemplateID
		cfg.LaunchTemplateName = launchTemplate.LaunchTemplateName
		cfg.LaunchTemplateVersion = launchTemplate.Version
	}

	if snapshotConfig != nil {
		cfg.HasSnapshotConfiguration = true
		cfg.SnapshotTargetResourceCount = snapshotConfig.TargetResourceCount
	}

	if err := h.Backend.EnableFastLaunch(imageID, cfg); err != nil {
		return nil, err
	}

	return &enableFastLaunchResponse{
		RequestID:             reqID,
		ImageID:               imageID,
		ResourceType:          resourceType,
		MaxParallelLaunches:   maxParallelLaunches,
		OwnerID:               h.AccountID,
		State:                 "enabling",
		LaunchTemplate:        launchTemplate,
		SnapshotConfiguration: snapshotConfig,
	}, nil
}

func (h *Handler) handleDisableFastLaunch(vals url.Values, reqID string) (any, error) {
	imageID := vals.Get("ImageId")

	prev, err := h.Backend.DisableFastLaunch(imageID)
	if err != nil {
		return nil, err
	}

	resp := &disableFastLaunchResponse{
		RequestID: reqID,
		ImageID:   imageID,
		OwnerID:   h.AccountID,
		State:     "disabling",
	}
	if prev != nil {
		resp.ResourceType = prev.ResourceType
		resp.MaxParallelLaunches = prev.MaxParallelLaunches
		if prev.HasLaunchTemplate {
			resp.LaunchTemplate = &fastLaunchLaunchTemplateItem{
				LaunchTemplateID:   prev.LaunchTemplateID,
				LaunchTemplateName: prev.LaunchTemplateName,
				Version:            prev.LaunchTemplateVersion,
			}
		}

		if prev.HasSnapshotConfiguration {
			resp.SnapshotConfiguration = &fastLaunchSnapshotConfigItem{
				TargetResourceCount: prev.SnapshotTargetResourceCount,
			}
		}
	}

	return resp, nil
}

func (h *Handler) handleDescribeFastLaunchImages(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "ImageId")
	items := h.Backend.DescribeFastLaunchImages(ids)

	maxResults, offset, err := parseEC2Pagination(vals, ec2PageMinDefault, ec2PageMaxDefault, ec2PageMaxDefault)
	if err != nil {
		return nil, err
	}

	var nextToken string
	items, nextToken = pageSlice(items, offset, maxResults)

	resp := &describeFastLaunchImagesResponse{RequestID: reqID, NextToken: nextToken}
	for _, item := range items {
		resp.FastLaunchImageSet.Items = append(
			resp.FastLaunchImageSet.Items,
			toFastLaunchImageItem(item, h.AccountID),
		)
	}

	return resp, nil
}

func (h *Handler) handleCopyImage(vals url.Values, reqID string) (any, error) {
	sourceImageID := vals.Get("SourceImageId")

	image, err := h.Backend.CopyImage(
		sourceImageID,
		vals.Get("Name"),
		vals.Get("Description"),
	)
	if err != nil {
		return nil, err
	}

	// Default: your user-defined AMI tags are not copied (ec2@v1.319.1
	// api_op_CopyImage.go's CopyImageTags doc comment).
	if vals.Get("CopyImageTags") == ec2BooleanTrue {
		if srcTags := h.Backend.TagsForResource(sourceImageID); len(srcTags) > 0 {
			if err = h.Backend.CreateTags([]string{image.ImageID}, srcTags); err != nil {
				return nil, err
			}
		}
	}

	return &copyImageResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		ImageID:   image.ImageID,
	}, nil
}

// handleDeregisterImage: DeregisterImageOutput also has DeleteSnapshotResults
// (ec2@v1.319.1 api_op_DeregisterImage.go), populated only when the request's
// DeleteAssociatedSnapshots=true and a snapshot backing the AMI was actually
// deleted. This backend doesn't track which snapshots back an AMI (AMIStub
// has no block-device-mapping/snapshot fields at all -- see store.go), so
// there's no real data to report for that case; left as a stub (Return only)
// rather than adding a field that would always be empty. See PARITY.md.
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
	NextToken         string   `xml:"nextToken,omitempty"`
	ImageReferenceSet struct {
		Items []imageReferenceItem `xml:"item"`
	} `xml:"imageReferenceSet"`
}

func (h *Handler) handleDescribeImageReferences(vals url.Values, reqID string) (any, error) {
	imageIDs := parseMemberList(vals, "ImageId")

	refs := h.Backend.DescribeImageReferences(imageIDs)

	// IncludeAllResourceTypes and ResourceType.N.ResourceType are the
	// documented either/or selectors (api_op_DescribeImageReferences.go);
	// this backend only models ec2:Instance/ec2:LaunchTemplate, so an
	// explicit ResourceTypes list restricts to those.
	includeAll, _ := strconv.ParseBool(vals.Get("IncludeAllResourceTypes"))
	if !includeAll {
		if types := parseResourceTypeRequestTypes(vals); len(types) > 0 {
			refs = slices.DeleteFunc(slices.Clone(refs), func(r *ImageReferenceEntry) bool {
				return !slices.Contains(types, r.ResourceType)
			})
		}
	}

	maxResults, offset, err := parseEC2Pagination(vals, ec2PageMinDefault, ec2PageMaxDefault, ec2PageMaxDefault)
	if err != nil {
		return nil, err
	}

	var nextToken string
	refs, nextToken = pageSlice(refs, offset, maxResults)

	resp := &describeImageReferencesResponse{RequestID: reqID, NextToken: nextToken}
	for _, r := range refs {
		resp.ImageReferenceSet.Items = append(resp.ImageReferenceSet.Items, imageReferenceItem{
			ImageID:      r.ImageID,
			ResourceType: r.ResourceType,
			Arn:          r.Arn,
		})
	}

	return resp, nil
}

// parseResourceTypeRequestTypes reads the DescribeImageReferences
// "ResourceType.N.ResourceType" struct list
// (awsEc2query_serializeDocumentResourceTypeRequestList); ResourceTypeOption
// sub-fields have no backing behavior in this backend and are ignored.
func parseResourceTypeRequestTypes(vals url.Values) []string {
	var out []string

	for i := 1; ; i++ {
		v := vals.Get(fmt.Sprintf("ResourceType.%d.ResourceType", i))
		if v == "" {
			return out
		}
		out = append(out, v)
	}
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

// replaceImageInstanceTypeSpecificationResponse mirrors the real
// ReplaceImageInstanceTypeSpecificationOutput: the wire field is "returnValue", not "return"
// (ec2@v1.329.0 deserializers.go, awsEc2query_deserializeOpDocumentReplaceImageInstanceTypeSpecificationOutput).
type replaceImageInstanceTypeSpecificationResponse struct {
	XMLName     xml.Name `xml:"ReplaceImageInstanceTypeSpecificationResponse"`
	Xmlns       string   `xml:"xmlns,attr"`
	RequestID   string   `xml:"requestId"`
	ReturnValue bool     `xml:"returnValue"`
}

// handleReplaceImageInstanceTypeSpecification replaces or (given no
// InstanceTypeSpecification member at all) removes an AMI's instance type
// compatibility rules. Wire field names verified against ec2@v1.329.0
// serializers.go's awsEc2query_serializeDocumentInstanceTypeSpecificationRequest:
// InstanceTypeSpecification.SupportedInstanceType.N / .UnsupportedInstanceType.N
// (FlatKey, no ".member." wrapper).
func (h *Handler) handleReplaceImageInstanceTypeSpecification(vals url.Values, reqID string) (any, error) {
	supported := parseMemberList(vals, "InstanceTypeSpecification.SupportedInstanceType")
	unsupported := parseMemberList(vals, "InstanceTypeSpecification.UnsupportedInstanceType")

	if err := h.Backend.ReplaceImageInstanceTypeSpecification(
		vals.Get("ImageId"), supported, unsupported,
	); err != nil {
		return nil, err
	}

	return &replaceImageInstanceTypeSpecificationResponse{
		Xmlns: ec2XMLNS, RequestID: reqID, ReturnValue: true,
	}, nil
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
	ops["ReplaceImageInstanceTypeSpecification"] = h.handleReplaceImageInstanceTypeSpecification
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
		"ReplaceImageInstanceTypeSpecification",
	}
}

// instanceTypeItem wraps a single instance type/wildcard pattern entry
// (ec2@v1.329.0 types.InstanceTypeItem: a struct with one InstanceType field, not a bare
// string -- confirmed via deserializers.go's awsEc2query_deserializeDocumentInstanceTypeItem).
type instanceTypeItem struct {
	InstanceType string `xml:"instanceType"`
}

// instanceTypeSpecificationItem mirrors ec2@v1.329.0 types.InstanceTypeSpecification
// (deserializers.go's awsEc2query_deserializeDocumentInstanceTypeSpecification):
// supportedInstanceTypeSet/unsupportedInstanceTypeSet, each wrapping <item> entries.
type instanceTypeSpecificationItem struct {
	SupportedInstanceTypeSet struct {
		Items []instanceTypeItem `xml:"item"`
	} `xml:"supportedInstanceTypeSet"`
	UnsupportedInstanceTypeSet struct {
		Items []instanceTypeItem `xml:"item"`
	} `xml:"unsupportedInstanceTypeSet"`
}

func toInstanceTypeSpecificationItem(spec *InstanceTypeSpecification) *instanceTypeSpecificationItem {
	if spec == nil {
		return nil
	}

	item := &instanceTypeSpecificationItem{}
	for _, t := range spec.SupportedInstanceTypes {
		item.SupportedInstanceTypeSet.Items = append(
			item.SupportedInstanceTypeSet.Items,
			instanceTypeItem{InstanceType: t},
		)
	}

	for _, t := range spec.UnsupportedInstanceTypes {
		item.UnsupportedInstanceTypeSet.Items = append(
			item.UnsupportedInstanceTypeSet.Items, instanceTypeItem{InstanceType: t},
		)
	}

	return item
}

type amiEbsBlockDeviceItem struct {
	SnapshotID          string `xml:"snapshotId,omitempty"`
	VolumeType          string `xml:"volumeType,omitempty"`
	VolumeSize          int32  `xml:"volumeSize,omitempty"`
	Iops                int32  `xml:"iops,omitempty"`
	Throughput          int32  `xml:"throughput,omitempty"`
	DeleteOnTermination bool   `xml:"deleteOnTermination"`
	Encrypted           bool   `xml:"encrypted"`
}

type amiBlockDeviceMappingItem struct {
	Ebs         *amiEbsBlockDeviceItem `xml:"ebs,omitempty"`
	NoDevice    *string                `xml:"noDevice,omitempty"`
	DeviceName  string                 `xml:"deviceName,omitempty"`
	VirtualName string                 `xml:"virtualName,omitempty"`
}

type amiItem struct {
	InstanceTypeSpecification *instanceTypeSpecificationItem `xml:"instanceTypeSpecification,omitempty"`
	EnaSupport                *bool                          `xml:"enaSupport,omitempty"`
	OwnerAlias                string                         `xml:"imageOwnerAlias,omitempty"`
	ImdsSupport               string                         `xml:"imdsSupport,omitempty"`
	Platform                  string                         `xml:"platform,omitempty"`
	State                     string                         `xml:"imageState"`
	ImageID                   string                         `xml:"imageId"`
	OwnerID                   string                         `xml:"imageOwnerId,omitempty"`
	Description               string                         `xml:"description,omitempty"`
	Architecture              string                         `xml:"architecture"`
	VirtualizationType        string                         `xml:"virtualizationType,omitempty"`
	DeprecationTime           string                         `xml:"deprecationTime,omitempty"`
	Name                      string                         `xml:"name"`
	SriovNetSupport           string                         `xml:"sriovNetSupport,omitempty"`
	RootDeviceName            string                         `xml:"rootDeviceName,omitempty"`
	BlockDeviceMapping        []amiBlockDeviceMappingItem    `xml:"blockDeviceMapping>item,omitempty"`
	TagSet                    []simpleTagItem                `xml:"tagSet>item,omitempty"`
}

// toAMIBlockDeviceMappingItems converts stored block device mappings to their
// wire shape (api_op_RegisterImage.go request / Image.BlockDeviceMappings
// response, both named blockDeviceMapping on the wire).
func toAMIBlockDeviceMappingItems(mappings []ImageBlockDeviceMapping) []amiBlockDeviceMappingItem {
	items := make([]amiBlockDeviceMappingItem, 0, len(mappings))

	for _, m := range mappings {
		item := amiBlockDeviceMappingItem{
			DeviceName:  m.DeviceName,
			VirtualName: m.VirtualName,
		}

		if m.NoDevice {
			empty := ""
			item.NoDevice = &empty
		}

		if m.SnapshotID != "" || m.VolumeType != "" || m.VolumeSize != 0 ||
			m.DeleteOnTermination || m.Encrypted {
			item.Ebs = &amiEbsBlockDeviceItem{
				SnapshotID:          m.SnapshotID,
				VolumeType:          m.VolumeType,
				VolumeSize:          m.VolumeSize,
				Iops:                m.Iops,
				Throughput:          m.Throughput,
				DeleteOnTermination: m.DeleteOnTermination,
				Encrypted:           m.Encrypted,
			}
		}

		items = append(items, item)
	}

	return items
}

// knownImageOwnerAliases holds this backend's well-known non-numeric AMIStub.OwnerID
// values (imageOwnerAlias), distinguishing them from a real numeric account ID
// (imageOwnerId). Only "amazon" is ever set by this backend today.
//
//nolint:gochecknoglobals // small lookup table, not mutated
var knownImageOwnerAliases = map[string]bool{
	imageOwnerAliasAmazon: true,
	"aws-marketplace":     true,
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
	ZoneName    string `xml:"zoneName"`
	RegionName  string `xml:"regionName"`
	State       string `xml:"zoneState"`
	GroupName   string `xml:"groupName,omitempty"`
	OptInStatus string `xml:"optInStatus,omitempty"`
	ZoneType    string `xml:"zoneType,omitempty"`
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

// collectRequestedImageIDs reads ImageId.1, ImageId.2, ... query params.
func collectRequestedImageIDs(vals url.Values) map[string]struct{} {
	requested := make(map[string]struct{})

	for i := 1; ; i++ {
		id := vals.Get(fmt.Sprintf("ImageId.%d", i))
		if id == "" {
			break
		}

		requested[id] = struct{}{}
	}

	return requested
}

// filterVisibleImages excludes disabled AMIs by default (ec2@v1.319.1
// api_op_DescribeImages.go's IncludeDisabled doc comment: "Default: No
// disabled AMIs are included in the response") -- except an image named
// explicitly by ImageId, which real AWS still returns (see
// TestDescribeImages_DisabledState_RealClient, wire_field_fixes_test.go).
func filterVisibleImages(idFiltered []*AMIStub, requested map[string]struct{}, includeDisabled bool) []*AMIStub {
	if includeDisabled {
		return idFiltered
	}

	visible := idFiltered[:0]

	for _, a := range idFiltered {
		_, explicit := requested[a.ImageID]
		if a.State != stateDisabledImg || explicit {
			visible = append(visible, a)
		}
	}

	return visible
}

// filterDeprecatedImages excludes deprecated AMIs by default
// (api_op_DescribeImages.go's IncludeDeprecated doc comment: "Default: No
// deprecated AMIs are included in the response. If you are the AMI owner,
// all deprecated AMIs where the AMI ID matches a value in the ImageIds
// parameter are shown, regardless of the value of IncludeDeprecated").
func filterDeprecatedImages(
	idFiltered []*AMIStub, requested map[string]struct{}, deprecated map[string]string, includeDeprecated bool,
) []*AMIStub {
	if includeDeprecated {
		return idFiltered
	}

	visible := idFiltered[:0]

	for _, a := range idFiltered {
		_, isDeprecated := deprecated[a.ImageID]
		_, explicit := requested[a.ImageID]

		if !isDeprecated || explicit {
			visible = append(visible, a)
		}
	}

	return visible
}

// filterImagesByOwner applies DescribeImages' Owner.N list
// (awsEc2query_serializeDocumentOwnerStringList), resolving the "self"
// alias to the caller's own account ID.
func filterImagesByOwner(idFiltered []*AMIStub, owners []string, accountID string) []*AMIStub {
	if len(owners) == 0 {
		return idFiltered
	}

	want := make(map[string]struct{}, len(owners))
	for _, o := range owners {
		if o == "self" {
			o = accountID
		}
		want[o] = struct{}{}
	}

	visible := idFiltered[:0]

	for _, a := range idFiltered {
		if _, ok := want[a.OwnerID]; ok {
			visible = append(visible, a)
		}
	}

	return visible
}

func (h *Handler) handleDescribeImages(vals url.Values, reqID string) (any, error) {
	amis := h.Backend.DescribeImages()
	requested := collectRequestedImageIDs(vals)

	// Pre-filter by ID, then apply named EC2 filters (name, architecture, state, etc.).
	idFiltered := make([]*AMIStub, 0, len(amis))
	found := make(map[string]struct{}, len(requested))
	for i := range amis {
		if len(requested) > 0 {
			if _, ok := requested[amis[i].ImageID]; !ok {
				continue
			}
			found[amis[i].ImageID] = struct{}{}
		}
		idFiltered = append(idFiltered, &amis[i])
	}

	if err := firstMissingID(requested, found, ErrImageNotFound); err != nil {
		return nil, err
	}

	filters := parseEC2Filters(vals)
	idFiltered = applyImageFilters(idFiltered, filters, h.Backend)
	idFiltered = filterVisibleImages(idFiltered, requested, vals.Get("IncludeDisabled") == ec2BooleanTrue)

	deprecation := h.Backend.ImageDeprecation()
	includeDeprecated := vals.Get("IncludeDeprecated") == ec2BooleanTrue
	idFiltered = filterDeprecatedImages(idFiltered, requested, deprecation, includeDeprecated)
	idFiltered = filterImagesByOwner(idFiltered, parseMemberList(vals, "Owner"), h.AccountID)

	filtered := make([]amiItem, 0, len(idFiltered))
	for _, a := range idFiltered {
		st := a.State
		if st == "" {
			st = stateAvailable
		}

		ownerID, ownerAlias := a.OwnerID, ""
		if knownImageOwnerAliases[a.OwnerID] {
			ownerID, ownerAlias = "", a.OwnerID
		}

		filtered = append(filtered, amiItem{
			ImageID:            a.ImageID,
			Name:               a.Name,
			Description:        a.Description,
			Architecture:       a.Architecture,
			Platform:           a.Platform,
			State:              st,
			RootDeviceName:     a.RootDeviceName,
			OwnerID:            ownerID,
			OwnerAlias:         ownerAlias,
			ImdsSupport:        a.ImdsSupport,
			VirtualizationType: a.VirtualizationType,
			DeprecationTime:    deprecation[a.ImageID],
			TagSet:             tagItemsFromMap(h.Backend.TagsForResource(a.ImageID)),
			InstanceTypeSpecification: toInstanceTypeSpecificationItem(
				h.Backend.GetImageInstanceTypeSpecification(a.ImageID),
			),
			BlockDeviceMapping: toAMIBlockDeviceMappingItems(a.BlockDeviceMappings),
			SriovNetSupport:    a.SriovNetSupport,
		})

		if a.EnaSupportSet {
			filtered[len(filtered)-1].EnaSupport = &a.EnaSupport
		}
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
			ZoneName:    az,
			RegionName:  effectiveRegion,
			State:       stateAvailable,
			GroupName:   effectiveRegion + "-zg-1",
			OptInStatus: "opt-in-not-required",
			ZoneType:    filterKeyAvailabilityZone,
		})
	}

	if vals.Get("AllAvailabilityZones") == ec2BooleanTrue {
		for groupName, optInStatus := range h.Backend.GetAvailabilityZoneGroups() {
			items = append(items, azItem{
				ZoneName:    groupName,
				RegionName:  effectiveRegion,
				State:       stateAvailable,
				GroupName:   groupName,
				OptInStatus: optInStatus,
				ZoneType:    zoneTypeForGroupName(groupName),
			})
		}
	}

	items = applyAvailabilityZoneFilters(items, parseEC2Filters(vals))

	return &describeAvailabilityZonesResponse{
		Xmlns:                ec2XMLNS,
		RequestID:            reqID,
		AvailabilityZoneInfo: azItemSet{Items: items},
	}, nil
}

// zoneTypeForGroupName infers a zone group's ZoneType from its name, matching
// real AWS's documented naming (api_op_DescribeAvailabilityZones.go examples:
// Local Zones "us-west-2-lax-1", Wavelength Zones "us-east-1-wl1-bos-wlz-1").
func zoneTypeForGroupName(groupName string) string {
	if strings.Contains(groupName, "-wl") {
		return "wavelength-zone"
	}

	return "local-zone"
}

// applyAvailabilityZoneFilters supports the documented group-name/zone-name/
// region-name/state/zone-type filters (api_op_DescribeAvailabilityZones.go).
func applyAvailabilityZoneFilters(items []azItem, filters map[string][]string) []azItem {
	if len(filters) == 0 {
		return items
	}

	out := items[:0:0]
itemLoop:
	for _, item := range items {
		for name, values := range filters {
			if !azItemMatchesFilter(item, name, values) {
				continue itemLoop
			}
		}

		out = append(out, item)
	}

	return out
}

func azItemMatchesFilter(item azItem, filterName string, values []string) bool {
	switch filterName {
	case "group-name":
		return anyEqual(item.GroupName, values)
	case "zone-name":
		return anyEqual(item.ZoneName, values)
	case "region-name":
		return anyEqual(item.RegionName, values)
	case filterKeyState:
		return anyEqual(item.State, values)
	case "zone-type":
		return anyEqual(item.ZoneType, values)
	case "opt-in-status":
		return anyEqual(item.OptInStatus, values)
	}

	return true
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
		accountIDs, public := h.Backend.GetImageLaunchPermission(imageID)

		items := make([]launchPermissionItem, 0, len(accountIDs)+1)
		if public {
			items = append(items, launchPermissionItem{Group: permissionGroupAll})
		}

		for _, id := range accountIDs {
			items = append(items, launchPermissionItem{UserID: id})
		}

		resp.LaunchPermission = launchPermissionList{Items: items}
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
