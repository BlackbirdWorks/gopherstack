package ec2

import (
	"encoding/xml"
	"net/url"
	"strconv"
	"time"
)

// handler_image_ops.go implements the HTTP handlers for the Allowed Images Settings,
// Store/Restore Image Task, Image Usage Report, and ConfirmProductInstance families, backed
// by image_ops.go.

// ---- Registration ----

func registerImageOpsHandlers(h *Handler, ops map[string]ec2ActionFn) {
	ops["EnableAllowedImagesSettings"] = h.handleEnableAllowedImagesSettings
	ops["DisableAllowedImagesSettings"] = h.handleDisableAllowedImagesSettings
	ops["GetAllowedImagesSettings"] = h.handleGetAllowedImagesSettings
	ops["ReplaceImageCriteriaInAllowedImagesSettings"] = h.handleReplaceImageCriteriaInAllowedImagesSettings

	ops["CreateStoreImageTask"] = h.handleCreateStoreImageTask
	ops["DescribeStoreImageTasks"] = h.handleDescribeStoreImageTasks
	ops["CreateRestoreImageTask"] = h.handleCreateRestoreImageTask

	ops["CreateImageUsageReport"] = h.handleCreateImageUsageReport
	ops["DeleteImageUsageReport"] = h.handleDeleteImageUsageReport
	ops["DescribeImageUsageReportEntries"] = h.handleDescribeImageUsageReportEntries

	ops["ConfirmProductInstance"] = h.handleConfirmProductInstance

	ops["AttachImageWatermark"] = h.handleAttachImageWatermark
	ops["DetachImageWatermark"] = h.handleDetachImageWatermark
}

// imageOpsSupportedOperations lists the operation names registered by
// registerImageOpsHandlers, for GetSupportedOperations().
func imageOpsSupportedOperations() []string {
	return []string{
		"EnableAllowedImagesSettings",
		"DisableAllowedImagesSettings",
		"GetAllowedImagesSettings",
		"ReplaceImageCriteriaInAllowedImagesSettings",
		"CreateStoreImageTask",
		"DescribeStoreImageTasks",
		"CreateRestoreImageTask",
		"CreateImageUsageReport",
		"DeleteImageUsageReport",
		"DescribeImageUsageReportEntries",
		"ConfirmProductInstance",
		"AttachImageWatermark",
		"DetachImageWatermark",
	}
}

// ---- Response shapes: Allowed Images Settings ----

type imageCriterionItem struct {
	CreationDateCondition *struct {
		MaximumDaysSinceCreated int32 `xml:"maximumDaysSinceCreated,omitempty"`
	} `xml:"creationDateCondition"`
	DeprecationTimeCondition *struct {
		MaximumDaysSinceDeprecated int32 `xml:"maximumDaysSinceDeprecated,omitempty"`
	} `xml:"deprecationTimeCondition"`
	ImageNameSet struct {
		Items []string `xml:"item"`
	} `xml:"imageNameSet"`
	ImageProviderSet struct {
		Items []string `xml:"item"`
	} `xml:"imageProviderSet"`
	MarketplaceProductCodeSet struct {
		Items []string `xml:"item"`
	} `xml:"marketplaceProductCodeSet"`
}

func toImageCriterionItem(c ImageCriterion) imageCriterionItem {
	item := imageCriterionItem{}

	if c.CreationDateCondition != nil {
		item.CreationDateCondition = &struct {
			MaximumDaysSinceCreated int32 `xml:"maximumDaysSinceCreated,omitempty"`
		}{MaximumDaysSinceCreated: c.CreationDateCondition.MaximumDaysSinceCreated}
	}

	if c.DeprecationTimeCondition != nil {
		item.DeprecationTimeCondition = &struct {
			MaximumDaysSinceDeprecated int32 `xml:"maximumDaysSinceDeprecated,omitempty"`
		}{MaximumDaysSinceDeprecated: c.DeprecationTimeCondition.MaximumDaysSinceDeprecated}
	}

	item.ImageNameSet.Items = append(item.ImageNameSet.Items, c.ImageNames...)
	item.ImageProviderSet.Items = append(item.ImageProviderSet.Items, c.ImageProviders...)
	item.MarketplaceProductCodeSet.Items = append(item.MarketplaceProductCodeSet.Items, c.MarketplaceProductCodes...)

	return item
}

type enableAllowedImagesSettingsResponse struct {
	XMLName                    xml.Name `xml:"EnableAllowedImagesSettingsResponse"`
	Xmlns                      string   `xml:"xmlns,attr"`
	RequestID                  string   `xml:"requestId"`
	AllowedImagesSettingsState string   `xml:"allowedImagesSettingsState"`
}

type disableAllowedImagesSettingsResponse struct {
	XMLName                    xml.Name `xml:"DisableAllowedImagesSettingsResponse"`
	Xmlns                      string   `xml:"xmlns,attr"`
	RequestID                  string   `xml:"requestId"`
	AllowedImagesSettingsState string   `xml:"allowedImagesSettingsState"`
}

type getAllowedImagesSettingsResponse struct {
	XMLName           xml.Name `xml:"GetAllowedImagesSettingsResponse"`
	Xmlns             string   `xml:"xmlns,attr"`
	RequestID         string   `xml:"requestId"`
	ManagedBy         string   `xml:"managedBy,omitempty"`
	State             string   `xml:"state,omitempty"`
	ImageCriterionSet struct {
		Items []imageCriterionItem `xml:"item"`
	} `xml:"imageCriterionSet"`
}

type replaceImageCriteriaInAllowedImagesSettingsResponse struct {
	XMLName   xml.Name `xml:"ReplaceImageCriteriaInAllowedImagesSettingsResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

// ---- Response shapes: Store / Restore Image Tasks ----

type storeImageTaskResultItem struct {
	AmiID                  string `xml:"amiId,omitempty"`
	Bucket                 string `xml:"bucket,omitempty"`
	S3ObjectKey            string `xml:"s3objectKey,omitempty"`
	StoreTaskFailureReason string `xml:"storeTaskFailureReason,omitempty"`
	StoreTaskState         string `xml:"storeTaskState,omitempty"`
	TaskStartTime          string `xml:"taskStartTime,omitempty"`
	ProgressPercentage     int32  `xml:"progressPercentage,omitempty"`
}

func toStoreImageTaskResultItem(t *StoreImageTask) storeImageTaskResultItem {
	item := storeImageTaskResultItem{
		AmiID:                  t.AmiID,
		Bucket:                 t.Bucket,
		ProgressPercentage:     t.ProgressPercentage,
		S3ObjectKey:            t.S3ObjectKey,
		StoreTaskFailureReason: t.StoreTaskFailureReason,
		StoreTaskState:         t.StoreTaskState,
	}
	if !t.TaskStartTime.IsZero() {
		item.TaskStartTime = t.TaskStartTime.Format(time.RFC3339)
	}

	return item
}

type createStoreImageTaskResponse struct {
	XMLName   xml.Name `xml:"CreateStoreImageTaskResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	ObjectKey string   `xml:"objectKey,omitempty"`
}

type describeStoreImageTasksResponse struct {
	XMLName                 xml.Name `xml:"DescribeStoreImageTasksResponse"`
	Xmlns                   string   `xml:"xmlns,attr"`
	RequestID               string   `xml:"requestId"`
	StoreImageTaskResultSet struct {
		Items []storeImageTaskResultItem `xml:"item"`
	} `xml:"storeImageTaskResultSet"`
}

type createRestoreImageTaskResponse struct {
	XMLName   xml.Name `xml:"CreateRestoreImageTaskResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	ImageID   string   `xml:"imageId,omitempty"`
}

// ---- Response shapes: Image Usage Reports ----

type imageUsageReportEntryItem struct {
	AccountID          string `xml:"accountId,omitempty"`
	ImageID            string `xml:"imageId,omitempty"`
	ReportCreationTime string `xml:"reportCreationTime,omitempty"`
	ReportID           string `xml:"reportId,omitempty"`
	ResourceType       string `xml:"resourceType,omitempty"`
	UsageCount         int64  `xml:"usageCount,omitempty"`
}

func toImageUsageReportEntryItem(e *UsageReportEntry) imageUsageReportEntryItem {
	item := imageUsageReportEntryItem{
		AccountID:    e.AccountID,
		ImageID:      e.ImageID,
		ReportID:     e.ReportID,
		ResourceType: e.ResourceType,
		UsageCount:   e.UsageCount,
	}
	if !e.ReportCreationTime.IsZero() {
		item.ReportCreationTime = e.ReportCreationTime.Format(time.RFC3339)
	}

	return item
}

type createImageUsageReportResponse struct {
	XMLName   xml.Name `xml:"CreateImageUsageReportResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	ReportID  string   `xml:"reportId,omitempty"`
}

type deleteImageUsageReportResponse struct {
	XMLName   xml.Name `xml:"DeleteImageUsageReportResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

type describeImageUsageReportEntriesResponse struct {
	XMLName                  xml.Name `xml:"DescribeImageUsageReportEntriesResponse"`
	Xmlns                    string   `xml:"xmlns,attr"`
	RequestID                string   `xml:"requestId"`
	ImageUsageReportEntrySet struct {
		Items []imageUsageReportEntryItem `xml:"item"`
	} `xml:"imageUsageReportEntrySet"`
}

// ---- Response shapes: ConfirmProductInstance ----

type confirmProductInstanceResponse struct {
	XMLName   xml.Name `xml:"ConfirmProductInstanceResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	OwnerID   string   `xml:"ownerId,omitempty"`
	Return    bool     `xml:"return"`
}

// ---- Handlers: Allowed Images Settings ----

func (h *Handler) handleEnableAllowedImagesSettings(vals url.Values, reqID string) (any, error) {
	state, err := h.Backend.EnableAllowedImagesSettings(vals.Get("AllowedImagesSettingsState"))
	if err != nil {
		return nil, err
	}

	return &enableAllowedImagesSettingsResponse{
		Xmlns: ec2XMLNS, RequestID: reqID, AllowedImagesSettingsState: state,
	}, nil
}

func (h *Handler) handleDisableAllowedImagesSettings(_ url.Values, reqID string) (any, error) {
	state := h.Backend.DisableAllowedImagesSettings()

	return &disableAllowedImagesSettingsResponse{
		Xmlns: ec2XMLNS, RequestID: reqID, AllowedImagesSettingsState: state,
	}, nil
}

func (h *Handler) handleGetAllowedImagesSettings(_ url.Values, reqID string) (any, error) {
	settings := h.Backend.GetAllowedImagesSettings()

	resp := &getAllowedImagesSettingsResponse{
		Xmlns: ec2XMLNS, RequestID: reqID, ManagedBy: settings.ManagedBy, State: settings.State,
	}
	for _, c := range settings.ImageCriteria {
		resp.ImageCriterionSet.Items = append(resp.ImageCriterionSet.Items, toImageCriterionItem(c))
	}

	return resp, nil
}

// parseImageCriteria parses the "ImageCriterion.N.*" indexed form values accepted by
// ReplaceImageCriteriaInAllowedImagesSettings.
func parseImageCriteria(vals url.Values) []ImageCriterion {
	var criteria []ImageCriterion

	for i := 1; ; i++ {
		prefix := "ImageCriterion." + strconv.Itoa(i)

		names := parseMemberList(vals, prefix+".ImageName")
		providers := parseMemberList(vals, prefix+".ImageProvider")
		codes := parseMemberList(vals, prefix+".MarketplaceProductCode")

		maxCreated := vals.Get(prefix + ".CreationDateCondition.MaximumDaysSinceCreated")
		maxDeprecated := vals.Get(prefix + ".DeprecationTimeCondition.MaximumDaysSinceDeprecated")

		if len(names) == 0 && len(providers) == 0 && len(codes) == 0 && maxCreated == "" && maxDeprecated == "" {
			break
		}

		c := ImageCriterion{ImageNames: names, ImageProviders: providers, MarketplaceProductCodes: codes}

		if maxCreated != "" {
			if v, err := strconv.ParseInt(maxCreated, 10, 32); err == nil {
				c.CreationDateCondition = &CreationDateCondition{MaximumDaysSinceCreated: int32(v)}
			}
		}

		if maxDeprecated != "" {
			if v, err := strconv.ParseInt(maxDeprecated, 10, 32); err == nil {
				c.DeprecationTimeCondition = &DeprecationTimeCondition{MaximumDaysSinceDeprecated: int32(v)}
			}
		}

		criteria = append(criteria, c)
	}

	return criteria
}

func (h *Handler) handleReplaceImageCriteriaInAllowedImagesSettings(vals url.Values, reqID string) (any, error) {
	criteria := parseImageCriteria(vals)
	ok := h.Backend.ReplaceImageCriteriaInAllowedImagesSettings(criteria)

	return &replaceImageCriteriaInAllowedImagesSettingsResponse{Xmlns: ec2XMLNS, RequestID: reqID, Return: ok}, nil
}

// ---- Handlers: Store / Restore Image Tasks ----

func (h *Handler) handleCreateStoreImageTask(vals url.Values, reqID string) (any, error) {
	task, err := h.Backend.CreateStoreImageTask(vals.Get("ImageId"), vals.Get("Bucket"))
	if err != nil {
		return nil, err
	}

	return &createStoreImageTaskResponse{Xmlns: ec2XMLNS, RequestID: reqID, ObjectKey: task.S3ObjectKey}, nil
}

func (h *Handler) handleDescribeStoreImageTasks(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "ImageId")
	tasks := h.Backend.DescribeStoreImageTasks(ids)

	resp := &describeStoreImageTasksResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, t := range tasks {
		resp.StoreImageTaskResultSet.Items = append(resp.StoreImageTaskResultSet.Items, toStoreImageTaskResultItem(t))
	}

	return resp, nil
}

func (h *Handler) handleCreateRestoreImageTask(vals url.Values, reqID string) (any, error) {
	img, err := h.Backend.CreateRestoreImageTask(vals.Get("Bucket"), vals.Get("ObjectKey"), vals.Get("Name"))
	if err != nil {
		return nil, err
	}

	return &createRestoreImageTaskResponse{Xmlns: ec2XMLNS, RequestID: reqID, ImageID: img.ImageID}, nil
}

// ---- Handlers: Image Usage Reports ----

// parseImageUsageResourceTypes parses "ResourceType.N.ResourceType" indexed form values.
func parseImageUsageResourceTypes(vals url.Values) []string {
	var out []string

	for i := 1; ; i++ {
		v := vals.Get("ResourceType." + strconv.Itoa(i) + ".ResourceType")
		if v == "" {
			break
		}

		out = append(out, v)
	}

	return out
}

func (h *Handler) handleCreateImageUsageReport(vals url.Values, reqID string) (any, error) {
	report, err := h.Backend.CreateImageUsageReport(
		vals.Get("ImageId"), parseImageUsageResourceTypes(vals), parseMemberList(vals, "AccountId"),
	)
	if err != nil {
		return nil, err
	}

	return &createImageUsageReportResponse{Xmlns: ec2XMLNS, RequestID: reqID, ReportID: report.ReportID}, nil
}

func (h *Handler) handleDeleteImageUsageReport(vals url.Values, reqID string) (any, error) {
	if err := h.Backend.DeleteImageUsageReport(vals.Get("ReportId")); err != nil {
		return nil, err
	}

	return &deleteImageUsageReportResponse{Xmlns: ec2XMLNS, RequestID: reqID, Return: true}, nil
}

func (h *Handler) handleDescribeImageUsageReportEntries(vals url.Values, reqID string) (any, error) {
	reportIDs := parseMemberList(vals, "ReportId")
	imageIDs := parseMemberList(vals, "ImageId")
	entries := applyImageUsageReportEntryFilters(
		h.Backend.DescribeImageUsageReportEntries(reportIDs, imageIDs), parseEC2Filters(vals),
	)

	resp := &describeImageUsageReportEntriesResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, e := range entries {
		resp.ImageUsageReportEntrySet.Items = append(
			resp.ImageUsageReportEntrySet.Items, toImageUsageReportEntryItem(e),
		)
	}

	return resp, nil
}

// ---- Handlers: ConfirmProductInstance ----

func (h *Handler) handleConfirmProductInstance(vals url.Values, reqID string) (any, error) {
	ok, err := h.Backend.ConfirmProductInstance(vals.Get("InstanceId"), vals.Get("ProductCode"))
	if err != nil {
		return nil, err
	}

	resp := &confirmProductInstanceResponse{Xmlns: ec2XMLNS, RequestID: reqID, Return: ok}
	if ok {
		resp.OwnerID = h.AccountID
	}

	return resp, nil
}

// ---- Handlers: Image Watermarks ----

type attachImageWatermarkResponse struct {
	XMLName      xml.Name `xml:"AttachImageWatermarkResponse"`
	Xmlns        string   `xml:"xmlns,attr"`
	RequestID    string   `xml:"requestId"`
	WatermarkKey string   `xml:"watermarkKey"`
}

func (h *Handler) handleAttachImageWatermark(vals url.Values, reqID string) (any, error) {
	key, err := h.Backend.AttachImageWatermark(vals.Get("ImageId"), vals.Get("WatermarkName"))
	if err != nil {
		return nil, err
	}

	return &attachImageWatermarkResponse{
		Xmlns:        ec2XMLNS,
		RequestID:    reqID,
		WatermarkKey: key,
	}, nil
}

type detachImageWatermarkResponse struct {
	XMLName   xml.Name `xml:"DetachImageWatermarkResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

func (h *Handler) handleDetachImageWatermark(vals url.Values, reqID string) (any, error) {
	if err := h.Backend.DetachImageWatermark(vals.Get("ImageId"), vals.Get("WatermarkKey")); err != nil {
		return nil, err
	}

	return &detachImageWatermarkResponse{Xmlns: ec2XMLNS, RequestID: reqID, Return: true}, nil
}
