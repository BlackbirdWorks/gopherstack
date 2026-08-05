package ec2

import (
	"encoding/xml"
	"net/url"
	"strconv"
)

func registerApplicationStatusChecksOps(h *Handler, ops map[string]ec2ActionFn) {
	ops["CreateApplicationStatusCheck"] = h.handleCreateApplicationStatusCheck
	ops["ModifyApplicationStatusCheck"] = h.handleModifyApplicationStatusCheck
	ops["DescribeApplicationStatusChecks"] = h.handleDescribeApplicationStatusChecks
	ops["DeleteApplicationStatusCheck"] = h.handleDeleteApplicationStatusCheck
	ops["AssociateApplicationStatusCheck"] = h.handleAssociateApplicationStatusCheck
	ops["DisassociateApplicationStatusCheck"] = h.handleDisassociateApplicationStatusCheck
	ops["DescribeApplicationStatusCheckAssociations"] = h.handleDescribeApplicationStatusCheckAssociations
	ops["EnableApplicationStatusCheckSuppression"] = h.handleEnableApplicationStatusCheckSuppression
	ops["DisableApplicationStatusCheckSuppression"] = h.handleDisableApplicationStatusCheckSuppression
	ops["DescribeApplicationStatus"] = h.handleDescribeApplicationStatus
}

func applicationStatusChecksSupportedOperations() []string {
	return []string{
		"CreateApplicationStatusCheck",
		"ModifyApplicationStatusCheck",
		"DescribeApplicationStatusChecks",
		"DeleteApplicationStatusCheck",
		"AssociateApplicationStatusCheck",
		"DisassociateApplicationStatusCheck",
		"DescribeApplicationStatusCheckAssociations",
		"EnableApplicationStatusCheckSuppression",
		"DisableApplicationStatusCheckSuppression",
		"DescribeApplicationStatus",
	}
}

// applicationStatusCheckItem mirrors the real ApplicationStatusCheckResponseObject
// shape (field-diffed against awsEc2query_deserializeDocumentApplicationStatusCheckResponseObject).
// HealthCheckPaths is intentionally omitted — not modeled, see PARITY.md gaps.
type applicationStatusCheckItem struct {
	ApplicationStatusCheckID         string          `xml:"applicationStatusCheckId,omitempty"`
	Aggregation                      string          `xml:"aggregation,omitempty"`
	CreationTime                     string          `xml:"creationTime,omitempty"`
	DeletionTime                     string          `xml:"deletionTime,omitempty"`
	IPScope                          string          `xml:"ipScope,omitempty"`
	IPVersion                        string          `xml:"ipVersion,omitempty"`
	LastUpdatedAt                    string          `xml:"lastUpdatedAt,omitempty"`
	ModifyTime                       string          `xml:"modifyTime,omitempty"`
	Path                             string          `xml:"path,omitempty"`
	Protocol                         string          `xml:"protocol,omitempty"`
	StatusCodeMatcher                string          `xml:"statusCodeMatcher,omitempty"`
	TagSet                           []simpleTagItem `xml:"tagSet>item"`
	TargetTagAssociationSet          []simpleTagItem `xml:"targetTagAssociationSet>item"`
	Port                             int             `xml:"port,omitempty"`
	DeviceIndex                      int             `xml:"deviceIndex,omitempty"`
	FailureThreshold                 int             `xml:"failureThreshold,omitempty"`
	InitializationGracePeriodSeconds int             `xml:"initializationGracePeriodSeconds,omitempty"`
	Interval                         int             `xml:"interval,omitempty"`
	SuccessThreshold                 int             `xml:"successThreshold,omitempty"`
	Timeout                          int             `xml:"timeout,omitempty"`
}

// applicationStatusCheckToItem converts a check to its wire shape. tags are
// the check's own resource tags (from CreateTags/TagSpecifications);
// tagAssociations are the check's tag-based TargetTagAssociations (a real
// ApplicationStatusCheckResponseObject field, backed by this check's own
// tag-type ApplicationStatusCheckAssociation entries).
func applicationStatusCheckToItem(
	c *ApplicationStatusCheck,
	tags map[string]string,
	tagAssociations []simpleTagItem,
) applicationStatusCheckItem {
	item := applicationStatusCheckItem{
		ApplicationStatusCheckID:         c.ApplicationStatusCheckID,
		Aggregation:                      c.Aggregation,
		IPScope:                          c.IPScope,
		IPVersion:                        c.IPVersion,
		Path:                             c.Path,
		Protocol:                         c.Protocol,
		StatusCodeMatcher:                c.StatusCodeMatcher,
		TagSet:                           tagItemsFromMap(tags),
		TargetTagAssociationSet:          tagAssociations,
		Port:                             c.Port,
		DeviceIndex:                      c.DeviceIndex,
		FailureThreshold:                 c.FailureThreshold,
		InitializationGracePeriodSeconds: c.InitializationGracePeriodSeconds,
		Interval:                         c.Interval,
		SuccessThreshold:                 c.SuccessThreshold,
		Timeout:                          c.Timeout,
	}

	if !c.CreationTime.IsZero() {
		item.CreationTime = c.CreationTime.UTC().Format(timeLayoutISO)
	}

	if !c.DeletionTime.IsZero() {
		item.DeletionTime = c.DeletionTime.UTC().Format(timeLayoutISO)
	}

	if !c.LastUpdatedAt.IsZero() {
		item.LastUpdatedAt = c.LastUpdatedAt.UTC().Format(timeLayoutISO)
	}

	if !c.ModifyTime.IsZero() {
		item.ModifyTime = c.ModifyTime.UTC().Format(timeLayoutISO)
	}

	return item
}

type createApplicationStatusCheckResponse struct {
	XMLName xml.Name `xml:"CreateApplicationStatusCheckResponse"`
	Xmlns   string   `xml:"xmlns,attr"`

	RequestID string                     `xml:"requestId"`
	Check     applicationStatusCheckItem `xml:"applicationStatusCheck"`
}

type modifyApplicationStatusCheckResponse struct {
	XMLName   xml.Name                   `xml:"ModifyApplicationStatusCheckResponse"`
	Xmlns     string                     `xml:"xmlns,attr"`
	RequestID string                     `xml:"requestId"`
	Check     applicationStatusCheckItem `xml:"applicationStatusCheck"`
}

type deleteApplicationStatusCheckResponse struct {
	XMLName   xml.Name                   `xml:"DeleteApplicationStatusCheckResponse"`
	Xmlns     string                     `xml:"xmlns,attr"`
	RequestID string                     `xml:"requestId"`
	Check     applicationStatusCheckItem `xml:"applicationStatusCheck"`
}

type describeApplicationStatusChecksResponse struct {
	XMLName   xml.Name `xml:"DescribeApplicationStatusChecksResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Checks    struct {
		Items []applicationStatusCheckItem `xml:"item"`
	} `xml:"applicationStatusCheckSet"`
}

// applicationStatusCheckAssociationItem mirrors the real AWS
// ApplicationStatusCheckAssociationObject shape.
type applicationStatusCheckAssociationItem struct {
	ApplicationStatusCheckID string `xml:"applicationStatusCheckId,omitempty"`
	AssociationType          string `xml:"associationType,omitempty"`
	Key                      string `xml:"key,omitempty"`
	Value                    string `xml:"value,omitempty"`
}

func applicationStatusCheckAssociationToItem(
	a *ApplicationStatusCheckAssociation,
) applicationStatusCheckAssociationItem {
	item := applicationStatusCheckAssociationItem{
		ApplicationStatusCheckID: a.ApplicationStatusCheckID,
		AssociationType:          a.AssociationType,
	}

	if a.AssociationType == appStatusAssocTypeTag {
		item.Key = a.TagKey
		item.Value = a.TagValue
	} else {
		item.Value = a.InstanceID
	}

	return item
}

type describeApplicationStatusCheckAssociationsResponse struct {
	XMLName      xml.Name `xml:"DescribeApplicationStatusCheckAssociationsResponse"`
	Xmlns        string   `xml:"xmlns,attr"`
	RequestID    string   `xml:"requestId"`
	Associations struct {
		Items []applicationStatusCheckAssociationItem `xml:"item"`
	} `xml:"associationSet"`
}

// successfulAssociationItem / unsuccessfulAssociationItem mirror
// Successful/UnsuccessfulAssociationResponseObject. Their AssociationType
// uses INSTANCE_ID/EC2TAG, unlike applicationStatusCheckAssociationItem's
// instance-id/tag — see appStatusAssocTypeInstanceWire/TagWire.
type successfulAssociationItem struct {
	ApplicationStatusCheckID string `xml:"applicationStatusCheckId,omitempty"`
	AssociationType          string `xml:"associationType,omitempty"`
	AssociationValue         string `xml:"associationValue,omitempty"`
}

type unsuccessfulAssociationItem struct {
	ApplicationStatusCheckID string `xml:"applicationStatusCheckId,omitempty"`
	AssociationType          string `xml:"associationType,omitempty"`
	AssociationValue         string `xml:"associationValue,omitempty"`
	Reason                   string `xml:"reason,omitempty"`
}

func successfulAssociationResultsToItems(
	results []ApplicationStatusAssociationResult,
) []successfulAssociationItem {
	successful := make([]successfulAssociationItem, 0, len(results))

	for _, r := range results {
		successful = append(successful, successfulAssociationItem{
			ApplicationStatusCheckID: r.ApplicationStatusCheckID,
			AssociationType:          r.AssociationType,
			AssociationValue:         r.AssociationValue,
		})
	}

	return successful
}

func unsuccessfulAssociationResultsToItems(
	results []ApplicationStatusAssociationResult,
) []unsuccessfulAssociationItem {
	unsuccessful := make([]unsuccessfulAssociationItem, 0, len(results))

	for _, r := range results {
		unsuccessful = append(unsuccessful, unsuccessfulAssociationItem(r))
	}

	return unsuccessful
}

type associateApplicationStatusCheckResponse struct {
	XMLName xml.Name `xml:"AssociateApplicationStatusCheckResponse"`
	Xmlns   string   `xml:"xmlns,attr"`

	RequestID  string `xml:"requestId"`
	Successful struct {
		Items []successfulAssociationItem `xml:"item"`
	} `xml:"successfulResultSet"`
	Unsuccessful struct {
		Items []unsuccessfulAssociationItem `xml:"item"`
	} `xml:"unsuccessfulResultSet"`
}

type disassociateApplicationStatusCheckResponse struct {
	XMLName    xml.Name `xml:"DisassociateApplicationStatusCheckResponse"`
	Xmlns      string   `xml:"xmlns,attr"`
	RequestID  string   `xml:"requestId"`
	Successful struct {
		Items []successfulAssociationItem `xml:"item"`
	} `xml:"successfulResultSet"`
	Unsuccessful struct {
		Items []unsuccessfulAssociationItem `xml:"item"`
	} `xml:"unsuccessfulResultSet"`
}

// successfulSuppressionItem / unsuccessfulSuppressionItem mirror the real
// SuccessfulSuppressionResponseObject / UnsuccessfulSuppressionResponseObject shapes.
type successfulSuppressionItem struct {
	InstanceID string `xml:"instanceId,omitempty"`
	ResumeAt   string `xml:"resumeAt,omitempty"`
	SuppressAt string `xml:"suppressAt,omitempty"`
}

type unsuccessfulSuppressionItem struct {
	InstanceID string `xml:"instanceId,omitempty"`
	Reason     string `xml:"reason,omitempty"`
	ResumeAt   string `xml:"resumeAt,omitempty"`
	SuppressAt string `xml:"suppressAt,omitempty"`
}

func suppressionsToItems(sups []*ApplicationStatusSuppression) []successfulSuppressionItem {
	items := make([]successfulSuppressionItem, 0, len(sups))

	for _, s := range sups {
		item := successfulSuppressionItem{InstanceID: s.InstanceID}
		if !s.SuppressAt.IsZero() {
			item.SuppressAt = s.SuppressAt.UTC().Format(timeLayoutISO)
		}

		if !s.ResumeAt.IsZero() {
			item.ResumeAt = s.ResumeAt.UTC().Format(timeLayoutISO)
		}

		items = append(items, item)
	}

	return items
}

func suppressionFailuresToItems(
	fails []ApplicationStatusSuppressionFailure,
) []unsuccessfulSuppressionItem {
	items := make([]unsuccessfulSuppressionItem, 0, len(fails))

	for _, f := range fails {
		items = append(items, unsuccessfulSuppressionItem{
			InstanceID: f.InstanceID,
			Reason:     f.Reason,
		})
	}

	return items
}

type enableApplicationStatusCheckSuppressionResponse struct {
	XMLName    xml.Name `xml:"EnableApplicationStatusCheckSuppressionResponse"`
	Xmlns      string   `xml:"xmlns,attr"`
	RequestID  string   `xml:"requestId"`
	Successful struct {
		Items []successfulSuppressionItem `xml:"item"`
	} `xml:"successfulResultSet"`
	Unsuccessful struct {
		Items []unsuccessfulSuppressionItem `xml:"item"`
	} `xml:"unsuccessfulResultSet"`
}

type disableApplicationStatusCheckSuppressionResponse struct {
	XMLName    xml.Name `xml:"DisableApplicationStatusCheckSuppressionResponse"`
	Xmlns      string   `xml:"xmlns,attr"`
	RequestID  string   `xml:"requestId"`
	Successful struct {
		Items []successfulSuppressionItem `xml:"item"`
	} `xml:"successfulResultSet"`
	Unsuccessful struct {
		Items []unsuccessfulSuppressionItem `xml:"item"`
	} `xml:"unsuccessfulResultSet"`
}

// instanceApplicationStatusItem mirrors the real InstanceApplicationStatus
// shape. AvailabilityZoneId and applicationStatus's detailSet/statusSince
// are always empty — documented gaps, see InstanceApplicationStatus.
type instanceApplicationStatusItem struct {
	ApplicationStatus struct {
		Status          string `xml:"status,omitempty"`
		ResumeAt        string `xml:"resumeAt,omitempty"`
		StatusTimeStamp string `xml:"statusTimeStamp,omitempty"`
	} `xml:"applicationStatus"`
	InstanceID         string          `xml:"instanceId,omitempty"`
	AvailabilityZone   string          `xml:"availabilityZone,omitempty"`
	AvailabilityZoneID string          `xml:"availabilityZoneId,omitempty"`
	TagSet             []simpleTagItem `xml:"tagSet>item"`
}

func instanceApplicationStatusToItem(
	s *InstanceApplicationStatus,
	tags map[string]string,
) instanceApplicationStatusItem {
	item := instanceApplicationStatusItem{
		InstanceID:         s.InstanceID,
		AvailabilityZone:   s.AvailabilityZone,
		AvailabilityZoneID: s.AvailabilityZoneID,
		TagSet:             tagItemsFromMap(tags),
	}
	item.ApplicationStatus.Status = s.Status

	if !s.StatusTimeStamp.IsZero() {
		item.ApplicationStatus.StatusTimeStamp = s.StatusTimeStamp.UTC().Format(timeLayoutISO)
	}

	if !s.ResumeAt.IsZero() {
		item.ApplicationStatus.ResumeAt = s.ResumeAt.UTC().Format(timeLayoutISO)
	}

	return item
}

type describeApplicationStatusResponse struct {
	XMLName             xml.Name `xml:"DescribeApplicationStatusResponse"`
	Xmlns               string   `xml:"xmlns,attr"`
	RequestID           string   `xml:"requestId"`
	ApplicationStatuses struct {
		Instances []instanceApplicationStatusItem `xml:"instanceSet>item"`
	} `xml:"applicationStatusesResponseType"`
}

// intFromVals parses key as an int, returning (0, false) if absent or
// invalid — distinguishes "not specified" from an explicit value.
func intFromVals(vals url.Values, key string) (int, bool) {
	s := vals.Get(key)
	if s == "" {
		return 0, false
	}

	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, false
	}

	return n, true
}

func intParamPtr(vals url.Values, key string) *int {
	if n, ok := intFromVals(vals, key); ok {
		return &n
	}

	return nil
}

func strParamPtr(vals url.Values, key string) *string {
	if v := vals.Get(key); v != "" {
		return &v
	}

	return nil
}

func applicationStatusCheckParamsFromVals(vals url.Values) ApplicationStatusCheckParams {
	return ApplicationStatusCheckParams{
		Protocol:                         strParamPtr(vals, "Protocol"),
		Aggregation:                      strParamPtr(vals, "Aggregation"),
		IPScope:                          strParamPtr(vals, "IpScope"),
		IPVersion:                        strParamPtr(vals, "IpVersion"),
		Path:                             strParamPtr(vals, "Path"),
		StatusCodeMatcher:                strParamPtr(vals, "StatusCodeMatcher"),
		Port:                             intParamPtr(vals, "Port"),
		DeviceIndex:                      intParamPtr(vals, "DeviceIndex"),
		FailureThreshold:                 intParamPtr(vals, "FailureThreshold"),
		InitializationGracePeriodSeconds: intParamPtr(vals, "InitializationGracePeriodSeconds"),
		Interval:                         intParamPtr(vals, "Interval"),
		SuccessThreshold:                 intParamPtr(vals, "SuccessThreshold"),
		Timeout:                          intParamPtr(vals, "Timeout"),
	}
}

// checkTagAssociationItems returns the check's own tag-based
// TargetTagAssociations, rendered as the check's own wire item field.
func (h *Handler) checkTagAssociationItems(checkID string) []simpleTagItem {
	assocs := h.Backend.DescribeApplicationStatusCheckAssociations(
		[]string{checkID},
		map[string][]string{"association-type": {appStatusAssocTypeTag}},
	)

	items := make([]simpleTagItem, 0, len(assocs))
	for _, a := range assocs {
		items = append(items, simpleTagItem{Key: a.TagKey, Value: a.TagValue})
	}

	return items
}

func (h *Handler) handleCreateApplicationStatusCheck(vals url.Values, reqID string) (any, error) {
	p := applicationStatusCheckParamsFromVals(vals)

	check, err := h.Backend.CreateApplicationStatusCheck(p)
	if err != nil {
		return nil, err
	}

	tags := parseTagSpecification(vals, "application-status-check")
	if len(tags) > 0 {
		if err = h.Backend.CreateTags([]string{check.ApplicationStatusCheckID}, tags); err != nil {
			return nil, err
		}
	}

	return &createApplicationStatusCheckResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Check:     applicationStatusCheckToItem(check, tags, nil),
	}, nil
}

func (h *Handler) handleModifyApplicationStatusCheck(vals url.Values, reqID string) (any, error) {
	id := vals.Get("ApplicationStatusCheckId")
	p := applicationStatusCheckParamsFromVals(vals)

	check, err := h.Backend.ModifyApplicationStatusCheck(id, p)
	if err != nil {
		return nil, err
	}

	tags := h.Backend.TagsForResource(check.ApplicationStatusCheckID)
	tagAssocs := h.checkTagAssociationItems(check.ApplicationStatusCheckID)

	return &modifyApplicationStatusCheckResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Check:     applicationStatusCheckToItem(check, tags, tagAssocs),
	}, nil
}

func (h *Handler) handleDeleteApplicationStatusCheck(vals url.Values, reqID string) (any, error) {
	id := vals.Get("ApplicationStatusCheckId")

	check, err := h.Backend.DeleteApplicationStatusCheck(id)
	if err != nil {
		return nil, err
	}

	tags := h.Backend.TagsForResource(check.ApplicationStatusCheckID)

	return &deleteApplicationStatusCheckResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Check:     applicationStatusCheckToItem(check, tags, nil),
	}, nil
}

func (h *Handler) handleDescribeApplicationStatusChecks(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "ApplicationStatusCheckId")
	filters := parseEC2Filters(vals)
	includeAll := vals.Get("IncludeAll") == ec2BooleanTrue

	checks := h.Backend.DescribeApplicationStatusChecks(ids, filters, includeAll)

	resp := &describeApplicationStatusChecksResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, c := range checks {
		tags := h.Backend.TagsForResource(c.ApplicationStatusCheckID)
		tagAssocs := h.checkTagAssociationItems(c.ApplicationStatusCheckID)
		resp.Checks.Items = append(resp.Checks.Items, applicationStatusCheckToItem(c, tags, tagAssocs))
	}

	return resp, nil
}

func (h *Handler) handleAssociateApplicationStatusCheck(vals url.Values, reqID string) (any, error) {
	checkID := vals.Get("ApplicationStatusCheckId")
	instanceIDs := parseMemberList(vals, "InstanceId")
	tagAssociations := parseCustomTagKeyValues(vals, "TargetTagAssociation")

	successful, unsuccessful, err := h.Backend.AssociateApplicationStatusCheck(
		checkID, instanceIDs, tagAssociations,
	)
	if err != nil {
		return nil, err
	}

	resp := &associateApplicationStatusCheckResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	resp.Successful.Items = successfulAssociationResultsToItems(successful)
	resp.Unsuccessful.Items = unsuccessfulAssociationResultsToItems(unsuccessful)

	return resp, nil
}

func (h *Handler) handleDisassociateApplicationStatusCheck(vals url.Values, reqID string) (any, error) {
	checkID := vals.Get("ApplicationStatusCheckId")
	instanceIDs := parseMemberList(vals, "InstanceId")
	tagAssociations := parseCustomTagKeyValues(vals, "TargetTagAssociation")

	successful, unsuccessful, err := h.Backend.DisassociateApplicationStatusCheck(
		checkID, instanceIDs, tagAssociations,
	)
	if err != nil {
		return nil, err
	}

	resp := &disassociateApplicationStatusCheckResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	resp.Successful.Items = successfulAssociationResultsToItems(successful)
	resp.Unsuccessful.Items = unsuccessfulAssociationResultsToItems(unsuccessful)

	return resp, nil
}

// parseCustomTagKeyValues parses "<prefix>.N.Key" / "<prefix>.N.Value" pairs.
func parseCustomTagKeyValues(vals url.Values, prefix string) []CustomTagKeyValue {
	var out []CustomTagKeyValue

	for i := 1; ; i++ {
		key := vals.Get(prefix + "." + strconv.Itoa(i) + ".Key")
		if key == "" && vals.Get(prefix+"."+strconv.Itoa(i)+".Value") == "" {
			break
		}

		out = append(out, CustomTagKeyValue{
			Key:   key,
			Value: vals.Get(prefix + "." + strconv.Itoa(i) + ".Value"),
		})
	}

	return out
}

func (h *Handler) handleDescribeApplicationStatusCheckAssociations(
	vals url.Values,
	reqID string,
) (any, error) {
	ids := parseMemberList(vals, "ApplicationStatusCheckId")
	filters := parseEC2Filters(vals)

	assocs := h.Backend.DescribeApplicationStatusCheckAssociations(ids, filters)

	resp := &describeApplicationStatusCheckAssociationsResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, a := range assocs {
		resp.Associations.Items = append(
			resp.Associations.Items, applicationStatusCheckAssociationToItem(a),
		)
	}

	return resp, nil
}

func (h *Handler) handleEnableApplicationStatusCheckSuppression(
	vals url.Values,
	reqID string,
) (any, error) {
	instanceIDs := parseMemberList(vals, "InstanceId")
	duration, _ := intFromVals(vals, "DurationSeconds")

	successful, unsuccessful := h.Backend.EnableApplicationStatusCheckSuppression(instanceIDs, duration)

	resp := &enableApplicationStatusCheckSuppressionResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	resp.Successful.Items = suppressionsToItems(successful)
	resp.Unsuccessful.Items = suppressionFailuresToItems(unsuccessful)

	return resp, nil
}

func (h *Handler) handleDisableApplicationStatusCheckSuppression(
	vals url.Values,
	reqID string,
) (any, error) {
	instanceIDs := parseMemberList(vals, "InstanceId")

	successful, unsuccessful := h.Backend.DisableApplicationStatusCheckSuppression(instanceIDs)

	resp := &disableApplicationStatusCheckSuppressionResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	resp.Successful.Items = suppressionsToItems(successful)
	resp.Unsuccessful.Items = suppressionFailuresToItems(unsuccessful)

	return resp, nil
}

func (h *Handler) handleDescribeApplicationStatus(vals url.Values, reqID string) (any, error) {
	instanceIDs := parseMemberList(vals, "InstanceId")
	filters := parseEC2Filters(vals)

	statuses := h.Backend.DescribeApplicationStatus(instanceIDs, filters)

	resp := &describeApplicationStatusResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, s := range statuses {
		tags := h.Backend.TagsForResource(s.InstanceID)
		resp.ApplicationStatuses.Instances = append(
			resp.ApplicationStatuses.Instances, instanceApplicationStatusToItem(s, tags),
		)
	}

	return resp, nil
}
