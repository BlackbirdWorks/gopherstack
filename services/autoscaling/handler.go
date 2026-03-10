package autoscaling

import (
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	autoscalingVersion = "2011-01-01"
	autoscalingXMLNS   = "http://autoscaling.amazonaws.com/doc/2011-01-01/"
)

// Handler is the Echo HTTP handler for Autoscaling operations.
type Handler struct {
	Backend StorageBackend
}

// NewHandler creates a new Autoscaling handler.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{Backend: backend}
}

// Name returns the service name.
func (h *Handler) Name() string { return "Autoscaling" }

// GetSupportedOperations returns the list of supported Autoscaling operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateAutoScalingGroup",
		"DescribeAutoScalingGroups",
		"UpdateAutoScalingGroup",
		"DeleteAutoScalingGroup",
		"CreateLaunchConfiguration",
		"DescribeLaunchConfigurations",
		"DeleteLaunchConfiguration",
		"DescribeScalingActivities",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "autoscaling" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler instance handles.
func (h *Handler) ChaosRegions() []string { return []string{config.DefaultRegion} }

// RouteMatcher returns a function that matches Autoscaling requests.
// Autoscaling requests are form-encoded POSTs with Version=2011-01-01.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		r := c.Request()
		if r.Method != http.MethodPost {
			return false
		}

		if strings.HasPrefix(r.URL.Path, "/dashboard/") {
			return false
		}

		ct := r.Header.Get("Content-Type")
		if !strings.Contains(ct, "application/x-www-form-urlencoded") {
			return false
		}

		body, err := httputils.ReadBody(r)
		if err != nil {
			return false
		}

		vals, err := url.ParseQuery(string(body))
		if err != nil {
			return false
		}

		return vals.Get("Version") == autoscalingVersion
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityFormStandard }

// ExtractOperation extracts the Autoscaling action from the request.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	r := c.Request()
	if err := r.ParseForm(); err != nil {
		return "Unknown"
	}

	action := r.Form.Get("Action")
	if action == "" {
		return "Unknown"
	}

	return action
}

// ExtractResource extracts the Auto Scaling group name from the request.
func (h *Handler) ExtractResource(c *echo.Context) string {
	r := c.Request()
	if err := r.ParseForm(); err != nil {
		return ""
	}

	return r.Form.Get("AutoScalingGroupName")
}

// Handler returns the Echo handler function for Autoscaling operations.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		r := c.Request()
		if err := r.ParseForm(); err != nil {
			return h.writeError(c, http.StatusInternalServerError, "InternalFailure", "failed to read request body")
		}

		vals := r.Form
		action := vals.Get("Action")
		if action == "" {
			return h.writeError(c, http.StatusBadRequest, "MissingAction", "missing Action parameter")
		}

		log := logger.Load(r.Context())
		log.Debug("autoscaling request", "action", action)

		resp, opErr := h.dispatch(action, vals)
		if opErr != nil {
			return h.handleOpError(c, action, opErr)
		}

		xmlBytes, err := marshalXML(resp)
		if err != nil {
			return h.writeError(c, http.StatusInternalServerError, "InternalFailure", "internal server error")
		}

		return c.Blob(http.StatusOK, "text/xml", xmlBytes)
	}
}

// dispatch routes the Autoscaling action to the appropriate handler.
func (h *Handler) dispatch(action string, vals url.Values) (any, error) {
	switch action {
	case "CreateAutoScalingGroup":
		return h.handleCreateAutoScalingGroup(vals)
	case "DescribeAutoScalingGroups":
		return h.handleDescribeAutoScalingGroups(vals)
	case "UpdateAutoScalingGroup":
		return h.handleUpdateAutoScalingGroup(vals)
	case "DeleteAutoScalingGroup":
		return h.handleDeleteAutoScalingGroup(vals)
	case "CreateLaunchConfiguration":
		return h.handleCreateLaunchConfiguration(vals)
	case "DescribeLaunchConfigurations":
		return h.handleDescribeLaunchConfigurations(vals)
	case "DeleteLaunchConfiguration":
		return h.handleDeleteLaunchConfiguration(vals)
	case "DescribeScalingActivities":
		return h.handleDescribeScalingActivities(vals)
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnknownAction, action)
	}
}

func (h *Handler) handleCreateAutoScalingGroup(vals url.Values) (any, error) {
	name := vals.Get("AutoScalingGroupName")
	lcName := vals.Get("LaunchConfigurationName")
	healthCheckType := vals.Get("HealthCheckType")

	minSize, err := parseIntVal(vals.Get("MinSize"))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid MinSize", ErrInvalidParameter)
	}

	maxSize, err := parseIntVal(vals.Get("MaxSize"))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid MaxSize", ErrInvalidParameter)
	}

	desiredCapacity, err := parseIntVal(vals.Get("DesiredCapacity"))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid DesiredCapacity", ErrInvalidParameter)
	}

	defaultCooldown, err := parseIntVal(vals.Get("DefaultCooldown"))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid DefaultCooldown", ErrInvalidParameter)
	}

	healthCheckGracePeriod, err := parseIntVal(vals.Get("HealthCheckGracePeriod"))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid HealthCheckGracePeriod", ErrInvalidParameter)
	}

	azs := parseMembers(vals, "AvailabilityZones.member")
	lbNames := parseMembers(vals, "LoadBalancerNames.member")
	targetGroupARNs := parseMembers(vals, "TargetGroupARNs.member")
	tags := parseTags(vals, "Tags.member")

	input := CreateAutoScalingGroupInput{
		AutoScalingGroupName:    name,
		LaunchConfigurationName: lcName,
		MinSize:                 minSize,
		MaxSize:                 maxSize,
		DesiredCapacity:         desiredCapacity,
		DefaultCooldown:         defaultCooldown,
		HealthCheckType:         healthCheckType,
		HealthCheckGracePeriod:  healthCheckGracePeriod,
		AvailabilityZones:       azs,
		LoadBalancerNames:       lbNames,
		TargetGroupARNs:         targetGroupARNs,
		Tags:                    tags,
	}

	_, createErr := h.Backend.CreateAutoScalingGroup(input)
	if createErr != nil {
		return nil, createErr
	}

	return &createAutoScalingGroupResponse{
		Xmlns:            autoscalingXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-" + name},
	}, nil
}

func (h *Handler) handleDescribeAutoScalingGroups(vals url.Values) (any, error) {
	names := parseMembers(vals, "AutoScalingGroupNames.member")

	groups, err := h.Backend.DescribeAutoScalingGroups(names)
	if err != nil {
		return nil, err
	}

	members := make([]xmlAutoScalingGroup, 0, len(groups))
	for i := range groups {
		members = append(members, toXMLGroup(&groups[i]))
	}

	return &describeAutoScalingGroupsResponse{
		Xmlns: autoscalingXMLNS,
		Result: describeAutoScalingGroupsResult{
			AutoScalingGroups: xmlAutoScalingGroupList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-describe-groups"},
	}, nil
}

func (h *Handler) handleUpdateAutoScalingGroup(vals url.Values) (any, error) {
	name := vals.Get("AutoScalingGroupName")

	input := UpdateAutoScalingGroupInput{
		AutoScalingGroupName:    name,
		LaunchConfigurationName: vals.Get("LaunchConfigurationName"),
		HealthCheckType:         vals.Get("HealthCheckType"),
		AvailabilityZones:       parseMembers(vals, "AvailabilityZones.member"),
	}

	if v := vals.Get("MinSize"); v != "" {
		n, err := parseIntVal(v)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid MinSize", ErrInvalidParameter)
		}

		input.MinSize = &n
	}

	if v := vals.Get("MaxSize"); v != "" {
		n, err := parseIntVal(v)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid MaxSize", ErrInvalidParameter)
		}

		input.MaxSize = &n
	}

	if v := vals.Get("DesiredCapacity"); v != "" {
		n, err := parseIntVal(v)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid DesiredCapacity", ErrInvalidParameter)
		}

		input.DesiredCapacity = &n
	}

	if v := vals.Get("DefaultCooldown"); v != "" {
		n, err := parseIntVal(v)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid DefaultCooldown", ErrInvalidParameter)
		}

		input.DefaultCooldown = &n
	}

	if v := vals.Get("HealthCheckGracePeriod"); v != "" {
		n, err := parseIntVal(v)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid HealthCheckGracePeriod", ErrInvalidParameter)
		}

		input.HealthCheckGracePeriod = &n
	}

	_, updateErr := h.Backend.UpdateAutoScalingGroup(input)
	if updateErr != nil {
		return nil, updateErr
	}

	return &updateAutoScalingGroupResponse{
		Xmlns:            autoscalingXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-update-" + name},
	}, nil
}

func (h *Handler) handleDeleteAutoScalingGroup(vals url.Values) (any, error) {
	name := vals.Get("AutoScalingGroupName")

	if err := h.Backend.DeleteAutoScalingGroup(name); err != nil {
		return nil, err
	}

	return &deleteAutoScalingGroupResponse{
		Xmlns:            autoscalingXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-delete-" + name},
	}, nil
}

func (h *Handler) handleCreateLaunchConfiguration(vals url.Values) (any, error) {
	name := vals.Get("LaunchConfigurationName")
	imageID := vals.Get("ImageId")
	instanceType := vals.Get("InstanceType")
	keyName := vals.Get("KeyName")
	iamInstanceProfile := vals.Get("IamInstanceProfile")
	userData := vals.Get("UserData")
	kernelID := vals.Get("KernelId")
	ramdiskID := vals.Get("RamdiskId")
	securityGroups := parseMembers(vals, "SecurityGroups.member")

	input := CreateLaunchConfigurationInput{
		LaunchConfigurationName: name,
		ImageID:                 imageID,
		InstanceType:            instanceType,
		KeyName:                 keyName,
		IAMInstanceProfile:      iamInstanceProfile,
		UserData:                userData,
		KernelID:                kernelID,
		RamdiskID:               ramdiskID,
		SecurityGroups:          securityGroups,
	}

	_, createErr := h.Backend.CreateLaunchConfiguration(input)
	if createErr != nil {
		return nil, createErr
	}

	return &createLaunchConfigurationResponse{
		Xmlns:            autoscalingXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-create-lc-" + name},
	}, nil
}

func (h *Handler) handleDescribeLaunchConfigurations(vals url.Values) (any, error) {
	names := parseMembers(vals, "LaunchConfigurationNames.member")

	lcs, err := h.Backend.DescribeLaunchConfigurations(names)
	if err != nil {
		return nil, err
	}

	members := make([]xmlLaunchConfiguration, 0, len(lcs))
	for i := range lcs {
		members = append(members, toXMLLaunchConfiguration(&lcs[i]))
	}

	return &describeLaunchConfigurationsResponse{
		Xmlns: autoscalingXMLNS,
		Result: describeLaunchConfigurationsResult{
			LaunchConfigurations: xmlLaunchConfigurationList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-describe-lcs"},
	}, nil
}

func (h *Handler) handleDeleteLaunchConfiguration(vals url.Values) (any, error) {
	name := vals.Get("LaunchConfigurationName")

	if err := h.Backend.DeleteLaunchConfiguration(name); err != nil {
		return nil, err
	}

	return &deleteLaunchConfigurationResponse{
		Xmlns:            autoscalingXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-delete-lc-" + name},
	}, nil
}

func (h *Handler) handleDescribeScalingActivities(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")

	activities, err := h.Backend.DescribeScalingActivities(groupName)
	if err != nil {
		return nil, err
	}

	members := make([]xmlScalingActivity, 0, len(activities))
	for i := range activities {
		members = append(members, toXMLScalingActivity(&activities[i]))
	}

	return &describeScalingActivitiesResponse{
		Xmlns: autoscalingXMLNS,
		Result: describeScalingActivitiesResult{
			Activities: xmlScalingActivityList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-describe-activities"},
	}, nil
}

// handleOpError translates an operation error into an HTTP response.
func (h *Handler) handleOpError(c *echo.Context, action string, opErr error) error {
	statusCode := http.StatusBadRequest
	code := autoscalingErrorCode(opErr)

	if code == "" {
		code = "InternalFailure"
		statusCode = http.StatusInternalServerError
		logger.Load(c.Request().Context()).Error("autoscaling internal error", "error", opErr, "action", action)
	}

	return h.writeError(c, statusCode, code, opErr.Error())
}

func autoscalingErrorCode(opErr error) string {
	type errorMapping struct {
		sentinel error
		code     string
	}

	mappings := []errorMapping{
		{ErrGroupNotFound, "ValidationError"},
		{ErrGroupAlreadyExists, "AlreadyExists"},
		{ErrLaunchConfigurationNotFound, "ValidationError"},
		{ErrLaunchConfigurationAlreadyExists, "AlreadyExists"},
		{ErrInvalidParameter, "ValidationError"},
		{ErrUnknownAction, "InvalidAction"},
	}

	for _, m := range mappings {
		if errors.Is(opErr, m.sentinel) {
			return m.code
		}
	}

	return ""
}

func (h *Handler) writeError(c *echo.Context, statusCode int, code, message string) error {
	errResp := &autoscalingErrorResponse{
		Xmlns:     autoscalingXMLNS,
		Error:     autoscalingError{Code: code, Message: message, Type: "Sender"},
		RequestID: "autoscaling-error",
	}

	xmlBytes, err := marshalXML(errResp)
	if err != nil {
		return c.String(http.StatusInternalServerError, "internal server error")
	}

	return c.Blob(statusCode, "text/xml", xmlBytes)
}

func marshalXML(v any) ([]byte, error) {
	raw, err := xml.Marshal(v)
	if err != nil {
		return nil, err
	}

	return append([]byte(xml.Header), raw...), nil
}

// --- helper functions ---

// parseIntVal parses a string to int32. Empty string returns 0, nil.
func parseIntVal(s string) (int32, error) {
	if s == "" {
		return 0, nil
	}

	n, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		return 0, err
	}

	return int32(n), nil
}

// parseMembers extracts indexed form values with the given prefix (e.g. "AvailabilityZones.member").
func parseMembers(vals url.Values, prefix string) []string {
	result := make([]string, 0)

	for i := 1; ; i++ {
		key := fmt.Sprintf("%s.%d", prefix, i)
		v := vals.Get(key)

		if v == "" {
			break
		}

		result = append(result, v)
	}

	return result
}

// parseTags extracts tags from the form values using the standard AWS Tags.member.N.Key/Value pattern.
func parseTags(vals url.Values, prefix string) []Tag {
	result := make([]Tag, 0)

	for i := 1; ; i++ {
		keyParam := fmt.Sprintf("%s.%d.Key", prefix, i)
		valParam := fmt.Sprintf("%s.%d.Value", prefix, i)
		k := vals.Get(keyParam)

		if k == "" {
			break
		}

		result = append(result, Tag{Key: k, Value: vals.Get(valParam)})
	}

	return result
}

// toXMLGroup converts an AutoScalingGroup to the XML response type.
func toXMLGroup(g *AutoScalingGroup) xmlAutoScalingGroup {
	azs := make([]xmlStringValue, 0, len(g.AvailabilityZones))
	for _, az := range g.AvailabilityZones {
		azs = append(azs, xmlStringValue{Value: az})
	}

	lbNames := make([]xmlStringValue, 0, len(g.LoadBalancerNames))
	for _, lb := range g.LoadBalancerNames {
		lbNames = append(lbNames, xmlStringValue{Value: lb})
	}

	tgARNs := make([]xmlStringValue, 0, len(g.TargetGroupARNs))
	for _, tg := range g.TargetGroupARNs {
		tgARNs = append(tgARNs, xmlStringValue{Value: tg})
	}

	tags := make([]xmlTag, 0, len(g.Tags))
	for _, t := range g.Tags {
		tags = append(tags, xmlTag(t))
	}

	instances := make([]xmlInstance, 0, len(g.Instances))
	for _, inst := range g.Instances {
		instances = append(instances, xmlInstance{
			InstanceID:              inst.InstanceID,
			AvailabilityZone:        inst.AvailabilityZone,
			LifecycleState:          inst.LifecycleState,
			HealthStatus:            inst.HealthStatus,
			LaunchConfigurationName: inst.LaunchConfigurationName,
		})
	}

	return xmlAutoScalingGroup{
		AutoScalingGroupName:    g.AutoScalingGroupName,
		AutoScalingGroupARN:     g.AutoScalingGroupARN,
		LaunchConfigurationName: g.LaunchConfigurationName,
		MinSize:                 g.MinSize,
		MaxSize:                 g.MaxSize,
		DesiredCapacity:         g.DesiredCapacity,
		DefaultCooldown:         g.DefaultCooldown,
		HealthCheckType:         g.HealthCheckType,
		HealthCheckGracePeriod:  g.HealthCheckGracePeriod,
		CreatedTime:             g.CreatedTime.UTC().Format(time.RFC3339),
		Status:                  g.Status,
		AvailabilityZones:       xmlStringValueList{Members: azs},
		LoadBalancerNames:       xmlStringValueList{Members: lbNames},
		TargetGroupARNs:         xmlStringValueList{Members: tgARNs},
		Tags:                    xmlTagList{Members: tags},
		Instances:               xmlInstanceList{Members: instances},
	}
}

// toXMLLaunchConfiguration converts a LaunchConfiguration to the XML response type.
func toXMLLaunchConfiguration(lc *LaunchConfiguration) xmlLaunchConfiguration {
	sgs := make([]xmlStringValue, 0, len(lc.SecurityGroups))
	for _, sg := range lc.SecurityGroups {
		sgs = append(sgs, xmlStringValue{Value: sg})
	}

	return xmlLaunchConfiguration{
		LaunchConfigurationName: lc.LaunchConfigurationName,
		LaunchConfigurationARN:  lc.LaunchConfigurationARN,
		ImageID:                 lc.ImageID,
		InstanceType:            lc.InstanceType,
		KeyName:                 lc.KeyName,
		IAMInstanceProfile:      lc.IAMInstanceProfile,
		CreatedTime:             lc.CreatedTime.UTC().Format(time.RFC3339),
		SecurityGroups:          xmlStringValueList{Members: sgs},
	}
}

// toXMLScalingActivity converts a ScalingActivity to the XML response type.
func toXMLScalingActivity(a *ScalingActivity) xmlScalingActivity {
	endTime := ""
	if !a.EndTime.IsZero() {
		endTime = a.EndTime.UTC().Format(time.RFC3339)
	}

	return xmlScalingActivity{
		ActivityID:           a.ActivityID,
		AutoScalingGroupName: a.AutoScalingGroupName,
		Description:          a.Description,
		StatusCode:           a.StatusCode,
		StatusMessage:        a.StatusMessage,
		Progress:             a.Progress,
		StartTime:            a.StartTime.UTC().Format(time.RFC3339),
		EndTime:              endTime,
	}
}

// --- XML response types ---

type xmlResponseMetadata struct {
	RequestID string `xml:"RequestId"`
}

type autoscalingError struct {
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
	Type    string `xml:"Type"`
}

type autoscalingErrorResponse struct {
	XMLName   xml.Name         `xml:"ErrorResponse"`
	Xmlns     string           `xml:"xmlns,attr"`
	Error     autoscalingError `xml:"Error"`
	RequestID string           `xml:"RequestId"`
}

type createAutoScalingGroupResponse struct {
	XMLName          xml.Name            `xml:"CreateAutoScalingGroupResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type updateAutoScalingGroupResponse struct {
	XMLName          xml.Name            `xml:"UpdateAutoScalingGroupResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type deleteAutoScalingGroupResponse struct {
	XMLName          xml.Name            `xml:"DeleteAutoScalingGroupResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type xmlStringValue struct {
	Value string `xml:",chardata"`
}

type xmlStringValueList struct {
	Members []xmlStringValue `xml:"member"`
}

type xmlTag struct {
	Key   string `xml:"Key"`
	Value string `xml:"Value"`
}

type xmlTagList struct {
	Members []xmlTag `xml:"member"`
}

type xmlInstance struct {
	InstanceID              string `xml:"InstanceId"`
	AvailabilityZone        string `xml:"AvailabilityZone"`
	LifecycleState          string `xml:"LifecycleState"`
	HealthStatus            string `xml:"HealthStatus"`
	LaunchConfigurationName string `xml:"LaunchConfigurationName,omitempty"`
}

type xmlInstanceList struct {
	Members []xmlInstance `xml:"member"`
}

type xmlAutoScalingGroup struct {
	AutoScalingGroupARN     string             `xml:"AutoScalingGroupARN"`
	Status                  string             `xml:"Status,omitempty"`
	CreatedTime             string             `xml:"CreatedTime"`
	HealthCheckType         string             `xml:"HealthCheckType"`
	LaunchConfigurationName string             `xml:"LaunchConfigurationName,omitempty"`
	AutoScalingGroupName    string             `xml:"AutoScalingGroupName"`
	Instances               xmlInstanceList    `xml:"Instances"`
	AvailabilityZones       xmlStringValueList `xml:"AvailabilityZones"`
	Tags                    xmlTagList         `xml:"Tags"`
	TargetGroupARNs         xmlStringValueList `xml:"TargetGroupARNs"`
	LoadBalancerNames       xmlStringValueList `xml:"LoadBalancerNames"`
	MinSize                 int32              `xml:"MinSize"`
	MaxSize                 int32              `xml:"MaxSize"`
	DesiredCapacity         int32              `xml:"DesiredCapacity"`
	DefaultCooldown         int32              `xml:"DefaultCooldown"`
	HealthCheckGracePeriod  int32              `xml:"HealthCheckGracePeriod"`
}

type xmlAutoScalingGroupList struct {
	Members []xmlAutoScalingGroup `xml:"member"`
}

type describeAutoScalingGroupsResult struct {
	NextToken         string                  `xml:"NextToken,omitempty"`
	AutoScalingGroups xmlAutoScalingGroupList `xml:"AutoScalingGroups"`
}

type describeAutoScalingGroupsResponse struct {
	XMLName          xml.Name                        `xml:"DescribeAutoScalingGroupsResponse"`
	Xmlns            string                          `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata             `xml:"ResponseMetadata"`
	Result           describeAutoScalingGroupsResult `xml:"DescribeAutoScalingGroupsResult"`
}

type xmlLaunchConfiguration struct {
	LaunchConfigurationName string             `xml:"LaunchConfigurationName"`
	LaunchConfigurationARN  string             `xml:"LaunchConfigurationARN"`
	ImageID                 string             `xml:"ImageId"`
	InstanceType            string             `xml:"InstanceType"`
	KeyName                 string             `xml:"KeyName,omitempty"`
	IAMInstanceProfile      string             `xml:"IamInstanceProfile,omitempty"`
	CreatedTime             string             `xml:"CreatedTime"`
	SecurityGroups          xmlStringValueList `xml:"SecurityGroups"`
}

type xmlLaunchConfigurationList struct {
	Members []xmlLaunchConfiguration `xml:"member"`
}

type describeLaunchConfigurationsResult struct {
	NextToken            string                     `xml:"NextToken,omitempty"`
	LaunchConfigurations xmlLaunchConfigurationList `xml:"LaunchConfigurations"`
}

type createLaunchConfigurationResponse struct {
	XMLName          xml.Name            `xml:"CreateLaunchConfigurationResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type describeLaunchConfigurationsResponse struct {
	XMLName          xml.Name                           `xml:"DescribeLaunchConfigurationsResponse"`
	Xmlns            string                             `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata                `xml:"ResponseMetadata"`
	Result           describeLaunchConfigurationsResult `xml:"DescribeLaunchConfigurationsResult"`
}

type deleteLaunchConfigurationResponse struct {
	XMLName          xml.Name            `xml:"DeleteLaunchConfigurationResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type xmlScalingActivity struct {
	ActivityID           string `xml:"ActivityId"`
	AutoScalingGroupName string `xml:"AutoScalingGroupName"`
	Description          string `xml:"Description,omitempty"`
	StatusCode           string `xml:"StatusCode"`
	StatusMessage        string `xml:"StatusMessage,omitempty"`
	StartTime            string `xml:"StartTime"`
	EndTime              string `xml:"EndTime,omitempty"`
	Progress             int32  `xml:"Progress"`
}

type xmlScalingActivityList struct {
	Members []xmlScalingActivity `xml:"member"`
}

type describeScalingActivitiesResult struct {
	NextToken  string                 `xml:"NextToken,omitempty"`
	Activities xmlScalingActivityList `xml:"Activities"`
}

type describeScalingActivitiesResponse struct {
	XMLName          xml.Name                        `xml:"DescribeScalingActivitiesResponse"`
	Xmlns            string                          `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata             `xml:"ResponseMetadata"`
	Result           describeScalingActivitiesResult `xml:"DescribeScalingActivitiesResult"`
}
