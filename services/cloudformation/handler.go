package cloudformation

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const cfnNS = "http://cloudformation.amazonaws.com/doc/2010-05-15/"

// Handler is the Echo HTTP service handler for CloudFormation operations.
type Handler struct {
	Backend StorageBackend
}

// NewHandler creates a new CloudFormation handler.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{Backend: backend}
}

// Name returns the service name.
func (h *Handler) Name() string { return "CloudFormation" }

// GetSupportedOperations returns all supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateStack",
		"UpdateStack",
		"DeleteStack",
		"DescribeStacks",
		"ListStacks",
		"DescribeStackEvents",
		"DescribeStackResource",
		"ListStackResources",
		"DescribeStackResources",
		"ListExports",
		"ListImports",
		"CreateChangeSet",
		"DescribeChangeSet",
		"ExecuteChangeSet",
		"DeleteChangeSet",
		"ListChangeSets",
		"GetTemplate",
		"DescribeType",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "cloudformation" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this CloudFormation instance handles.
func (h *Handler) ChaosRegions() []string { return []string{config.DefaultRegion} }

// RouteMatcher returns a matcher for CloudFormation query-protocol requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		r := c.Request()
		if r.Method != http.MethodPost {
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

		action := vals.Get("Action")

		return slices.Contains(h.GetSupportedOperations(), action)
	}
}

const cfnMatchPriority = 80

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return cfnMatchPriority }

// ExtractOperation extracts the Action from the form.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	r := c.Request()
	if err := r.ParseForm(); err != nil {
		return ""
	}

	return r.Form.Get("Action")
}

// ExtractResource extracts the StackName from the form.
func (h *Handler) ExtractResource(c *echo.Context) string {
	r := c.Request()
	if err := r.ParseForm(); err != nil {
		return ""
	}

	return r.Form.Get("StackName")
}

// Handler returns the Echo handler function.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		r := c.Request()
		if err := r.ParseForm(); err != nil {
			return h.xmlError(c, "InvalidParameterValue", "cannot parse form body")
		}
		action := r.Form.Get("Action")
		c.Response().Header().Set("Content-Type", "text/xml")

		return h.dispatch(action, r.Form, c)
	}
}

func (h *Handler) dispatch(action string, form url.Values, c *echo.Context) error {
	if handled, err := h.dispatchStackOps(action, form, c); handled {
		return err
	}

	if handled, err := h.dispatchResourceOps(action, form, c); handled {
		return err
	}

	if handled, err := h.dispatchChangeSetOps(action, form, c); handled {
		return err
	}

	if action == "DescribeType" {
		return h.handleDescribeType(form, c)
	}

	return h.xmlError(c, "InvalidAction", "unknown action: "+action)
}

// typeSchemaFor returns the primary identifier property name for a given CloudFormation resource type.
// The primary identifier is used by Terraform's AWS provider to construct resource IDs.
// Falls back to "Id" for unknown types.
func typeSchemaFor(typeName string) string {
	primaryIdentifiers := map[string]string{
		"AWS::Logs::LogGroup":         "LogGroupName",
		"AWS::S3::Bucket":             "BucketName",
		"AWS::Lambda::Function":       "FunctionName",
		"AWS::SNS::Topic":             "TopicArn",
		"AWS::SQS::Queue":             "QueueUrl",
		"AWS::DynamoDB::Table":        "TableName",
		"AWS::IAM::Role":              "RoleName",
		"AWS::EC2::VPC":               "VpcId",
		"AWS::EC2::Instance":          "InstanceId",
		"AWS::RDS::DBInstance":        "DBInstanceIdentifier",
		"AWS::ECS::Cluster":           "ClusterArn",
		"AWS::KMS::Key":               "KeyId",
		"AWS::SecretsManager::Secret": "Id",
	}

	if prop, ok := primaryIdentifiers[typeName]; ok {
		return prop
	}

	return "Id"
}

// handleDescribeType returns a minimal CloudFormation type schema for the requested TypeName.
// This is called by the Terraform AWS provider before creating a CloudControl API resource.
func (h *Handler) handleDescribeType(form url.Values, c *echo.Context) error {
	typeName := form.Get("TypeName")
	if typeName == "" {
		return h.xmlError(c, "CFNRegistryException", "TypeName is required")
	}

	primaryProp := typeSchemaFor(typeName)

	// Build a minimal CloudFormation registry schema JSON using structured data.
	// The Terraform provider uses primaryIdentifier to construct resource IDs.
	type cfnSchema struct {
		Properties           map[string]struct{} `json:"properties"`
		TypeName             string              `json:"typeName"`
		Description          string              `json:"description"`
		PrimaryIdentifier    []string            `json:"primaryIdentifier"`
		AdditionalProperties bool                `json:"additionalProperties"`
	}

	schemaObj := cfnSchema{
		TypeName:             typeName,
		Description:          "Stub schema for " + typeName,
		PrimaryIdentifier:    []string{"/properties/" + primaryProp},
		Properties:           map[string]struct{}{primaryProp: {}},
		AdditionalProperties: true,
	}

	schemaBytes, err := json.Marshal(schemaObj)
	if err != nil {
		return h.xmlError(c, "InternalFailure", "failed to marshal schema: "+err.Error())
	}

	arn := fmt.Sprintf("arn:aws:cloudformation:%s::type/resource/%s/00000001",
		config.DefaultRegion, strings.ReplaceAll(typeName, "::", "-"))

	type typeResult struct {
		Arn              string `xml:"Arn"`
		DefaultVersionID string `xml:"DefaultVersionId"`
		Schema           string `xml:"Schema"`
		Type             string `xml:"Type"`
		TypeName         string `xml:"TypeName"`
		Visibility       string `xml:"Visibility"`
		IsActivated      bool   `xml:"IsActivated"`
		IsDefaultVersion bool   `xml:"IsDefaultVersion"`
	}
	type response struct {
		XMLName   xml.Name   `xml:"DescribeTypeResponse"`
		Xmlns     string     `xml:"xmlns,attr"`
		RequestID string     `xml:"ResponseMetadata>RequestId"`
		Result    typeResult `xml:"DescribeTypeResult"`
	}

	return writeXML(c, response{
		Xmlns:     cfnNS,
		RequestID: uuid.New().String(),
		Result: typeResult{
			Arn:              arn,
			DefaultVersionID: "00000001",
			IsActivated:      true,
			IsDefaultVersion: true,
			Schema:           string(schemaBytes),
			Type:             "RESOURCE",
			TypeName:         typeName,
			Visibility:       "PUBLIC",
		},
	})
}

func (h *Handler) dispatchStackOps(action string, form url.Values, c *echo.Context) (bool, error) {
	switch action {
	case "CreateStack":
		return true, h.handleCreateStack(form, c)
	case "UpdateStack":
		return true, h.handleUpdateStack(form, c)
	case "DeleteStack":
		return true, h.handleDeleteStack(form, c)
	case "DescribeStacks":
		return true, h.handleDescribeStacks(form, c)
	case "ListStacks":
		return true, h.handleListStacks(form, c)
	case "DescribeStackEvents":
		return true, h.handleDescribeStackEvents(form, c)
	case "GetTemplate":
		return true, h.handleGetTemplate(form, c)
	default:
		return false, nil
	}
}

func (h *Handler) dispatchResourceOps(action string, form url.Values, c *echo.Context) (bool, error) {
	switch action {
	case "DescribeStackResource":
		return true, h.handleDescribeStackResource(form, c)
	case "ListStackResources":
		return true, h.handleListStackResources(form, c)
	case "DescribeStackResources":
		return true, h.handleDescribeStackResources(form, c)
	case "ListExports":
		return true, h.handleListExports(form, c)
	case "ListImports":
		return true, h.handleListImports(form, c)
	default:
		return false, nil
	}
}

func (h *Handler) dispatchChangeSetOps(action string, form url.Values, c *echo.Context) (bool, error) {
	switch action {
	case "CreateChangeSet":
		return true, h.handleCreateChangeSet(form, c)
	case "DescribeChangeSet":
		return true, h.handleDescribeChangeSet(form, c)
	case "ExecuteChangeSet":
		return true, h.handleExecuteChangeSet(form, c)
	case "DeleteChangeSet":
		return true, h.handleDeleteChangeSet(form, c)
	case "ListChangeSets":
		return true, h.handleListChangeSets(form, c)
	default:
		return false, nil
	}
}

func (h *Handler) xmlError(c *echo.Context, code, message string) error {
	type xmlErrBody struct {
		XMLName   xml.Name `xml:"ErrorResponse"`
		Code      string   `xml:"Error>Code"`
		Message   string   `xml:"Error>Message"`
		RequestID string   `xml:"RequestId"`
	}
	w := c.Response()
	w.Header().Set("Content-Type", "text/xml")
	w.WriteHeader(http.StatusBadRequest)
	enc := xml.NewEncoder(w)
	_ = enc.Encode(xmlErrBody{Code: code, Message: message, RequestID: uuid.New().String()})

	return nil
}

func writeXML(c *echo.Context, v any) error {
	w := c.Response()
	w.Header().Set("Content-Type", "text/xml")
	w.WriteHeader(http.StatusOK)
	if _, err := fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?>`); err != nil {
		return err
	}

	return xml.NewEncoder(w).Encode(v)
}

// parseMemberList parses form values like "Prefix.member.1", "Prefix.member.2".
func parseMemberList(form url.Values, prefix string) []string {
	var result []string
	for i := 1; ; i++ {
		v := form.Get(fmt.Sprintf("%smember.%d", prefix, i))
		if v == "" {
			return result
		}
		result = append(result, v)
	}
}

func parseParams(form url.Values) []Parameter {
	var params []Parameter
	for i := 1; ; i++ {
		prefix := fmt.Sprintf("Parameters.member.%d.", i)
		key := form.Get(prefix + "ParameterKey")
		if key == "" {
			return params
		}
		params = append(params, Parameter{
			ParameterKey:   key,
			ParameterValue: form.Get(prefix + "ParameterValue"),
		})
	}
}

func parseTags(form url.Values) []Tag {
	var tags []Tag
	for i := 1; ; i++ {
		prefix := fmt.Sprintf("Tags.member.%d.", i)
		key := form.Get(prefix + "Key")
		if key == "" {
			return tags
		}
		tags = append(tags, Tag{
			Key:   key,
			Value: form.Get(prefix + "Value"),
		})
	}
}

func (h *Handler) handleCreateStack(form url.Values, c *echo.Context) error {
	stackName := form.Get("StackName")
	if stackName == "" {
		return h.xmlError(c, "ValidationError", "StackName is required")
	}
	templateBody := form.Get("TemplateBody")
	params := parseParams(form)
	tags := parseTags(form)

	stack, err := h.Backend.CreateStack(c.Request().Context(), stackName, templateBody, params, tags)
	if err != nil {
		return h.xmlError(c, "AlreadyExistsException", err.Error())
	}

	type result struct {
		StackID string `xml:"StackId"`
	}
	type response struct {
		XMLName   xml.Name `xml:"CreateStackResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		Result    result   `xml:"CreateStackResult"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{
		Xmlns:     cfnNS,
		Result:    result{StackID: stack.StackID},
		RequestID: uuid.New().String(),
	})
}

func (h *Handler) handleUpdateStack(form url.Values, c *echo.Context) error {
	stackName := form.Get("StackName")
	if stackName == "" {
		return h.xmlError(c, "ValidationError", "StackName is required")
	}
	templateBody := form.Get("TemplateBody")
	params := parseParams(form)

	stack, err := h.Backend.UpdateStack(c.Request().Context(), stackName, templateBody, params)
	if err != nil {
		return h.xmlError(c, "ValidationError", err.Error())
	}

	type result struct {
		StackID string `xml:"StackId"`
	}
	type response struct {
		XMLName   xml.Name `xml:"UpdateStackResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		Result    result   `xml:"UpdateStackResult"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{
		Xmlns:     cfnNS,
		Result:    result{StackID: stack.StackID},
		RequestID: uuid.New().String(),
	})
}

func (h *Handler) handleDeleteStack(form url.Values, c *echo.Context) error {
	stackName := form.Get("StackName")
	if stackName == "" {
		return h.xmlError(c, "ValidationError", "StackName is required")
	}

	if err := h.Backend.DeleteStack(c.Request().Context(), stackName); err != nil {
		return h.xmlError(c, "ValidationError", err.Error())
	}

	type response struct {
		XMLName   xml.Name `xml:"DeleteStackResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cfnNS, RequestID: uuid.New().String()})
}

func (h *Handler) handleDescribeStacks(form url.Values, c *echo.Context) error {
	stackName := form.Get("StackName")

	type stackXML struct {
		StackID           string      `xml:"StackId"`
		StackName         string      `xml:"StackName"`
		Description       string      `xml:"Description,omitempty"`
		StackStatus       string      `xml:"StackStatus"`
		StackStatusReason string      `xml:"StackStatusReason,omitempty"`
		CreationTime      string      `xml:"CreationTime"`
		Parameters        []Parameter `xml:"Parameters>member,omitempty"`
		Outputs           []Output    `xml:"Outputs>member,omitempty"`
		Tags              []Tag       `xml:"Tags>member,omitempty"`
	}

	var stacks []stackXML

	if stackName != "" {
		s, err := h.Backend.DescribeStack(stackName)
		if err != nil {
			return h.xmlError(c, "ValidationError", err.Error())
		}
		stacks = append(stacks, stackXML{
			StackID:           s.StackID,
			StackName:         s.StackName,
			Description:       s.Description,
			StackStatus:       s.StackStatus,
			StackStatusReason: s.StackStatusReason,
			CreationTime:      s.CreationTime.Format("2006-01-02T15:04:05Z"),
			Parameters:        s.Parameters,
			Outputs:           s.Outputs,
			Tags:              s.Tags,
		})
	} else {
		all := h.Backend.ListAll()
		for _, s := range all {
			stacks = append(stacks, stackXML{
				StackID:           s.StackID,
				StackName:         s.StackName,
				Description:       s.Description,
				StackStatus:       s.StackStatus,
				StackStatusReason: s.StackStatusReason,
				CreationTime:      s.CreationTime.Format("2006-01-02T15:04:05Z"),
				Parameters:        s.Parameters,
				Outputs:           s.Outputs,
				Tags:              s.Tags,
			})
		}
	}

	type descResult struct {
		Stacks []stackXML `xml:"Stacks>member"`
	}
	type response struct {
		XMLName   xml.Name   `xml:"DescribeStacksResponse"`
		Xmlns     string     `xml:"xmlns,attr"`
		RequestID string     `xml:"ResponseMetadata>RequestId"`
		Result    descResult `xml:"DescribeStacksResult"`
	}

	return writeXML(c, response{
		Xmlns:     cfnNS,
		Result:    descResult{Stacks: stacks},
		RequestID: uuid.New().String(),
	})
}

func (h *Handler) handleListStacks(form url.Values, c *echo.Context) error {
	statusFilter := parseMemberList(form, "StackStatusFilter.")
	nextToken := form.Get("NextToken")

	p, err := h.Backend.ListStacks(statusFilter, nextToken)
	if err != nil {
		return h.xmlError(c, "ValidationError", err.Error())
	}
	summaries := p.Data

	type summaryXML struct {
		StackID      string `xml:"StackId"`
		StackName    string `xml:"StackName"`
		StackStatus  string `xml:"StackStatus"`
		CreationTime string `xml:"CreationTime"`
	}
	members := make([]summaryXML, 0, len(summaries))
	for _, s := range summaries {
		members = append(members, summaryXML{
			StackID:      s.StackID,
			StackName:    s.StackName,
			StackStatus:  s.StackStatus,
			CreationTime: s.CreationTime.Format("2006-01-02T15:04:05Z"),
		})
	}

	type listResult struct {
		NextToken      string       `xml:"NextToken,omitempty"`
		StackSummaries []summaryXML `xml:"StackSummaries>member"`
	}
	type response struct {
		XMLName   xml.Name   `xml:"ListStacksResponse"`
		Xmlns     string     `xml:"xmlns,attr"`
		RequestID string     `xml:"ResponseMetadata>RequestId"`
		Result    listResult `xml:"ListStacksResult"`
	}

	return writeXML(c, response{
		Xmlns:     cfnNS,
		Result:    listResult{StackSummaries: members, NextToken: p.Next},
		RequestID: uuid.New().String(),
	})
}

func (h *Handler) handleDescribeStackEvents(form url.Values, c *echo.Context) error {
	stackName := form.Get("StackName")
	if stackName == "" {
		return h.xmlError(c, "ValidationError", "StackName is required")
	}

	events, err := h.Backend.DescribeStackEvents(stackName)
	if err != nil {
		return h.xmlError(c, "ValidationError", err.Error())
	}

	type eventXML struct {
		EventID              string `xml:"EventId"`
		StackID              string `xml:"StackId"`
		StackName            string `xml:"StackName"`
		LogicalResourceID    string `xml:"LogicalResourceId"`
		PhysicalResourceID   string `xml:"PhysicalResourceId,omitempty"`
		ResourceType         string `xml:"ResourceType"`
		ResourceStatus       string `xml:"ResourceStatus"`
		ResourceStatusReason string `xml:"ResourceStatusReason,omitempty"`
		Timestamp            string `xml:"Timestamp"`
	}
	members := make([]eventXML, 0, len(events))
	for _, e := range events {
		members = append(members, eventXML{
			EventID:              e.EventID,
			StackID:              e.StackID,
			StackName:            e.StackName,
			LogicalResourceID:    e.LogicalResourceID,
			PhysicalResourceID:   e.PhysicalResourceID,
			ResourceType:         e.ResourceType,
			ResourceStatus:       e.ResourceStatus,
			ResourceStatusReason: e.ResourceStatusReason,
			Timestamp:            e.Timestamp.Format("2006-01-02T15:04:05Z"),
		})
	}

	type eventsResult struct {
		StackEvents []eventXML `xml:"StackEvents>member"`
	}
	type response struct {
		XMLName   xml.Name     `xml:"DescribeStackEventsResponse"`
		Xmlns     string       `xml:"xmlns,attr"`
		RequestID string       `xml:"ResponseMetadata>RequestId"`
		Result    eventsResult `xml:"DescribeStackEventsResult"`
	}

	return writeXML(c, response{
		Xmlns:     cfnNS,
		Result:    eventsResult{StackEvents: members},
		RequestID: uuid.New().String(),
	})
}

func (h *Handler) handleCreateChangeSet(form url.Values, c *echo.Context) error {
	stackName := form.Get("StackName")
	changeSetName := form.Get("ChangeSetName")
	if stackName == "" || changeSetName == "" {
		return h.xmlError(c, "ValidationError", "StackName and ChangeSetName are required")
	}

	templateBody := form.Get("TemplateBody")
	description := form.Get("Description")
	params := parseParams(form)

	cs, err := h.Backend.CreateChangeSet(
		c.Request().Context(), stackName, changeSetName, templateBody, description, params,
	)
	if err != nil {
		return h.xmlError(c, "AlreadyExistsException", err.Error())
	}

	type result struct {
		ChangeSetID string `xml:"Id"`
		StackID     string `xml:"StackId"`
	}
	type response struct {
		XMLName   xml.Name `xml:"CreateChangeSetResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		Result    result   `xml:"CreateChangeSetResult"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{
		Xmlns:     cfnNS,
		Result:    result{ChangeSetID: cs.ChangeSetID, StackID: cs.StackID},
		RequestID: uuid.New().String(),
	})
}

func (h *Handler) handleDescribeChangeSet(form url.Values, c *echo.Context) error {
	stackName := form.Get("StackName")
	changeSetName := form.Get("ChangeSetName")

	cs, err := h.Backend.DescribeChangeSet(stackName, changeSetName)
	if err != nil {
		return h.xmlError(c, "ChangeSetNotFoundException", err.Error())
	}

	type resourceChangeXML struct {
		Action       string `xml:"Action"`
		LogicalID    string `xml:"LogicalResourceId"`
		ResourceType string `xml:"ResourceType"`
	}
	type changeXML struct {
		Type           string            `xml:"Type"`
		ResourceChange resourceChangeXML `xml:"ResourceChange"`
	}
	changes := make([]changeXML, 0, len(cs.Changes))
	for _, ch := range cs.Changes {
		changes = append(changes, changeXML{
			Type: ch.Type,
			ResourceChange: resourceChangeXML{
				Action:       ch.ResourceChange.Action,
				LogicalID:    ch.ResourceChange.LogicalID,
				ResourceType: ch.ResourceChange.ResourceType,
			},
		})
	}

	type descResult struct {
		ChangeSetID   string      `xml:"ChangeSetId"`
		ChangeSetName string      `xml:"ChangeSetName"`
		StackID       string      `xml:"StackId"`
		StackName     string      `xml:"StackName"`
		Status        string      `xml:"Status"`
		StatusReason  string      `xml:"StatusReason,omitempty"`
		CreationTime  string      `xml:"CreationTime"`
		Description   string      `xml:"Description,omitempty"`
		Changes       []changeXML `xml:"Changes>member"`
	}
	type response struct {
		XMLName   xml.Name   `xml:"DescribeChangeSetResponse"`
		Xmlns     string     `xml:"xmlns,attr"`
		RequestID string     `xml:"ResponseMetadata>RequestId"`
		Result    descResult `xml:"DescribeChangeSetResult"`
	}

	return writeXML(c, response{
		Xmlns: cfnNS,
		Result: descResult{
			ChangeSetID:   cs.ChangeSetID,
			ChangeSetName: cs.ChangeSetName,
			StackID:       cs.StackID,
			StackName:     cs.StackName,
			Status:        cs.Status,
			StatusReason:  cs.StatusReason,
			CreationTime:  cs.CreationTime.Format("2006-01-02T15:04:05Z"),
			Description:   cs.Description,
			Changes:       changes,
		},
		RequestID: uuid.New().String(),
	})
}

func (h *Handler) handleExecuteChangeSet(form url.Values, c *echo.Context) error {
	stackName := form.Get("StackName")
	changeSetName := form.Get("ChangeSetName")

	if err := h.Backend.ExecuteChangeSet(c.Request().Context(), stackName, changeSetName); err != nil {
		return h.xmlError(c, "ChangeSetNotFoundException", err.Error())
	}

	type response struct {
		XMLName   xml.Name `xml:"ExecuteChangeSetResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cfnNS, RequestID: uuid.New().String()})
}

func (h *Handler) handleDeleteChangeSet(form url.Values, c *echo.Context) error {
	stackName := form.Get("StackName")
	changeSetName := form.Get("ChangeSetName")

	if err := h.Backend.DeleteChangeSet(stackName, changeSetName); err != nil {
		return h.xmlError(c, "ChangeSetNotFoundException", err.Error())
	}

	type response struct {
		XMLName   xml.Name `xml:"DeleteChangeSetResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cfnNS, RequestID: uuid.New().String()})
}

func (h *Handler) handleListChangeSets(form url.Values, c *echo.Context) error {
	stackName := form.Get("StackName")
	nextToken := form.Get("NextToken")

	p, err := h.Backend.ListChangeSets(stackName, nextToken)
	if err != nil {
		return h.xmlError(c, "ValidationError", err.Error())
	}
	summaries := p.Data

	type summaryXML struct {
		ChangeSetID   string `xml:"ChangeSetId"`
		ChangeSetName string `xml:"ChangeSetName"`
		StackID       string `xml:"StackId"`
		StackName     string `xml:"StackName"`
		Status        string `xml:"Status"`
		CreationTime  string `xml:"CreationTime"`
		Description   string `xml:"Description,omitempty"`
	}
	members := make([]summaryXML, 0, len(summaries))
	for _, s := range summaries {
		members = append(members, summaryXML{
			ChangeSetID:   s.ChangeSetID,
			ChangeSetName: s.ChangeSetName,
			StackID:       s.StackID,
			StackName:     s.StackName,
			Status:        s.Status,
			CreationTime:  s.CreationTime.Format("2006-01-02T15:04:05Z"),
			Description:   s.Description,
		})
	}

	type listResult struct {
		NextToken string       `xml:"NextToken,omitempty"`
		Summaries []summaryXML `xml:"Summaries>member"`
	}
	type response struct {
		XMLName   xml.Name   `xml:"ListChangeSetsResponse"`
		Xmlns     string     `xml:"xmlns,attr"`
		RequestID string     `xml:"ResponseMetadata>RequestId"`
		Result    listResult `xml:"ListChangeSetsResult"`
	}

	return writeXML(c, response{
		Xmlns:     cfnNS,
		Result:    listResult{Summaries: members, NextToken: p.Next},
		RequestID: uuid.New().String(),
	})
}

func (h *Handler) handleGetTemplate(form url.Values, c *echo.Context) error {
	stackName := form.Get("StackName")
	if stackName == "" {
		return h.xmlError(c, "ValidationError", "StackName is required")
	}

	body, err := h.Backend.GetTemplate(stackName)
	if err != nil {
		return h.xmlError(c, "ValidationError", err.Error())
	}

	type result struct {
		TemplateBody string `xml:"TemplateBody"`
	}
	type response struct {
		XMLName   xml.Name `xml:"GetTemplateResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		Result    result   `xml:"GetTemplateResult"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{
		Xmlns:     cfnNS,
		Result:    result{TemplateBody: body},
		RequestID: uuid.New().String(),
	})
}

func (h *Handler) handleDescribeStackResource(form url.Values, c *echo.Context) error {
	stackName := form.Get("StackName")
	logicalID := form.Get("LogicalResourceId")

	if stackName == "" || logicalID == "" {
		return h.xmlError(c, "ValidationError", "StackName and LogicalResourceId are required")
	}

	res, err := h.Backend.DescribeStackResource(stackName, logicalID)
	if err != nil {
		return h.xmlError(c, "ValidationError", err.Error())
	}

	type resourceDetailXML struct {
		StackID            string `xml:"StackId,omitempty"`
		StackName          string `xml:"StackName,omitempty"`
		LogicalResourceID  string `xml:"LogicalResourceId"`
		PhysicalResourceID string `xml:"PhysicalResourceId,omitempty"`
		ResourceType       string `xml:"ResourceType"`
		ResourceStatus     string `xml:"ResourceStatus"`
		LastUpdated        string `xml:"LastUpdatedTimestamp"`
	}
	type descResult struct {
		StackResourceDetail resourceDetailXML `xml:"StackResourceDetail"`
	}
	type response struct {
		XMLName   xml.Name   `xml:"DescribeStackResourceResponse"`
		Xmlns     string     `xml:"xmlns,attr"`
		RequestID string     `xml:"ResponseMetadata>RequestId"`
		Result    descResult `xml:"DescribeStackResourceResult"`
	}

	return writeXML(c, response{
		Xmlns: cfnNS,
		Result: descResult{
			StackResourceDetail: resourceDetailXML{
				StackID:            res.StackID,
				StackName:          res.StackName,
				LogicalResourceID:  res.LogicalID,
				PhysicalResourceID: res.PhysicalID,
				ResourceType:       res.Type,
				ResourceStatus:     res.Status,
				LastUpdated:        res.Timestamp.Format("2006-01-02T15:04:05Z"),
			},
		},
		RequestID: uuid.New().String(),
	})
}

func (h *Handler) handleListStackResources(form url.Values, c *echo.Context) error {
	stackName := form.Get("StackName")
	nextToken := form.Get("NextToken")

	if stackName == "" {
		return h.xmlError(c, "ValidationError", "StackName is required")
	}

	p, err := h.Backend.ListStackResources(stackName, nextToken)
	if err != nil {
		return h.xmlError(c, "ValidationError", err.Error())
	}

	type summaryXML struct {
		LogicalResourceID  string `xml:"LogicalResourceId"`
		PhysicalResourceID string `xml:"PhysicalResourceId,omitempty"`
		ResourceType       string `xml:"ResourceType"`
		ResourceStatus     string `xml:"ResourceStatus"`
		LastUpdated        string `xml:"LastUpdatedTimestamp"`
	}
	members := make([]summaryXML, 0, len(p.Data))
	for _, s := range p.Data {
		members = append(members, summaryXML{
			LogicalResourceID:  s.LogicalResourceID,
			PhysicalResourceID: s.PhysicalResourceID,
			ResourceType:       s.ResourceType,
			ResourceStatus:     s.ResourceStatus,
			LastUpdated:        s.Timestamp.Format("2006-01-02T15:04:05Z"),
		})
	}

	type listResult struct {
		NextToken              string       `xml:"NextToken,omitempty"`
		StackResourceSummaries []summaryXML `xml:"StackResourceSummaries>member"`
	}
	type response struct {
		XMLName   xml.Name   `xml:"ListStackResourcesResponse"`
		Xmlns     string     `xml:"xmlns,attr"`
		RequestID string     `xml:"ResponseMetadata>RequestId"`
		Result    listResult `xml:"ListStackResourcesResult"`
	}

	return writeXML(c, response{
		Xmlns:     cfnNS,
		Result:    listResult{StackResourceSummaries: members, NextToken: p.Next},
		RequestID: uuid.New().String(),
	})
}

func (h *Handler) handleDescribeStackResources(form url.Values, c *echo.Context) error {
	stackName := form.Get("StackName")
	if stackName == "" {
		return h.xmlError(c, "ValidationError", "StackName is required")
	}

	resources, err := h.Backend.DescribeStackResources(stackName)
	if err != nil {
		return h.xmlError(c, "ValidationError", err.Error())
	}

	type resourceXML struct {
		StackID            string `xml:"StackId,omitempty"`
		StackName          string `xml:"StackName,omitempty"`
		LogicalResourceID  string `xml:"LogicalResourceId"`
		PhysicalResourceID string `xml:"PhysicalResourceId,omitempty"`
		ResourceType       string `xml:"ResourceType"`
		ResourceStatus     string `xml:"ResourceStatus"`
		Timestamp          string `xml:"Timestamp"`
	}
	members := make([]resourceXML, 0, len(resources))
	for _, r := range resources {
		members = append(members, resourceXML{
			StackID:            r.StackID,
			StackName:          r.StackName,
			LogicalResourceID:  r.LogicalID,
			PhysicalResourceID: r.PhysicalID,
			ResourceType:       r.Type,
			ResourceStatus:     r.Status,
			Timestamp:          r.Timestamp.Format("2006-01-02T15:04:05Z"),
		})
	}

	type descResult struct {
		StackResources []resourceXML `xml:"StackResources>member"`
	}
	type response struct {
		XMLName   xml.Name   `xml:"DescribeStackResourcesResponse"`
		Xmlns     string     `xml:"xmlns,attr"`
		RequestID string     `xml:"ResponseMetadata>RequestId"`
		Result    descResult `xml:"DescribeStackResourcesResult"`
	}

	return writeXML(c, response{
		Xmlns:     cfnNS,
		Result:    descResult{StackResources: members},
		RequestID: uuid.New().String(),
	})
}

func (h *Handler) handleListExports(form url.Values, c *echo.Context) error {
	nextToken := form.Get("NextToken")

	p, err := h.Backend.ListExports(nextToken)
	if err != nil {
		return h.xmlError(c, "ValidationError", err.Error())
	}

	type exportXML struct {
		ExportingStackID string `xml:"ExportingStackId"`
		Name             string `xml:"Name"`
		Value            string `xml:"Value"`
	}
	members := make([]exportXML, 0, len(p.Data))
	for _, exp := range p.Data {
		members = append(members, exportXML(exp))
	}

	type listResult struct {
		NextToken string      `xml:"NextToken,omitempty"`
		Exports   []exportXML `xml:"Exports>member"`
	}
	type response struct {
		XMLName   xml.Name   `xml:"ListExportsResponse"`
		Xmlns     string     `xml:"xmlns,attr"`
		RequestID string     `xml:"ResponseMetadata>RequestId"`
		Result    listResult `xml:"ListExportsResult"`
	}

	return writeXML(c, response{
		Xmlns:     cfnNS,
		Result:    listResult{Exports: members, NextToken: p.Next},
		RequestID: uuid.New().String(),
	})
}

func (h *Handler) handleListImports(form url.Values, c *echo.Context) error {
	exportName := form.Get("ExportName")
	nextToken := form.Get("NextToken")

	if exportName == "" {
		return h.xmlError(c, "ValidationError", "ExportName is required")
	}

	p, err := h.Backend.ListImports(exportName, nextToken)
	if err != nil {
		return h.xmlError(c, "ValidationError", err.Error())
	}

	type listResult struct {
		NextToken string   `xml:"NextToken,omitempty"`
		Imports   []string `xml:"Imports>member"`
	}
	type response struct {
		XMLName   xml.Name   `xml:"ListImportsResponse"`
		Xmlns     string     `xml:"xmlns,attr"`
		RequestID string     `xml:"ResponseMetadata>RequestId"`
		Result    listResult `xml:"ListImportsResult"`
	}

	return writeXML(c, response{
		Xmlns:     cfnNS,
		Result:    listResult{Imports: p.Data, NextToken: p.Next},
		RequestID: uuid.New().String(),
	})
}
