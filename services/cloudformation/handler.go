package cloudformation

import (
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
)

const (
	resTypeS3Bucket = "AWS::S3::Bucket"
	resTypeSNSTopic = "AWS::SNS::Topic"
	resTypeSQSQueue = "AWS::SQS::Queue"
	resTypeRDSDB    = "AWS::RDS::DBInstance"
	resTypeSecret   = "AWS::SecretsManager::Secret" //nolint:gosec // CloudFormation resource type, not a credential
)

const (
	resTypeLogGroup       = "AWS::Logs::LogGroup"
	resTypeLambdaFunction = "AWS::Lambda::Function"
	resTypeDynamoDBTable  = "AWS::DynamoDB::Table"
	resTypeIAMRole        = "AWS::IAM::Role"
	resTypeEC2VPC         = "AWS::EC2::VPC"
	resTypeEC2Instance    = "AWS::EC2::Instance"
	resTypeECSCluster     = "AWS::ECS::Cluster"
	resTypeKMSKey         = "AWS::KMS::Key"
)

const (
	resTypeEC2SecurityGroup = "AWS::EC2::SecurityGroup"
	resTypeCloudWatchAlarm  = "AWS::CloudWatch::Alarm"
)

const (
	resTypeRoute53HostedZone = "AWS::Route53::HostedZone"
	resTypeRoute53RecordSet  = "AWS::Route53::RecordSet"
	resTypeELBv2LB           = "AWS::ElasticLoadBalancingV2::LoadBalancer"
	resTypeELBv2TargetGroup  = "AWS::ElasticLoadBalancingV2::TargetGroup"
)

const cfnNS = "http://cloudformation.amazonaws.com/doc/2010-05-15/"

// errCodeValidation is the AWS CloudFormation generic validation error code.
const errCodeValidation = "ValidationError"

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
		// Drift detection
		"DetectStackDrift",
		"DetectStackResourceDrift",
		"DescribeStackDriftDetectionStatus",
		"DescribeStackResourceDrifts",
		// Stack policy
		"SetStackPolicy",
		"GetStackPolicy",
		// Template analysis
		"GetTemplateSummary",
		"EstimateTemplateCost",
		// Stack management
		"ContinueUpdateRollback",
		"CancelUpdateStack",
		"DescribeAccountLimits",
		// Stack Sets
		"CreateStackSet",
		"UpdateStackSet",
		"DeleteStackSet",
		"DescribeStackSet",
		"ListStackSets",
		"CreateStackInstances",
		"DeleteStackInstances",
		"UpdateStackInstances",
		"ListStackInstances",
		"DescribeStackInstance",
		"DetectStackSetDrift",
		"ListStackSetOperations",
		"DescribeStackSetOperation",
		"StopStackSetOperation",
		"ListStackSetOperationResults",
		"ListStackSetAutoDeploymentTargets",
		"ImportStacksToStackSet",
		"ListStackInstanceResourceDrifts",
		// Generated templates
		"CreateGeneratedTemplate",
		"UpdateGeneratedTemplate",
		"DeleteGeneratedTemplate",
		"DescribeGeneratedTemplate",
		"GetGeneratedTemplate",
		"ListGeneratedTemplates",
		// Resource scans
		"StartResourceScan",
		"DescribeResourceScan",
		"ListResourceScans",
		"ListResourceScanResources",
		"ListResourceScanRelatedResources",
		// Type management
		"ActivateType",
		"DeactivateType",
		"RegisterType",
		"DeregisterType",
		"PublishType",
		"SetTypeDefaultVersion",
		"SetTypeConfiguration",
		"BatchDescribeTypeConfigurations",
		"ListTypes",
		"ListTypeVersions",
		"ListTypeRegistrations",
		"DescribeTypeRegistration",
		"TestType",
		"RegisterPublisher",
		"DescribePublisher",
		// Stack refactor
		"CreateStackRefactor",
		"DescribeStackRefactor",
		"ExecuteStackRefactor",
		"ListStackRefactors",
		"ListStackRefactorActions",
		// Org access
		"ActivateOrganizationsAccess",
		"DeactivateOrganizationsAccess",
		"DescribeOrganizationsAccess",
		// Misc
		"SignalResource",
		"RollbackStack",
		"RecordHandlerProgress",
		"GetHookResult",
		"ListHookResults",
		"DescribeChangeSetHooks",
		"DescribeEvents",
		"UpdateTerminationProtection",
		"ValidateTemplate",
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

	if handled, err := h.dispatchDriftOps(action, form, c); handled {
		return err
	}

	if handled, err := h.dispatchStackPolicyOps(action, form, c); handled {
		return err
	}

	if handled, err := h.dispatchTemplateOps(action, form, c); handled {
		return err
	}

	if handled, err := h.dispatchStackSetOps(action, form, c); handled {
		return err
	}

	if handled, err := h.dispatchGeneratedTemplateOps(action, form, c); handled {
		return err
	}

	if handled, err := h.dispatchStackRefactorOps(action, form, c); handled {
		return err
	}

	if handled, err := h.dispatchTypeOps(action, form, c); handled {
		return err
	}

	if handled, err := h.dispatchHookOps(action, form, c); handled {
		return err
	}

	if action == "DescribeType" {
		return h.handleDescribeType(form, c)
	}

	return h.xmlError(c, "InvalidAction", "unknown action: "+action)
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

func parseCapabilities(form url.Values) []string {
	var caps []string
	for i := 1; ; i++ {
		v := form.Get(fmt.Sprintf("Capabilities.member.%d", i))
		if v == "" {
			return caps
		}
		caps = append(caps, v)
	}
}

func parseNotificationARNs(form url.Values) []string {
	var arns []string
	for i := 1; ; i++ {
		v := form.Get(fmt.Sprintf("NotificationARNs.member.%d", i))
		if v == "" {
			return arns
		}
		arns = append(arns, v)
	}
}

func parseRollbackConfiguration(form url.Values) *RollbackConfiguration {
	monStr := form.Get("RollbackConfiguration.MonitoringTimeInMinutes")
	var triggers []RollbackTrigger
	for i := 1; ; i++ {
		arn := form.Get(fmt.Sprintf("RollbackConfiguration.RollbackTriggers.member.%d.Arn", i))
		if arn == "" {
			break
		}
		triggers = append(triggers, RollbackTrigger{
			ARN:  arn,
			Type: form.Get(fmt.Sprintf("RollbackConfiguration.RollbackTriggers.member.%d.Type", i)),
		})
	}
	if monStr == "" && len(triggers) == 0 {
		return nil
	}
	mon, _ := strconv.Atoi(monStr)

	return &RollbackConfiguration{
		MonitoringTimeInMinutes: mon,
		RollbackTriggers:        triggers,
	}
}

func parseStackOptions(form url.Values) StackOptions {
	timeoutStr := form.Get("TimeoutInMinutes")
	timeout, _ := strconv.Atoi(timeoutStr)
	disableRollback := strings.EqualFold(form.Get("DisableRollback"), "true")

	return StackOptions{
		Tags:                  parseTags(form),
		Capabilities:          parseCapabilities(form),
		NotificationARNs:      parseNotificationARNs(form),
		RoleARN:               form.Get("RoleARN"),
		OnFailure:             form.Get("OnFailure"),
		TimeoutInMinutes:      timeout,
		DisableRollback:       disableRollback,
		RollbackConfiguration: parseRollbackConfiguration(form),
	}
}

// mapCreateStackError maps a CreateStack backend error to the AWS error code
// and message. AWS distinguishes AlreadyExistsException from capability and
// role-ARN validation failures rather than collapsing them all into one code.
func mapCreateStackError(err error) (string, string) {
	switch {
	case errors.Is(err, ErrStackAlreadyExists):
		return "AlreadyExistsException", err.Error()
	case errors.Is(err, ErrInsufficientCapabilities):
		return "InsufficientCapabilitiesException", err.Error()
	default:
		return errCodeValidation, err.Error()
	}
}

// mapUpdateStackError maps an UpdateStack backend error to the AWS error code.
func mapUpdateStackError(err error) (string, string) {
	if errors.Is(err, ErrInsufficientCapabilities) {
		return "InsufficientCapabilitiesException", err.Error()
	}

	return errCodeValidation, err.Error()
}
