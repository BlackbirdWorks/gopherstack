package cloudformation

import (
	"context"
	"fmt"
	"maps"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	connectbackend "github.com/blackbirdworks/gopherstack/services/connect"
)

const (
	resTypeConnectApprovedOrigin           = "AWS::Connect::ApprovedOrigin"
	resTypeConnectSecurityKey              = "AWS::Connect::SecurityKey"
	resTypeConnectInstanceStorageConfig    = "AWS::Connect::InstanceStorageConfig"
	resTypeConnectIntegrationAssociation   = "AWS::Connect::IntegrationAssociation"
	resTypeConnectUseCase                  = "AWS::Connect::UseCase"
	resTypeConnectTrafficDistributionGroup = "AWS::Connect::TrafficDistributionGroup"
	resTypeConnectWorkspace                = "AWS::Connect::Workspace"
	resTypeConnectEvaluationForm           = "AWS::Connect::EvaluationForm"
	resTypeConnectDataTable                = "AWS::Connect::DataTable"
	resTypeConnectDataTableAttribute       = "AWS::Connect::DataTableAttribute"
	resTypeConnectUser                     = "AWS::Connect::User"
	resTypeConnectSecurityProfile          = "AWS::Connect::SecurityProfile"
	resTypeConnectUserHierarchyGroup       = "AWS::Connect::UserHierarchyGroup"
	resTypeConnectPhoneNumber              = "AWS::Connect::PhoneNumber"
	resTypeConnectRule                     = "AWS::Connect::Rule"
	resTypeConnectView                     = "AWS::Connect::View"
	resTypeConnectViewVersion              = "AWS::Connect::ViewVersion"
	resTypeConnectVocabulary               = "AWS::Connect::Vocabulary"
	resTypeConnectEmailAddress             = "AWS::Connect::EmailAddress"
	resTypeConnectPredefinedAttribute      = "AWS::Connect::PredefinedAttribute"
	resTypeConnectTaskTemplate             = "AWS::Connect::TaskTemplate"
	resTypeConnectAgentStatus              = "AWS::Connect::AgentStatus"
	resTypeConnectContactFlow              = "AWS::Connect::ContactFlow"
	resTypeConnectContactFlowModule        = "AWS::Connect::ContactFlowModule"
	resTypeConnectHoursOfOperation         = "AWS::Connect::HoursOfOperation"
	resTypeConnectInstance                 = "AWS::Connect::Instance"
	resTypeConnectPrompt                   = "AWS::Connect::Prompt"
	resTypeConnectQueue                    = "AWS::Connect::Queue"
	resTypeConnectQuickConnect             = "AWS::Connect::QuickConnect"
	resTypeConnectRoutingProfile           = "AWS::Connect::RoutingProfile"
)

const (
	connectInstanceArnProp = "InstanceArn"
	connectViewSha         = "ViewContentSha256"
	connectTagsProp        = "Tags"
	connectStatusProp      = "Status"
	connectLastModified    = "LastModifiedTime"
	connectLastModRegion   = "LastModifiedRegion"
	connectStateActive     = hsmStateActive
)

const connectTypePrefix = "AWS::Connect::"

// connectCall carries one Amazon Connect resource operation: the backend, and the properties as the wire
// protocol spells them (intrinsics resolved, Tags as a map, every *Arn also present as the matching *Id).
type connectCall struct {
	ctx       context.Context
	be        *connectbackend.Backend
	phys      map[string]string
	wire      map[string]any
	logicalID string
}

type connectOps struct {
	create func(c *connectCall) (string, error)
	update func(c *connectCall, physicalID string) error
	del    func(c *connectCall, physicalID string) error
}

func newConnectCall(
	ctx context.Context,
	be *connectbackend.Backend,
	logicalID string,
	props map[string]any,
	params, phys map[string]string,
) *connectCall {
	wire, _ := resolveDeep(props, params, phys).(map[string]any)
	if wire == nil {
		wire = map[string]any{}
	}

	if _, isList := props[connectTagsProp].([]any); isList {
		wire[connectTagsProp] = tagListProp(props, params, phys)
	}

	addConnectIDs(wire)

	return &connectCall{ctx: ctx, be: be, phys: phys, wire: wire, logicalID: logicalID}
}

// addConnectIDs adds, beside every FooArn / FooArns value, the FooId / FooIds the Connect API takes.
func addConnectIDs(v any) {
	switch t := v.(type) {
	case map[string]any:
		for k, e := range t {
			addConnectIDs(e)

			switch {
			case strings.HasSuffix(k, "Arn"):
				if s, ok := e.(string); ok && strings.HasPrefix(s, "arn:") {
					setIfAbsent(t, strings.TrimSuffix(k, "Arn")+"Id", lastPathSegment(s))
				}
			case strings.HasSuffix(k, "Arns"):
				if list, ok := e.([]any); ok {
					setIfAbsent(t, strings.TrimSuffix(k, "Arns")+"Ids", arnListIDs(list))
				}
			}
		}
	case []any:
		for _, e := range t {
			addConnectIDs(e)
		}
	}
}

func setIfAbsent(m map[string]any, key string, value any) {
	if _, ok := m[key]; !ok {
		m[key] = value
	}
}

func arnListIDs(list []any) []any {
	out := make([]any, 0, len(list))

	for _, e := range list {
		if s, ok := e.(string); ok {
			out = append(out, lastPathSegment(s))
		}
	}

	return out
}

// decode JSON-decodes the wire properties into a backend input struct, leaving out the dropped keys.
func (c *connectCall) decode(dst any, drop ...string) error {
	return decodeCFNProps(dst, maps.Clone(c.wire), nil, nil, drop...)
}

func (c *connectCall) str(key string) string {
	s, _ := c.wire[key].(string)

	return s
}

func (c *connectCall) tags() map[string]string {
	tags, _ := c.wire[connectTagsProp].(map[string]string)

	return tags
}

// instance is the InstanceArn (or InstanceId) property.
func (c *connectCall) instance() string {
	if s := c.str(connectInstanceArnProp); s != "" {
		return s
	}

	return c.str("InstanceId")
}

// stash records GetAtt values that cannot be derived from the physical ID.
func (c *connectCall) stash(attrs map[string]string) {
	stashConnectAttrs(c.phys, c.logicalID, attrs)
}

func stashConnectAttrs(phys map[string]string, logicalID string, attrs map[string]string) {
	for k, v := range attrs {
		phys[logicalID+"/"+k] = v
	}
}

func connectTime(t *awstime.EpochTime) string {
	if t == nil {
		return ""
	}

	return t.Time.UTC().Format(time.RFC3339)
}

// splitConnectARN returns the instance and resource IDs of ...:instance/<inst>/<kind>/<id>.
func splitConnectARN(resourceARN string) (string, string) {
	_, rest, ok := strings.Cut(resourceARN, ":instance/")
	if !ok {
		return "", lastPathSegment(resourceARN)
	}

	inst, _, _ := strings.Cut(rest, "/")

	return inst, lastPathSegment(resourceARN)
}

func (rc *ResourceCreator) createConnectResource(
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	ops, ok := connectTypes()[resourceType]
	if !ok {
		return "", false, nil
	}

	if rc.backends.Connect == nil {
		return logicalID + "-stub", true, nil
	}

	c := newConnectCall(ctx, rc.backends.Connect.Backend, logicalID, props, params, physicalIDs)
	id, err := ops.create(c)
	if err != nil {
		return "", true, fmt.Errorf("create %s %s: %w", resourceType, logicalID, err)
	}

	return id, true, nil
}

func (rc *ResourceCreator) updateConnectResource(
	ctx context.Context,
	resourceType, physicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (bool, error) {
	ops, ok := connectTypes()[resourceType]
	if !ok {
		return false, nil
	}

	if rc.backends.Connect == nil || ops.update == nil {
		return true, nil
	}

	c := newConnectCall(
		ctx,
		rc.backends.Connect.Backend,
		logicalIDOf(physicalIDs, physicalID),
		props,
		params,
		physicalIDs,
	)
	if err := ops.update(c, physicalID); err != nil {
		return true, fmt.Errorf("update %s %s: %w", resourceType, physicalID, err)
	}

	return true, nil
}

func (rc *ResourceCreator) deleteConnectResource(ctx context.Context, resourceType, physicalID string) (bool, error) {
	ops, ok := connectTypes()[resourceType]
	if !ok {
		return false, nil
	}

	if rc.backends.Connect == nil {
		return true, nil
	}

	c := &connectCall{ctx: ctx, be: rc.backends.Connect.Backend}

	return true, ignoreRESTNotFound(ops.del(c, physicalID))
}

// logicalIDOf finds the logical ID whose physical ID is physicalID.
func logicalIDOf(physicalIDs map[string]string, physicalID string) string {
	for k, v := range physicalIDs {
		if v == physicalID && !strings.Contains(k, "/") {
			return k
		}
	}

	return physicalID
}

// syncConnectTags makes the resource's tags equal the template's.
func (c *connectCall) syncTags(resourceARN string) error {
	current, err := c.be.ListTagsForResource(resourceARN)
	if err != nil {
		return err
	}

	return syncTagSet(
		current,
		c.tags(),
		func(m map[string]string) error { return c.be.TagResource(resourceARN, m) },
		func(keys []string) error { return c.be.UntagResource(resourceARN, keys) },
	)
}

// rejectConnectProps fails when a property the backend cannot represent is present.
func (c *connectCall) reject(names ...string) error {
	for _, n := range names {
		if v, ok := c.wire[n]; ok && v != nil {
			return fmt.Errorf("%w: property %s", errUnsupportedCFNValue, n)
		}
	}

	return nil
}

// getConnectAttribute derives GetAtt values of the self-ARN-identified Connect types from the ARN.
func getConnectAttribute(resType, physID, attrName string) (string, bool) {
	if !strings.HasPrefix(resType, connectTypePrefix) {
		return "", false
	}

	if !strings.HasPrefix(physID, "arn:") {
		return connectCompositeAttribute(physID, attrName)
	}

	switch {
	case strings.HasSuffix(attrName, "Arn"), strings.HasSuffix(attrName, "ARN"), attrName == "Arn":
		return physID, true
	case attrName == "Id", strings.HasSuffix(attrName, "Id"):
		return lastPathSegment(physID), true
	default:
		return "", false
	}
}

// usesStashedAttrs reports whether GetAtt values may have been stashed in the physical ID map at create time.
func usesStashedAttrs(resType string) bool {
	return resType == cfnTypeCustomResource || strings.HasPrefix(resType, "Custom::") ||
		strings.HasPrefix(resType, connectTypePrefix)
}

// connectCompositeAttribute reads the Id-style attributes of "a|b|c" physical IDs: the last part.
func connectCompositeAttribute(physID, attrName string) (string, bool) {
	if !strings.Contains(physID, "|") || !strings.HasSuffix(attrName, "Id") {
		return "", false
	}

	_, last, _ := strings.CutLast(physID, "|")

	return last, true
}
