package cloudformation

import (
	"fmt"
	"strconv"
	"strings"

	connectbackend "github.com/blackbirdworks/gopherstack/services/connect"
)

const (
	resTypeConnectContactFlowVersion       = "AWS::Connect::ContactFlowVersion"
	resTypeConnectContactFlowModuleVersion = "AWS::Connect::ContactFlowModuleVersion"
	resTypeConnectContactFlowModuleAlias   = "AWS::Connect::ContactFlowModuleAlias"
	resTypeConnectDataTableRecord          = "AWS::Connect::DataTableRecord"
	resTypeConnectNotification             = "AWS::Connect::Notification"
	resTypeConnectTestCase                 = "AWS::Connect::TestCase"
	resTypeConnectMetric                   = "AWS::Connect::Metric"
	resTypeConnectUserHierarchyStructure   = "AWS::Connect::UserHierarchyStructure"
)

const connectAliasParts = 3

func connectVersionedTypes() map[string]connectOps {
	return map[string]connectOps{
		resTypeConnectContactFlowVersion:       {create: createConnectFlowVersion, del: deleteConnectFlowVersion},
		resTypeConnectContactFlowModuleVersion: {create: createConnectModuleVersion, del: deleteConnectModuleVersion},
		resTypeConnectContactFlowModuleAlias: {
			create: createConnectModuleAlias, update: updateConnectModuleAlias, del: deleteConnectModuleAlias,
		},
		resTypeConnectDataTableRecord: {
			create: createConnectDataTableRecord, update: updateConnectDataTableRecord,
			del: deleteConnectDataTableRecord,
		},
		resTypeConnectNotification: {
			create: createConnectNotification, update: updateConnectNotification, del: deleteConnectNotification,
		},
		resTypeConnectTestCase: {
			create: createConnectTestCase, update: updateConnectTestCase, del: deleteConnectTestCase,
		},
		resTypeConnectMetric: {create: createConnectMetric, update: updateConnectMetric, del: deleteConnectMetric},
		resTypeConnectUserHierarchyStructure: {
			create: createConnectHierarchyStructure, update: updateConnectHierarchyStructure,
			del: func(*connectCall, string) error { return nil },
		},
	}
}

func connectVersionedImmutableProps() map[string][]string {
	return map[string][]string{
		resTypeConnectContactFlowVersion:       {"ContactFlowId", samKeyDesc},
		resTypeConnectContactFlowModuleVersion: {"ContactFlowModuleId", samKeyDesc},
		resTypeConnectContactFlowModuleAlias:   {"ContactFlowModuleId"},
		resTypeConnectDataTableRecord:          {connectInstanceArnProp, "DataTableArn"},
		resTypeConnectNotification:             {connectInstanceArnProp, "ExpiresAt", "Priority", "Recipients"},
		resTypeConnectTestCase:                 {},
		resTypeConnectMetric:                   {connectInstanceArnProp, connectStatusProp},
		resTypeConnectUserHierarchyStructure:   {connectInstanceArnProp},
	}
}

// connectIntProp reads an Integer property, which a template may spell as a string.
func (c *connectCall) intProp(key string) (int64, bool) {
	switch n := c.wire[key].(type) {
	case float64:
		return int64(n), true
	case int:
		return int64(n), true
	case int64:
		return n, true
	case string:
		v, err := strconv.ParseInt(n, 10, 64)

		return v, err == nil
	default:
		return 0, false
	}
}

// ---- ContactFlowVersion / ContactFlowModuleVersion ----

// createConnectFlowVersion publishes a flow version. Ref and GetAtt ContactFlowVersionARN return <flow ARN>:<version>.
func createConnectFlowVersion(c *connectCall) (string, error) {
	var in connectbackend.VersionInput

	in.Description = c.str("Description")
	in.InstanceID, in.ParentID = splitConnectARN(c.str("ContactFlowId"))

	out, err := c.be.CreateFlowVersion(c.ctx, &in)
	if err != nil {
		return "", err
	}

	version := strconv.FormatInt(out.Version, 10)
	c.stash(map[string]string{samKeyVersion: version, "FlowContentSha256": out.Sha})

	return out.Arn + ":" + version, nil
}

func deleteConnectFlowVersion(c *connectCall, physicalID string) error {
	flowARN, version, _ := strings.CutLast(physicalID, ":")

	in := connectbackend.VersionInput{PathVersion: version}
	in.InstanceID, in.ParentID = splitConnectARN(flowARN)
	_, err := c.be.DeleteFlowVersion(&in)

	return err
}

func createConnectModuleVersion(c *connectCall) (string, error) {
	var in connectbackend.VersionInput

	in.Description = c.str("Description")
	in.InstanceID, in.ModuleID = splitConnectARN(c.str("ContactFlowModuleId"))

	out, err := c.be.CreateModuleVersion(c.ctx, &in)
	if err != nil {
		return "", err
	}

	version := strconv.FormatInt(out.Version, 10)
	c.stash(map[string]string{samKeyVersion: version, "FlowModuleContentSha256": out.Sha})

	return out.Arn + ":" + version, nil
}

func deleteConnectModuleVersion(c *connectCall, physicalID string) error {
	moduleARN, version, _ := strings.CutLast(physicalID, ":")

	in := connectbackend.VersionInput{ModulePathVersion: version}
	in.InstanceID, in.ModuleID = splitConnectARN(moduleARN)
	_, err := c.be.DeleteModuleVersion(&in)

	return err
}

// ---- ContactFlowModuleAlias ----

// aliasInput parses "<instance ID>|<module ID>|<alias ID>", an ID that survives a rename.
func (c *connectCall) aliasInput(physicalID string) (*connectbackend.AliasInput, error) {
	parts := strings.Split(physicalID, "|")
	if len(parts) != connectAliasParts {
		return nil, fmt.Errorf("%w: alias ID %q", errUnsupportedCFNValue, physicalID)
	}

	in := connectbackend.AliasInput{ModuleID: parts[1], AliasID: parts[2],
		InstanceID: parts[0]}

	return &in, nil
}

func (c *connectCall) aliasVersion() (*int64, error) {
	v, ok := c.intProp("ContactFlowModuleVersion")
	if !ok {
		return nil, fmt.Errorf("%w: ContactFlowModuleVersion must be an integer", errUnsupportedCFNValue)
	}

	return &v, nil
}

func (c *connectCall) stashModuleAlias(in *connectbackend.AliasInput) {
	q := connectbackend.AliasInput{ModuleID: in.ModuleID,
		InstanceID: in.InstanceID}

	list, _, err := c.be.ListModuleAliases(&q)
	if err != nil {
		return
	}

	for _, a := range list {
		if a.AliasID == in.AliasID {
			c.stash(map[string]string{"AliasId": a.AliasID, "ContactFlowModuleAliasARN": a.Arn})
		}
	}
}

func createConnectModuleAlias(c *connectCall) (string, error) {
	ver, err := c.aliasVersion()
	if err != nil {
		return "", err
	}

	in := connectbackend.AliasInput{AliasName: c.str("Name"), ModuleVersion: ver}
	in.InstanceID, in.ModuleID = splitConnectARN(c.str("ContactFlowModuleId"))

	if d, ok := c.wire["Description"].(string); ok {
		in.Description = &d
	}

	out, err := c.be.CreateModuleAlias(c.ctx, &in)
	if err != nil {
		return "", err
	}

	in.AliasID = out.ID
	c.stashModuleAlias(&in)

	return connectCompositeID(in.InstanceID, in.ModuleID, out.ID), nil
}

func updateConnectModuleAlias(c *connectCall, physicalID string) error {
	in, err := c.aliasInput(physicalID)
	if err != nil {
		return err
	}

	if in.ModuleVersion, err = c.aliasVersion(); err != nil {
		return err
	}

	name, desc := c.str("Name"), c.str("Description")
	in.Name, in.Description = &name, &desc

	if _, err = c.be.UpdateModuleAlias(c.ctx, in); err != nil {
		return err
	}

	c.stashModuleAlias(in)

	return nil
}

func deleteConnectModuleAlias(c *connectCall, physicalID string) error {
	in, err := c.aliasInput(physicalID)
	if err != nil {
		return err
	}

	_, err = c.be.DeleteModuleAlias(in)

	return err
}
