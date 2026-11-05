package cloudformation

import (
	"encoding/json"
	"maps"
)

func samTableAllowed() map[string]bool {
	return keySet("PrimaryKey", "ProvisionedThroughput", samKeyTableName, scopeTags, "SSESpecification",
		"BillingMode", "PointInTimeRecoverySpecification")
}

func samLayerAllowed() map[string]bool {
	return keySet("LayerName", samKeyDesc, "ContentUri", "CompatibleRuntimes",
		"CompatibleArchitectures", "LicenseInfo", "RetentionPolicy")
}

func samSMAllowed() map[string]bool {
	return keySet("Definition", "DefinitionUri", "DefinitionSubstitutions", "Role", "Policies",
		"Name", "Type", "Tracing", "Logging", scopeTags, "PermissionsBoundary", samKeyEvents)
}

func (t *samTranslator) translateSimpleTable(id string, r map[string]any) error {
	props := t.resProps("SimpleTable", r)
	if err := rejectUnknown(id, props, samTableAllowed()); err != nil {
		return err
	}
	key, typ := "id", samTypeString
	if pk := asMap(props["PrimaryKey"]); pk != nil {
		if n, ok := pk["Name"].(string); ok {
			key = n
		}
		if ty, ok := pk["Type"].(string); ok {
			typ = ty
		}
	}
	attrType := map[string]string{samTypeString: "S", "Number": "N", "Binary": "B"}[typ]
	if attrType == "" {
		return samErr(id, "Type '%s' of PrimaryKey is invalid; must be String, Number or Binary.", typ)
	}
	tbl := map[string]any{
		"AttributeDefinitions": []any{map[string]any{"AttributeName": key, "AttributeType": attrType}},
		"KeySchema":            []any{map[string]any{"AttributeName": key, "KeyType": "HASH"}},
	}
	if tp := props["ProvisionedThroughput"]; tp != nil {
		tbl["ProvisionedThroughput"] = tp
	} else {
		tbl["BillingMode"] = "PAY_PER_REQUEST"
	}
	for _, k := range []string{samKeyTableName, "SSESpecification", "PointInTimeRecoverySpecification"} {
		if v, ok := props[k]; ok {
			tbl[k] = v
		}
	}
	if tags := asMap(props[scopeTags]); tags != nil {
		tbl[scopeTags] = samTagList(tags)
	}
	base := map[string]any{samKeyType: "AWS::DynamoDB::Table", samKeyProps: tbl}
	carryAttrs(r, base)

	return t.put(id, base)
}

func samTagList(tags map[string]any) []any {
	out := samTags(tags)

	return out[1:]
}

func (t *samTranslator) translateLayer(id string, r map[string]any) error {
	props := asMap(r[samKeyProps])
	if err := rejectUnknown(id, props, samLayerAllowed()); err != nil {
		return err
	}
	layer := map[string]any{"LayerName": id}
	for _, k := range []string{"LayerName", samKeyDesc, "CompatibleRuntimes", "CompatibleArchitectures", "LicenseInfo"} {
		if v, ok := props[k]; ok {
			layer[k] = v
		}
	}
	content, err := s3Location(id, "ContentUri", props["ContentUri"], "S3Bucket", "S3Key", "S3ObjectVersion")
	if err != nil {
		return err
	}
	layer["Content"] = content
	layerID := samLogicalID(id, map[string]any{id: layerHashBody(r)})
	t.refs[id] = layerID
	base := map[string]any{samKeyType: samTypeLayer, samKeyProps: layer}
	carryAttrs(r, base)
	if props["RetentionPolicy"] == deletionPolicyRetain || props["RetentionPolicy"] == nil {
		base[samKeyDelPolicy] = deletionPolicyRetain
	}

	return t.put(layerID, base)
}

func (t *samTranslator) translateStateMachine(id string, r map[string]any) error {
	props := asMap(r[samKeyProps])
	if err := rejectUnknown(id, props, samSMAllowed()); err != nil {
		return err
	}
	sm := map[string]any{}
	for _, k := range []string{"DefinitionSubstitutions", "Logging", scopeTags} {
		if v, ok := props[k]; ok {
			sm[k] = v
		}
	}
	if err := smDefinition(id, props, sm); err != nil {
		return err
	}
	if v, ok := props["Name"]; ok {
		sm["StateMachineName"] = v
	}
	if v, ok := props["Type"]; ok {
		sm["StateMachineType"] = v
	}
	if props["Tracing"] != nil {
		sm["TracingConfiguration"] = props["Tracing"]
	}
	if props["Role"] != nil {
		sm["RoleArn"] = props["Role"]
	} else {
		sm["RoleArn"] = samGetAtt(id+"Role", attrNameArn)
		if err := t.putSMRole(id, props); err != nil {
			return err
		}
	}
	base := map[string]any{samKeyType: "AWS::StepFunctions::StateMachine", samKeyProps: sm}
	carryAttrs(r, base)
	if err := t.put(id, base); err != nil {
		return err
	}

	return t.translateStateMachineEvents(id, asMap(props[samKeyEvents]))
}

func smDefinition(id string, props, sm map[string]any) error {
	switch {
	case props["Definition"] != nil:
		raw, err := json.Marshal(props["Definition"])
		if err != nil {
			return samErr(id, "Definition is not valid JSON: %v", err)
		}
		sm["DefinitionString"] = string(raw)
	case props["DefinitionUri"] != nil:
		loc, err := s3Location(id, "DefinitionUri", props["DefinitionUri"], "Bucket", samKeyKey, "Version")
		if err != nil {
			return err
		}
		sm["DefinitionS3Location"] = loc
	default:
		return samErr(id, "Missing required property 'Definition' or 'DefinitionUri'.")
	}

	return nil
}

func (t *samTranslator) putSMRole(id string, props map[string]any) error {
	pol := &samRolePolicies{}
	if err := t.applyPolicies(id, id+"Role", props["Policies"], pol); err != nil {
		return err
	}
	role := map[string]any{
		samKeyAssumeRole:  servicePrincipalTrust("states.amazonaws.com"),
		samKeyManagedARNs: toSubs(pol.managed),
	}
	if len(pol.inline) > 0 {
		role["Policies"] = pol.inline
	}
	if props["PermissionsBoundary"] != nil {
		role["PermissionsBoundary"] = props["PermissionsBoundary"]
	}

	return t.put(id+"Role", map[string]any{samKeyType: resTypeIAMRole, samKeyProps: role})
}

// layerHashBody is the SAM resource as written, minus passthrough attributes
// that SAM excludes from the layer's hash.
func layerHashBody(r map[string]any) map[string]any {
	body := maps.Clone(r)
	for _, k := range []string{samKeyDelPolicy, samKeyUpdPolicy, "Metadata"} {
		delete(body, k)
	}

	return body
}
