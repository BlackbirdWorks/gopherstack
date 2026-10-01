package cloudformation

import "encoding/json"

func samTableAllowed() map[string]bool {
	return keySet("PrimaryKey", "ProvisionedThroughput", samKeyTableName, "Tags", "SSESpecification",
		"BillingMode", "PointInTimeRecoverySpecification")
}

func samLayerAllowed() map[string]bool {
	return keySet("LayerName", samKeyDesc, "ContentUri", "CompatibleRuntimes",
		"CompatibleArchitectures", "LicenseInfo", "RetentionPolicy")
}

func samSMAllowed() map[string]bool {
	return keySet("Definition", "DefinitionUri", "DefinitionSubstitutions", "Role", "Policies",
		"Name", "Type", "Tracing", "Logging", "Tags", "PermissionsBoundary")
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
	if tags := asMap(props["Tags"]); tags != nil {
		tbl["Tags"] = samTagList(tags)
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
	layerID := id + samHash(layer)
	t.refs[id] = layerID
	base := map[string]any{samKeyType: samTypeLayer, samKeyProps: layer}
	carryAttrs(r, base)
	if props["RetentionPolicy"] == "Retain" || props["RetentionPolicy"] == nil {
		base["DeletionPolicy"] = "Retain"
	}

	return t.put(layerID, base)
}

func (t *samTranslator) translateStateMachine(id string, r map[string]any) error {
	props := asMap(r[samKeyProps])
	if err := rejectUnknown(id, props, samSMAllowed()); err != nil {
		return err
	}
	sm := map[string]any{}
	for _, k := range []string{"DefinitionSubstitutions", "Logging", "Tags"} {
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
		sm["RoleArn"] = samGetAtt(id+"Role", "Arn")
		if err := t.putSMRole(id, props); err != nil {
			return err
		}
	}
	base := map[string]any{samKeyType: "AWS::StepFunctions::StateMachine", samKeyProps: sm}
	carryAttrs(r, base)

	return t.put(id, base)
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
		loc, err := s3Location(id, "DefinitionUri", props["DefinitionUri"], "Bucket", "Key", "Version")
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
	var managed []any
	inline, err := splitPolicies(id, props["Policies"], id+"Role", &managed)
	if err != nil {
		return err
	}
	role := map[string]any{
		"AssumeRolePolicyDocument": servicePrincipalTrust("states.amazonaws.com"),
		"ManagedPolicyArns":        toSubs(managed),
	}
	if len(inline) > 0 {
		role["Policies"] = inline
	}
	if props["PermissionsBoundary"] != nil {
		role["PermissionsBoundary"] = props["PermissionsBoundary"]
	}

	return t.put(id+"Role", map[string]any{samKeyType: resTypeIAMRole, samKeyProps: role})
}
