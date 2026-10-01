package cloudformation

import (
	"fmt"
	"strings"
)

const (
	samLambdaBasicExec = "arn:${AWS::Partition}:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
	samLambdaVPCExec   = "arn:${AWS::Partition}:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
	samXRayWrite       = "arn:${AWS::Partition}:iam::aws:policy/AWSXrayWriteOnlyAccess"
)

func samFnPassthrough() map[string]bool {
	return keySet("Handler", "Runtime", "MemorySize", "Timeout", samKeyDesc, "FunctionName",
		"Layers", "Architectures", "VpcConfig", "PackageType", "KmsKeyArn", "EphemeralStorage",
		"ReservedConcurrentExecutions", "FileSystemConfigs", "CodeSigningConfigArn", "SnapStart",
		"LoggingConfig", "RuntimeManagementConfig")
}

func samFnHandled() map[string]bool {
	return keySet("CodeUri", "InlineCode", "ImageUri", "Environment", "Tags", "Role", "Policies",
		"Events", "AutoPublishAlias", "Tracing", "DeadLetterQueue", "PermissionsBoundary",
		"VersionDescription", samKeyAssumeRole)
}

func (t *samTranslator) translateFunction(id string, r map[string]any) error {
	props := t.resProps(samKeyFunction, r)
	if err := rejectUnknown(id, props, samFnPassthrough(), samFnHandled()); err != nil {
		return err
	}
	fn, err := buildLambdaProps(id, props)
	if err != nil {
		return err
	}
	base := map[string]any{samKeyType: samTypeFunction, samKeyProps: fn}
	carryAttrs(r, base)

	managed := []any{samLambdaBasicExec}
	if props["Role"] == nil {
		fn["Role"] = samGetAtt(id+"Role", attrNameArn)
	} else {
		fn["Role"] = props["Role"]
	}
	if props["VpcConfig"] != nil {
		managed = append(managed, samLambdaVPCExec)
	}
	if props["Tracing"] == "Active" {
		managed = append(managed, samXRayWrite)
	}
	if err = t.put(id, base); err != nil {
		return err
	}
	ev := &samEventCtx{id: id, managed: &managed}
	if err = t.translateEvents(ev, asMap(props["Events"])); err != nil {
		return err
	}
	if props["Role"] == nil {
		if err = t.putFunctionRole(id, props, managed); err != nil {
			return err
		}
	}

	return t.putAlias(id, props)
}

func buildLambdaProps(id string, props map[string]any) (map[string]any, error) {
	fn := map[string]any{}
	for k := range samFnPassthrough() {
		if v, ok := props[k]; ok {
			fn[k] = v
		}
	}
	code, err := lambdaCode(id, props)
	if err != nil {
		return nil, err
	}
	if len(code) > 0 {
		fn["Code"] = code
	}
	if env := asMap(props["Environment"]); env != nil {
		fn["Environment"] = env
	}
	if props["Tracing"] != nil {
		fn["TracingConfig"] = map[string]any{"Mode": props["Tracing"]}
	}
	if dlq := asMap(props["DeadLetterQueue"]); dlq != nil {
		fn["DeadLetterConfig"] = map[string]any{"TargetArn": dlq["TargetArn"]}
	}
	fn["Tags"] = samTags(asMap(props["Tags"]))

	return fn, nil
}

func lambdaCode(id string, props map[string]any) (map[string]any, error) {
	if inline, ok := props["InlineCode"]; ok {
		return map[string]any{"ZipFile": inline}, nil
	}
	if img, ok := props["ImageUri"]; ok {
		return map[string]any{"ImageUri": img}, nil
	}
	uri, ok := props["CodeUri"]
	if !ok {
		if props["PackageType"] == "Image" {
			return map[string]any{}, nil
		}

		return nil, samErr(id, "Missing required property 'CodeUri' or 'InlineCode'.")
	}

	return s3Location(id, "CodeUri", uri, "S3Bucket", "S3Key", "S3ObjectVersion")
}

// s3Location turns an s3:// URI or {Bucket,Key,Version} map into CFN S3 fields.
func s3Location(id, prop string, v any, bucketKey, keyKey, verKey string) (map[string]any, error) {
	switch u := v.(type) {
	case string:
		rest, ok := strings.CutPrefix(u, "s3://")
		bucket, key, hasKey := strings.Cut(rest, "/")
		if !ok || bucket == "" || !hasKey || key == "" {
			return nil, samErr(id, "'%s' is not a valid S3 Uri of the form \"s3://bucket/key\".", prop)
		}

		return map[string]any{bucketKey: bucket, keyKey: key}, nil
	case map[string]any:
		out := map[string]any{bucketKey: u["Bucket"], keyKey: u["Key"]}
		if u[samKeyVersion] != nil {
			out[verKey] = u[samKeyVersion]
		}

		return out, nil
	default:
		return nil, samErr(id, "'%s' must be an S3 Uri string or an object with Bucket and Key.", prop)
	}
}

// putFunctionRole emits <Function>Role from Policies and the managed policies
// accumulated from the function's properties and event sources.
func (t *samTranslator) putFunctionRole(id string, props map[string]any, managed []any) error {
	inline, err := splitPolicies(id, props["Policies"], id+"Role", &managed)
	if err != nil {
		return err
	}
	role := map[string]any{
		samKeyAssumeRole:    servicePrincipalTrust("lambda.amazonaws.com"),
		"ManagedPolicyArns": toSubs(managed),
		"Tags":              samTags(nil),
	}
	if len(inline) > 0 {
		role["Policies"] = inline
	}
	if props["PermissionsBoundary"] != nil {
		role["PermissionsBoundary"] = props["PermissionsBoundary"]
	}

	return t.put(id+"Role", map[string]any{samKeyType: resTypeIAMRole, samKeyProps: role})
}

func toSubs(arns []any) []any {
	out := make([]any, len(arns))
	for i, a := range arns {
		if s, ok := a.(string); ok && strings.Contains(s, "${") {
			out[i] = map[string]any{samKeyFnSub: s}
		} else {
			out[i] = a
		}
	}

	return out
}

func servicePrincipalTrust(principal string) map[string]any {
	return map[string]any{
		samKeyVersion: samPolicyVersion,
		samKeyStatement: []any{map[string]any{
			samKeyEffect:    stackPolicyEffectAllow,
			samKeyPrincipal: map[string]any{"Service": []any{principal}},
			samKeyAction:    []any{"sts:AssumeRole"},
		}},
	}
}

// splitPolicies sorts SAM Policies entries into managed-policy ARNs (appended
// to managed) and inline policy documents.
func splitPolicies(id string, v any, roleID string, managed *[]any) ([]any, error) {
	var inline []any
	for i, p := range asList(v) {
		switch pt := p.(type) {
		case string:
			*managed = append(*managed, pt)
		case map[string]any:
			if pt[samKeyStatement] == nil {
				return nil, samErr(id, "Policy template or intrinsic in Policies is not supported by "+
					"this emulator's SAM transform; use a managed policy ARN or a policy document.")
			}
			inline = append(inline, map[string]any{
				samKeyPolicyName: fmt.Sprintf("%sPolicy%d", roleID, i),
				"PolicyDocument": pt,
			})
		default:
			return nil, samErr(id, "Policies entries must be a policy ARN or a policy document.")
		}
	}

	return inline, nil
}

func (t *samTranslator) putAlias(id string, props map[string]any) error {
	alias, ok := props["AutoPublishAlias"].(string)
	if !ok {
		return nil
	}
	fnProps := asMap(asMap(t.out[id])[samKeyProps])
	verID := id + samKeyVersion + samHash(fnProps)
	verProps := map[string]any{samKeyFnName: samRef(id)}
	if d := props["VersionDescription"]; d != nil {
		verProps[samKeyDesc] = d
	}
	if err := t.put(verID, map[string]any{samKeyType: samTypeVersion, samKeyProps: verProps}); err != nil {
		return err
	}
	aliasID := id + "Alias" + alias
	t.refs[id+".Alias"] = aliasID
	t.refs[id+".Version"] = verID

	return t.put(aliasID, map[string]any{samKeyType: samTypeAlias, samKeyProps: map[string]any{
		samKeyFnName:      samRef(id),
		"FunctionVersion": samGetAtt(verID, samKeyVersion),
		attrNameName:      alias,
	}})
}
