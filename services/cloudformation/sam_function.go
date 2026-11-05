package cloudformation

import (
	"maps"
	"net/url"
	"strings"
)

const (
	samLambdaDurableExec = "arn:${AWS::Partition}:iam::aws:policy/service-role/AWSLambdaBasicDurableExecutionRolePolicy"
	samLambdaBasicExec = "arn:${AWS::Partition}:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
	samLambdaVPCExec   = "arn:${AWS::Partition}:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
	samXRayWrite       = "arn:${AWS::Partition}:iam::aws:policy/AWSXrayWriteOnlyAccess"
	samTracingDisabled = "Disabled"
	samKeyAutoPublish  = "AutoPublishAlias"
)

func samFnPassthrough() map[string]bool {
	return keySet("Handler", "Runtime", "MemorySize", "Timeout", samKeyDesc, "FunctionName",
		"Layers", "Architectures", "VpcConfig", "PackageType", "KmsKeyArn", "EphemeralStorage",
		"ReservedConcurrentExecutions", "FileSystemConfigs", "CodeSigningConfigArn", "SnapStart",
		"LoggingConfig", "RuntimeManagementConfig", "ImageConfig", "RecursiveLoop", "TenancyConfig",
		"DurableConfig", "FunctionScalingConfig", "PublishToLatestPublished")
}

func samFnHandled() map[string]bool {
	return keySet("CodeUri", "InlineCode", "ImageUri", "Environment", scopeTags, "Role", "Policies",
		samKeyEvents, samKeyAutoPublish, "Tracing", "DeadLetterQueue", "PermissionsBoundary",
		"VersionDescription", "VersionDeletionPolicy", samKeyAssumeRole, "FunctionUrlConfig", "EventInvokeConfig",
		"RolePath", "PropagateTags", "AutoPublishAliasAllProperties", "AutoPublishCodeSha256",
		"ProvisionedConcurrencyConfig", "SourceKMSKeyArn", "CapacityProviderConfig", samKeyDeployPref)
}

func samDeadLetterActions() map[string]string {
	return map[string]string{samEventSQS: samKSqsSendMessage, samEventSNS: "sns:Publish"}
}

func (t *samTranslator) translateFunction(id string, r map[string]any) error {
	props := t.resProps(samKeyFunction, r)
	if err := rejectUnknown(id, props, samFnPassthrough(), samFnHandled()); err != nil {
		return err
	}
	t.pass = passAttrs(r)
	defer func() { t.pass = nil }()
	if v, ok := props["AutoPublishCodeSha256"]; ok {
		props["AutoPublishCodeSha256"] = t.paramDefault(v)
	}
	if v, ok := props[samKeyAutoPublish]; ok {
		alias, isStr := t.paramDefault(v).(string)
		if !isStr || alias == "" {
			return samErr(id, "'AutoPublishAlias' must be a string or a Ref to a template parameter")
		}
		props[samKeyAutoPublish] = alias
	}
	if err := validateFunctionProps(id, props); err != nil {
		return err
	}
	fn, err := t.buildLambdaProps(id, props)
	if err != nil {
		return err
	}
	base := map[string]any{samKeyType: samTypeFunction, samKeyProps: fn}
	carryAttrs(r, base)
	if err = t.put(id, base); err != nil {
		return err
	}
	if err = t.putAlias(id, props); err != nil {
		return err
	}
	if err = t.putFunctionURL(id, props); err != nil {
		return err
	}
	if err = t.deploymentPreference(id, r, props); err != nil {
		return err
	}
	eicPolicies, err := t.putEventInvokeConfig(id, r, props)
	if err != nil {
		return err
	}

	return t.finishFunction(id, props, eicPolicies)
}

func validateFunctionProps(id string, props map[string]any) error {
	if props["ProvisionedConcurrencyConfig"] != nil && props[samKeyAutoPublish] == nil {
		return samErr(id, "To set ProvisionedConcurrencyConfig AutoPublishALias must be defined on the function")
	}
	if dlq, ok := props["DeadLetterQueue"]; ok {
		cfg := asMap(dlq)
		typ, _ := cfg[samKeyType].(string)
		if cfg == nil || typ == "" || cfg["TargetArn"] == nil {
			return samErr(id, "'DeadLetterQueue' requires Type and TargetArn properties to be specified.")
		}
		if _, known := samDeadLetterActions()[typ]; !known {
			return samErr(id, "'DeadLetterQueue' requires Type of [SQS SNS]")
		}
	}
	if props["VersionDeletionPolicy"] != nil && props[samKeyAutoPublish] == nil {
		return samErr(id, "'VersionDeletionPolicy' requires 'AutoPublishAlias'.")
	}

	return nil
}

// finishFunction builds the execution role and the events, which share one set of policies.
func (t *samTranslator) finishFunction(id string, props map[string]any, eicPolicies []any) error {
	role := &samRolePolicies{}
	managed := t.functionManagedPolicies(props)
	roleID := id + "Role"
	createRole, roleValue, roleCond, err := t.functionRoleValue(id, props)
	if err != nil {
		return err
	}
	fn := asMap(asMap(t.out[id])[samKeyProps])
	fn["Role"] = roleValue
	if createRole {
		for _, p := range t.functionInlineBase(props, eicPolicies) {
			role.addInline(p)
		}
		if err = t.applyPolicies(id, roleID, props["Policies"], role); err != nil {
			return err
		}
	}
	ev := &samEventCtx{id: id, role: &samRolePolicies{}, alias: samAliasID(id, props)}
	if err = t.translateEvents(ev, asMap(props[samKeyEvents])); err != nil {
		return err
	}
	if !createRole {
		return nil
	}
	for _, p := range ev.role.managed {
		role.addManaged(p)
	}
	for _, p := range ev.role.inline {
		role.addInline(asMap(p))
	}

	return t.putFunctionRole(id, props, managed, role, roleCond)
}

// functionManagedPolicies is the fixed set of managed policies SAM attaches before user and event policies.
func (t *samTranslator) functionManagedPolicies(props map[string]any) []any {
	managed := []any{samLambdaBasicExec}
	if dc, has := props["DurableConfig"]; has && dc != nil {
		managed = []any{samLambdaDurableExec}
	}
	if tr, ok := props["Tracing"]; ok && tr != nil && t.paramDefault(tr) != samTracingDisabled {
		managed = append(managed, samXRayWrite)
	}
	if props["VpcConfig"] != nil {
		managed = append(managed, samLambdaVPCExec)
	}

	return managed
}

// functionInlineBase is the dead-letter and destination policies that precede the user's Policies.
func (t *samTranslator) functionInlineBase(props map[string]any, eic []any) []map[string]any {
	var out []map[string]any
	if dlq := asMap(props["DeadLetterQueue"]); dlq != nil {
		typ, _ := dlq[samKeyType].(string)
		out = append(out, map[string]any{
			samKeyPolicyName: "DeadLetterQueuePolicy",
			samKeyPolicyDoc: map[string]any{
				samKeyVersion: samPolicyVersion,
				samKeyStatement: []any{map[string]any{
					samKeyAction: samDeadLetterActions()[typ], changeTypeResource: dlq["TargetArn"],
					samKeyEffect: stackPolicyEffectAllow,
				}},
			},
		})
	}
	for _, p := range eic {
		out = append(out, asMap(p))
	}

	return out
}

// functionRoleValue decides who owns the function's Role: SAM generates <Fn>Role unless a Role is given;
// an Fn::If Role gets a conditional role whose Condition follows the branch SAM leaves empty.
func (t *samTranslator) functionRoleValue(id string, props map[string]any) (bool, any, string, error) {
	generated := samGetAtt(id+"Role", attrNameArn)
	role, given := props["Role"]
	if !given || role == nil {
		return true, generated, "", nil
	}
	cond, isIf := asMap(role)[fnIf].([]any)
	if !isIf || len(asMap(role)) != 1 {
		return false, role, "", nil
	}
	const ifItems = 3
	if len(cond) != ifItems {
		return false, nil, "", samErr(id, "Malformed 'Role' property: Fn::If needs 3 arguments.")
	}
	name, _ := cond[0].(string)
	yes, no := cond[1], cond[2]
	switch {
	case isNoValue(yes) && isNoValue(no):
		return true, generated, "", nil
	case isNoValue(yes):
		return true, map[string]any{fnIf: []any{name, generated, no}}, name, nil
	case isNoValue(no):
		notName := t.addNotCondition(id, name)

		return true, map[string]any{fnIf: []any{name, yes, generated}}, notName, nil
	}

	return false, role, "", nil
}

func (t *samTranslator) putFunctionRole(
	id string, props map[string]any, managed []any, role *samRolePolicies, cond string,
) error {
	for _, p := range role.managed {
		if !containsJSON(managed, p) {
			managed = append(managed, p)
		}
	}
	trust := servicePrincipalTrust("lambda.amazonaws.com")
	if doc, ok := props[samKeyAssumeRole]; ok {
		trust = asMap(doc)
	}
	tags := samTags(asMap(props[scopeTags]))
	res := map[string]any{
		samKeyAssumeRole:  trust,
		samKeyManagedARNs: toSubs(managed),
		scopeTags:         tags,
	}
	if len(role.inline) > 0 {
		res["Policies"] = role.inline
	}
	if props["PermissionsBoundary"] != nil {
		res["PermissionsBoundary"] = props["PermissionsBoundary"]
	}
	if props["RolePath"] != nil {
		res["Path"] = props["RolePath"]
	}
	out := map[string]any{samKeyType: resTypeIAMRole, samKeyProps: res}
	if cond != "" {
		out[samKeyCondition] = cond
	}

	return t.put(id+"Role", out)
}

func containsJSON(list []any, v any) bool {
	for _, e := range list {
		if sameJSON(e, v) {
			return true
		}
	}

	return false
}

func (t *samTranslator) buildLambdaProps(id string, props map[string]any) (map[string]any, error) {
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
	if tr, ok := props["Tracing"]; ok && tr != nil && t.paramDefault(tr) != samTracingDisabled {
		fn["TracingConfig"] = map[string]any{samKMode: tr}
	}
	if dlq := asMap(props["DeadLetterQueue"]); dlq != nil {
		fn["DeadLetterConfig"] = map[string]any{"TargetArn": dlq["TargetArn"]}
	}
	if cp := asMap(props["CapacityProviderConfig"]); cp != nil {
		fn["CapacityProviderConfig"] = capacityProviderConfig(cp)
	}
	if sha, ok := props["AutoPublishCodeSha256"].(string); ok && sha != "" {
		fn[samKeyDesc] = autoPublishDescription(fn[samKeyDesc], sha)
	}
	tags := samTags(asMap(props[scopeTags]))
	fn[scopeTags] = tags

	return fn, nil
}

func autoPublishDescription(desc any, sha string) any {
	switch d := desc.(type) {
	case nil:
		return sha
	case string:
		if d == "" {
			return sha
		}

		return d + " " + sha
	}

	return map[string]any{samFnJoin: []any{" ", []any{desc, sha}}}
}

// capacityProviderConfig nests SAM's flat CapacityProviderConfig under LambdaManagedInstancesCapacityProviderConfig.
func capacityProviderConfig(cp map[string]any) map[string]any {
	inner := map[string]any{"CapacityProviderArn": cp["Arn"]}
	for _, k := range []string{"PerExecutionEnvironmentMaxConcurrency", "ExecutionEnvironmentMemoryGiBPerVCpu"} {
		if v, ok := cp[k]; ok && v != nil {
			inner[k] = v
		}
	}

	return map[string]any{"LambdaManagedInstancesCapacityProviderConfig": inner}
}

func lambdaCode(id string, props map[string]any) (map[string]any, error) {
	var code map[string]any
	switch {
	case props["InlineCode"] != nil:
		code = map[string]any{"ZipFile": props["InlineCode"]}
	case props["ImageUri"] != nil:
		code = map[string]any{"ImageUri": props["ImageUri"]}
	case props["CodeUri"] != nil:
		loc, err := s3Location(id, "CodeUri", props["CodeUri"], "S3Bucket", "S3Key", "S3ObjectVersion")
		if err != nil {
			return nil, err
		}
		code = loc
	case props["PackageType"] == samKeyImage:
		return map[string]any{}, nil
	default:
		return nil, samErr(id, "Missing required property 'CodeUri' or 'InlineCode'.")
	}
	if k := props["SourceKMSKeyArn"]; k != nil && props["PackageType"] != samKeyImage {
		code["SourceKMSKeyArn"] = k
	}

	return code, nil
}

// s3Location turns an s3:// URI (optionally ?versionId=) or {Bucket,Key,Version} map into CFN S3 fields.
func s3Location(id, prop string, v any, bucketKey, keyKey, verKey string) (map[string]any, error) {
	switch u := v.(type) {
	case string:
		rest, ok := strings.CutPrefix(u, "s3://")
		bucket, key, hasKey := strings.Cut(rest, "/")
		key, query, _ := strings.Cut(key, "?")
		if !ok || bucket == "" || !hasKey || key == "" {
			return nil, samErr(id, "'%s' is not a valid S3 Uri of the form \"s3://bucket/key\".", prop)
		}
		out := map[string]any{bucketKey: bucket, keyKey: key}
		if vals, err := url.ParseQuery(query); err == nil && vals.Get("versionId") != "" {
			out[verKey] = vals.Get("versionId")
		}

		return out, nil
	case map[string]any:
		out := map[string]any{bucketKey: u["Bucket"], keyKey: u[samKeyKey]}
		if u[samKeyVersion] != nil {
			out[verKey] = u[samKeyVersion]
		}
		if mode := u["StorageMode"]; mode != nil && keyKey == "S3Key" {
			out["S3ObjectStorageMode"] = mode
		}

		return out, nil
	default:
		return nil, samErr(id, "'%s' must be an S3 Uri string or an object with Bucket and Key.", prop)
	}
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
			samKeyPrincipal: map[string]any{samKService: []any{principal}},
			samKeyAction:    []any{"sts:AssumeRole"},
		}},
	}
}

func (t *samTranslator) putAlias(id string, props map[string]any) error {
	alias, ok := props[samKeyAutoPublish].(string)
	if !ok {
		return nil
	}
	fnProps := asMap(asMap(t.out[id])[samKeyProps])
	verID := samVersionID(id, props, fnProps)
	verProps := map[string]any{samKeyFnName: samRef(id)}
	if d := props["VersionDescription"]; d != nil {
		verProps[samKeyDesc] = d
	}
	version := map[string]any{samKeyType: samTypeVersion, samKeyProps: verProps}
	if _, has := t.pass[samKeyDelPolicy]; !has {
		version[samKeyDelPolicy] = deletionPolicyRetain
		if p, hasPolicy := props["VersionDeletionPolicy"]; hasPolicy {
			version[samKeyDelPolicy] = p
		}
	}
	if err := t.put(verID, version); err != nil {
		return err
	}
	if !aliasNamePattern(alias) {
		return samErr(id, "AutoPublishAlias name ('%s') must contain only alphanumeric characters, hyphens, or "+
			"underscores matching (?!^[0-9]+$)([a-zA-Z0-9-_]+) pattern.", alias)
	}
	aliasID := samAliasID(id, props)
	t.refs[id+".Alias"] = aliasID
	t.refs[id+".Version"] = verID
	aliasProps := map[string]any{
		samKeyFnName:      samRef(id),
		"FunctionVersion": samGetAtt(verID, samKeyVersion),
		attrNameName:      alias,
	}
	if pc := props["ProvisionedConcurrencyConfig"]; pc != nil {
		aliasProps["ProvisionedConcurrencyConfig"] = pc
	}

	return t.put(aliasID, map[string]any{samKeyType: samTypeAlias, samKeyProps: aliasProps})
}

func aliasNamePattern(name string) bool {
	numeric := name != ""
	for _, r := range name {
		if r < '0' || r > '9' {
			numeric = false
		}
		if !isAlnum(string(r)) && r != '-' && r != '_' {
			return false
		}
	}

	return name != "" && !numeric
}

// samVersionID mirrors SAM's Lambda version id: the hash covers the function
// code, environment, memory and active SnapStart (or every property with
// AutoPublishAliasAllProperties); AutoPublishCodeSha256 replaces the hash.
func samVersionID(id string, props, fnProps map[string]any) string {
	prefix := id + samKeyVersion
	if sha, ok := props["AutoPublishCodeSha256"].(string); ok && sha != "" {
		return prefix + sha[:min(len(sha), samHashLen)]
	}
	if props["AutoPublishAliasAllProperties"] == true {
		return samLogicalID(prefix, samPartitionLiteral(fnProps))
	}
	data := maps.Clone(asMap(fnProps["Code"]))
	if data == nil {
		data = map[string]any{}
	}
	if env := asMap(fnProps["Environment"]); env != nil {
		maps.Copy(data, env)
	}
	if mem, ok := fnProps["MemorySize"]; ok && mem != nil && mem != 0.0 {
		data["MemorySize"] = mem
	}
	if ss := asMap(fnProps["SnapStart"]); ss != nil {
		if apply, _ := ss["ApplyOn"].(string); apply != "" && apply != "None" {
			data["SnapStart"] = ss
		}
	}

	return samLogicalID(prefix, data)
}

// samAliasID is SAM's alias logical id: <Function>Alias<name> with "-" and "_" spelled out as D and U.
func samAliasID(id string, props map[string]any) string {
	alias, ok := props[samKeyAutoPublish].(string)
	if !ok || alias == "" {
		return ""
	}

	return id + "Alias" + strings.NewReplacer("-", "D", "_", "U").Replace(alias)
}
