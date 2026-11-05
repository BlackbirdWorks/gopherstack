package cloudformation

import "slices"

const (
	samTypeEventInvokeCfg = "AWS::Lambda::EventInvokeConfig"
	samTypeLambdaURL      = "AWS::Lambda::Url"
	samTypeSQSQueue       = "AWS::SQS::Queue"
	samTypeSNSTopic       = "AWS::SNS::Topic"
	samKeyQualifier       = "Qualifier"
	samKeyAuthType        = "AuthType"
	samActionInvokeFn     = "lambda:InvokeFunction"
	samKeyDestination     = "Destination"
	samLatest             = "$LATEST"
)

func samDestinationTypes() []string {
	return []string{samEventSQS, samEventSNS, "EventBridge", samDestLambda, "S3Bucket"}
}

// samDestinationPolicy is the role policy SAM attaches for one EventInvokeConfig destination.
func samDestinationPolicy(typ, logicalID string, dest any) map[string]any {
	statement := func(action string, resource any) map[string]any {
		return map[string]any{
			samKeyPolicyName: logicalID + typ + samKPolicy,
			samKeyPolicyDoc: map[string]any{samKeyStatement: []any{
				map[string]any{
					samKeyAction:       action,
					samKeyEffect:       stackPolicyEffectAllow,
					changeTypeResource: resource,
				},
			}},
		}
	}
	switch typ {
	case samEventSQS:
		return statement(samKSqsSendMessage, dest)
	case samEventSNS:
		return statement("sns:publish", dest)
	case "EventBridge":
		return statement("events:PutEvents", dest)
	case samDestLambda:
		return statement(samActionInvokeFn, dest)
	default:
		return map[string]any{
			samKeyPolicyName: logicalID + "S3Policy",
			samKeyPolicyDoc: map[string]any{samKeyStatement: []any{
				map[string]any{samKeyAction: "s3:PutObject", samKeyEffect: stackPolicyEffectAllow,
					changeTypeResource: map[string]any{samFnJoin: []any{"/", []any{dest, "*"}}}},
				map[string]any{
					samKeyAction:       "s3:ListBucket",
					samKeyEffect:       stackPolicyEffectAllow,
					changeTypeResource: dest,
				},
			}},
		}
	}
}

// putEventInvokeConfig emits <Function>EventInvokeConfig, auto-creating the SQS queue or SNS topic of a
// destination that names no ARN, and returns the role policies the destinations need.
func (t *samTranslator) putEventInvokeConfig(id string, raw, props map[string]any) ([]any, error) {
	cfg := asMap(props["EventInvokeConfig"])
	if cfg == nil {
		return nil, nil
	}
	if err := rejectUnknown(id, cfg, keySet("DestinationConfig", "MaximumEventAgeInSeconds",
		samKMaximumRetryAttempts)); err != nil {
		return nil, err
	}
	logicalID := id + "EventInvokeConfig"
	out := map[string]any{samKeyFnName: samRef(id), samKeyQualifier: samLatest}
	var deps []any
	if alias := samAliasID(id, props); alias != "" {
		out[samKeyQualifier] = props[samKeyAutoPublish]
		deps = append(deps, alias)
	}
	for _, k := range []string{"MaximumEventAgeInSeconds", samKMaximumRetryAttempts} {
		if cfg[k] != nil {
			out[k] = cfg[k]
		}
	}
	policies, destCfg, err := t.eventInvokeDestinations(id, logicalID, cfg)
	if err != nil {
		return nil, err
	}
	if destCfg != nil {
		out["DestinationConfig"] = destCfg
	}
	res := map[string]any{samKeyType: samTypeEventInvokeCfg, samKeyProps: out}
	carryAttrs(raw, res)
	delete(res, samKeyDependsOn)
	if len(deps) > 0 {
		res[samKeyDependsOn] = deps
	}

	return policies, t.put(logicalID, res)
}

func (t *samTranslator) eventInvokeDestinations(
	id, logicalID string,
	cfg map[string]any,
) ([]any, map[string]any, error) {
	dc := asMap(cfg["DestinationConfig"])
	if dc == nil {
		return nil, nil, nil
	}
	var policies []any
	destCfg := map[string]any{}
	for _, event := range []string{"OnSuccess", "OnFailure"} {
		d := asMap(dc[event])
		if d == nil {
			continue
		}
		dest, policy, err := t.eventInvokeDestination(id, logicalID, event, d)
		if err != nil {
			return nil, nil, err
		}
		destCfg[event] = map[string]any{samKeyDestination: dest}
		if policy != nil {
			policies = append(policies, policy)
		}
	}

	return policies, destCfg, nil
}

func (t *samTranslator) eventInvokeDestination(id, logicalID, event string, d map[string]any) (any, any, error) {
	typ, _ := d[samKeyType].(string)
	if !containsString(samDestinationTypes(), typ) {
		return nil, nil, samErr(id, "'Type: %s' must be one of %v", typ, samDestinationTypes())
	}
	resID := logicalID + event
	dest := d[samKeyDestination]
	propCond, destARN, err := t.destinationCondition(id, dest)
	if err != nil {
		return nil, nil, err
	}
	if dest != nil && propCond == "" {
		return dest, samDestinationPolicy(typ, resID, dest), nil
	}
	if typ != samEventSQS && typ != samEventSNS {
		return nil, nil, samErr(id, "Destination is required if Type is not [SQS SNS]")
	}
	suffix, resType := samKQueue, samTypeSQSQueue
	generated := resID + suffix
	ref := any(samGetAtt(generated, attrNameArn))
	if typ == samEventSNS {
		suffix, resType = "Topic", samTypeSNSTopic
		generated = resID + suffix
		ref = samRef(generated)
	}
	res := map[string]any{samKeyType: resType, samKeyProps: map[string]any{}}
	if combined := t.andConditions(id, asString(t.pass[samKeyCondition]), propCond); combined != "" {
		res[samKeyCondition] = combined
	}
	if err = t.put(generated, res); err != nil {
		return nil, nil, err
	}
	dest = ref
	if propCond != "" {
		dest = map[string]any{fnIf: []any{propCond, ref, destARN}}
	}

	return dest, samDestinationPolicy(typ, resID, dest), nil
}

func asString(v any) string {
	s, _ := v.(string)

	return s
}

func containsString(list []string, s string) bool { return slices.Contains(list, s) }

// destinationCondition reads an Fn::If Destination with one empty branch: the condition under which SAM
// generates the queue or topic, and the ARN of the other branch.
func (t *samTranslator) destinationCondition(id string, dest any) (string, any, error) {
	m := asMap(dest)
	list, isIf := m[fnIf].([]any)
	if !isIf || len(m) != 1 {
		return "", nil, nil
	}
	const ifItems = 3
	if len(list) != ifItems {
		return "", nil, samErr(id, "Malformed 'Destination' property: Fn::If needs 3 arguments.")
	}
	name, _ := list[0].(string)
	switch {
	case isNoValue(list[1]) && isNoValue(list[2]):
		return "", nil, nil
	case isNoValue(list[1]):
		return name, list[2], nil
	case isNoValue(list[2]):
		return t.addNotCondition(id, name), list[1], nil
	}

	return "", nil, nil
}

// addNotCondition registers NOT<name><hash> as the negation of a template condition and returns its name.
func (t *samTranslator) addNotCondition(id, name string) string {
	not := samCappedName("NOT"+name, id)
	t.conditions[not] = map[string]any{samFnNot: []any{map[string]any{samKeyCondition: name}}}

	return not
}

// andConditions combines two optional conditions the way SAM does, registering the AND condition.
func (t *samTranslator) andConditions(id, resourceCond, propCond string) string {
	switch {
	case resourceCond == "":
		return propCond
	case propCond == "":
		return resourceCond
	}
	name := samCappedName(resourceCond+"AND"+propCond, id)
	t.conditions[name] = map[string]any{samFnAnd: []any{
		map[string]any{samKeyCondition: resourceCond}, map[string]any{samKeyCondition: propCond},
	}}

	return name
}

const samConditionNameLimit = 255

func samCappedName(name, hashInput string) string {
	full := name + samLogicalID("", hashInput)
	if len(full) > samConditionNameLimit {
		return full[:samConditionNameLimit]
	}

	return full
}

// putFunctionURL emits <Function>Url and, for AuthType NONE, the two public-access permissions.
func (t *samTranslator) putFunctionURL(id string, props map[string]any) error {
	cfg := asMap(props["FunctionUrlConfig"])
	if cfg == nil {
		return nil
	}
	if err := rejectUnknown(id, cfg, keySet(samKeyAuthType, samKeyCorsProp, "InvokeMode")); err != nil {
		return err
	}
	auth, _ := cfg[samKeyAuthType].(string)
	if auth != samAuthNone && auth != "AWS_IAM" && !isIntrinsicValue(cfg[samKeyAuthType]) {
		return samErr(id, "AuthType is required to configure function property `FunctionUrlConfig`. "+
			"Please provide either AWS_IAM or NONE.")
	}
	if err := validateURLCors(id, cfg[samKeyCorsProp]); err != nil {
		return err
	}
	target := any(samRef(id))
	if alias := samAliasID(id, props); alias != "" {
		target = samRef(alias)
	}
	out := map[string]any{samKeyAuthType: cfg[samKeyAuthType], "TargetFunctionArn": target}
	for _, k := range []string{samKeyCorsProp, "InvokeMode"} {
		if cfg[k] != nil {
			out[k] = cfg[k]
		}
	}
	if err := t.put(id+"Url", map[string]any{samKeyType: samTypeLambdaURL, samKeyProps: out}); err != nil {
		return err
	}
	if auth != samAuthNone {
		return nil
	}
	if err := t.put(id+"UrlPublicPermissions", map[string]any{samKeyType: samTypePerm, samKeyProps: map[string]any{
		samKeyAction:          "lambda:InvokeFunctionUrl",
		samKeyFnName:          target,
		samKeyPrincipal:       "*",
		"FunctionUrlAuthType": samAuthNone,
	}}); err != nil {
		return err
	}

	return t.put(id+"URLInvokeAllowPublicAccess", map[string]any{samKeyType: samTypePerm, samKeyProps: map[string]any{
		samKeyAction:            samActionInvokeFn,
		samKeyFnName:            target,
		samKeyPrincipal:         "*",
		"InvokedViaFunctionUrl": true,
	}})
}

func validateURLCors(id string, cors any) error {
	if cors == nil || isIntrinsicValue(cors) {
		return nil
	}
	m := asMap(cors)
	if m == nil {
		return samErr(id, "FunctionUrlConfig.Cors must be a map.")
	}
	lists := keySet("AllowOrigins", "AllowMethods", "AllowHeaders", "ExposeHeaders")
	for k, v := range m {
		switch {
		case lists[k]:
			if _, ok := v.([]any); !ok && !isIntrinsicValue(v) {
				return samErr(id, "%s must be of type list.", k)
			}
		case k == samKeyAllowCreds:
			if _, ok := v.(bool); !ok && !isIntrinsicValue(v) {
				return samErr(id, "%s must be of type bool.", k)
			}
		case k == "MaxAge":
			if _, ok := v.(float64); !ok && !isIntrinsicValue(v) {
				return samErr(id, "%s must be of type int.", k)
			}
		default:
			return samErr(id, "%s is not a valid property for configuring Cors.", k)
		}
	}

	return nil
}
