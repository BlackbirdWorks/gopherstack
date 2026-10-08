package cloudformation

import "maps"

const (
	samTypeEventInvokeCfg = "AWS::Lambda::EventInvokeConfig"
	samTypeLambdaURL      = "AWS::Lambda::Url"
	samKeyQualifier       = "Qualifier"
	samKeyAuthType        = "AuthType"
	samActionInvokeFn     = "lambda:InvokeFunction"
)

func samDestinationActions() map[string]string {
	return map[string]string{
		"SQS":         "sqs:SendMessage",
		"SNS":         "sns:Publish",
		"Lambda":      samActionInvokeFn,
		"EventBridge": "events:PutEvents",
	}
}

// eventInvokeDestinationPolicies returns the inline policy documents SAM adds to
// a generated function role so the function may reach its async destinations.
func eventInvokeDestinationPolicies(id string, cfg map[string]any) ([]any, error) {
	dc := asMap(cfg["DestinationConfig"])
	var out []any
	for _, key := range []string{"OnSuccess", "OnFailure"} {
		d := asMap(dc[key])
		if d == nil {
			continue
		}
		typ, _ := d[samKeyType].(string)
		action, ok := samDestinationActions()[typ]
		if !ok {
			return nil, samErr(id,
				"EventInvokeConfig DestinationConfig Type must be one of SQS, SNS, Lambda, EventBridge.")
		}
		if d["Destination"] == nil {
			return nil, samErr(id, "EventInvokeConfig DestinationConfig Destination is required; "+
				"generating the destination resource is not supported by this emulator's SAM transform.")
		}
		out = append(out, map[string]any{
			samKeyVersion: samPolicyVersion,
			samKeyStatement: []any{map[string]any{
				samKeyEffect: stackPolicyEffectAllow, samKeyAction: action, "Resource": d["Destination"],
			}},
		})
	}

	return out, nil
}

// withDestinationPolicies returns props with the EventInvokeConfig destination
// policies appended to Policies; props is not mutated.
func withDestinationPolicies(id string, props map[string]any) (map[string]any, error) {
	cfg := asMap(props["EventInvokeConfig"])
	if cfg == nil {
		return props, nil
	}
	extra, err := eventInvokeDestinationPolicies(id, cfg)
	if err != nil || len(extra) == 0 {
		return props, err
	}
	out := make(map[string]any, len(props)+1)
	maps.Copy(out, props)
	out["Policies"] = append(append([]any{}, asList(props["Policies"])...), extra...)

	return out, nil
}

// putEventInvokeConfig emits <Function>EventInvokeConfig.
func (t *samTranslator) putEventInvokeConfig(id string, props map[string]any) error {
	cfg := asMap(props["EventInvokeConfig"])
	if cfg == nil {
		return nil
	}
	if err := rejectUnknown(id, cfg, keySet("DestinationConfig", "MaximumEventAgeInSeconds",
		"MaximumRetryAttempts")); err != nil {
		return err
	}
	out := map[string]any{samKeyFnName: samRef(id), samKeyQualifier: "$LATEST"}
	var deps []any
	if alias, ok := props["AutoPublishAlias"].(string); ok {
		out[samKeyQualifier] = alias
		deps = append(deps, t.refs[id+".Alias"])
	}
	for _, k := range []string{"MaximumEventAgeInSeconds", "MaximumRetryAttempts"} {
		if cfg[k] != nil {
			out[k] = cfg[k]
		}
	}
	if dc := asMap(cfg["DestinationConfig"]); dc != nil {
		conv := map[string]any{}
		for _, key := range []string{"OnSuccess", "OnFailure"} {
			if d := asMap(dc[key]); d != nil {
				conv[key] = map[string]any{"Destination": d["Destination"]}
			}
		}
		out["DestinationConfig"] = conv
	}
	res := map[string]any{samKeyType: samTypeEventInvokeCfg, samKeyProps: out}
	if len(deps) > 0 {
		res[samKeyDependsOn] = deps
	}

	return t.put(id+"EventInvokeConfig", res)
}

// putFunctionURL emits <Function>Url and, for AuthType NONE, <Function>UrlPublicPermissions.
func (t *samTranslator) putFunctionURL(id string, props map[string]any) error {
	cfg := asMap(props["FunctionUrlConfig"])
	if cfg == nil {
		return nil
	}
	if err := rejectUnknown(id, cfg, keySet(samKeyAuthType, "Cors", "InvokeMode")); err != nil {
		return err
	}
	auth, _ := cfg[samKeyAuthType].(string)
	if auth != samAuthNone && auth != "AWS_IAM" {
		return samErr(id, "FunctionUrlConfig AuthType must be NONE or AWS_IAM.")
	}
	out := map[string]any{samKeyAuthType: auth, "TargetFunctionArn": samRef(id)}
	res := map[string]any{samKeyType: samTypeLambdaURL, samKeyProps: out}
	if alias, ok := props["AutoPublishAlias"].(string); ok {
		out[samKeyQualifier] = alias
		res[samKeyDependsOn] = []any{t.refs[id+".Alias"]}
	}
	for _, k := range []string{"Cors", "InvokeMode"} {
		if cfg[k] != nil {
			out[k] = cfg[k]
		}
	}
	if err := t.put(id+"Url", res); err != nil {
		return err
	}
	if auth != samAuthNone {
		return nil
	}

	return t.put(id+"UrlPublicPermissions", map[string]any{samKeyType: samTypePerm, samKeyProps: map[string]any{
		samKeyAction:          "lambda:InvokeFunctionUrl",
		samKeyFnName:          samRef(id),
		samKeyPrincipal:       "*",
		"FunctionUrlAuthType": samAuthNone,
	}})
}
