package cloudformation

import (
	"fmt"
	"maps"
	"slices"
)

const (
	samKeyRoleArn   = "RoleArn"
	samKeyDLC       = "DeadLetterConfig"
	samKeyRetry     = "RetryPolicy"
	samUnescapeFlag = "UnescapeMappingTemplate"
	samHTTPPost     = "POST"
	samKeyPolicyDoc = "PolicyDocument"
	samKeyResource  = "Resource"
	samKeySchedExpr = "ScheduleExpression"
	samKeyStatusCd  = "statusCode"

	smRequestTemplate = `{"input": "$util.escapeJavaScript($input.json('$'))", "stateMachineArn": "${%s}"}`
	smUnescapedInput  = `{"input": "$util.escapeJavaScript($input.json('$')).replaceAll("\\'","'")", ` +
		`"stateMachineArn": "${%s}"}`
	smStartExecutionURI = "arn:${AWS::Partition}:apigateway:${AWS::Region}:states:action/StartExecution"
)

// translateStateMachineEvents expands the Events of an AWS::Serverless::StateMachine.
func (t *samTranslator) translateStateMachineEvents(smID, targetID string, events map[string]any, boundary any) error {
	for _, name := range slices.Sorted(maps.Keys(events)) {
		e := asMap(events[name])
		typ, _ := e[samKeyType].(string)
		props := asMap(e[samKeyProps])

		var err error

		switch typ {
		case samEventSchedule:
			err = t.smScheduleEvent(smID, targetID, name, props, boundary)
		case samEventScheduleV2:
			err = t.smScheduleV2Event(smID, targetID, name, props)
		case samEventRule, samEventCWEvent:
			err = t.smRuleEvent(smID, targetID, name, props, boundary)
		case samEventAPI:
			err = t.smAPIEvent(smID, targetID, name, props, boundary)
		default:
			err = samErr(smID, "Event type '%s' (event [%s]) is not supported by this emulator's SAM transform",
				typ, name)
		}

		if err != nil {
			return err
		}
	}

	return nil
}

func startExecutionPolicy(smID, policyName string) map[string]any {
	return map[string]any{
		samKeyPolicyName: policyName,
		samKeyPolicyDoc: map[string]any{
			samKeyStatement: []any{map[string]any{
				samKeyEffect: stackPolicyEffectAllow, samKeyAction: "states:StartExecution",
				samKeyResource: samRef(smID),
			}},
		},
	}
}

// putSMEventRole emits <role>Role letting principal start the state machine.
func (t *samTranslator) putSMEventRole(smID, roleID, principal string, boundary any) error {
	role := map[string]any{
		samKeyAssumeRole: servicePrincipalTrust(principal),
		"Policies":       []any{startExecutionPolicy(smID, roleID+"StartExecutionPolicy")},
	}
	if boundary != nil {
		role["PermissionsBoundary"] = boundary
	}

	return t.put(roleID, map[string]any{samKeyType: samTypeRole, samKeyProps: role})
}

func (t *samTranslator) smDLQ(smID, name string, props map[string]any, ruleARN any) (any, error) {
	cfg := asMap(props[samKeyDLC])
	if cfg == nil {
		return nil, nil //nolint:nilnil // no DeadLetterConfig is valid
	}

	return t.eventBridgeDLQ(smID+name, cfg, ruleARN, true)
}

func (t *samTranslator) smScheduleEvent(smID, targetID, name string, props map[string]any, boundary any) error {
	id := smID + name
	if err := rejectUnknown(id, props, keySet("Schedule", samKeyInput, samKeyEnabled, samKeyState, "Name", samKeyDesc,
		samKeyDLC, samKeyRetry, samKeyRoleArn, samKeyTarget)); err != nil {
		return err
	}
	if props["Schedule"] == nil {
		return samErr(smID, "Event [%s] of type Schedule is missing required property 'Schedule'.", name)
	}
	if props[samKeyState] != nil && props[samKeyEnabled] != nil {
		return samErr(id, "State and Enabled Properties cannot both be specified.")
	}
	rule := map[string]any{samKeySchedExpr: props["Schedule"]}
	if props[samKeyState] != nil {
		rule[samKeyState] = props[samKeyState]
	}
	if en, ok := props[samKeyEnabled].(bool); ok {
		rule[samKeyState] = samStateEnabled
		if !en {
			rule[samKeyState] = samStateDisabled
		}
	}
	for _, k := range []string{attrNameName, samKeyDesc} {
		if props[k] != nil {
			rule[k] = props[k]
		}
	}

	return t.putSMRule(smID, targetID, name, rule, props, boundary)
}

func (t *samTranslator) smRuleEvent(smID, targetID, name string, props map[string]any, boundary any) error {
	id := smID + name
	if err := rejectUnknown(id, props, keySet("Pattern", samKeyInput, "InputPath", "EventBusName", samKeyState,
		samKRuleName, samKeyDLC, samKeyRetry, samKeyTarget, "InputTransformer")); err != nil {
		return err
	}
	rule := map[string]any{}
	for src, dst := range map[string]string{"Pattern": "EventPattern", samKeyEventBus: "EventBusName",
		samKRuleName: attrNameName, samKeyState: samKeyState} {
		if props[src] != nil {
			rule[dst] = props[src]
		}
	}

	return t.putSMRule(smID, targetID, name, rule, props, boundary)
}

// putSMRule emits the Events::Rule, its invoke role (unless RoleArn) and the optional dead-letter queue.
func (t *samTranslator) putSMRule(smID, targetID, name string, rule, props map[string]any, boundary any) error {
	id := smID + name
	target := map[string]any{attrNameArn: samRef(targetID), "Id": samTargetIDSuffix(id, "StepFunctionsTarget")}
	if tid := asMap(props[samKeyTarget])["Id"]; tid != nil {
		target["Id"] = tid
	}
	for _, k := range []string{samKeyInput, "InputPath", samKeyRetry, "InputTransformer"} {
		if v, ok := props[k]; ok {
			target[k] = v
		}
	}
	roleARN := props[samKeyRoleArn]
	if roleARN == nil {
		roleID := id + "Role"
		if err := t.putSMEventRole(targetID, roleID, samEventsPrincipl, boundary); err != nil {
			return err
		}
		roleARN = samGetAtt(roleID, attrNameArn)
	}
	target[samKeyRoleArn] = roleARN
	dlq, err := t.smDLQ(smID, name, props, samGetAtt(id, attrNameArn))
	if err != nil {
		return err
	}
	if dlq != nil {
		target[samKeyDLC] = map[string]any{attrNameArn: dlq}
	}
	rule["Targets"] = []any{target}

	return t.put(id, map[string]any{samKeyType: samTypeRule, samKeyProps: rule})
}

func (t *samTranslator) smScheduleV2Event(smID, targetID, name string, props map[string]any) error {
	id := smID + name
	target := samSchedulerTarget{
		arn:    samRef(targetID),
		policy: startExecutionPolicy(targetID, id+"StartExecutionPolicy"),
	}

	return t.putSchedule(smID, id, props, target)
}

func (t *samTranslator) smAPIEvent(smID, targetID, name string, props map[string]any, boundary any) error {
	id := smID + name
	if err := rejectUnknown(
		id,
		props,
		keySet("Path", "Method", samKRestAPIID, samAuthKey, samUnescapeFlag),
	); err != nil {
		return err
	}

	path, _ := props["Path"].(string)
	method, _ := props["Method"].(string)

	if path == "" || method == "" {
		return samErr(smID, "Event [%s] of type Api requires 'Path' and 'Method'.", name)
	}

	api, err := t.apiFor(smID, props[samKRestAPIID])
	if err != nil {
		return err
	}

	authorizer, apiKey, scopes, err := eventAuth(smID, name, props)
	if err != nil {
		return err
	}

	roleID := id + "Role"
	if err = t.putSMEventRole(targetID, roleID, "apigateway.amazonaws.com", boundary); err != nil {
		return err
	}

	if v, has := props[samUnescapeFlag]; has && v != nil {
		if _, isBool := v.(bool); !isBool {
			return samErr(id, "Type of property '%s' is invalid.", samUnescapeFlag)
		}
	}
	unescape, _ := props[samUnescapeFlag].(bool)
	api.addRoute(
		samRoute{
			fnID:       targetID,
			path:       path,
			method:     method,
			authorizer: authorizer,
			roleID:     roleID,
			unescape:   unescape,
			apiKey:     apiKey,
			scopes:     scopes,
		},
		t.pass,
	)

	return nil
}

// smIntegration is the API Gateway "aws" integration that starts an execution.
func smIntegration(rt samRoute) map[string]any {
	tmpl := smRequestTemplate
	if rt.unescape {
		tmpl = smUnescapedInput
	}

	return map[string]any{
		"credentials":    samGetAtt(rt.roleID, attrNameArn),
		samKeyHTTPMethod: samHTTPPost,
		"requestTemplates": map[string]any{
			samJSONMime: map[string]any{samKeyFnSub: fmt.Sprintf(tmpl, rt.fnID)},
		},
		samKeyResponses: map[string]any{
			"200": map[string]any{samKeyStatusCd: "200"},
			"400": map[string]any{samKeyStatusCd: "400"},
		},
		samKeyTypeLower: "aws",
		samKeyURI:       map[string]any{samKeyFnSub: smStartExecutionURI},
	}
}
