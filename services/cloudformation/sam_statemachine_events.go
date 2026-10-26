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
func (t *samTranslator) translateStateMachineEvents(smID string, events map[string]any) error {
	for _, name := range slices.Sorted(maps.Keys(events)) {
		e := asMap(events[name])
		typ, _ := e[samKeyType].(string)
		props := asMap(e[samKeyProps])

		var err error

		switch typ {
		case samEventSchedule:
			err = t.smScheduleEvent(smID, name, props)
		case samEventScheduleV2:
			err = t.smScheduleV2Event(smID, name, props)
		case samEventRule, samEventCWEvent:
			err = t.smRuleEvent(smID, name, props)
		case samEventAPI:
			err = t.smAPIEvent(smID, name, props)
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
			samKeyVersion: samPolicyVersion,
			samKeyStatement: []any{map[string]any{
				samKeyEffect: stackPolicyEffectAllow, samKeyAction: "states:StartExecution",
				samKeyResource: samRef(smID),
			}},
		},
	}
}

// putSMEventRole emits <role>Role letting principal start the state machine.
func (t *samTranslator) putSMEventRole(smID, roleID, principal, policyName string, extra []any, boundary any) error {
	role := map[string]any{
		samKeyAssumeRole: servicePrincipalTrust(principal),
		"Policies":       append([]any{startExecutionPolicy(smID, policyName)}, extra...),
	}
	if boundary != nil {
		role["PermissionsBoundary"] = boundary
	}

	return t.put(roleID, map[string]any{samKeyType: samTypeRole, samKeyProps: role})
}

func smDLQPolicy(policyName string, dlc map[string]any) []any {
	arn := dlc[attrNameArn]
	if arn == nil {
		return nil
	}

	return []any{map[string]any{
		samKeyPolicyName: policyName,
		samKeyPolicyDoc: map[string]any{
			samKeyVersion: samPolicyVersion,
			samKeyStatement: []any{map[string]any{
				samKeyEffect: stackPolicyEffectAllow, samKeyAction: "sqs:SendMessage", samKeyResource: arn,
			}},
		},
	}}
}

func smDLCArn(smID, name string, props map[string]any) (map[string]any, error) {
	dlc := asMap(props[samKeyDLC])
	if dlc == nil {
		return nil, nil //nolint:nilnil // no DeadLetterConfig is a valid state
	}

	if dlc[attrNameArn] == nil {
		return nil, samErr(
			smID,
			"Event [%s] DeadLetterConfig needs an 'Arn'; generating the queue (Type: SQS) is not supported.",
			name,
		)
	}

	return map[string]any{attrNameArn: dlc[attrNameArn]}, nil
}

// smRuleTarget builds the Events::Rule target and emits the invoke role unless RoleArn is given.
func (t *samTranslator) smRuleTarget(smID, name string, props map[string]any) (map[string]any, error) {
	target := map[string]any{attrNameArn: samRef(smID), "Id": smID + name + "StepFunctionsTarget"}
	if tid := asMap(props[samKeyTarget])["Id"]; tid != nil {
		target["Id"] = tid
	}

	for _, k := range []string{samKeyInput, "InputPath", samKeyRetry} {
		if v, ok := props[k]; ok {
			target[k] = v
		}
	}

	dlc, err := smDLCArn(smID, name, props)
	if err != nil {
		return nil, err
	}

	if dlc != nil {
		target[samKeyDLC] = dlc
	}

	if props[samKeyRoleArn] != nil {
		target[samKeyRoleArn] = props[samKeyRoleArn]

		return target, nil
	}

	roleID := smID + name + "Role"
	if err = t.putSMEventRole(smID, roleID, samEventsPrincipl, roleID+"StartExecutionPolicy", nil, nil); err != nil {
		return nil, err
	}

	target[samKeyRoleArn] = samGetAtt(roleID, attrNameArn)

	return target, nil
}

func (t *samTranslator) smScheduleEvent(smID, name string, props map[string]any) error {
	id := smID + name
	if err := rejectUnknown(id, props, keySet("Schedule", samKeyInput, "Enabled", samKeyState, "Name", samKeyDesc,
		samKeyDLC, samKeyRetry, samKeyRoleArn, samKeyTarget)); err != nil {
		return err
	}

	if props["Schedule"] == nil {
		return samErr(smID, "Event [%s] of type Schedule is missing required property 'Schedule'.", name)
	}

	rule := map[string]any{samKeySchedExpr: props["Schedule"], samKeyState: ruleState(props)}
	for _, k := range []string{attrNameName, samKeyDesc} {
		if props[k] != nil {
			rule[k] = props[k]
		}
	}

	return t.putSMRule(smID, name, rule, props)
}

func (t *samTranslator) smRuleEvent(smID, name string, props map[string]any) error {
	id := smID + name
	if err := rejectUnknown(id, props, keySet("Pattern", samKeyInput, "InputPath", "EventBusName", samKeyState,
		"RuleName", samKeyDLC, samKeyRetry, samKeyRoleArn, samKeyTarget)); err != nil {
		return err
	}

	if props["Pattern"] == nil {
		return samErr(smID, "Event [%s] is missing required property 'Pattern'.", name)
	}

	rule := map[string]any{"EventPattern": props["Pattern"], samKeyState: ruleState(props)}
	if props["EventBusName"] != nil {
		rule["EventBusName"] = props["EventBusName"]
	}

	if props["RuleName"] != nil {
		rule[attrNameName] = props["RuleName"]
	}

	return t.putSMRule(smID, name, rule, props)
}

func (t *samTranslator) putSMRule(smID, name string, rule, props map[string]any) error {
	target, err := t.smRuleTarget(smID, name, props)
	if err != nil {
		return err
	}

	rule["Targets"] = []any{target}

	return t.put(smID+name, map[string]any{samKeyType: samTypeRule, samKeyProps: rule})
}

func (t *samTranslator) smScheduleV2Event(smID, name string, props map[string]any) error {
	id := smID + name
	if err := rejectUnknown(id, props, keySet(samKeySchedExpr, "ScheduleExpressionTimezone", "StartDate",
		"EndDate", samKeyState, "Name", "OmitName", samKeyDesc, "GroupName", "FlexibleTimeWindow", samKeyInput,
		"KmsKeyArn", samKeyRetry, samKeyRoleArn, "PermissionsBoundary", samKeyDLC)); err != nil {
		return err
	}

	if props[samKeySchedExpr] == nil {
		return samErr(smID, "Event [%s] of type ScheduleV2 is missing required property 'ScheduleExpression'.", name)
	}

	target := map[string]any{attrNameArn: samRef(smID)}
	for _, k := range []string{samKeyInput, samKeyRetry} {
		if v, ok := props[k]; ok {
			target[k] = v
		}
	}

	dlc, err := smDLCArn(smID, name, props)
	if err != nil {
		return err
	}

	if dlc != nil {
		target[samKeyDLC] = dlc
	}

	if props[samKeyRoleArn] != nil {
		target[samKeyRoleArn] = props[samKeyRoleArn]
	} else {
		roleID := id + "Role"
		extra := smDLQPolicy(id+"SQSPolicy", dlc)

		if err = t.putSMEventRole(
			smID, roleID, "scheduler.amazonaws.com", id+"StartExecutionPolicy", extra, props["PermissionsBoundary"],
		); err != nil {
			return err
		}

		target[samKeyRoleArn] = samGetAtt(roleID, attrNameArn)
	}

	ev := &samEventCtx{id: smID}

	return t.put(id, map[string]any{samKeyType: samTypeSchedule, samKeyProps: scheduleProps(ev, name, props, target)})
}

func (t *samTranslator) smAPIEvent(smID, name string, props map[string]any) error {
	id := smID + name
	if err := rejectUnknown(id, props, keySet("Path", "Method", "RestApiId", samAuthKey, samUnescapeFlag)); err != nil {
		return err
	}

	path, _ := props["Path"].(string)
	method, _ := props["Method"].(string)

	if path == "" || method == "" {
		return samErr(smID, "Event [%s] of type Api requires 'Path' and 'Method'.", name)
	}

	api, err := t.apiFor(smID, props["RestApiId"])
	if err != nil {
		return err
	}

	authorizer, apiKey, err := eventAuth(smID, name, props)
	if err != nil {
		return err
	}

	roleID := id + "Role"
	if err = t.putSMEventRole(
		smID,
		roleID,
		"apigateway.amazonaws.com",
		roleID+"StartExecutionPolicy",
		nil,
		nil,
	); err != nil {
		return err
	}

	unescape, _ := props[samUnescapeFlag].(bool)
	api.addRoute(
		samRoute{
			fnID:       smID,
			path:       path,
			method:     method,
			authorizer: authorizer,
			roleID:     roleID,
			unescape:   unescape,
			apiKey:     apiKey,
		},
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
		"credentials": samGetAtt(rt.roleID, attrNameArn),
		"httpMethod":  samHTTPPost,
		"requestTemplates": map[string]any{
			samJSONMime: map[string]any{samKeyFnSub: fmt.Sprintf(tmpl, rt.fnID)},
		},
		samKeyResponses: map[string]any{
			"200": map[string]any{samKeyStatusCd: "200"},
			"400": map[string]any{samKeyStatusCd: "400"},
		},
		samKeyTypeLower: "aws",
		"uri":           map[string]any{samKeyFnSub: smStartExecutionURI},
	}
}
