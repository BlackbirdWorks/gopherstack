package cloudformation

// samSchedulerTarget is what a ScheduleV2 event invokes: a function or a state machine.
type samSchedulerTarget struct {
	arn    any
	policy map[string]any
}

func (t *samTranslator) scheduleV2Event(ev *samEventCtx, name string, props map[string]any) error {
	logicalID := ev.id + name
	target := samSchedulerTarget{
		arn:    ev.fnArn(),
		policy: lambdaInvokePolicy(logicalID+"LambdaPolicy", samGetAtt(ev.id, attrNameArn)),
	}

	return t.putSchedule(ev.id, logicalID, props, target)
}

func lambdaInvokePolicy(policyName string, resource any) map[string]any {
	return map[string]any{
		samKeyPolicyName: policyName,
		samKeyPolicyDoc: map[string]any{samKeyStatement: []any{map[string]any{
			samKeyAction: samActionInvokeFn, samKeyEffect: stackPolicyEffectAllow, samKeyResource: resource,
		}}},
	}
}

func samScheduleKeys() map[string]bool {
	return keySet("ScheduleExpression", "ScheduleExpressionTimezone", "StartDate", "EndDate", samKeyState, attrNameName,
		"OmitName", samKeyDesc, "GroupName", "FlexibleTimeWindow", samKeyInput, "KmsKeyArn", samKeyRetry, samKeyRoleArn,
		"PermissionsBoundary", samKeyDLC)
}

// putSchedule emits the Schedule, its dead-letter queue and, unless RoleArn is given, the execution role.
func (t *samTranslator) putSchedule(ownerID, logicalID string, props map[string]any, target samSchedulerTarget) error {
	if err := rejectUnknown(logicalID, props, samScheduleKeys()); err != nil {
		return err
	}
	if props["ScheduleExpression"] == nil {
		return samErr(ownerID, "Event [%s] of type ScheduleV2 is missing required property 'ScheduleExpression'.",
			logicalID)
	}
	sched, err := scheduleBody(logicalID, props)
	if err != nil {
		return err
	}
	var dlqARN any
	if cfg := asMap(props[samKeyDLC]); cfg != nil {
		arn, dlqErr := t.eventBridgeDLQ(logicalID, cfg, nil, false)
		if dlqErr != nil {
			return dlqErr
		}
		dlqARN = arn
	}
	roleARN := props[samKeyRoleArn]
	if roleARN == nil {
		roleID, roleErr := t.putSchedulerRole(logicalID, props["PermissionsBoundary"], target, dlqARN)
		if roleErr != nil {
			return roleErr
		}
		roleARN = samGetAtt(roleID, attrNameArn)
	}
	tgt := map[string]any{attrNameArn: target.arn, samKeyRoleArn: roleARN}
	for _, k := range []string{samKeyInput, samKeyRetry} {
		if props[k] != nil {
			tgt[k] = props[k]
		}
	}
	if dlqARN != nil {
		tgt[samKeyDLC] = map[string]any{attrNameArn: dlqARN}
	}
	sched[samKeyTarget] = tgt

	return t.put(logicalID, map[string]any{samKeyType: samTypeSchedule, samKeyProps: sched})
}

func (t *samTranslator) putSchedulerRole(
	logicalID string,
	boundary any,
	target samSchedulerTarget,
	dlq any,
) (string, error) {
	policies := []any{target.policy}
	if dlq != nil {
		policies = append(policies, map[string]any{
			samKeyPolicyName: logicalID + "SQSPolicy",
			samKeyPolicyDoc: map[string]any{samKeyStatement: []any{map[string]any{
				samKeyAction: samKSqsSendMessage, samKeyEffect: stackPolicyEffectAllow, samKeyResource: dlq,
			}}},
		})
	}
	role := map[string]any{samKeyAssumeRole: servicePrincipalTrust("scheduler.amazonaws.com"), "Policies": policies}
	if boundary != nil {
		role["PermissionsBoundary"] = boundary
	}
	id := logicalID + "Role"

	return id, t.put(id, map[string]any{samKeyType: samTypeRole, samKeyProps: role})
}

func scheduleBody(logicalID string, props map[string]any) (map[string]any, error) {
	sched := map[string]any{
		"ScheduleExpression": props["ScheduleExpression"],
		"FlexibleTimeWindow": map[string]any{"Mode": "OFF"},
	}
	for _, k := range []string{samKeyState, samKeyDesc, "StartDate", "EndDate", "ScheduleExpressionTimezone", "GroupName",
		"KmsKeyArn", "FlexibleTimeWindow"} {
		if props[k] != nil {
			sched[k] = props[k]
		}
	}
	switch {
	case props["OmitName"] == true && props[attrNameName] != nil:
		return nil, samErr(logicalID, "Name cannot be set when OmitName is True")
	case props["OmitName"] == true:
	case props[attrNameName] != nil:
		sched[attrNameName] = props[attrNameName]
	default:
		sched[attrNameName] = logicalID
	}

	return sched, nil
}
