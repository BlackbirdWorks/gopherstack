package cloudformation

const (
	samTypeBucket   = "AWS::S3::Bucket"
	samTypeSchedule = "AWS::Scheduler::Schedule"
	samTypeRole     = "AWS::IAM::Role"
	samKeyNotifCfg  = "NotificationConfiguration"
	samKeyLambdaCfg = "LambdaConfigurations"
	samKeyTarget    = "Target"
	samKeyInput     = "Input"
)

func (t *samTranslator) scheduleV2Event(ev *samEventCtx, name string, props map[string]any) error {
	err := rejectUnknown(ev.id+name, props, keySet(
		"ScheduleExpression",
		"ScheduleExpressionTimezone",
		"StartDate",
		"EndDate",
		samKeyState,
		"Name",
		"OmitName",
		samKeyDesc,
		"GroupName",
		"FlexibleTimeWindow",
		samKeyInput,
		"KmsKeyArn",
		"RetryPolicy",
		"RoleArn",
		"PermissionsBoundary",
		"DeadLetterConfig",
	))
	if err != nil {
		return err
	}
	if props["ScheduleExpression"] == nil {
		return samErr(ev.id, "Event [%s] of type ScheduleV2 is missing required property 'ScheduleExpression'.", name)
	}
	target := map[string]any{attrNameArn: ev.fnArn()}
	for _, k := range []string{samKeyInput, "RetryPolicy"} {
		if v, ok := props[k]; ok {
			target[k] = v
		}
	}
	if dlc := asMap(props["DeadLetterConfig"]); dlc != nil {
		if dlc[attrNameArn] == nil {
			return samErr(
				ev.id,
				"Event [%s] DeadLetterConfig needs an 'Arn'; generating the queue (Type: SQS) is not supported.",
				name,
			)
		}
		target["DeadLetterConfig"] = map[string]any{attrNameArn: dlc[attrNameArn]}
	}
	if props["RoleArn"] != nil {
		target["RoleArn"] = props["RoleArn"]
	} else {
		roleID := ev.id + name + "Role"
		if err = t.put(roleID, schedulerInvokeRole(ev.id, props["PermissionsBoundary"])); err != nil {
			return err
		}
		target["RoleArn"] = samGetAtt(roleID, attrNameArn)
	}

	return t.put(
		ev.id+name,
		map[string]any{samKeyType: samTypeSchedule, samKeyProps: scheduleProps(ev, name, props, target)},
	)
}

func scheduleProps(ev *samEventCtx, name string, props, target map[string]any) map[string]any {
	sched := map[string]any{
		samKeyTarget:         target,
		"FlexibleTimeWindow": map[string]any{samKMode: "OFF"},
		samKeyState:          samStateEnabled,
	}
	for _, k := range []string{"ScheduleExpression", "ScheduleExpressionTimezone", "StartDate", "EndDate", samKeyState,
		samKeyDesc, "GroupName", "FlexibleTimeWindow", "KmsKeyArn"} {
		if v, ok := props[k]; ok {
			sched[k] = v
		}
	}
	if props["OmitName"] != true {
		sched["Name"] = ev.id + name
		if v, ok := props["Name"]; ok {
			sched["Name"] = v
		}
	}

	return sched
}

func schedulerInvokeRole(fnID string, boundary any) map[string]any {
	role := map[string]any{
		samKeyAssumeRole: map[string]any{
			samKeyVersion: samPolicyVersion,
			samKeyStatement: []any{map[string]any{
				samKeyEffect: stackPolicyEffectAllow, samKeyAction: "sts:AssumeRole",
				samKeyPrincipal: map[string]any{samKService: "scheduler.amazonaws.com"},
			}},
		},
		"Policies": []any{map[string]any{
			samKeyPolicyName: "SchedulerRoleLambdaInvokePolicy",
			samKeyPolicyDoc: map[string]any{
				samKeyVersion: samPolicyVersion,
				samKeyStatement: []any{map[string]any{
					samKeyEffect: stackPolicyEffectAllow, samKeyAction: samActionInvokeFn,
					changeTypeResource: samGetAtt(fnID, attrNameArn),
				}},
			},
		}},
	}
	if boundary != nil {
		role["PermissionsBoundary"] = boundary
	}

	return map[string]any{samKeyType: samTypeRole, samKeyProps: role}
}
