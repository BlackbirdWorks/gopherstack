package cloudformation

import (
	"maps"
	"slices"
)

const (
	samTypeBucket   = "AWS::S3::Bucket"
	samTypeSchedule = "AWS::Scheduler::Schedule"
	samTypeRole     = "AWS::IAM::Role"
	samKeyNotifCfg  = "NotificationConfiguration"
	samKeyLambdaCfg = "LambdaConfigurations"
	samKeyTarget    = "Target"
	samKeyInput     = "Input"
)

func (t *samTranslator) s3Event(ev *samEventCtx, name string, props map[string]any) error {
	if err := rejectUnknown(ev.id+name, props, keySet("Bucket", "Events", "Filter")); err != nil {
		return err
	}
	bucket, _ := asMap(props["Bucket"])[samKeyRef].(string)
	if bucket == "" {
		return samErr(
			ev.id,
			"Event [%s] of type S3 requires 'Bucket' to be a Ref to an AWS::S3::Bucket in this template.",
			name,
		)
	}
	events := asList(props["Events"])
	if len(events) == 0 {
		return samErr(ev.id, "Event [%s] of type S3 is missing required property 'Events'.", name)
	}
	for _, e := range events {
		cfg := map[string]any{"Event": e, samKeyFunction: samGetAtt(ev.id, attrNameArn)}
		if props["Filter"] != nil {
			cfg["Filter"] = props["Filter"]
		}
		t.s3Configs[bucket] = append(t.s3Configs[bucket], cfg)
	}
	perm := lambdaPermission(ev.id, "s3.amazonaws.com", samGetAtt(bucket, attrNameArn))
	asMap(perm[samKeyProps])["SourceAccount"] = samRef("AWS::AccountId")

	return t.put(ev.id+name+"Permission", perm)
}

// finalizeS3Events adds each bucket's collected Lambda configurations to its NotificationConfiguration.
func (t *samTranslator) finalizeS3Events() error {
	for _, id := range slices.Sorted(maps.Keys(t.s3Configs)) {
		b := asMap(t.out[id])
		if b[samKeyType] != samTypeBucket {
			return samErr(id, "S3 event Bucket must reference an AWS::S3::Bucket resource in this template.")
		}
		props := maps.Clone(asMap(b[samKeyProps]))
		if props == nil {
			props = map[string]any{}
		}
		nc := maps.Clone(asMap(props[samKeyNotifCfg]))
		if nc == nil {
			nc = map[string]any{}
		}
		nc[samKeyLambdaCfg] = slices.Concat(asList(nc[samKeyLambdaCfg]), t.s3Configs[id])
		props[samKeyNotifCfg] = nc
		nb := maps.Clone(b)
		nb[samKeyProps] = props
		t.out[id] = nb
	}

	return nil
}

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
	target := map[string]any{attrNameArn: samGetAtt(ev.id, attrNameArn)}
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
		"FlexibleTimeWindow": map[string]any{"Mode": "OFF"},
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
				samKeyPrincipal: map[string]any{"Service": "scheduler.amazonaws.com"},
			}},
		},
		"Policies": []any{map[string]any{
			samKeyPolicyName: "SchedulerRoleLambdaInvokePolicy",
			"PolicyDocument": map[string]any{
				samKeyVersion: samPolicyVersion,
				samKeyStatement: []any{map[string]any{
					samKeyEffect: stackPolicyEffectAllow, samKeyAction: samActionInvokeFn,
					"Resource": samGetAtt(fnID, attrNameArn),
				}},
			},
		}},
	}
	if boundary != nil {
		role["PermissionsBoundary"] = boundary
	}

	return map[string]any{samKeyType: samTypeRole, samKeyProps: role}
}
