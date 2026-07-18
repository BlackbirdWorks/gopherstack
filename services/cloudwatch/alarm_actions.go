package cloudwatch

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// alarmActionDeps carries the collaborators needed to fire alarm actions plus the
// instance IDs (from the alarm's InstanceId dimension) that EC2 automate actions
// target.
type alarmActionDeps struct {
	snsPub      SNSPublisher
	lambdaInv   LambdaInvoker
	ec2         EC2InstanceActioner
	asg         AutoScalingPolicyExecutor
	instanceIDs []string
}

// instanceIDsFromDimensions extracts EC2 instance IDs from an alarm's dimensions.
// CloudWatch fires arn:aws:automate EC2 actions against the instance named by the
// alarm's "InstanceId" dimension.
func instanceIDsFromDimensions(dims []Dimension) []string {
	var ids []string
	for _, d := range dims {
		if d.Name == "InstanceId" && d.Value != "" {
			ids = append(ids, d.Value)
		}
	}

	return ids
}

// buildAlarmActionPayload builds the JSON payload sent to SNS/Lambda when an alarm fires.
func (b *InMemoryBackend) buildAlarmActionPayload(
	alarmName, alarmDesc, alarmArn, oldState, newState, reason string,
) []byte {
	data := map[string]string{
		keyAlarmName:        alarmName,
		keyAlarmDescription: alarmDesc,
		keyAlarmArn:         alarmArn,
		"AWSAccountId":      b.accountID,
		"Region":            b.region,
		"NewStateValue":     newState,
		"NewStateReason":    reason,
		"OldStateValue":     oldState,
		"StateChangeTime":   time.Now().UTC().Format(time.RFC3339),
	}
	// map[string]string marshaling cannot fail; error is intentionally ignored.
	bs, _ := json.Marshal(data)

	return bs
}

// executeActions delivers the alarm action notifications to SNS topics, Lambda
// functions, EC2 instances (arn:aws:automate), and Auto Scaling scaling policies.
// Delivery errors are logged as warnings but do not prevent other actions from
// running. Each fired action is recorded as an Action history entry on the alarm,
// tagged with alarmTypeName ("MetricAlarm" or "CompositeAlarm") so
// DescribeAlarmHistory's AlarmType filter matches composite-alarm action entries
// too (previously hardcoded to "MetricAlarm" even when firing for a composite alarm).
func (b *InMemoryBackend) executeActions(
	ctx context.Context,
	actions []string,
	alarmName, alarmTypeName string,
	payload []byte,
	deps alarmActionDeps,
) {
	log := logger.Load(ctx)

	for _, action := range actions {
		var actionResult string

		switch {
		case strings.HasPrefix(action, "arn:aws:sns:"):
			actionResult = "SNS"
			if deps.snsPub != nil {
				if err := deps.snsPub.PublishToTopic(action, string(payload)); err != nil {
					log.WarnContext(ctx, "cloudwatch: alarm SNS action delivery failed",
						"topic_arn", action, "error", err)
					actionResult = "SNS (failed)"
				}
			}
		case strings.HasPrefix(action, "arn:aws:lambda:"):
			actionResult = "Lambda"
			if deps.lambdaInv != nil {
				if _, _, err := deps.lambdaInv.InvokeFunction(ctx, action, "Event", payload); err != nil {
					log.WarnContext(ctx, "cloudwatch: alarm Lambda action delivery failed",
						"function_arn", action, "error", err)
					actionResult = "Lambda (failed)"
				}
			}
		case strings.HasPrefix(action, "arn:aws:automate:"):
			actionResult = b.executeEC2Action(ctx, action, deps)
		case strings.HasPrefix(action, "arn:aws:autoscaling:"):
			actionResult = b.executeAutoScalingAction(ctx, action, deps)
		default:
			log.WarnContext(ctx, "cloudwatch: unrecognised alarm action skipped",
				"action", action)
			actionResult = "unknown (skipped)"
		}

		if alarmName != "" {
			summary := fmt.Sprintf("Alarm %q action executed: %s → %s", alarmName, action, actionResult)
			func() {
				b.mu.Lock("executeActions-history")
				defer b.mu.Unlock()
				b.appendHistory(alarmName, alarmTypeName, historyTypeAction, summary, "")
			}()
		}
	}
}

// executeEC2Action performs an arn:aws:automate:<region>:ec2:<verb> alarm action
// against the instances named by the alarm's InstanceId dimension. It returns a
// short human-readable result string recorded in alarm history.
func (b *InMemoryBackend) executeEC2Action(
	ctx context.Context,
	action string,
	deps alarmActionDeps,
) string {
	log := logger.Load(ctx)

	verb, ok := parseEC2AutomateVerb(action)
	if !ok {
		log.WarnContext(ctx, "cloudwatch: unrecognised EC2 automate action", "action", action)

		return "EC2 automate (invalid ARN)"
	}

	if deps.ec2 == nil {
		return "EC2 " + verb + " (no EC2 backend wired)"
	}

	if len(deps.instanceIDs) == 0 {
		return "EC2 " + verb + " (no InstanceId dimension)"
	}

	var err error
	switch verb {
	case "stop":
		err = deps.ec2.StopInstances(deps.instanceIDs)
	case "terminate":
		err = deps.ec2.TerminateInstances(deps.instanceIDs)
	case "reboot":
		err = deps.ec2.RebootInstances(deps.instanceIDs)
	case "recover":
		// Recover keeps the instance running on new underlying hardware; there is
		// no observable state change to emulate, so it is a validated no-op.
		return "EC2 recover"
	default:
		return "EC2 automate (unsupported verb)"
	}

	if err != nil {
		log.WarnContext(ctx, "cloudwatch: EC2 alarm action failed",
			"action", action, "error", err)

		return "EC2 " + verb + " (failed)"
	}

	return "EC2 " + verb
}

// executeAutoScalingAction performs an Auto Scaling scaling-policy alarm action.
// The scaling-policy ARN encodes both the Auto Scaling group name and the policy
// name, which are parsed and passed to the executor.
func (b *InMemoryBackend) executeAutoScalingAction(
	ctx context.Context,
	action string,
	deps alarmActionDeps,
) string {
	log := logger.Load(ctx)

	asgName, policyName, ok := parseScalingPolicyARN(action)
	if !ok {
		log.WarnContext(ctx, "cloudwatch: unrecognised scaling-policy ARN", "action", action)

		return "AutoScaling (invalid ARN)"
	}

	if deps.asg == nil {
		return "AutoScaling (no AutoScaling backend wired)"
	}

	if err := deps.asg.ExecuteScalingPolicy(asgName, policyName); err != nil {
		log.WarnContext(ctx, "cloudwatch: AutoScaling alarm action failed",
			"action", action, "error", err)

		return "AutoScaling (failed)"
	}

	return "AutoScaling policy executed"
}

// parseEC2AutomateVerb extracts the action verb from an
// arn:aws:automate:<region>:ec2:<verb> ARN.
func parseEC2AutomateVerb(action string) (string, bool) {
	parts := strings.Split(action, ":")
	// arn:aws:automate:<region>:ec2:<verb> → 6 colon-separated parts.
	const automateParts = 6
	if len(parts) != automateParts || parts[4] != "ec2" {
		return "", false
	}

	switch parts[5] {
	case "stop", "terminate", "reboot", "recover":
		return parts[5], true
	}

	return "", false
}

// parseScalingPolicyARN extracts the Auto Scaling group name and policy name from a
// scaling-policy ARN of the form
// arn:aws:autoscaling:<region>:<account>:scalingPolicy:<uuid>:autoScalingGroupName/<asg>:policyName/<name>.
func parseScalingPolicyARN(action string) (string, string, bool) {
	var asgName, policyName string

	for seg := range strings.SplitSeq(action, ":") {
		switch {
		case strings.HasPrefix(seg, "autoScalingGroupName/"):
			asgName = strings.TrimPrefix(seg, "autoScalingGroupName/")
		case strings.HasPrefix(seg, "policyName/"):
			policyName = strings.TrimPrefix(seg, "policyName/")
		}
	}

	if asgName == "" || policyName == "" {
		return "", "", false
	}

	return asgName, policyName, true
}
