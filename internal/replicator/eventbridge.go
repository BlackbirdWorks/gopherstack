package replicator

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	ebtypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
)

const defaultBus = "default"

func (r *run) replicateEventBridge(ctx context.Context) {
	var buses []ebtypes.EventBus

	var token *string

	for {
		out, err := r.src.eb.ListEventBuses(ctx, &eventbridge.ListEventBusesInput{NextToken: token})
		if err != nil {
			r.listFail(TypeEventBridge, err)

			return
		}

		buses = append(buses, out.EventBuses...)

		if token = out.NextToken; token == nil {
			break
		}
	}

	for _, b := range buses {
		name := aws.ToString(b.Name)

		if name != defaultBus && r.match(name) {
			r.copyBus(ctx, b)
		}

		r.copyRules(ctx, name)
	}
}

func (r *run) copyBus(ctx context.Context, b ebtypes.EventBus) {
	name := aws.ToString(b.Name)
	id := "bus/" + name

	_, err := r.dst.eb.DescribeEventBus(ctx, &eventbridge.DescribeEventBusInput{Name: &name})

	exists, err := existsErr(err)
	if err != nil {
		r.fail(TypeEventBridge, id, err)

		return
	}

	if r.cfg.DryRun {
		r.add(TypeEventBridge, id, upsert(exists), "")

		return
	}

	if exists {
		r.add(TypeEventBridge, id, ActionSkipped, "bus exists")

		return
	}

	in := &eventbridge.CreateEventBusInput{Name: &name, Description: b.Description}

	if tags, terr := r.src.eb.ListTagsForResource(ctx, &eventbridge.ListTagsForResourceInput{
		ResourceARN: b.Arn,
	}); terr == nil {
		in.Tags = tags.Tags
	}

	_, err = r.dst.eb.CreateEventBus(ctx, in)
	r.done(TypeEventBridge, id, ActionCreated, err)
}

func (r *run) copyRules(ctx context.Context, bus string) {
	var token *string

	for {
		out, err := r.src.eb.ListRules(ctx, &eventbridge.ListRulesInput{EventBusName: &bus, NextToken: token})
		if err != nil {
			r.fail(TypeEventBridge, "bus/"+bus, fmt.Errorf("list rules: %w", err))

			return
		}

		for _, rule := range out.Rules {
			name := aws.ToString(rule.Name)
			if rule.ManagedBy != nil || !r.match(name, bus) {
				continue
			}

			r.copyRule(ctx, bus, name)
		}

		if token = out.NextToken; token == nil {
			return
		}
	}
}

func (r *run) copyRule(ctx context.Context, bus, name string) {
	id := bus + "/" + name

	d, err := r.src.eb.DescribeRule(ctx, &eventbridge.DescribeRuleInput{Name: &name, EventBusName: &bus})
	if err != nil {
		r.fail(TypeEventBridge, id, err)

		return
	}

	_, err = r.dst.eb.DescribeRule(ctx, &eventbridge.DescribeRuleInput{Name: &name, EventBusName: &bus})

	exists, err := existsErr(err)
	if err != nil {
		r.fail(TypeEventBridge, id, err)

		return
	}

	if r.cfg.DryRun {
		r.add(TypeEventBridge, id, upsert(exists), "")

		return
	}

	if err = r.writeRule(ctx, bus, d); err != nil {
		r.fail(TypeEventBridge, id, err)

		return
	}

	r.add(TypeEventBridge, id, upsert(exists), "")
}

func (r *run) writeRule(ctx context.Context, bus string, d *eventbridge.DescribeRuleOutput) error {
	in := &eventbridge.PutRuleInput{
		Name: d.Name, EventBusName: &bus, Description: d.Description, EventPattern: d.EventPattern,
		ScheduleExpression: d.ScheduleExpression, State: d.State,
	}
	if d.RoleArn != nil {
		in.RoleArn = aws.String(r.rm.ARN(*d.RoleArn))
	}

	if _, err := r.dst.eb.PutRule(ctx, in); err != nil {
		return fmt.Errorf("put rule: %w", err)
	}

	if err := r.copyRuleTags(ctx, d); err != nil {
		return err
	}

	var targets []ebtypes.Target

	var token *string

	for {
		out, err := r.src.eb.ListTargetsByRule(ctx, &eventbridge.ListTargetsByRuleInput{
			Rule: d.Name, EventBusName: &bus, NextToken: token,
		})
		if err != nil {
			return fmt.Errorf("list targets: %w", err)
		}

		for _, t := range out.Targets {
			targets = append(targets, r.remapTarget(t))
		}

		if token = out.NextToken; token == nil {
			break
		}
	}

	if len(targets) == 0 {
		return nil
	}

	if _, err := r.dst.eb.PutTargets(ctx, &eventbridge.PutTargetsInput{
		Rule: d.Name, EventBusName: &bus, Targets: targets,
	}); err != nil {
		return fmt.Errorf("put targets: %w", err)
	}

	return nil
}

func (r *run) remapTarget(t ebtypes.Target) ebtypes.Target {
	t.Arn = aws.String(r.rm.ARN(aws.ToString(t.Arn)))

	if t.RoleArn != nil {
		t.RoleArn = aws.String(r.rm.ARN(*t.RoleArn))
	}

	if t.DeadLetterConfig != nil && t.DeadLetterConfig.Arn != nil {
		t.DeadLetterConfig = &ebtypes.DeadLetterConfig{Arn: aws.String(r.rm.ARN(*t.DeadLetterConfig.Arn))}
	}

	return t
}

func (r *run) copyRuleTags(ctx context.Context, d *eventbridge.DescribeRuleOutput) error {
	tags, err := r.src.eb.ListTagsForResource(ctx, &eventbridge.ListTagsForResourceInput{ResourceARN: d.Arn})
	if err != nil {
		return fmt.Errorf("list rule tags: %w", err)
	}

	if len(tags.Tags) == 0 {
		return nil
	}

	dst := r.rm.ARN(aws.ToString(d.Arn))
	if _, err = r.dst.eb.TagResource(
		ctx,
		&eventbridge.TagResourceInput{ResourceARN: &dst, Tags: tags.Tags},
	); err != nil {
		return fmt.Errorf("tag rule: %w", err)
	}

	return nil
}
