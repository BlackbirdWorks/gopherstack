package replicator

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	logstypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
)

func (r *run) replicateLogs(ctx context.Context) {
	pager := cloudwatchlogs.NewDescribeLogGroupsPaginator(r.src.logs, &cloudwatchlogs.DescribeLogGroupsInput{})
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			r.listFail(TypeLogs, err)

			return
		}

		for _, g := range page.LogGroups {
			if name := aws.ToString(g.LogGroupName); r.match(name) {
				r.copyLogGroup(ctx, g)
			}
		}
	}
}

func (r *run) copyLogGroup(ctx context.Context, g logstypes.LogGroup) {
	name := aws.ToString(g.LogGroupName)

	out, err := r.dst.logs.DescribeLogGroups(ctx, &cloudwatchlogs.DescribeLogGroupsInput{LogGroupNamePrefix: &name})
	if err != nil {
		r.fail(TypeLogs, name, err)

		return
	}

	exists := false

	for _, e := range out.LogGroups {
		if aws.ToString(e.LogGroupName) == name {
			exists = true
		}
	}

	if r.cfg.DryRun {
		r.add(TypeLogs, name, upsert(exists), "")

		return
	}

	if err = r.writeLogGroup(ctx, g, exists); err != nil {
		r.fail(TypeLogs, name, err)

		return
	}

	r.add(TypeLogs, name, upsert(exists), "")
}

func (r *run) writeLogGroup(ctx context.Context, g logstypes.LogGroup, exists bool) error {
	tags := map[string]string{}

	if arn := strings.TrimSuffix(aws.ToString(g.Arn), ":*"); arn != "" {
		if t, err := r.src.logs.ListTagsForResource(
			ctx,
			&cloudwatchlogs.ListTagsForResourceInput{ResourceArn: &arn},
		); err == nil {
			tags = t.Tags
		}
	}

	if !exists {
		in := &cloudwatchlogs.CreateLogGroupInput{
			LogGroupName:  g.LogGroupName,
			Tags:          tags,
			LogGroupClass: g.LogGroupClass,
		}
		if _, err := r.dst.logs.CreateLogGroup(ctx, in); err != nil {
			return fmt.Errorf("create log group: %w", err)
		}
	}

	if g.RetentionInDays != nil {
		if _, err := r.dst.logs.PutRetentionPolicy(ctx, &cloudwatchlogs.PutRetentionPolicyInput{
			LogGroupName: g.LogGroupName, RetentionInDays: g.RetentionInDays,
		}); err != nil {
			return fmt.Errorf("retention: %w", err)
		}
	}

	return nil
}
