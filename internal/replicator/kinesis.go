package replicator

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	kintypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

const kinesisDefaultRetentionHours = 24

func (r *run) replicateKinesis(ctx context.Context) {
	pager := kinesis.NewListStreamsPaginator(r.src.kin, &kinesis.ListStreamsInput{})
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			r.listFail(TypeKinesis, err)

			return
		}

		for _, name := range page.StreamNames {
			if r.match(name) {
				r.copyStream(ctx, name)
			}
		}
	}
}

func (r *run) copyStream(ctx context.Context, name string) {
	sum, err := r.src.kin.DescribeStreamSummary(ctx, &kinesis.DescribeStreamSummaryInput{StreamName: &name})
	if err != nil {
		r.fail(TypeKinesis, name, err)

		return
	}

	_, err = r.dst.kin.DescribeStreamSummary(ctx, &kinesis.DescribeStreamSummaryInput{StreamName: &name})

	exists, err := existsErr(err)
	if err != nil {
		r.fail(TypeKinesis, name, err)

		return
	}

	if r.cfg.DryRun {
		r.add(TypeKinesis, name, upsert(exists), "")

		return
	}

	if err = r.writeStream(ctx, sum.StreamDescriptionSummary, exists); err != nil {
		r.fail(TypeKinesis, name, err)

		return
	}

	r.add(TypeKinesis, name, upsert(exists), "")
}

func (r *run) writeStream(ctx context.Context, s *kintypes.StreamDescriptionSummary, exists bool) error {
	name := s.StreamName

	if !exists {
		in := &kinesis.CreateStreamInput{StreamName: name}
		if s.StreamModeDetails != nil {
			in.StreamModeDetails = s.StreamModeDetails
		}

		if s.StreamModeDetails == nil || s.StreamModeDetails.StreamMode == kintypes.StreamModeProvisioned {
			in.ShardCount = s.OpenShardCount
		}

		if _, err := r.dst.kin.CreateStream(ctx, in); err != nil {
			return fmt.Errorf("create stream: %w", err)
		}
	}

	if hrs := aws.ToInt32(s.RetentionPeriodHours); hrs > kinesisDefaultRetentionHours {
		if _, err := r.dst.kin.IncreaseStreamRetentionPeriod(ctx, &kinesis.IncreaseStreamRetentionPeriodInput{
			StreamName: name, RetentionPeriodHours: &hrs,
		}); err != nil {
			return fmt.Errorf("retention: %w", err)
		}
	}

	tags, err := r.src.kin.ListTagsForStream(ctx, &kinesis.ListTagsForStreamInput{StreamName: name})
	if err != nil {
		return fmt.Errorf("list tags: %w", err)
	}

	if len(tags.Tags) == 0 {
		return nil
	}

	m := make(map[string]string, len(tags.Tags))
	for _, t := range tags.Tags {
		m[aws.ToString(t.Key)] = aws.ToString(t.Value)
	}

	if _, err = r.dst.kin.AddTagsToStream(ctx, &kinesis.AddTagsToStreamInput{StreamName: name, Tags: m}); err != nil {
		return fmt.Errorf("add tags: %w", err)
	}

	return nil
}
