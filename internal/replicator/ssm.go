package replicator

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	ssmtypes "github.com/aws/aws-sdk-go-v2/service/ssm/types"
)

var errNeedsDecryption = errors.New("SecureString requires --with-decryption")

func (r *run) replicateSSM(ctx context.Context) {
	pager := ssm.NewDescribeParametersPaginator(r.src.ssm, &ssm.DescribeParametersInput{})
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			r.listFail(TypeSSM, err)

			return
		}

		for _, p := range page.Parameters {
			name := aws.ToString(p.Name)
			if r.match(name) {
				r.copyParameter(ctx, p)
			}
		}
	}
}

func (r *run) copyParameter(ctx context.Context, meta ssmtypes.ParameterMetadata) {
	name := aws.ToString(meta.Name)

	if meta.Type == ssmtypes.ParameterTypeSecureString && !r.cfg.WithDecryption {
		r.add(TypeSSM, name, ActionSkipped, errNeedsDecryption.Error())

		return
	}

	_, err := r.dst.ssm.GetParameter(ctx, &ssm.GetParameterInput{Name: &name})

	exists, err := existsErr(err)
	if err != nil {
		r.fail(TypeSSM, name, err)

		return
	}

	if r.cfg.DryRun {
		r.add(TypeSSM, name, upsert(exists), "")

		return
	}

	got, err := r.src.ssm.GetParameter(ctx, &ssm.GetParameterInput{Name: &name, WithDecryption: aws.Bool(true)})
	if err != nil {
		r.fail(TypeSSM, name, err)

		return
	}

	if err = r.putParameter(ctx, meta, got.Parameter); err != nil {
		r.fail(TypeSSM, name, err)

		return
	}

	r.add(TypeSSM, name, upsert(exists), "")
}

func (r *run) putParameter(ctx context.Context, meta ssmtypes.ParameterMetadata, p *ssmtypes.Parameter) error {
	in := &ssm.PutParameterInput{
		Name: p.Name, Value: p.Value, Type: p.Type, Overwrite: aws.Bool(true),
		Description: meta.Description, AllowedPattern: meta.AllowedPattern,
		DataType: p.DataType,
	}
	if meta.Tier != "" {
		in.Tier = meta.Tier
	}

	if _, err := r.dst.ssm.PutParameter(ctx, in); err != nil {
		return fmt.Errorf("put parameter: %w", err)
	}

	tags, err := r.src.ssm.ListTagsForResource(ctx, &ssm.ListTagsForResourceInput{
		ResourceType: ssmtypes.ResourceTypeForTaggingParameter, ResourceId: p.Name,
	})
	if err != nil {
		return fmt.Errorf("list tags: %w", err)
	}

	if len(tags.TagList) == 0 {
		return nil
	}

	if _, err = r.dst.ssm.AddTagsToResource(ctx, &ssm.AddTagsToResourceInput{
		ResourceType: ssmtypes.ResourceTypeForTaggingParameter, ResourceId: p.Name, Tags: tags.TagList,
	}); err != nil {
		return fmt.Errorf("add tags: %w", err)
	}

	return nil
}
