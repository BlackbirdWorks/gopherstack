package replicator

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/blackbirdworks/gopherstack/internal/awswire/iam"
)

func (r *run) copyGroups(ctx context.Context) {
	pager := iam.NewListGroupsPaginator(r.src.iam, &iam.ListGroupsInput{})
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			r.listFail(TypeIAM, err)

			return
		}

		for _, g := range page.Groups {
			if name := aws.ToString(g.GroupName); r.match(name) {
				r.copyGroup(ctx, "group/"+name, name, g.Path)
			}
		}
	}
}

func (r *run) copyGroup(ctx context.Context, id, name string, path *string) {
	_, err := r.dst.iam.GetGroup(ctx, &iam.GetGroupInput{GroupName: &name, MaxItems: aws.Int32(1)})

	exists, err := existsErr(err)
	if err != nil {
		r.fail(TypeIAM, id, err)

		return
	}

	if r.cfg.DryRun {
		r.add(TypeIAM, id, upsert(exists), "")

		return
	}

	if !exists {
		if _, err = r.dst.iam.CreateGroup(ctx, &iam.CreateGroupInput{GroupName: &name, Path: path}); err != nil {
			r.fail(TypeIAM, id, fmt.Errorf("create group: %w", err))

			return
		}
	}

	r.done(TypeIAM, id, upsert(exists), r.copyGroupPolicies(ctx, name))
}

func (r *run) copyGroupInline(ctx context.Context, group, policy string) error {
	got, err := r.src.iam.GetGroupPolicy(ctx, &iam.GetGroupPolicyInput{GroupName: &group, PolicyName: &policy})
	if err != nil {
		return fmt.Errorf("get inline %s: %w", policy, err)
	}

	doc := r.rm.Text(decodeDoc(aws.ToString(got.PolicyDocument)))
	if _, err = r.dst.iam.PutGroupPolicy(ctx, &iam.PutGroupPolicyInput{
		GroupName: &group, PolicyName: &policy, PolicyDocument: &doc,
	}); err != nil {
		return fmt.Errorf("put inline %s: %w", policy, err)
	}

	return nil
}
