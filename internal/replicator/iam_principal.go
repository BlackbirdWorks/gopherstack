package replicator

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	iamtypes "github.com/aws/aws-sdk-go-v2/service/iam/types"

	"github.com/blackbirdworks/gopherstack/internal/awswire/iam"
)

type principalKind int

const (
	kindRole principalKind = iota
	kindUser
	kindGroup
)

type principal struct {
	name string
	kind principalKind
}

func (r *run) listAttached(ctx context.Context, p principal) ([]string, error) {
	switch p.kind {
	case kindUser:
		it := iam.NewListAttachedUserPoliciesPaginator(r.src.iam, &iam.ListAttachedUserPoliciesInput{UserName: &p.name})

		return collect(
			ctx,
			it,
			func(o *iam.ListAttachedUserPoliciesOutput) []string { return arnsOf(o.AttachedPolicies) },
		)
	case kindGroup:
		it := iam.NewListAttachedGroupPoliciesPaginator(
			r.src.iam,
			&iam.ListAttachedGroupPoliciesInput{GroupName: &p.name},
		)

		return collect(
			ctx,
			it,
			func(o *iam.ListAttachedGroupPoliciesOutput) []string { return arnsOf(o.AttachedPolicies) },
		)
	default:
		it := iam.NewListAttachedRolePoliciesPaginator(r.src.iam, &iam.ListAttachedRolePoliciesInput{RoleName: &p.name})

		return collect(
			ctx,
			it,
			func(o *iam.ListAttachedRolePoliciesOutput) []string { return arnsOf(o.AttachedPolicies) },
		)
	}
}

func (r *run) attachPolicy(ctx context.Context, p principal, arn string) error {
	var err error

	switch p.kind {
	case kindUser:
		_, err = r.dst.iam.AttachUserPolicy(ctx, &iam.AttachUserPolicyInput{UserName: &p.name, PolicyArn: &arn})
	case kindGroup:
		_, err = r.dst.iam.AttachGroupPolicy(ctx, &iam.AttachGroupPolicyInput{GroupName: &p.name, PolicyArn: &arn})
	default:
		_, err = r.dst.iam.AttachRolePolicy(ctx, &iam.AttachRolePolicyInput{RoleName: &p.name, PolicyArn: &arn})
	}

	return err
}

func (r *run) listInline(ctx context.Context, p principal) ([]string, error) {
	switch p.kind {
	case kindUser:
		it := iam.NewListUserPoliciesPaginator(r.src.iam, &iam.ListUserPoliciesInput{UserName: &p.name})

		return collect(ctx, it, func(o *iam.ListUserPoliciesOutput) []string { return o.PolicyNames })
	case kindGroup:
		it := iam.NewListGroupPoliciesPaginator(r.src.iam, &iam.ListGroupPoliciesInput{GroupName: &p.name})

		return collect(ctx, it, func(o *iam.ListGroupPoliciesOutput) []string { return o.PolicyNames })
	default:
		it := iam.NewListRolePoliciesPaginator(r.src.iam, &iam.ListRolePoliciesInput{RoleName: &p.name})

		return collect(ctx, it, func(o *iam.ListRolePoliciesOutput) []string { return o.PolicyNames })
	}
}

func (r *run) copyInline(ctx context.Context, p principal, policy string) error {
	switch p.kind {
	case kindUser:
		return r.copyUserInline(ctx, p.name, policy)
	case kindGroup:
		return r.copyGroupInline(ctx, p.name, policy)
	default:
		return r.copyInlinePolicy(ctx, p.name, policy)
	}
}

func (r *run) copyPolicies(ctx context.Context, p principal) error {
	arns, err := r.listAttached(ctx, p)
	if err != nil {
		return fmt.Errorf("list attached: %w", err)
	}

	for _, a := range arns {
		target := r.rm.ARN(a)
		if err = r.attachPolicy(ctx, p, target); err != nil {
			return fmt.Errorf("attach %s: %w", target, err)
		}
	}

	names, err := r.listInline(ctx, p)
	if err != nil {
		return fmt.Errorf("list inline: %w", err)
	}

	for _, n := range names {
		if err = r.copyInline(ctx, p, n); err != nil {
			return err
		}
	}

	return nil
}

type pager[O any] interface {
	HasMorePages() bool
	NextPage(ctx context.Context) (O, error)
}

// collect gathers pick(page) across every page.
func collect[O any](ctx context.Context, p pager[O], pick func(O) []string) ([]string, error) {
	var out []string

	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return nil, err
		}

		out = append(out, pick(page)...)
	}

	return out, nil
}

func arnsOf(ps []iamtypes.AttachedPolicy) []string {
	out := make([]string, 0, len(ps))
	for _, p := range ps {
		out = append(out, aws.ToString(p.PolicyArn))
	}

	return out
}

func (r *run) copyRolePolicies(ctx context.Context, name string) error {
	return r.copyPolicies(ctx, principal{name: name, kind: kindRole})
}

func (r *run) copyUserPolicies(ctx context.Context, name string) error {
	return r.copyPolicies(ctx, principal{name: name, kind: kindUser})
}

func (r *run) copyGroupPolicies(ctx context.Context, name string) error {
	return r.copyPolicies(ctx, principal{name: name, kind: kindGroup})
}
