package replicator

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	iamtypes "github.com/aws/aws-sdk-go-v2/service/iam/types"

	"github.com/blackbirdworks/gopherstack/internal/awswire/iam"
)

// copyUsers copies users, policies and group membership. Access keys, login profiles, MFA
// devices and SSH keys are secrets and are never copied.
func (r *run) copyUsers(ctx context.Context) {
	pager := iam.NewListUsersPaginator(r.src.iam, &iam.ListUsersInput{})
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			r.listFail(TypeIAM, err)

			return
		}

		for i := range page.Users {
			if name := aws.ToString(page.Users[i].UserName); r.match(name) {
				r.copyUser(ctx, name)
			}
		}
	}
}

func (r *run) copyUser(ctx context.Context, name string) {
	id := "user/" + name

	got, err := r.src.iam.GetUser(ctx, &iam.GetUserInput{UserName: &name})
	if err != nil {
		r.fail(TypeIAM, id, err)

		return
	}

	_, err = r.dst.iam.GetUser(ctx, &iam.GetUserInput{UserName: &name})

	exists, err := existsErr(err)
	if err != nil {
		r.fail(TypeIAM, id, err)

		return
	}

	if r.cfg.DryRun {
		r.add(TypeIAM, id, upsert(exists), "")

		return
	}

	if err = r.writeUser(ctx, got.User, exists); err == nil {
		err = r.copyUserPolicies(ctx, name)
	}

	if err == nil {
		err = r.copyMemberships(ctx, name)
	}

	r.done(TypeIAM, id, upsert(exists), err)
}

func (r *run) writeUser(ctx context.Context, u *iamtypes.User, exists bool) error {
	if exists {
		if len(u.Tags) == 0 {
			return nil
		}

		if _, err := r.dst.iam.TagUser(ctx, &iam.TagUserInput{UserName: u.UserName, Tags: u.Tags}); err != nil {
			return fmt.Errorf("tag user: %w", err)
		}

		return nil
	}

	in := &iam.CreateUserInput{UserName: u.UserName, Path: u.Path, Tags: u.Tags}
	if u.PermissionsBoundary != nil {
		in.PermissionsBoundary = aws.String(r.rm.ARN(aws.ToString(u.PermissionsBoundary.PermissionsBoundaryArn)))
	}

	if _, err := r.dst.iam.CreateUser(ctx, in); err != nil {
		return fmt.Errorf("create user: %w", err)
	}

	return nil
}

func (r *run) copyUserInline(ctx context.Context, user, policy string) error {
	got, err := r.src.iam.GetUserPolicy(ctx, &iam.GetUserPolicyInput{UserName: &user, PolicyName: &policy})
	if err != nil {
		return fmt.Errorf("get inline %s: %w", policy, err)
	}

	doc := r.rm.Text(decodeDoc(aws.ToString(got.PolicyDocument)))
	if _, err = r.dst.iam.PutUserPolicy(ctx, &iam.PutUserPolicyInput{
		UserName: &user, PolicyName: &policy, PolicyDocument: &doc,
	}); err != nil {
		return fmt.Errorf("put inline %s: %w", policy, err)
	}

	return nil
}

func (r *run) copyMemberships(ctx context.Context, user string) error {
	pager := iam.NewListGroupsForUserPaginator(r.src.iam, &iam.ListGroupsForUserInput{UserName: &user})
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("list groups: %w", err)
		}

		for _, g := range page.Groups {
			if _, err = r.dst.iam.AddUserToGroup(ctx, &iam.AddUserToGroupInput{
				UserName: &user, GroupName: g.GroupName,
			}); err != nil {
				return fmt.Errorf("add to group %s: %w", aws.ToString(g.GroupName), err)
			}
		}
	}

	return nil
}
