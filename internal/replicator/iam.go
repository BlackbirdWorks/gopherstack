package replicator

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	iamtypes "github.com/aws/aws-sdk-go-v2/service/iam/types"
)

func decodeDoc(s string) string {
	if strings.HasPrefix(s, "%") {
		if d, err := url.QueryUnescape(s); err == nil {
			return d
		}
	}

	return s
}

func isServiceLinked(path, name string) bool {
	return strings.HasPrefix(path, "/aws-service-role/") || strings.HasPrefix(name, "AWSServiceRole")
}

func (r *run) replicateIAM(ctx context.Context) {
	r.copyLocalPolicies(ctx)
	r.copyRoles(ctx)
	r.copyGroups(ctx)
	r.copyUsers(ctx)
	r.copyInstanceProfiles(ctx)
}

func (r *run) copyRoles(ctx context.Context) {
	pager := iam.NewListRolesPaginator(r.src.iam, &iam.ListRolesInput{})
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			r.listFail(TypeIAM, err)

			return
		}

		for _, role := range page.Roles {
			name := aws.ToString(role.RoleName)
			if !r.match(name) || isServiceLinked(aws.ToString(role.Path), name) {
				continue
			}

			r.copyRole(ctx, name)
		}
	}
}

func (r *run) copyLocalPolicies(ctx context.Context) {
	pager := iam.NewListPoliciesPaginator(r.src.iam, &iam.ListPoliciesInput{Scope: iamtypes.PolicyScopeTypeLocal})
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			r.listFail(TypeIAM, err)

			return
		}

		for _, p := range page.Policies {
			if r.match(aws.ToString(p.PolicyName), aws.ToString(p.Arn)) {
				r.copyPolicy(ctx, p)
			}
		}
	}
}

func (r *run) copyPolicy(ctx context.Context, p iamtypes.Policy) {
	id := "policy/" + aws.ToString(p.PolicyName)

	ver, err := r.src.iam.GetPolicyVersion(ctx, &iam.GetPolicyVersionInput{
		PolicyArn: p.Arn, VersionId: p.DefaultVersionId,
	})
	if err != nil {
		r.fail(TypeIAM, id, err)

		return
	}

	doc := r.rm.Text(decodeDoc(aws.ToString(ver.PolicyVersion.Document)))
	dstARN := r.rm.ARN(aws.ToString(p.Arn))

	cur, err := r.dst.iam.GetPolicy(ctx, &iam.GetPolicyInput{PolicyArn: &dstARN})

	exists, err := existsErr(err)
	if err != nil {
		r.fail(TypeIAM, id, err)

		return
	}

	switch {
	case !exists:
		if r.cfg.DryRun {
			r.add(TypeIAM, id, ActionCreated, "")

			return
		}

		_, err = r.dst.iam.CreatePolicy(ctx, &iam.CreatePolicyInput{
			PolicyName: p.PolicyName, Path: p.Path, Description: p.Description, PolicyDocument: &doc,
		})
		r.done(TypeIAM, id, ActionCreated, err)
	default:
		r.updatePolicy(ctx, id, dstARN, doc, cur.Policy.DefaultVersionId)
	}
}

func (r *run) updatePolicy(ctx context.Context, id, dstARN, doc string, curVersion *string) {
	curDoc, err := r.dst.iam.GetPolicyVersion(
		ctx,
		&iam.GetPolicyVersionInput{PolicyArn: &dstARN, VersionId: curVersion},
	)
	if err != nil {
		r.fail(TypeIAM, id, err)

		return
	}

	if decodeDoc(aws.ToString(curDoc.PolicyVersion.Document)) == doc {
		r.add(TypeIAM, id, ActionSkipped, "document unchanged")

		return
	}

	if r.cfg.DryRun {
		r.add(TypeIAM, id, ActionUpdated, "")

		return
	}

	if err = r.pruneOldVersion(ctx, dstARN); err != nil {
		r.fail(TypeIAM, id, err)

		return
	}

	_, err = r.dst.iam.CreatePolicyVersion(ctx, &iam.CreatePolicyVersionInput{
		PolicyArn: &dstARN, PolicyDocument: &doc, SetAsDefault: true,
	})
	r.done(TypeIAM, id, ActionUpdated, err)
}

const maxPolicyVersions = 5

// pruneOldVersion deletes the oldest non-default version when the 5-version cap is reached.
func (r *run) pruneOldVersion(ctx context.Context, policyARN string) error {
	vs, err := r.dst.iam.ListPolicyVersions(ctx, &iam.ListPolicyVersionsInput{PolicyArn: &policyARN})
	if err != nil {
		return fmt.Errorf("list versions: %w", err)
	}

	if len(vs.Versions) < maxPolicyVersions {
		return nil
	}

	var oldest *iamtypes.PolicyVersion

	for i := range vs.Versions {
		v := &vs.Versions[i]
		if v.IsDefaultVersion {
			continue
		}

		if oldest == nil || v.CreateDate.Before(*oldest.CreateDate) {
			oldest = v
		}
	}

	if oldest == nil {
		return nil
	}

	_, err = r.dst.iam.DeletePolicyVersion(ctx, &iam.DeletePolicyVersionInput{
		PolicyArn: &policyARN, VersionId: oldest.VersionId,
	})

	return err
}

func (r *run) copyRole(ctx context.Context, name string) {
	got, err := r.src.iam.GetRole(ctx, &iam.GetRoleInput{RoleName: &name})
	if err != nil {
		r.fail(TypeIAM, name, err)

		return
	}

	role := got.Role
	trust := r.rm.Text(decodeDoc(aws.ToString(role.AssumeRolePolicyDocument)))

	_, err = r.dst.iam.GetRole(ctx, &iam.GetRoleInput{RoleName: &name})

	exists, err := existsErr(err)
	if err != nil {
		r.fail(TypeIAM, name, err)

		return
	}

	if r.cfg.DryRun {
		r.add(TypeIAM, name, upsert(exists), "")

		return
	}

	if err = r.writeRole(ctx, role, trust, exists); err != nil {
		r.fail(TypeIAM, name, err)

		return
	}

	if err = r.copyRolePolicies(ctx, name); err != nil {
		r.fail(TypeIAM, name, err)

		return
	}

	r.add(TypeIAM, name, upsert(exists), "")
}

func (r *run) writeRole(ctx context.Context, role *iamtypes.Role, trust string, exists bool) error {
	if exists {
		if err := r.refreshRole(ctx, role, trust); err != nil {
			return err
		}
	} else if err := r.createRole(ctx, role, trust); err != nil {
		return err
	}

	if _, err := r.dst.iam.UpdateRole(ctx, &iam.UpdateRoleInput{
		RoleName: role.RoleName, Description: role.Description, MaxSessionDuration: role.MaxSessionDuration,
	}); err != nil {
		return fmt.Errorf("update role: %w", err)
	}

	return nil
}

func (r *run) createRole(ctx context.Context, role *iamtypes.Role, trust string) error {
	in := &iam.CreateRoleInput{
		RoleName: role.RoleName, Path: role.Path, AssumeRolePolicyDocument: &trust,
		Description: role.Description, MaxSessionDuration: role.MaxSessionDuration,
		Tags: role.Tags,
	}
	if role.PermissionsBoundary != nil {
		in.PermissionsBoundary = aws.String(r.rm.ARN(aws.ToString(role.PermissionsBoundary.PermissionsBoundaryArn)))
	}

	if _, err := r.dst.iam.CreateRole(ctx, in); err != nil {
		return fmt.Errorf("create role: %w", err)
	}

	return nil
}

func (r *run) refreshRole(ctx context.Context, role *iamtypes.Role, trust string) error {
	if _, err := r.dst.iam.UpdateAssumeRolePolicy(ctx, &iam.UpdateAssumeRolePolicyInput{
		RoleName: role.RoleName, PolicyDocument: &trust,
	}); err != nil {
		return fmt.Errorf("update trust: %w", err)
	}

	if len(role.Tags) == 0 {
		return nil
	}

	if _, err := r.dst.iam.TagRole(ctx, &iam.TagRoleInput{RoleName: role.RoleName, Tags: role.Tags}); err != nil {
		return fmt.Errorf("tag role: %w", err)
	}

	return nil
}

func (r *run) copyInlinePolicy(ctx context.Context, role, policy string) error {
	got, err := r.src.iam.GetRolePolicy(ctx, &iam.GetRolePolicyInput{RoleName: &role, PolicyName: &policy})
	if err != nil {
		return fmt.Errorf("get inline %s: %w", policy, err)
	}

	doc := r.rm.Text(decodeDoc(aws.ToString(got.PolicyDocument)))
	if _, err = r.dst.iam.PutRolePolicy(ctx, &iam.PutRolePolicyInput{
		RoleName: &role, PolicyName: &policy, PolicyDocument: &doc,
	}); err != nil {
		return fmt.Errorf("put inline %s: %w", policy, err)
	}

	return nil
}
