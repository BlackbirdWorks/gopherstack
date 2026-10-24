package replicator

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	iamtypes "github.com/aws/aws-sdk-go-v2/service/iam/types"

	"github.com/blackbirdworks/gopherstack/internal/awswire/iam"
)

func (r *run) copyInstanceProfiles(ctx context.Context) {
	pager := iam.NewListInstanceProfilesPaginator(r.src.iam, &iam.ListInstanceProfilesInput{})
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			r.listFail(TypeIAM, err)

			return
		}

		for i := range page.InstanceProfiles {
			p := &page.InstanceProfiles[i]
			if name := aws.ToString(p.InstanceProfileName); r.match(name) &&
				!isServiceLinked(aws.ToString(p.Path), name) {
				r.copyInstanceProfile(ctx, p)
			}
		}
	}
}

func (r *run) copyInstanceProfile(ctx context.Context, p *iamtypes.InstanceProfile) {
	name := aws.ToString(p.InstanceProfileName)
	id := "instance-profile/" + name

	cur, err := r.dst.iam.GetInstanceProfile(ctx, &iam.GetInstanceProfileInput{InstanceProfileName: &name})

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
		if _, err = r.dst.iam.CreateInstanceProfile(ctx, &iam.CreateInstanceProfileInput{
			InstanceProfileName: &name, Path: p.Path, Tags: p.Tags,
		}); err != nil {
			r.fail(TypeIAM, id, fmt.Errorf("create instance profile: %w", err))

			return
		}
	}

	var have []iamtypes.Role
	if exists {
		have = cur.InstanceProfile.Roles
	}

	r.done(TypeIAM, id, upsert(exists), r.addProfileRoles(ctx, name, p.Roles, have))
}

func (r *run) addProfileRoles(ctx context.Context, name string, want, have []iamtypes.Role) error {
	for _, role := range want {
		if hasRole(have, aws.ToString(role.RoleName)) {
			continue
		}

		if _, err := r.dst.iam.AddRoleToInstanceProfile(ctx, &iam.AddRoleToInstanceProfileInput{
			InstanceProfileName: &name, RoleName: role.RoleName,
		}); err != nil {
			return fmt.Errorf("add role %s: %w", aws.ToString(role.RoleName), err)
		}
	}

	return nil
}

func hasRole(roles []iamtypes.Role, name string) bool {
	for _, r := range roles {
		if aws.ToString(r.RoleName) == name {
			return true
		}
	}

	return false
}
