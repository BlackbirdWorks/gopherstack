package replicator

import (
	"bytes"
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
)

func (r *run) replicateSecrets(ctx context.Context) {
	pager := secretsmanager.NewListSecretsPaginator(r.src.sm, &secretsmanager.ListSecretsInput{})
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			r.listFail(TypeSecrets, err)

			return
		}

		for _, s := range page.SecretList {
			name := aws.ToString(s.Name)
			if s.DeletedDate != nil || !r.match(name, aws.ToString(s.ARN)) {
				continue
			}

			r.copySecret(ctx, name)
		}
	}
}

func (r *run) copySecret(ctx context.Context, name string) {
	desc, err := r.src.sm.DescribeSecret(ctx, &secretsmanager.DescribeSecretInput{SecretId: &name})
	if err != nil {
		r.fail(TypeSecrets, name, err)

		return
	}

	cur, err := r.dst.sm.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretId: &name})

	exists, err := existsErr(err)
	if err != nil {
		r.fail(TypeSecrets, name, err)

		return
	}

	if r.cfg.DryRun {
		r.add(TypeSecrets, name, upsert(exists), "")

		return
	}

	val, err := r.src.sm.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretId: &name})
	if err != nil {
		r.fail(TypeSecrets, name, err)

		return
	}

	if !exists {
		_, err = r.dst.sm.CreateSecret(ctx, &secretsmanager.CreateSecretInput{
			Name: &name, Description: desc.Description, SecretString: val.SecretString,
			SecretBinary: val.SecretBinary, Tags: desc.Tags,
		})
		if err == nil {
			err = r.copySecretPolicy(ctx, name)
		}

		r.done(TypeSecrets, name, ActionCreated, err)

		return
	}

	action, err := r.updateSecret(ctx, name, desc, val, cur)
	r.done(TypeSecrets, name, action, err)
}

func (r *run) updateSecret(
	ctx context.Context, name string, desc *secretsmanager.DescribeSecretOutput,
	val, cur *secretsmanager.GetSecretValueOutput,
) (string, error) {
	action := ActionUpdated

	if aws.ToString(val.SecretString) == aws.ToString(cur.SecretString) &&
		bytes.Equal(val.SecretBinary, cur.SecretBinary) {
		action = ActionSkipped
	} else if _, err := r.dst.sm.PutSecretValue(
		ctx,
		&secretsmanager.PutSecretValueInput{
			SecretId: &name, SecretString: val.SecretString, SecretBinary: val.SecretBinary,
		},
	); err != nil {
		return action, fmt.Errorf("put value: %w", err)
	}

	if _, err := r.dst.sm.UpdateSecret(ctx, &secretsmanager.UpdateSecretInput{
		SecretId: &name, Description: desc.Description,
	}); err != nil {
		return action, fmt.Errorf("update: %w", err)
	}

	if len(desc.Tags) > 0 {
		if _, err := r.dst.sm.TagResource(ctx, &secretsmanager.TagResourceInput{
			SecretId: &name, Tags: desc.Tags,
		}); err != nil {
			return action, fmt.Errorf("tag: %w", err)
		}
	}

	return action, r.copySecretPolicy(ctx, name)
}

func (r *run) copySecretPolicy(ctx context.Context, name string) error {
	p, err := r.src.sm.GetResourcePolicy(ctx, &secretsmanager.GetResourcePolicyInput{SecretId: &name})
	if err != nil {
		return fmt.Errorf("get resource policy: %w", err)
	}

	if aws.ToString(p.ResourcePolicy) == "" {
		return nil
	}

	_, err = r.dst.sm.PutResourcePolicy(ctx, &secretsmanager.PutResourcePolicyInput{
		SecretId: &name, ResourcePolicy: aws.String(r.rm.Text(aws.ToString(p.ResourcePolicy))),
	})
	if err != nil {
		return fmt.Errorf("put resource policy: %w", err)
	}

	return nil
}
