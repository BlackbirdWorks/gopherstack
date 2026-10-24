package replicator

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/blackbirdworks/gopherstack/internal/awswire/secretsmanager"
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

	kmsID, kmsNote := r.secretKMS(desc)

	if !exists {
		_, err = r.dst.sm.CreateSecret(ctx, &secretsmanager.CreateSecretInput{
			Name: &name, Description: desc.Description, SecretString: val.SecretString,
			SecretBinary: val.SecretBinary, Tags: desc.Tags, KmsKeyId: kmsID,
			ClientRequestToken: val.VersionId,
		})
		if err == nil {
			err = r.copySecretPolicy(ctx, name)
		}

		r.finishSecret(ctx, name, ActionCreated, kmsNote, err)

		return
	}

	action, err := r.updateSecret(ctx, name, desc, val, cur, kmsID)
	r.finishSecret(ctx, name, action, kmsNote, err)
}

// finishSecret copies the older staged versions, then records the secret result.
func (r *run) finishSecret(ctx context.Context, name, action, note string, err error) {
	if err == nil {
		err = r.copySecretVersions(ctx, name)
	}

	if err != nil {
		r.fail(TypeSecrets, name, err)

		return
	}

	r.add(TypeSecrets, name, action, note)
}

func (r *run) updateSecret(
	ctx context.Context, name string, desc *secretsmanager.DescribeSecretOutput,
	val, cur *secretsmanager.GetSecretValueOutput, kmsID *string,
) (string, error) {
	action := ActionUpdated

	if aws.ToString(val.SecretString) == aws.ToString(cur.SecretString) &&
		bytes.Equal(val.SecretBinary, cur.SecretBinary) {
		action = ActionSkipped
	} else if _, err := r.dst.sm.PutSecretValue(
		ctx,
		&secretsmanager.PutSecretValueInput{
			SecretId: &name, SecretString: val.SecretString, SecretBinary: val.SecretBinary,
			ClientRequestToken: val.VersionId,
		},
	); err != nil {
		return action, fmt.Errorf("put value: %w", err)
	}

	if _, err := r.dst.sm.UpdateSecret(ctx, &secretsmanager.UpdateSecretInput{
		SecretId: &name, Description: desc.Description, KmsKeyId: kmsID,
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

// secretKMS maps the source KmsKeyId to a replicated key; AWS-managed or unreplicated keys
// fall back to the target default with a note.
func (r *run) secretKMS(desc *secretsmanager.DescribeSecretOutput) (*string, string) {
	ref := aws.ToString(desc.KmsKeyId)
	if ref == "" {
		return nil, ""
	}

	if id, ok := r.rm.KeyRef(ref); ok {
		return &id, ""
	}

	if strings.HasPrefix(ref, "alias/aws/") {
		return nil, ""
	}

	return nil, "KmsKeyId " + ref + " was not replicated (replicate kms first); using the target default key"
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
