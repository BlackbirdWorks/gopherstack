package replicator

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	smtypes "github.com/aws/aws-sdk-go-v2/service/secretsmanager/types"

	"github.com/blackbirdworks/gopherstack/internal/awswire/secretsmanager"
)

const (
	stageCurrent    = "AWSCURRENT"
	replicatingTemp = "gopherstack-replicating"
)

func listSecretVersions(
	ctx context.Context, c *secretsmanager.Client, name string,
) ([]smtypes.SecretVersionsListEntry, error) {
	var out []smtypes.SecretVersionsListEntry

	pager := secretsmanager.NewListSecretVersionIdsPaginator(
		c,
		&secretsmanager.ListSecretVersionIdsInput{SecretId: &name},
	)
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("list versions: %w", err)
		}

		out = append(out, page.Versions...)
	}

	return out, nil
}

func holderOf(vs []smtypes.SecretVersionsListEntry, stage string) string {
	for _, v := range vs {
		if slices.Contains(v.VersionStages, stage) {
			return aws.ToString(v.VersionId)
		}
	}

	return ""
}

// copySecretVersions copies the labelled non-current source versions (AWSPREVIOUS and custom
// staging labels), keeping their version ids, and re-attaches the labels. Unlabelled
// (deprecated) versions are not copied.
func (r *run) copySecretVersions(ctx context.Context, name string) error {
	src, err := listSecretVersions(ctx, r.src.sm, name)
	if err != nil {
		return err
	}

	dst, err := listSecretVersions(ctx, r.dst.sm, name)
	if err != nil {
		return err
	}

	missing := missingVersions(src, dst)
	if len(missing) == 0 && !labelsDiffer(src, dst) {
		return nil
	}

	for _, v := range missing {
		if err = r.putOldVersion(ctx, name, aws.ToString(v.VersionId)); err != nil {
			return err
		}

		r.add(TypeSecrets, name+"#"+aws.ToString(v.VersionId), ActionCreated,
			"staging labels: "+strings.Join(v.VersionStages, ","))
	}

	if err = r.reconcileLabels(ctx, name, src); err != nil {
		return err
	}

	return r.dropTempLabel(ctx, name, missing)
}

func missingVersions(src, dst []smtypes.SecretVersionsListEntry) []smtypes.SecretVersionsListEntry {
	have := map[string]bool{}
	for _, v := range dst {
		have[aws.ToString(v.VersionId)] = true
	}

	var out []smtypes.SecretVersionsListEntry

	for _, v := range src {
		if !slices.Contains(v.VersionStages, stageCurrent) && !have[aws.ToString(v.VersionId)] {
			out = append(out, v)
		}
	}

	slices.SortFunc(out, func(a, b smtypes.SecretVersionsListEntry) int {
		return a.CreatedDate.Compare(aws.ToTime(b.CreatedDate))
	})

	return out
}

func labelsDiffer(src, dst []smtypes.SecretVersionsListEntry) bool {
	for _, v := range src {
		for _, l := range v.VersionStages {
			if l != stageCurrent && holderOf(dst, l) != aws.ToString(v.VersionId) {
				return true
			}
		}
	}

	return false
}

func (r *run) putOldVersion(ctx context.Context, name, versionID string) error {
	val, err := r.src.sm.GetSecretValue(
		ctx,
		&secretsmanager.GetSecretValueInput{SecretId: &name, VersionId: &versionID},
	)
	if err != nil {
		return fmt.Errorf("get version %s: %w", versionID, err)
	}

	if _, err = r.dst.sm.PutSecretValue(ctx, &secretsmanager.PutSecretValueInput{
		SecretId: &name, ClientRequestToken: &versionID, SecretString: val.SecretString,
		SecretBinary: val.SecretBinary, VersionStages: []string{replicatingTemp},
	}); err != nil {
		return fmt.Errorf("put version %s: %w", versionID, err)
	}

	return nil
}

func (r *run) reconcileLabels(ctx context.Context, name string, src []smtypes.SecretVersionsListEntry) error {
	dst, err := listSecretVersions(ctx, r.dst.sm, name)
	if err != nil {
		return err
	}

	for _, v := range src {
		id := aws.ToString(v.VersionId)

		for _, l := range v.VersionStages {
			holder := holderOf(dst, l)
			if l == stageCurrent || holder == id {
				continue
			}

			in := &secretsmanager.UpdateSecretVersionStageInput{
				SecretId: &name, VersionStage: aws.String(l), MoveToVersionId: &id,
			}
			if holder != "" {
				in.RemoveFromVersionId = &holder
			}

			if _, err = r.dst.sm.UpdateSecretVersionStage(ctx, in); err != nil {
				return fmt.Errorf("move label %s: %w", l, err)
			}
		}
	}

	return nil
}

func (r *run) dropTempLabel(ctx context.Context, name string, put []smtypes.SecretVersionsListEntry) error {
	for _, v := range put {
		if _, err := r.dst.sm.UpdateSecretVersionStage(ctx, &secretsmanager.UpdateSecretVersionStageInput{
			SecretId: &name, VersionStage: aws.String(replicatingTemp), RemoveFromVersionId: v.VersionId,
		}); err != nil {
			return fmt.Errorf("drop temp label: %w", err)
		}
	}

	return nil
}
