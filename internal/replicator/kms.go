package replicator

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	kmstypes "github.com/aws/aws-sdk-go-v2/service/kms/types"
)

const kmsSourceTag = "gopherstack:replicated-from"

// replicateKMS copies key metadata and aliases only; key material cannot be exported.
func (r *run) replicateKMS(ctx context.Context) {
	existing, err := r.targetKeysBySource(ctx)
	if err != nil {
		r.listFail(TypeKMS, err)

		return
	}

	pager := kms.NewListKeysPaginator(r.src.kms, &kms.ListKeysInput{})
	for pager.HasMorePages() {
		page, perr := pager.NextPage(ctx)
		if perr != nil {
			r.listFail(TypeKMS, perr)

			return
		}

		for _, k := range page.Keys {
			r.copyKey(ctx, aws.ToString(k.KeyId), existing)
		}
	}
}

func (r *run) targetKeysBySource(ctx context.Context) (map[string]string, error) {
	out := map[string]string{}

	pager := kms.NewListKeysPaginator(r.dst.kms, &kms.ListKeysInput{})
	for pager.HasMorePages() {
		page, perr := pager.NextPage(ctx)
		if perr != nil {
			return nil, fmt.Errorf("list target keys: %w", perr)
		}

		for _, k := range page.Keys {
			tags, terr := r.dst.kms.ListResourceTags(ctx, &kms.ListResourceTagsInput{KeyId: k.KeyId})
			if terr != nil {
				return nil, fmt.Errorf("list target key tags: %w", terr)
			}

			for _, t := range tags.Tags {
				if aws.ToString(t.TagKey) == kmsSourceTag {
					out[aws.ToString(t.TagValue)] = aws.ToString(k.KeyId)
				}
			}
		}
	}

	return out, nil
}

func (r *run) copyKey(ctx context.Context, keyID string, existing map[string]string) {
	desc, err := r.src.kms.DescribeKey(ctx, &kms.DescribeKeyInput{KeyId: &keyID})
	if err != nil {
		r.fail(TypeKMS, keyID, err)

		return
	}

	md := desc.KeyMetadata

	aliases, err := r.keyAliases(ctx, keyID)
	if err != nil {
		r.fail(TypeKMS, keyID, err)

		return
	}

	if md.KeyManager == kmstypes.KeyManagerTypeAws || md.KeyState == kmstypes.KeyStatePendingDeletion ||
		!r.match(append(aliases, keyID)...) {
		return
	}

	dstID, exists := existing[keyID]
	if exists {
		r.rm.MapKey(keyID, dstID)
	}

	if r.cfg.DryRun {
		r.add(TypeKMS, keyID, upsert(exists), "metadata only")

		return
	}

	if !exists {
		if dstID, err = r.createKey(ctx, md); err != nil {
			r.fail(TypeKMS, keyID, err)

			return
		}

		r.rm.MapKey(keyID, dstID)
	}

	if err = r.syncAliases(ctx, dstID, aliases); err != nil {
		r.fail(TypeKMS, keyID, err)

		return
	}

	r.add(TypeKMS, keyID, upsert(exists), "metadata only; key material is not exported")
}

func (r *run) keyAliases(ctx context.Context, keyID string) ([]string, error) {
	var out []string

	pager := kms.NewListAliasesPaginator(r.src.kms, &kms.ListAliasesInput{KeyId: &keyID})
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("list aliases: %w", err)
		}

		for _, a := range page.Aliases {
			if n := aws.ToString(a.AliasName); !strings.HasPrefix(n, "alias/aws/") {
				out = append(out, n)
			}
		}
	}

	return out, nil
}

func (r *run) createKey(ctx context.Context, md *kmstypes.KeyMetadata) (string, error) {
	in := &kms.CreateKeyInput{
		Description: md.Description, KeyUsage: md.KeyUsage, KeySpec: md.KeySpec,
		MultiRegion: md.MultiRegion,
		Tags:        []kmstypes.Tag{{TagKey: aws.String(kmsSourceTag), TagValue: md.KeyId}},
	}

	if pol, err := r.src.kms.GetKeyPolicy(ctx, &kms.GetKeyPolicyInput{
		KeyId: md.KeyId, PolicyName: aws.String("default"),
	}); err == nil {
		in.Policy = aws.String(r.rm.Text(aws.ToString(pol.Policy)))
	}

	if tags, err := r.src.kms.ListResourceTags(ctx, &kms.ListResourceTagsInput{KeyId: md.KeyId}); err == nil {
		in.Tags = append(in.Tags, tags.Tags...)
	}

	out, err := r.dst.kms.CreateKey(ctx, in)
	if err != nil {
		return "", fmt.Errorf("create key: %w", err)
	}

	id := aws.ToString(out.KeyMetadata.KeyId)

	if !md.Enabled {
		if _, err = r.dst.kms.DisableKey(ctx, &kms.DisableKeyInput{KeyId: &id}); err != nil {
			return id, fmt.Errorf("disable key: %w", err)
		}
	}

	return id, nil
}

func (r *run) syncAliases(ctx context.Context, dstID string, aliases []string) error {
	for _, a := range aliases {
		_, err := r.dst.kms.CreateAlias(ctx, &kms.CreateAliasInput{AliasName: &a, TargetKeyId: &dstID})
		if err == nil {
			continue
		}

		if _, err = r.dst.kms.UpdateAlias(ctx, &kms.UpdateAliasInput{AliasName: &a, TargetKeyId: &dstID}); err != nil {
			return fmt.Errorf("alias %s: %w", a, err)
		}
	}

	return nil
}
