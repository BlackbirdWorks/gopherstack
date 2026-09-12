package ecr_test

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ecrsdk "github.com/aws/aws-sdk-go-v2/service/ecr"
	ecrtypes "github.com/aws/aws-sdk-go-v2/service/ecr/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTypedSlice11RealClient drives ecr's typed-coverage-blind ops
// (gopherstack-n3zi slice 11) through the real aws-sdk-go-v2 client.
func TestTypedSlice11RealClient(t *testing.T) {
	t.Parallel()

	t.Run("tags", func(t *testing.T) {
		t.Parallel()

		client := newTestECRClient(t, newTestHandler(t))
		ctx := t.Context()

		repoOut, err := client.CreateRepository(ctx, &ecrsdk.CreateRepositoryInput{
			RepositoryName: aws.String("s11-tags-repo"),
		})
		require.NoError(t, err)
		repoArn := aws.ToString(repoOut.Repository.RepositoryArn)

		_, err = client.TagResource(ctx, &ecrsdk.TagResourceInput{
			ResourceArn: aws.String(repoArn),
			Tags:        []ecrtypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
		})
		require.NoError(t, err)

		listOut, err := client.ListTagsForResource(ctx, &ecrsdk.ListTagsForResourceInput{
			ResourceArn: aws.String(repoArn),
		})
		require.NoError(t, err)
		tagMap := map[string]string{}
		for _, tg := range listOut.Tags {
			tagMap[aws.ToString(tg.Key)] = aws.ToString(tg.Value)
		}
		assert.Equal(t, "test", tagMap["env"])

		_, err = client.UntagResource(ctx, &ecrsdk.UntagResourceInput{
			ResourceArn: aws.String(repoArn),
			TagKeys:     []string{"env"},
		})
		require.NoError(t, err)

		afterOut, err := client.ListTagsForResource(ctx, &ecrsdk.ListTagsForResourceInput{
			ResourceArn: aws.String(repoArn),
		})
		require.NoError(t, err)
		assert.Empty(t, afterOut.Tags)
	})

	t.Run("account settings and pull time exclusions", func(t *testing.T) {
		t.Parallel()

		client := newTestECRClient(t, newTestHandler(t))
		ctx := t.Context()

		_, err := client.PutAccountSetting(ctx, &ecrsdk.PutAccountSettingInput{
			Name:  aws.String("BASIC_SCAN_TYPE_VERSION"),
			Value: aws.String("CLAIR"),
		})
		require.NoError(t, err)

		getOut, err := client.GetAccountSetting(ctx, &ecrsdk.GetAccountSettingInput{
			Name: aws.String("BASIC_SCAN_TYPE_VERSION"),
		})
		require.NoError(t, err)
		assert.Equal(t, "CLAIR", aws.ToString(getOut.Value))

		principalArn := "arn:aws:iam::000000000000:role/s11-role"
		_, err = client.RegisterPullTimeUpdateExclusion(ctx, &ecrsdk.RegisterPullTimeUpdateExclusionInput{
			PrincipalArn: aws.String(principalArn),
		})
		require.NoError(t, err)

		listOut, err := client.ListPullTimeUpdateExclusions(ctx, &ecrsdk.ListPullTimeUpdateExclusionsInput{})
		require.NoError(t, err)
		assert.Contains(t, listOut.PullTimeUpdateExclusions, principalArn)

		_, err = client.DeregisterPullTimeUpdateExclusion(ctx, &ecrsdk.DeregisterPullTimeUpdateExclusionInput{
			PrincipalArn: aws.String(principalArn),
		})
		require.NoError(t, err)

		afterOut, err := client.ListPullTimeUpdateExclusions(ctx, &ecrsdk.ListPullTimeUpdateExclusionsInput{})
		require.NoError(t, err)
		assert.NotContains(t, afterOut.PullTimeUpdateExclusions, principalArn)
	})

	t.Run("registry policy", func(t *testing.T) {
		t.Parallel()

		client := newTestECRClient(t, newTestHandler(t))
		ctx := t.Context()

		policy := `{"Version":"2012-10-17","Statement":[]}`
		_, err := client.PutRegistryPolicy(ctx, &ecrsdk.PutRegistryPolicyInput{
			PolicyText: aws.String(policy),
		})
		require.NoError(t, err)

		getOut, err := client.GetRegistryPolicy(ctx, &ecrsdk.GetRegistryPolicyInput{})
		require.NoError(t, err)
		assert.JSONEq(t, policy, aws.ToString(getOut.PolicyText))

		_, err = client.DeleteRegistryPolicy(ctx, &ecrsdk.DeleteRegistryPolicyInput{})
		require.NoError(t, err)

		_, err = client.GetRegistryPolicy(ctx, &ecrsdk.GetRegistryPolicyInput{})
		require.Error(t, err, "GetRegistryPolicy after delete must fail")
	})

	t.Run("repository policy", func(t *testing.T) {
		t.Parallel()

		client := newTestECRClient(t, newTestHandler(t))
		ctx := t.Context()

		_, err := client.CreateRepository(ctx, &ecrsdk.CreateRepositoryInput{
			RepositoryName: aws.String("s11-policy-repo"),
		})
		require.NoError(t, err)

		policy := `{"Version":"2012-10-17","Statement":[]}`
		_, err = client.SetRepositoryPolicy(ctx, &ecrsdk.SetRepositoryPolicyInput{
			RepositoryName: aws.String("s11-policy-repo"),
			PolicyText:     aws.String(policy),
		})
		require.NoError(t, err)

		getOut, err := client.GetRepositoryPolicy(ctx, &ecrsdk.GetRepositoryPolicyInput{
			RepositoryName: aws.String("s11-policy-repo"),
		})
		require.NoError(t, err)
		assert.JSONEq(t, policy, aws.ToString(getOut.PolicyText))

		_, err = client.DeleteRepositoryPolicy(ctx, &ecrsdk.DeleteRepositoryPolicyInput{
			RepositoryName: aws.String("s11-policy-repo"),
		})
		require.NoError(t, err)

		_, err = client.GetRepositoryPolicy(ctx, &ecrsdk.GetRepositoryPolicyInput{
			RepositoryName: aws.String("s11-policy-repo"),
		})
		require.Error(t, err, "GetRepositoryPolicy after delete must fail")
	})

	t.Run("lifecycle policy", func(t *testing.T) {
		t.Parallel()

		client := newTestECRClient(t, newTestHandler(t))
		ctx := t.Context()

		_, err := client.CreateRepository(ctx, &ecrsdk.CreateRepositoryInput{
			RepositoryName: aws.String("s11-lifecycle-repo"),
		})
		require.NoError(t, err)

		policyText := `{"rules":[{"rulePriority":1,"selection":{"tagStatus":"untagged",` +
			`"countType":"imageCountMoreThan","countNumber":1},"action":{"type":"expire"}}]}`

		startOut, err := client.StartLifecyclePolicyPreview(ctx, &ecrsdk.StartLifecyclePolicyPreviewInput{
			RepositoryName:      aws.String("s11-lifecycle-repo"),
			LifecyclePolicyText: aws.String(policyText),
		})
		require.NoError(t, err)
		assert.Equal(t, "s11-lifecycle-repo", aws.ToString(startOut.RepositoryName))

		previewOut, err := client.GetLifecyclePolicyPreview(ctx, &ecrsdk.GetLifecyclePolicyPreviewInput{
			RepositoryName: aws.String("s11-lifecycle-repo"),
		})
		require.NoError(t, err)
		assert.Equal(t, ecrtypes.LifecyclePolicyPreviewStatusComplete, previewOut.Status)

		_, err = client.PutLifecyclePolicy(ctx, &ecrsdk.PutLifecyclePolicyInput{
			RepositoryName:      aws.String("s11-lifecycle-repo"),
			LifecyclePolicyText: aws.String(policyText),
		})
		require.NoError(t, err)

		getOut, err := client.GetLifecyclePolicy(ctx, &ecrsdk.GetLifecyclePolicyInput{
			RepositoryName: aws.String("s11-lifecycle-repo"),
		})
		require.NoError(t, err)
		assert.JSONEq(t, policyText, aws.ToString(getOut.LifecyclePolicyText))

		_, err = client.DeleteLifecyclePolicy(ctx, &ecrsdk.DeleteLifecyclePolicyInput{
			RepositoryName: aws.String("s11-lifecycle-repo"),
		})
		require.NoError(t, err)

		_, err = client.GetLifecyclePolicy(ctx, &ecrsdk.GetLifecyclePolicyInput{
			RepositoryName: aws.String("s11-lifecycle-repo"),
		})
		require.Error(t, err, "GetLifecyclePolicy after delete must fail")
	})

	t.Run("pull through cache rule", func(t *testing.T) {
		t.Parallel()

		client := newTestECRClient(t, newTestHandler(t))
		ctx := t.Context()

		_, err := client.CreatePullThroughCacheRule(ctx, &ecrsdk.CreatePullThroughCacheRuleInput{
			EcrRepositoryPrefix: aws.String("s11-ptc"),
			UpstreamRegistryUrl: aws.String("public.ecr.aws"),
		})
		require.NoError(t, err)

		validOut, err := client.ValidatePullThroughCacheRule(ctx, &ecrsdk.ValidatePullThroughCacheRuleInput{
			EcrRepositoryPrefix: aws.String("s11-ptc"),
		})
		require.NoError(t, err)
		assert.True(t, validOut.IsValid)

		_, err = client.UpdatePullThroughCacheRule(ctx, &ecrsdk.UpdatePullThroughCacheRuleInput{
			EcrRepositoryPrefix: aws.String("s11-ptc"),
			CredentialArn:       aws.String("arn:aws:secretsmanager:us-east-1:000000000000:secret:s11-cred"),
		})
		require.NoError(t, err)

		descOut, err := client.DescribePullThroughCacheRules(ctx, &ecrsdk.DescribePullThroughCacheRulesInput{
			EcrRepositoryPrefixes: []string{"s11-ptc"},
		})
		require.NoError(t, err)
		require.Len(t, descOut.PullThroughCacheRules, 1)
		assert.Equal(t, "arn:aws:secretsmanager:us-east-1:000000000000:secret:s11-cred",
			aws.ToString(descOut.PullThroughCacheRules[0].CredentialArn))

		_, err = client.DeletePullThroughCacheRule(ctx, &ecrsdk.DeletePullThroughCacheRuleInput{
			EcrRepositoryPrefix: aws.String("s11-ptc"),
		})
		require.NoError(t, err)

		_, err = client.DescribePullThroughCacheRules(ctx, &ecrsdk.DescribePullThroughCacheRulesInput{
			EcrRepositoryPrefixes: []string{"s11-ptc"},
		})
		require.Error(t, err, "DescribePullThroughCacheRules after delete must fail for an explicit prefix")
	})

	t.Run("repository creation template", func(t *testing.T) {
		t.Parallel()

		client := newTestECRClient(t, newTestHandler(t))
		ctx := t.Context()

		_, err := client.CreateRepositoryCreationTemplate(ctx, &ecrsdk.CreateRepositoryCreationTemplateInput{
			Prefix:      aws.String("s11-template"),
			Description: aws.String("initial"),
			AppliedFor:  []ecrtypes.RCTAppliedFor{ecrtypes.RCTAppliedForPullThroughCache},
		})
		require.NoError(t, err)

		updOut, err := client.UpdateRepositoryCreationTemplate(ctx, &ecrsdk.UpdateRepositoryCreationTemplateInput{
			Prefix:      aws.String("s11-template"),
			Description: aws.String("updated"),
		})
		require.NoError(t, err)
		assert.Equal(t, "updated", aws.ToString(updOut.RepositoryCreationTemplate.Description))

		_, err = client.DeleteRepositoryCreationTemplate(ctx, &ecrsdk.DeleteRepositoryCreationTemplateInput{
			Prefix: aws.String("s11-template"),
		})
		require.NoError(t, err)

		descOut, err := client.DescribeRepositoryCreationTemplates(
			ctx, &ecrsdk.DescribeRepositoryCreationTemplatesInput{
				Prefixes: []string{"s11-template"},
			},
		)
		require.NoError(t, err)
		assert.Empty(t, descOut.RepositoryCreationTemplates)
	})

	t.Run("layer upload lifecycle", func(t *testing.T) {
		t.Parallel()

		client := newTestECRClient(t, newTestHandler(t))
		ctx := t.Context()

		_, err := client.CreateRepository(ctx, &ecrsdk.CreateRepositoryInput{
			RepositoryName: aws.String("s11-layer-repo"),
		})
		require.NoError(t, err)

		initOut, err := client.InitiateLayerUpload(ctx, &ecrsdk.InitiateLayerUploadInput{
			RepositoryName: aws.String("s11-layer-repo"),
		})
		require.NoError(t, err)
		uploadID := aws.ToString(initOut.UploadId)
		require.NotEmpty(t, uploadID)
		require.Positive(t, aws.ToInt64(initOut.PartSize))

		blob := make([]byte, 5*1024*1024)
		for i := range blob {
			blob[i] = byte(i % 251)
		}

		partOut, err := client.UploadLayerPart(ctx, &ecrsdk.UploadLayerPartInput{
			RepositoryName: aws.String("s11-layer-repo"),
			UploadId:       aws.String(uploadID),
			PartFirstByte:  aws.Int64(0),
			PartLastByte:   aws.Int64(int64(len(blob) - 1)),
			LayerPartBlob:  blob,
		})
		require.NoError(t, err)
		assert.Equal(t, uploadID, aws.ToString(partOut.UploadId))

		sum := sha256.Sum256(blob)
		digest := "sha256:" + hex.EncodeToString(sum[:])

		completeOut, err := client.CompleteLayerUpload(ctx, &ecrsdk.CompleteLayerUploadInput{
			RepositoryName: aws.String("s11-layer-repo"),
			UploadId:       aws.String(uploadID),
			LayerDigests:   []string{digest},
		})
		require.NoError(t, err)
		assert.Equal(t, digest, aws.ToString(completeOut.LayerDigest))

		availOut, err := client.BatchCheckLayerAvailability(ctx, &ecrsdk.BatchCheckLayerAvailabilityInput{
			RepositoryName: aws.String("s11-layer-repo"),
			LayerDigests:   []string{digest},
		})
		require.NoError(t, err)
		require.Len(t, availOut.Layers, 1)
		assert.Equal(t, ecrtypes.LayerAvailabilityAvailable, availOut.Layers[0].LayerAvailability)

		urlOut, err := client.GetDownloadUrlForLayer(ctx, &ecrsdk.GetDownloadUrlForLayerInput{
			RepositoryName: aws.String("s11-layer-repo"),
			LayerDigest:    aws.String(digest),
		})
		require.NoError(t, err)
		assert.NotEmpty(t, aws.ToString(urlOut.DownloadUrl))
	})

	t.Run("batch get image and list referrers", func(t *testing.T) {
		t.Parallel()

		client := newTestECRClient(t, newTestHandler(t))
		ctx := t.Context()

		_, err := client.CreateRepository(ctx, &ecrsdk.CreateRepositoryInput{
			RepositoryName: aws.String("s11-image-repo"),
		})
		require.NoError(t, err)

		manifest := `{"schemaVersion":2,"mediaType":"application/vnd.docker.distribution.manifest.v2+json"}`
		putOut, err := client.PutImage(ctx, &ecrsdk.PutImageInput{
			RepositoryName: aws.String("s11-image-repo"),
			ImageTag:       aws.String("v1"),
			ImageManifest:  aws.String(manifest),
		})
		require.NoError(t, err)
		digest := aws.ToString(putOut.Image.ImageId.ImageDigest)

		batchOut, err := client.BatchGetImage(ctx, &ecrsdk.BatchGetImageInput{
			RepositoryName: aws.String("s11-image-repo"),
			ImageIds:       []ecrtypes.ImageIdentifier{{ImageTag: aws.String("v1")}},
		})
		require.NoError(t, err)
		require.Len(t, batchOut.Images, 1)
		assert.Equal(t, manifest, aws.ToString(batchOut.Images[0].ImageManifest))
		assert.Empty(t, batchOut.Failures)

		referrersOut, err := client.ListImageReferrers(ctx, &ecrsdk.ListImageReferrersInput{
			RepositoryName: aws.String("s11-image-repo"),
			SubjectId:      &ecrtypes.SubjectIdentifier{ImageDigest: aws.String(digest)},
		})
		require.NoError(t, err)
		// gopherstack doesn't model OCI referrer relationships (disclosed
		// gap): the honest response is an empty list, not fabricated
		// referrer artifacts.
		assert.Empty(t, referrersOut.Referrers)
	})
}
