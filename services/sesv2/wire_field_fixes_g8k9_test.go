package sesv2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sesv2sdk "github.com/aws/aws-sdk-go-v2/service/sesv2"
	sesv2types "github.com/aws/aws-sdk-go-v2/service/sesv2/types"
	"github.com/stretchr/testify/require"
)

// These tests prove that sesv2's central resourceTags side map (written by
// TagResource/UntagResource) and each resource's own creation-time Tags
// snapshot are two independently-written stores -- Get ops that echoed the
// snapshot directly went stale as soon as TagResource was called after
// creation, exactly the shape fixed for ecs's DescribeTasks/StopTask/
// UpdateCapacityProvider (see gopherstack-g8k9's ecs session, commit
// 9a40453a2). sesv2 already carried a comment in tags.go documenting a prior
// fix of this exact shape for CreateTenant/CreateEmailIdentity's *creation*
// path ("storing tags only on the resource's own domain struct... left
// ListTagsForResource... blind to creation-time tags") -- but the read side
// (Get*) was never fixed to stop reading that same domain snapshot.

func tagMapFromSDKTags(tags []sesv2types.Tag) map[string]string {
	out := make(map[string]string, len(tags))
	for _, tag := range tags {
		out[aws.ToString(tag.Key)] = aws.ToString(tag.Value)
	}

	return out
}

func TestGetEmailIdentity_TagResource_LiveSync_RealClient(t *testing.T) {
	t.Parallel()

	h, _ := newSESv2TestHandler(t)
	client := newSESv2SDKClient(t, h)
	ctx := t.Context()

	_, err := client.CreateEmailIdentity(ctx, &sesv2sdk.CreateEmailIdentityInput{
		EmailIdentity: aws.String("livesync@example.com"),
		Tags:          []sesv2types.Tag{{Key: aws.String("owner"), Value: aws.String("sre")}},
	})
	require.NoError(t, err)

	_, err = client.TagResource(ctx, &sesv2sdk.TagResourceInput{
		ResourceArn: aws.String("arn:aws:ses:us-east-1:000000000000:identity/livesync@example.com"),
		Tags:        []sesv2types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
	})
	require.NoError(t, err)

	out, err := client.GetEmailIdentity(ctx, &sesv2sdk.GetEmailIdentityInput{
		EmailIdentity: aws.String("livesync@example.com"),
	})
	require.NoError(t, err)
	require.Equal(t, map[string]string{"owner": "sre", "env": "prod"}, tagMapFromSDKTags(out.Tags))

	_, err = client.UntagResource(ctx, &sesv2sdk.UntagResourceInput{
		ResourceArn: aws.String("arn:aws:ses:us-east-1:000000000000:identity/livesync@example.com"),
		TagKeys:     []string{"owner"},
	})
	require.NoError(t, err)

	out, err = client.GetEmailIdentity(ctx, &sesv2sdk.GetEmailIdentityInput{
		EmailIdentity: aws.String("livesync@example.com"),
	})
	require.NoError(t, err)
	require.Equal(t, map[string]string{"env": "prod"}, tagMapFromSDKTags(out.Tags))
}

func TestGetContactList_TagResource_LiveSync_RealClient(t *testing.T) {
	t.Parallel()

	h, _ := newSESv2TestHandler(t)
	client := newSESv2SDKClient(t, h)
	ctx := t.Context()

	_, err := client.CreateContactList(ctx, &sesv2sdk.CreateContactListInput{
		ContactListName: aws.String("livesync-list"),
		Tags:            []sesv2types.Tag{{Key: aws.String("owner"), Value: aws.String("sre")}},
	})
	require.NoError(t, err)

	_, err = client.TagResource(ctx, &sesv2sdk.TagResourceInput{
		ResourceArn: aws.String("arn:aws:ses:us-east-1:000000000000:contact-list/livesync-list"),
		Tags:        []sesv2types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
	})
	require.NoError(t, err)

	out, err := client.GetContactList(ctx, &sesv2sdk.GetContactListInput{
		ContactListName: aws.String("livesync-list"),
	})
	require.NoError(t, err)
	require.Equal(t, map[string]string{"owner": "sre", "env": "prod"}, tagMapFromSDKTags(out.Tags))
}

func TestGetEmailTemplate_TagResource_LiveSync_RealClient(t *testing.T) {
	t.Parallel()

	h, _ := newSESv2TestHandler(t)
	client := newSESv2SDKClient(t, h)
	ctx := t.Context()

	_, err := client.CreateEmailTemplate(ctx, &sesv2sdk.CreateEmailTemplateInput{
		TemplateName: aws.String("livesync-template"),
		TemplateContent: &sesv2types.EmailTemplateContent{
			Subject: aws.String("hello"),
			Text:    aws.String("hi"),
		},
		Tags: []sesv2types.Tag{{Key: aws.String("owner"), Value: aws.String("sre")}},
	})
	require.NoError(t, err)

	_, err = client.TagResource(ctx, &sesv2sdk.TagResourceInput{
		ResourceArn: aws.String("arn:aws:ses:us-east-1:000000000000:template/livesync-template"),
		Tags:        []sesv2types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
	})
	require.NoError(t, err)

	out, err := client.GetEmailTemplate(ctx, &sesv2sdk.GetEmailTemplateInput{
		TemplateName: aws.String("livesync-template"),
	})
	require.NoError(t, err)
	require.Equal(t, map[string]string{"owner": "sre", "env": "prod"}, tagMapFromSDKTags(out.Tags))
}

func TestGetConfigurationSet_TagResource_LiveSync_RealClient(t *testing.T) {
	t.Parallel()

	h, _ := newSESv2TestHandler(t)
	client := newSESv2SDKClient(t, h)
	ctx := t.Context()

	_, err := client.CreateConfigurationSet(ctx, &sesv2sdk.CreateConfigurationSetInput{
		ConfigurationSetName: aws.String("livesync-cs"),
		Tags:                 []sesv2types.Tag{{Key: aws.String("owner"), Value: aws.String("sre")}},
	})
	require.NoError(t, err)

	_, err = client.TagResource(ctx, &sesv2sdk.TagResourceInput{
		ResourceArn: aws.String("arn:aws:ses:us-east-1:000000000000:configuration-set/livesync-cs"),
		Tags:        []sesv2types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
	})
	require.NoError(t, err)

	out, err := client.GetConfigurationSet(ctx, &sesv2sdk.GetConfigurationSetInput{
		ConfigurationSetName: aws.String("livesync-cs"),
	})
	require.NoError(t, err)
	require.Equal(t, map[string]string{"owner": "sre", "env": "prod"}, tagMapFromSDKTags(out.Tags))
}

func TestGetTenant_TagResource_LiveSync_RealClient(t *testing.T) {
	t.Parallel()

	h, _ := newSESv2TestHandler(t)
	client := newSESv2SDKClient(t, h)
	ctx := t.Context()

	createOut, err := client.CreateTenant(ctx, &sesv2sdk.CreateTenantInput{
		TenantName: aws.String("livesync-tenant"),
		Tags:       []sesv2types.Tag{{Key: aws.String("owner"), Value: aws.String("sre")}},
	})
	require.NoError(t, err)

	_, err = client.TagResource(ctx, &sesv2sdk.TagResourceInput{
		ResourceArn: createOut.TenantArn,
		Tags:        []sesv2types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
	})
	require.NoError(t, err)

	out, err := client.GetTenant(ctx, &sesv2sdk.GetTenantInput{
		TenantName: aws.String("livesync-tenant"),
	})
	require.NoError(t, err)
	require.Equal(t, map[string]string{"owner": "sre", "env": "prod"}, tagMapFromSDKTags(out.Tenant.Tags))
}
