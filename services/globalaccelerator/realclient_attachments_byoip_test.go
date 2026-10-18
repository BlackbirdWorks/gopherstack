package globalaccelerator_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	gasdk "github.com/aws/aws-sdk-go-v2/service/globalaccelerator"
	gatypes "github.com/aws/aws-sdk-go-v2/service/globalaccelerator/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
)

func TestRealClient_CrossAccountAttachments(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	ctx := t.Context()
	alb := "arn:aws:elasticloadbalancing:us-west-2:111122223333:loadbalancer/app/shared/abc"

	created, err := f.client.CreateCrossAccountAttachment(ctx, &gasdk.CreateCrossAccountAttachmentInput{
		Name:       aws.String("share"),
		Principals: []string{awsmeta.DefaultAccount, "444455556666"},
		Resources:  []gatypes.Resource{{EndpointId: aws.String(alb), Region: aws.String("us-west-2")}},
		Tags:       []gatypes.Tag{{Key: aws.String("t"), Value: aws.String("v")}},
	})
	require.NoError(t, err)

	att := created.CrossAccountAttachment
	attARN := att.AttachmentArn
	assert.Regexp(t, `^arn:aws:globalaccelerator::123456789012:attachment/[0-9a-f-]{36}$`, aws.ToString(attARN))
	assert.Len(t, att.Principals, 2)
	require.Len(t, att.Resources, 1)
	assert.Equal(t, alb, aws.ToString(att.Resources[0].EndpointId))

	got, err := f.client.DescribeCrossAccountAttachment(
		ctx,
		&gasdk.DescribeCrossAccountAttachmentInput{AttachmentArn: attARN},
	)
	require.NoError(t, err)
	assert.Equal(t, "share", aws.ToString(got.CrossAccountAttachment.Name))

	f.clock.advance(time.Hour)

	upd, err := f.client.UpdateCrossAccountAttachment(ctx, &gasdk.UpdateCrossAccountAttachmentInput{
		AttachmentArn: attARN, Name: aws.String("share2"),
		AddPrincipals: []string{"777788889999"}, RemovePrincipals: []string{"444455556666"},
		AddResources:    []gatypes.Resource{{EndpointId: aws.String(alb + "2")}},
		RemoveResources: []gatypes.Resource{{EndpointId: aws.String(alb)}},
	})
	require.NoError(t, err)
	assert.Equal(t, "share2", aws.ToString(upd.CrossAccountAttachment.Name))
	assert.ElementsMatch(t, []string{awsmeta.DefaultAccount, "777788889999"}, upd.CrossAccountAttachment.Principals)
	require.Len(t, upd.CrossAccountAttachment.Resources, 1)
	assert.Equal(t, alb+"2", aws.ToString(upd.CrossAccountAttachment.Resources[0].EndpointId))
	assert.Greater(t, upd.CrossAccountAttachment.LastModifiedTime.Unix(), upd.CrossAccountAttachment.CreatedTime.Unix())

	list, err := f.client.ListCrossAccountAttachments(ctx, &gasdk.ListCrossAccountAttachmentsInput{})
	require.NoError(t, err)
	require.Len(t, list.CrossAccountAttachments, 1)

	accounts, err := f.client.ListCrossAccountResourceAccounts(ctx, &gasdk.ListCrossAccountResourceAccountsInput{})
	require.NoError(t, err)
	assert.Equal(t, []string{awsmeta.DefaultAccount}, accounts.ResourceOwnerAwsAccountIds)

	res, err := f.client.ListCrossAccountResources(ctx, &gasdk.ListCrossAccountResourcesInput{
		ResourceOwnerAwsAccountId: aws.String(awsmeta.DefaultAccount),
	})
	require.NoError(t, err)
	require.Len(t, res.CrossAccountResources, 1)
	assert.Equal(t, aws.ToString(attARN), aws.ToString(res.CrossAccountResources[0].AttachmentArn))

	other, err := f.client.ListCrossAccountResources(ctx, &gasdk.ListCrossAccountResourcesInput{
		ResourceOwnerAwsAccountId: aws.String("999999999999"),
	})
	require.NoError(t, err)
	assert.Empty(t, other.CrossAccountResources)

	_, err = f.client.ListCrossAccountResources(ctx, &gasdk.ListCrossAccountResourcesInput{
		ResourceOwnerAwsAccountId: aws.String(awsmeta.DefaultAccount),
		AcceleratorArn:            aws.String("arn:aws:globalaccelerator::123456789012:accelerator/none"),
	})
	assert.Equal(t, "AcceleratorNotFoundException", apiCode(err))

	tags, err := f.client.ListTagsForResource(ctx, &gasdk.ListTagsForResourceInput{ResourceArn: attARN})
	require.NoError(t, err)
	require.Len(t, tags.Tags, 1)

	t.Run("in_use_attachment_cannot_be_deleted", func(t *testing.T) {
		t.Parallel()

		accARN := newStandardAccelerator(t, f)
		listenerARN := newListener(t, f, accARN)

		_, gerr := f.client.CreateEndpointGroup(ctx, &gasdk.CreateEndpointGroupInput{
			ListenerArn:         listenerARN,
			EndpointGroupRegion: aws.String("us-west-2"),
			EndpointConfigurations: []gatypes.EndpointConfiguration{
				{EndpointId: aws.String(alb + "2"), AttachmentArn: attARN},
			},
		})
		require.NoError(t, gerr)

		_, derr := f.client.DeleteCrossAccountAttachment(
			ctx,
			&gasdk.DeleteCrossAccountAttachmentInput{AttachmentArn: attARN},
		)
		assert.Equal(t, "ConflictException", apiCode(derr))

		used, lerr := f.client.ListCrossAccountResources(ctx, &gasdk.ListCrossAccountResourcesInput{
			ResourceOwnerAwsAccountId: aws.String(awsmeta.DefaultAccount), AcceleratorArn: accARN,
		})
		require.NoError(t, lerr)
		assert.Len(t, used.CrossAccountResources, 1)
	})
}

func TestRealClient_CrossAccountAttachmentErrors(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	ctx := t.Context()

	cases := []struct {
		call func() error
		name string
		code string
	}{
		{func() error {
			_, err := f.client.DescribeCrossAccountAttachment(ctx, &gasdk.DescribeCrossAccountAttachmentInput{
				AttachmentArn: aws.String("arn:aws:globalaccelerator::123456789012:attachment/none"),
			})

			return err
		}, "describe_missing", "AttachmentNotFoundException"},
		{func() error {
			_, err := f.client.DeleteCrossAccountAttachment(ctx, &gasdk.DeleteCrossAccountAttachmentInput{
				AttachmentArn: aws.String("arn:aws:globalaccelerator::123456789012:attachment/none"),
			})

			return err
		}, "delete_missing", "AttachmentNotFoundException"},
		{func() error {
			_, err := f.client.CreateCrossAccountAttachment(ctx, &gasdk.CreateCrossAccountAttachmentInput{
				Name: aws.String("x"), Principals: []string{"not-an-account"},
			})

			return err
		}, "bad_principal", "InvalidArgumentException"},
		{func() error {
			_, err := f.client.CreateCrossAccountAttachment(ctx, &gasdk.CreateCrossAccountAttachmentInput{
				Name: aws.String("x"), Resources: []gatypes.Resource{{EndpointId: aws.String("i-123")}},
			})

			return err
		}, "endpoint_must_be_arn", "InvalidArgumentException"},
		{func() error {
			_, err := f.client.CreateCrossAccountAttachment(ctx, &gasdk.CreateCrossAccountAttachmentInput{
				Name: aws.String("x"), Resources: []gatypes.Resource{{Cidr: aws.String("198.51.100.0/24")}},
			})

			return err
		}, "cidr_not_provisioned", "ByoipCidrNotFoundException"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.code, apiCode(tc.call()))
		})
	}
}

func TestRealClient_Byoip(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	ctx := t.Context()
	cidr := aws.String("198.51.100.0/24")
	authz := &gatypes.CidrAuthorizationContext{Message: aws.String("msg"), Signature: aws.String("sig")}

	state := func() gatypes.ByoipCidrState {
		out, err := f.client.ListByoipCidrs(ctx, &gasdk.ListByoipCidrsInput{})
		require.NoError(t, err)
		require.Len(t, out.ByoipCidrs, 1)

		return out.ByoipCidrs[0].State
	}

	_, err := f.client.AdvertiseByoipCidr(ctx, &gasdk.AdvertiseByoipCidrInput{Cidr: cidr})
	assert.Equal(t, "ByoipCidrNotFoundException", apiCode(err))

	_, err = f.client.ProvisionByoipCidr(
		ctx,
		&gasdk.ProvisionByoipCidrInput{Cidr: aws.String("198.51.100.0/25"), CidrAuthorizationContext: authz},
	)
	assert.Equal(t, "InvalidArgumentException", apiCode(err), "IPv4 ranges are /24 or larger")

	_, err = f.client.ProvisionByoipCidr(ctx, &gasdk.ProvisionByoipCidrInput{
		Cidr: cidr,
		CidrAuthorizationContext: &gatypes.CidrAuthorizationContext{
			Message:   aws.String(""),
			Signature: aws.String("sig"),
		},
	})
	assert.Equal(t, "InvalidArgumentException", apiCode(err), "authorization message must not be empty")

	prov, err := f.client.ProvisionByoipCidr(
		ctx,
		&gasdk.ProvisionByoipCidrInput{Cidr: cidr, CidrAuthorizationContext: authz},
	)
	require.NoError(t, err)
	assert.Equal(t, gatypes.ByoipCidrStatePendingProvisioning, prov.ByoipCidr.State)
	require.Len(t, prov.ByoipCidr.Events, 1)

	_, err = f.client.ProvisionByoipCidr(
		ctx,
		&gasdk.ProvisionByoipCidrInput{Cidr: cidr, CidrAuthorizationContext: authz},
	)
	assert.Equal(t, "ConflictException", apiCode(err))

	_, err = f.client.AdvertiseByoipCidr(ctx, &gasdk.AdvertiseByoipCidrInput{Cidr: cidr})
	assert.Equal(t, "IncorrectCidrStateException", apiCode(err), "still provisioning")

	f.clock.advance(time.Minute)
	assert.Equal(t, gatypes.ByoipCidrStateReady, state())

	_, err = f.client.WithdrawByoipCidr(ctx, &gasdk.WithdrawByoipCidrInput{Cidr: cidr})
	assert.Equal(t, "IncorrectCidrStateException", apiCode(err), "not advertising")

	adv, err := f.client.AdvertiseByoipCidr(ctx, &gasdk.AdvertiseByoipCidrInput{Cidr: cidr})
	require.NoError(t, err)
	assert.Equal(t, gatypes.ByoipCidrStatePendingAdvertising, adv.ByoipCidr.State)

	f.clock.advance(time.Minute)
	assert.Equal(t, gatypes.ByoipCidrStateAdvertising, state())

	acc, err := f.client.CreateAccelerator(ctx, &gasdk.CreateAcceleratorInput{
		Name: aws.String("byoip"), IpAddresses: []string{"198.51.100.10"},
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"198.51.100.10", "75.2.0.1"}, acc.Accelerator.IpSets[0].IpAddresses)

	_, err = f.client.CreateAccelerator(ctx, &gasdk.CreateAcceleratorInput{
		Name: aws.String("dup"), IpAddresses: []string{"198.51.100.10"},
	})
	assert.Equal(t, "InvalidArgumentException", apiCode(err), "address already assigned")

	wd, err := f.client.WithdrawByoipCidr(ctx, &gasdk.WithdrawByoipCidrInput{Cidr: cidr})
	require.NoError(t, err)
	assert.Equal(t, gatypes.ByoipCidrStatePendingWithdrawing, wd.ByoipCidr.State)

	f.clock.advance(time.Minute)
	assert.Equal(t, gatypes.ByoipCidrStateReady, state())

	_, err = f.client.DeprovisionByoipCidr(ctx, &gasdk.DeprovisionByoipCidrInput{Cidr: cidr})
	assert.Equal(t, "IncorrectCidrStateException", apiCode(err), "addresses are assigned to an accelerator")

	_, err = f.client.UpdateAccelerator(
		ctx,
		&gasdk.UpdateAcceleratorInput{AcceleratorArn: acc.Accelerator.AcceleratorArn, Enabled: aws.Bool(false)},
	)
	require.NoError(t, err)
	_, err = f.client.DeleteAccelerator(
		ctx,
		&gasdk.DeleteAcceleratorInput{AcceleratorArn: acc.Accelerator.AcceleratorArn},
	)
	require.NoError(t, err)

	dep, err := f.client.DeprovisionByoipCidr(ctx, &gasdk.DeprovisionByoipCidrInput{Cidr: cidr})
	require.NoError(t, err)
	assert.Equal(t, gatypes.ByoipCidrStatePendingDeprovisioning, dep.ByoipCidr.State)

	f.clock.advance(time.Minute)
	assert.Equal(t, gatypes.ByoipCidrStateDeprovisioned, state())

	again, err := f.client.ProvisionByoipCidr(
		ctx,
		&gasdk.ProvisionByoipCidrInput{Cidr: cidr, CidrAuthorizationContext: authz},
	)
	require.NoError(t, err)
	assert.Equal(t, gatypes.ByoipCidrStatePendingProvisioning, again.ByoipCidr.State)
	assert.Len(t, again.ByoipCidr.Events, 5, "history accumulates across re-provisioning")
}
