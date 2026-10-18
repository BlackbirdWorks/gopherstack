package app

import (
	"encoding/json"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudcontrol"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cfnbackend "github.com/blackbirdworks/gopherstack/services/cloudformation"
)

// A CloudFormation-provisionable type with no dedicated Cloud Control handler runs
// through the stack engine's creators and acts on the real IAM backend.
func TestCloudControlProvisionedType_Lifecycle(t *testing.T) {
	t.Parallel()

	fx := newSFNFixture(t)
	cc := cloudcontrol.NewFromConfig(fx.cfg)
	iamc := iam.NewFromConfig(fx.cfg)
	const typeName = "AWS::IAM::User"

	created, err := cc.CreateResource(t.Context(), &cloudcontrol.CreateResourceInput{
		TypeName: aws.String(typeName), DesiredState: aws.String(`{"UserName":"cc-prov-user","Path":"/"}`),
	})
	require.NoError(t, err)

	id := aws.ToString(created.ProgressEvent.Identifier)
	require.NotEmpty(t, id)

	user, err := iamc.GetUser(t.Context(), &iam.GetUserInput{UserName: aws.String("cc-prov-user")})
	require.NoError(t, err, "the IAM backend must hold the user Cloud Control created")
	assert.Equal(t, "cc-prov-user", aws.ToString(user.User.UserName))

	got, err := cc.GetResource(t.Context(), &cloudcontrol.GetResourceInput{
		TypeName: aws.String(typeName), Identifier: aws.String(id),
	})
	require.NoError(t, err)

	var props map[string]any
	require.NoError(t, json.Unmarshal([]byte(aws.ToString(got.ResourceDescription.Properties)), &props))
	assert.Equal(t, "cc-prov-user", props["UserName"])

	listed, err := cc.ListResources(t.Context(), &cloudcontrol.ListResourcesInput{TypeName: aws.String(typeName)})
	require.NoError(t, err)
	require.Len(t, listed.ResourceDescriptions, 1)

	_, err = cc.UpdateResource(t.Context(), &cloudcontrol.UpdateResourceInput{
		TypeName: aws.String(typeName), Identifier: aws.String(id),
		PatchDocument: aws.String(`[{"op":"replace","path":"/Path","value":"/other/"}]`),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "NotUpdatableException", apiErr.ErrorCode())

	_, err = cc.DeleteResource(t.Context(), &cloudcontrol.DeleteResourceInput{
		TypeName: aws.String(typeName), Identifier: aws.String(id),
	})
	require.NoError(t, err)

	_, err = iamc.GetUser(t.Context(), &iam.GetUserInput{UserName: aws.String("cc-prov-user")})
	require.Error(t, err, "deleting through Cloud Control must delete the IAM user")
}

func TestCloudControlUnprovisionableAWSTypeIsUnsupported(t *testing.T) {
	t.Parallel()

	fx := newSFNFixture(t)
	cc := cloudcontrol.NewFromConfig(fx.cfg)
	typeName := aws.String("AWS::EC2::CapacityReservationFleet")

	tests := []struct {
		call func() error
		name string
	}{
		{name: "create", call: func() error {
			_, err := cc.CreateResource(t.Context(), &cloudcontrol.CreateResourceInput{
				TypeName: typeName, DesiredState: aws.String(`{}`),
			})

			return err
		}},
		{name: "get", call: func() error {
			_, err := cc.GetResource(t.Context(), &cloudcontrol.GetResourceInput{
				TypeName: typeName, Identifier: aws.String("x"),
			})

			return err
		}},
		{name: "list", call: func() error {
			_, err := cc.ListResources(t.Context(), &cloudcontrol.ListResourcesInput{TypeName: typeName})

			return err
		}},
		{name: "delete", call: func() error {
			_, err := cc.DeleteResource(t.Context(), &cloudcontrol.DeleteResourceInput{
				TypeName: typeName, Identifier: aws.String("x"),
			})

			return err
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := tc.call()
			require.Error(t, err)

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, "UnsupportedActionException", apiErr.ErrorCode())
		})
	}
}

// Cloud Control coverage equals CloudFormation coverage: every provisionable type lists.
func TestCloudControlCoversEveryProvisionableType(t *testing.T) {
	t.Parallel()

	fx := newSFNFixture(t)
	cc := cloudcontrol.NewFromConfig(fx.cfg)
	types := cfnbackend.ProvisionableTypes()
	require.Greater(t, len(types), 400)

	for _, typeName := range types {
		_, err := cc.ListResources(t.Context(), &cloudcontrol.ListResourcesInput{TypeName: aws.String(typeName)})
		assert.NoError(t, err, typeName)
	}
}
