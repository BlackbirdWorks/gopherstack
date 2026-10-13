package iot_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	iotsdk "github.com/aws/aws-sdk-go-v2/service/iot"
	"github.com/aws/aws-sdk-go-v2/service/iot/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iot"
)

func TestAttachOps_RequireExistingResources_RealClient(t *testing.T) {
	t.Parallel()

	const certArn = "arn:aws:iot:us-east-1:000000000000:cert/abc"

	tests := []struct {
		run     func(c *iotsdk.Client) error
		name    string
		seed    string
		wantErr bool
	}{
		{
			name: "attach_policy_unknown_policy", wantErr: true,
			run: func(c *iotsdk.Client) error {
				_, err := c.AttachPolicy(
					t.Context(),
					&iotsdk.AttachPolicyInput{PolicyName: aws.String("nope"), Target: aws.String(certArn)},
				)

				return err
			},
		},
		{
			name: "attach_policy_known_policy", seed: "policy",
			run: func(c *iotsdk.Client) error {
				_, err := c.AttachPolicy(
					t.Context(),
					&iotsdk.AttachPolicyInput{PolicyName: aws.String("p"), Target: aws.String(certArn)},
				)

				return err
			},
		},
		{
			name: "attach_principal_unknown_thing", wantErr: true,
			run: func(c *iotsdk.Client) error {
				_, err := c.AttachThingPrincipal(
					t.Context(),
					&iotsdk.AttachThingPrincipalInput{ThingName: aws.String("nope"), Principal: aws.String(certArn)},
				)

				return err
			},
		},
		{
			name: "attach_principal_known_thing", seed: "thing",
			run: func(c *iotsdk.Client) error {
				_, err := c.AttachThingPrincipal(
					t.Context(),
					&iotsdk.AttachThingPrincipalInput{ThingName: aws.String("t"), Principal: aws.String(certArn)},
				)

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newTestIoTClient(t, iot.NewHandler(iot.NewInMemoryBackend(), nil))

			switch tt.seed {
			case "policy":
				_, err := c.CreatePolicy(t.Context(), &iotsdk.CreatePolicyInput{
					PolicyName: aws.String("p"), PolicyDocument: aws.String(`{"Version":"2012-10-17","Statement":[]}`),
				})
				require.NoError(t, err)
			case "thing":
				_, err := c.CreateThing(t.Context(), &iotsdk.CreateThingInput{ThingName: aws.String("t")})
				require.NoError(t, err)
			}

			err := tt.run(c)
			if !tt.wantErr {
				require.NoError(t, err)

				return
			}

			var nf *types.ResourceNotFoundException
			require.ErrorAs(t, err, &nf)
		})
	}
}
