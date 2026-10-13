package s3control_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3csdk "github.com/aws/aws-sdk-go-v2/service/s3control"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3control"
)

func TestAccessPoint_RequestValidation(t *testing.T) {
	t.Parallel()

	const acct = "000000000000"

	tests := []struct {
		call     func(*s3csdk.Client) error
		name     string
		wantCode string
	}{
		{
			name: "uppercase name",
			call: func(c *s3csdk.Client) error {
				_, err := c.CreateAccessPoint(t.Context(), &s3csdk.CreateAccessPointInput{
					AccountId: aws.String(acct), Name: aws.String("BadName"), Bucket: aws.String("b"),
				})

				return err
			},
			wantCode: "InvalidRequest",
		},
		{
			name: "duplicate name",
			call: func(c *s3csdk.Client) error {
				in := &s3csdk.CreateAccessPointInput{
					AccountId: aws.String(acct), Name: aws.String("dup-ap"), Bucket: aws.String("b"),
				}
				if _, err := c.CreateAccessPoint(t.Context(), in); err != nil {
					return err
				}
				_, err := c.CreateAccessPoint(t.Context(), in)

				return err
			},
			wantCode: "AccessPointAlreadyOwnedByYou",
		},
		{
			name: "malformed policy",
			call: func(c *s3csdk.Client) error {
				if _, err := c.CreateAccessPoint(t.Context(), &s3csdk.CreateAccessPointInput{
					AccountId: aws.String(acct), Name: aws.String("pol-ap"), Bucket: aws.String("b"),
				}); err != nil {
					return err
				}
				_, err := c.PutAccessPointPolicy(t.Context(), &s3csdk.PutAccessPointPolicyInput{
					AccountId: aws.String(acct), Name: aws.String("pol-ap"), Policy: aws.String("notjson"),
				})

				return err
			},
			wantCode: "MalformedPolicy",
		},
		{
			name: "missing access point",
			call: func(c *s3csdk.Client) error {
				_, err := c.GetAccessPoint(t.Context(), &s3csdk.GetAccessPointInput{
					AccountId: aws.String(acct), Name: aws.String("nope"),
				})

				return err
			},
			wantCode: "NoSuchAccessPoint",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestS3ControlClient(t, s3control.NewHandler(s3control.NewInMemoryBackend()))
			err := tt.call(client)
			require.Error(t, err)

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, tt.wantCode, apiErr.ErrorCode())
			assert.NotEqual(t, tt.wantCode, apiErr.ErrorMessage())
		})
	}
}
