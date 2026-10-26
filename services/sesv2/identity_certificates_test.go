package sesv2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sesv2sdk "github.com/aws/aws-sdk-go-v2/service/sesv2"
	sesv2types "github.com/aws/aws-sdk-go-v2/service/sesv2/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sesv2"
)

const testCertARN = "arn:aws:acm:us-east-1:000000000000:certificate/11111111-2222-3333-4444-555555555555"

func apiErrCode(t *testing.T, err error) string {
	t.Helper()

	var ae smithy.APIError

	require.ErrorAs(t, err, &ae)

	return ae.ErrorCode()
}

func TestEmailIdentityCertificates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, c *sesv2sdk.Client, b *sesv2.InMemoryBackend)
		name string
	}{
		{
			name: "associate list disassociate",
			run: func(t *testing.T, c *sesv2sdk.Client, _ *sesv2.InMemoryBackend) {
				t.Helper()
				ctx := t.Context()

				_, err := c.CreateEmailIdentity(ctx, &sesv2sdk.CreateEmailIdentityInput{
					EmailIdentity: aws.String("example.com"),
				})
				require.NoError(t, err)

				_, err = c.AssociateEmailIdentityCertificate(ctx, &sesv2sdk.AssociateEmailIdentityCertificateInput{
					EmailIdentity:  aws.String("example.com"),
					FromAddress:    aws.String("a@example.com"),
					CertificateArn: aws.String(testCertARN),
				})
				require.NoError(t, err)

				out, err := c.ListEmailIdentityCertificates(ctx, &sesv2sdk.ListEmailIdentityCertificatesInput{
					EmailIdentity: aws.String("example.com"),
				})
				require.NoError(t, err)
				require.Len(t, out.Certificates, 1)
				assert.Equal(t, testCertARN, aws.ToString(out.Certificates[0].CertificateArn))
				assert.Equal(t, "a@example.com", aws.ToString(out.Certificates[0].FromAddress))
				assert.Equal(t, sesv2types.IdentityCertificateStatusActive, out.Certificates[0].Status)

				_, err = c.DisassociateEmailIdentityCertificate(
					ctx, &sesv2sdk.DisassociateEmailIdentityCertificateInput{
						EmailIdentity: aws.String("example.com"),
						FromAddress:   aws.String("a@example.com"),
					})
				require.NoError(t, err)

				out, err = c.ListEmailIdentityCertificates(ctx, &sesv2sdk.ListEmailIdentityCertificatesInput{
					EmailIdentity: aws.String("example.com"),
				})
				require.NoError(t, err)
				assert.Empty(t, out.Certificates)
			},
		},
		{
			name: "reassociate replaces arn",
			run: func(t *testing.T, c *sesv2sdk.Client, _ *sesv2.InMemoryBackend) {
				t.Helper()
				ctx := t.Context()
				other := "arn:aws:acm:us-east-1:000000000000:certificate/99999999-2222-3333-4444-555555555555"

				_, err := c.CreateEmailIdentity(ctx, &sesv2sdk.CreateEmailIdentityInput{
					EmailIdentity: aws.String("a@example.com"),
				})
				require.NoError(t, err)

				for _, arn := range []string{testCertARN, other} {
					_, err = c.AssociateEmailIdentityCertificate(
						ctx, &sesv2sdk.AssociateEmailIdentityCertificateInput{
							EmailIdentity:  aws.String("a@example.com"),
							FromAddress:    aws.String("a@example.com"),
							CertificateArn: aws.String(arn),
						})
					require.NoError(t, err)
				}

				out, err := c.ListEmailIdentityCertificates(ctx, &sesv2sdk.ListEmailIdentityCertificatesInput{
					EmailIdentity: aws.String("a@example.com"),
				})
				require.NoError(t, err)
				require.Len(t, out.Certificates, 1)
				assert.Equal(t, other, aws.ToString(out.Certificates[0].CertificateArn))
			},
		},
		{
			name: "list paginates",
			run: func(t *testing.T, c *sesv2sdk.Client, _ *sesv2.InMemoryBackend) {
				t.Helper()
				ctx := t.Context()

				_, err := c.CreateEmailIdentity(ctx, &sesv2sdk.CreateEmailIdentityInput{
					EmailIdentity: aws.String("example.com"),
				})
				require.NoError(t, err)

				for _, from := range []string{"a@example.com", "b@example.com", "c@example.com"} {
					_, err = c.AssociateEmailIdentityCertificate(
						ctx, &sesv2sdk.AssociateEmailIdentityCertificateInput{
							EmailIdentity:  aws.String("example.com"),
							FromAddress:    aws.String(from),
							CertificateArn: aws.String(testCertARN),
						})
					require.NoError(t, err)
				}

				var got []string

				var token *string

				for range 5 {
					out, listErr := c.ListEmailIdentityCertificates(
						ctx, &sesv2sdk.ListEmailIdentityCertificatesInput{
							EmailIdentity: aws.String("example.com"),
							PageSize:      aws.Int32(2),
							NextToken:     token,
						})
					require.NoError(t, listErr)

					for _, cert := range out.Certificates {
						got = append(got, aws.ToString(cert.FromAddress))
					}

					if out.NextToken == nil {
						break
					}

					token = out.NextToken
				}

				assert.Equal(t, []string{"a@example.com", "b@example.com", "c@example.com"}, got)
			},
		},
		{
			name: "errors",
			run: func(t *testing.T, c *sesv2sdk.Client, _ *sesv2.InMemoryBackend) {
				t.Helper()
				ctx := t.Context()

				_, err := c.AssociateEmailIdentityCertificate(ctx, &sesv2sdk.AssociateEmailIdentityCertificateInput{
					EmailIdentity:  aws.String("missing.example.com"),
					FromAddress:    aws.String("a@missing.example.com"),
					CertificateArn: aws.String(testCertARN),
				})
				require.Error(t, err)
				assert.Equal(t, "NotFoundException", apiErrCode(t, err))

				_, err = c.ListEmailIdentityCertificates(ctx, &sesv2sdk.ListEmailIdentityCertificatesInput{
					EmailIdentity: aws.String("missing.example.com"),
				})
				require.Error(t, err)
				assert.Equal(t, "NotFoundException", apiErrCode(t, err))

				_, err = c.CreateEmailIdentity(ctx, &sesv2sdk.CreateEmailIdentityInput{
					EmailIdentity: aws.String("example.com"),
				})
				require.NoError(t, err)

				_, err = c.AssociateEmailIdentityCertificate(ctx, &sesv2sdk.AssociateEmailIdentityCertificateInput{
					EmailIdentity:  aws.String("example.com"),
					FromAddress:    aws.String("a@example.com"),
					CertificateArn: aws.String("not-an-arn"),
				})
				require.Error(t, err)
				assert.Equal(t, "BadRequestException", apiErrCode(t, err))

				_, err = c.DisassociateEmailIdentityCertificate(
					ctx, &sesv2sdk.DisassociateEmailIdentityCertificateInput{
						EmailIdentity: aws.String("example.com"),
						FromAddress:   aws.String("never@example.com"),
					})
				require.Error(t, err)
				assert.Equal(t, "NotFoundException", apiErrCode(t, err))
			},
		},
		{
			name: "persists and dies with identity",
			run: func(t *testing.T, c *sesv2sdk.Client, b *sesv2.InMemoryBackend) {
				t.Helper()
				ctx := t.Context()

				_, err := c.CreateEmailIdentity(ctx, &sesv2sdk.CreateEmailIdentityInput{
					EmailIdentity: aws.String("example.com"),
				})
				require.NoError(t, err)

				_, err = c.AssociateEmailIdentityCertificate(ctx, &sesv2sdk.AssociateEmailIdentityCertificateInput{
					EmailIdentity:  aws.String("example.com"),
					FromAddress:    aws.String("a@example.com"),
					CertificateArn: aws.String(testCertARN),
				})
				require.NoError(t, err)

				fresh := sesv2.NewInMemoryBackend()
				require.NoError(t, fresh.Restore(ctx, b.Snapshot(ctx)))

				c2 := newSESv2SDKClient(t, sesv2.NewHandler(fresh))
				out, err := c2.ListEmailIdentityCertificates(ctx, &sesv2sdk.ListEmailIdentityCertificatesInput{
					EmailIdentity: aws.String("example.com"),
				})
				require.NoError(t, err)
				require.Len(t, out.Certificates, 1)

				_, err = c.DeleteEmailIdentity(ctx, &sesv2sdk.DeleteEmailIdentityInput{
					EmailIdentity: aws.String("example.com"),
				})
				require.NoError(t, err)

				_, err = c.CreateEmailIdentity(ctx, &sesv2sdk.CreateEmailIdentityInput{
					EmailIdentity: aws.String("example.com"),
				})
				require.NoError(t, err)

				out, err = c.ListEmailIdentityCertificates(ctx, &sesv2sdk.ListEmailIdentityCertificatesInput{
					EmailIdentity: aws.String("example.com"),
				})
				require.NoError(t, err)
				assert.Empty(t, out.Certificates)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newSESv2TestHandler(t)
			tt.run(t, newSESv2SDKClient(t, h), b)
		})
	}
}
