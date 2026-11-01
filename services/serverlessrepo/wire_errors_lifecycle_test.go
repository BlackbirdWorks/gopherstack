package serverlessrepo_test

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	sarsdk "github.com/aws/aws-sdk-go-v2/service/serverlessapplicationrepository"
	"github.com/aws/aws-sdk-go-v2/service/serverlessapplicationrepository/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/sdktest"
	"github.com/blackbirdworks/gopherstack/services/serverlessrepo"
)

func newInMemorySARClient(t *testing.T, h *serverlessrepo.Handler) *sarsdk.Client {
	t.Helper()

	return sarsdk.NewFromConfig(aws.Config{
		Region:       "us-east-1",
		BaseEndpoint: aws.String(sdktest.InMemoryEndpoint),
		HTTPClient:   sdktest.ServeInMemory(t, h),
		Credentials:  credentials.NewStaticCredentialsProvider("test", "test", ""),
	})
}

func newSARFixture(t *testing.T, b *serverlessrepo.InMemoryBackend) *sarsdk.Client {
	t.Helper()

	client := newInMemorySARClient(t, serverlessrepo.NewHandler(b))
	_, err := client.CreateApplication(t.Context(), &sarsdk.CreateApplicationInput{
		Name:            aws.String("app"),
		Description:     aws.String("d"),
		Author:          aws.String("a"),
		SemanticVersion: aws.String("1.0.0"),
		SourceCodeUrl:   aws.String("https://example.com/src"),
	})
	require.NoError(t, err)

	return client
}

func TestSDK_ErrorMessagesOmitCode(t *testing.T) {
	t.Parallel()

	client, _ := newTestSARSDKClient(
		t,
		serverlessrepo.NewHandler(serverlessrepo.NewInMemoryBackend("000000000000", "us-east-1")),
	)

	_, err := client.GetApplication(t.Context(), &sarsdk.GetApplicationInput{ApplicationId: aws.String("nope")})

	var nf *types.NotFoundException
	require.ErrorAs(t, err, &nf)
	assert.NotContains(t, aws.ToString(nf.Message), "NotFoundException")
	assert.Equal(t, 'C', rune(aws.ToString(nf.Message)[0]))
}

func TestSDK_SemanticVersionAndNames(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		version string
		wantErr bool
	}{
		{name: "plain", version: "1.2.3"},
		{name: "prerelease_build", version: "1.2.3-rc.1+build.5"},
		{name: "trailing_garbage", version: "1.2.3garbage", wantErr: true},
		{name: "leading_zero", version: "01.2.3", wantErr: true},
		{name: "two_parts", version: "1.2", wantErr: true},
		{name: "empty_prerelease", version: "1.2.3-", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newSARFixture(t, serverlessrepo.NewInMemoryBackend("000000000000", "us-east-1"))
			_, err := client.CreateApplicationVersion(t.Context(), &sarsdk.CreateApplicationVersionInput{
				ApplicationId:   aws.String("app"),
				SemanticVersion: aws.String(tt.version),
				SourceCodeUrl:   aws.String("https://example.com/src"),
			})

			if tt.wantErr {
				var bad *types.BadRequestException
				require.ErrorAs(t, err, &bad)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestSDK_VersionsSortBySemver(t *testing.T) {
	t.Parallel()

	client := newSARFixture(t, serverlessrepo.NewInMemoryBackend("000000000000", "us-east-1"))

	for _, v := range []string{"1.10.0", "1.2.0", "1.0.0-rc.1"} {
		_, err := client.CreateApplicationVersion(t.Context(), &sarsdk.CreateApplicationVersionInput{
			ApplicationId:   aws.String("app"),
			SemanticVersion: aws.String(v),
			SourceCodeUrl:   aws.String("https://example.com/src"),
		})
		require.NoError(t, err)
	}

	var got []string

	p := sarsdk.NewListApplicationVersionsPaginator(client, &sarsdk.ListApplicationVersionsInput{
		ApplicationId: aws.String("app"),
		MaxItems:      aws.Int32(1),
	})
	for p.HasMorePages() {
		page, err := p.NextPage(t.Context())
		require.NoError(t, err)

		for _, v := range page.Versions {
			got = append(got, aws.ToString(v.SemanticVersion))
		}
	}

	assert.Equal(t, []string{"1.0.0-rc.1", "1.0.0", "1.2.0", "1.10.0"}, got)
}

func TestSDK_PaginationBounds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		token    string
		maxItems int32
		wantErr  bool
	}{
		{name: "garbage_token", token: "%%%", wantErr: true},
		{name: "max_too_large", maxItems: 101, wantErr: true},
		{name: "max_ok", maxItems: 100},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newSARFixture(t, serverlessrepo.NewInMemoryBackend("000000000000", "us-east-1"))
			in := &sarsdk.ListApplicationsInput{}
			if tt.token != "" {
				in.NextToken = aws.String(tt.token)
			}

			if tt.maxItems != 0 {
				in.MaxItems = aws.Int32(tt.maxItems)
			}

			_, err := client.ListApplications(t.Context(), in)
			if tt.wantErr {
				var bad *types.BadRequestException
				require.ErrorAs(t, err, &bad)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestSDK_CloudFormationInputs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call    func(c *sarsdk.Client) error
		name    string
		wantErr string
	}{
		{
			name: "template_unknown_version",
			call: func(c *sarsdk.Client) error {
				_, err := c.CreateCloudFormationTemplate(t.Context(), &sarsdk.CreateCloudFormationTemplateInput{
					ApplicationId: aws.String("app"), SemanticVersion: aws.String("9.9.9"),
				})

				return err
			},
			wantErr: "NotFoundException",
		},
		{
			name: "changeset_unknown_version",
			call: func(c *sarsdk.Client) error {
				_, err := c.CreateCloudFormationChangeSet(t.Context(), &sarsdk.CreateCloudFormationChangeSetInput{
					ApplicationId: aws.String("app"), StackName: aws.String("s"), SemanticVersion: aws.String("9.9.9"),
				})

				return err
			},
			wantErr: "BadRequestException",
		},
		{
			name: "changeset_bad_stack_name",
			call: func(c *sarsdk.Client) error {
				_, err := c.CreateCloudFormationChangeSet(t.Context(), &sarsdk.CreateCloudFormationChangeSetInput{
					ApplicationId: aws.String("app"), StackName: aws.String("bad_name!"),
				})

				return err
			},
			wantErr: "BadRequestException",
		},
		{
			name: "changeset_ok",
			call: func(c *sarsdk.Client) error {
				_, err := c.CreateCloudFormationChangeSet(t.Context(), &sarsdk.CreateCloudFormationChangeSetInput{
					ApplicationId: aws.String("app"), StackName: aws.String("my-stack"),
					ChangeSetName: aws.String("cs-1"), SemanticVersion: aws.String("1.0.0"),
				})

				return err
			},
		},
		{
			name: "policy_bad_principal",
			call: func(c *sarsdk.Client) error {
				_, err := c.PutApplicationPolicy(t.Context(), &sarsdk.PutApplicationPolicyInput{
					ApplicationId: aws.String("app"),
					Statements: []types.ApplicationPolicyStatement{{
						Actions: []string{"Deploy"}, Principals: []string{"not-an-account"},
					}},
				})

				return err
			},
			wantErr: "BadRequestException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newSARFixture(t, serverlessrepo.NewInMemoryBackend("000000000000", "us-east-1"))
			err := tt.call(client)

			if tt.wantErr == "" {
				require.NoError(t, err)

				return
			}

			var apiErr interface{ ErrorCode() string }

			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, tt.wantErr, apiErr.ErrorCode())
		})
	}
}

func TestSDK_TemplateStatusLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus types.Status
		delay      time.Duration
	}{
		{name: "default_active", wantStatus: types.StatusActive},
		{name: "delayed_preparing", delay: time.Hour, wantStatus: types.StatusPreparing},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := serverlessrepo.NewInMemoryBackend("000000000000", "us-east-1")
			b.SetTemplatePrepareDelay(tt.delay)
			client := newSARFixture(t, b)

			created, err := client.CreateCloudFormationTemplate(t.Context(), &sarsdk.CreateCloudFormationTemplateInput{
				ApplicationId: aws.String("app"),
			})
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, created.Status)

			got, err := client.GetCloudFormationTemplate(t.Context(), &sarsdk.GetCloudFormationTemplateInput{
				ApplicationId: aws.String("app"), TemplateId: created.TemplateId,
			})
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, got.Status)
		})
	}
}

func TestSDK_TemplateStatusCompletes(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b := serverlessrepo.NewInMemoryBackend("000000000000", "us-east-1")
		b.SetTemplatePrepareDelay(20 * time.Millisecond)
		client := newSARFixture(t, b)

		created, err := client.CreateCloudFormationTemplate(t.Context(), &sarsdk.CreateCloudFormationTemplateInput{
			ApplicationId: aws.String("app"),
		})
		require.NoError(t, err)
		require.Equal(t, types.StatusPreparing, created.Status)

		require.Eventually(t, func() bool {
			got, getErr := client.GetCloudFormationTemplate(t.Context(), &sarsdk.GetCloudFormationTemplateInput{
				ApplicationId: aws.String("app"), TemplateId: created.TemplateId,
			})

			return getErr == nil && got.Status == types.StatusActive
		}, 5*time.Second, 10*time.Millisecond)
	})
}
