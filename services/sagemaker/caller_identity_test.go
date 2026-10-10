package sagemaker_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	sagemakersdk "github.com/aws/aws-sdk-go-v2/service/sagemaker"
	smtypes "github.com/aws/aws-sdk-go-v2/service/sagemaker/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	callerIdentityARN = "arn:aws:iam::123456789012:user/alice"
	callerIdentityBob = "arn:aws:iam::123456789012:user/bob"
)

func newCallerIdentityClient(t *testing.T, callerARN string) *sagemakersdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(newTestHandler(t)))

	e.Use(func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			meta := awsmeta.Get(c.Request().Context())
			m := *meta
			m.Principal = &awsmeta.Principal{Arn: callerARN, UserID: "AIDAALICE"}
			c.SetRequest(c.Request().WithContext(awsmeta.Set(c.Request().Context(), &m)))

			return next(c)
		}
	})
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	return sagemakersdk.NewFromConfig(cfg, func(o *sagemakersdk.Options) { o.BaseEndpoint = aws.String(srv.URL) })
}

func TestCallerIdentity_Attribution(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, c *sagemakersdk.Client) *smtypes.UserContext
		name string
	}{
		{
			name: "model_package_group",
			run: func(t *testing.T, c *sagemakersdk.Client) *smtypes.UserContext {
				t.Helper()

				_, err := c.CreateModelPackageGroup(t.Context(), &sagemakersdk.CreateModelPackageGroupInput{
					ModelPackageGroupName: aws.String("grp"),
				})
				require.NoError(t, err)

				out, err := c.DescribeModelPackageGroup(t.Context(), &sagemakersdk.DescribeModelPackageGroupInput{
					ModelPackageGroupName: aws.String("grp"),
				})
				require.NoError(t, err)

				return out.CreatedBy
			},
		},
		{
			name: "model_package",
			run: func(t *testing.T, c *sagemakersdk.Client) *smtypes.UserContext {
				t.Helper()

				created, err := c.CreateModelPackage(t.Context(), &sagemakersdk.CreateModelPackageInput{
					ModelPackageName: aws.String("pkg"),
				})
				require.NoError(t, err)

				out, err := c.DescribeModelPackage(t.Context(), &sagemakersdk.DescribeModelPackageInput{
					ModelPackageName: created.ModelPackageArn,
				})
				require.NoError(t, err)
				require.NotNil(t, out.LastModifiedBy)
				assert.Equal(t, callerIdentityARN, aws.ToString(out.LastModifiedBy.IamIdentity.Arn))

				return out.CreatedBy
			},
		},
		{
			name: "project",
			run: func(t *testing.T, c *sagemakersdk.Client) *smtypes.UserContext {
				t.Helper()

				_, err := c.CreateProject(t.Context(), &sagemakersdk.CreateProjectInput{ProjectName: aws.String("prj")})
				require.NoError(t, err)

				out, err := c.DescribeProject(t.Context(), &sagemakersdk.DescribeProjectInput{
					ProjectName: aws.String("prj"),
				})
				require.NoError(t, err)

				return out.CreatedBy
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := tt.run(t, newCallerIdentityClient(t, callerIdentityARN))
			require.NotNil(t, got)
			require.NotNil(t, got.IamIdentity)
			assert.Equal(t, callerIdentityARN, aws.ToString(got.IamIdentity.Arn))
			assert.Equal(t, "AIDAALICE", aws.ToString(got.IamIdentity.PrincipalId))
		})
	}
}

func TestModelCard_RequiredUserContext(t *testing.T) {
	t.Parallel()

	c := newCallerIdentityClient(t, callerIdentityARN)

	_, err := c.CreateModelCard(t.Context(), &sagemakersdk.CreateModelCardInput{
		ModelCardName:   aws.String("card"),
		Content:         aws.String("{}"),
		ModelCardStatus: smtypes.ModelCardStatusDraft,
	})
	require.NoError(t, err)

	out, err := c.DescribeModelCard(t.Context(), &sagemakersdk.DescribeModelCardInput{
		ModelCardName: aws.String("card"),
	})
	require.NoError(t, err)
	assert.NotNil(t, out.CreatedBy)
	assert.NotNil(t, out.LastModifiedBy)
}

func TestUpdateProject_ProvisioningAndTemplates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		update  *sagemakersdk.UpdateProjectInput
		verify  func(t *testing.T, out *sagemakersdk.DescribeProjectOutput)
		name    string
		wantErr bool
	}{
		{
			name: "service_catalog",
			update: &sagemakersdk.UpdateProjectInput{
				ServiceCatalogProvisioningUpdateDetails: &smtypes.ServiceCatalogProvisioningUpdateDetails{
					ProvisioningArtifactId: aws.String("pa-new"),
					ProvisioningParameters: []smtypes.ProvisioningParameter{
						{Key: aws.String("k"), Value: aws.String("v")},
					},
				},
			},
			verify: func(t *testing.T, out *sagemakersdk.DescribeProjectOutput) {
				t.Helper()

				d := out.ServiceCatalogProvisioningDetails
				require.NotNil(t, d)
				assert.Equal(t, "prod-1", aws.ToString(d.ProductId))
				assert.Equal(t, "pa-new", aws.ToString(d.ProvisioningArtifactId))
				require.Len(t, d.ProvisioningParameters, 1)
				assert.Equal(t, "v", aws.ToString(d.ProvisioningParameters[0].Value))
			},
		},
		{
			name: "template_provider",
			update: &sagemakersdk.UpdateProjectInput{
				TemplateProvidersToUpdate: []smtypes.UpdateTemplateProvider{{
					CfnTemplateProvider: &smtypes.CfnUpdateTemplateProvider{
						TemplateName: aws.String("tpl"),
						TemplateURL:  aws.String("https://example.com/new.yaml"),
					},
				}},
			},
			verify: func(t *testing.T, out *sagemakersdk.DescribeProjectOutput) {
				t.Helper()

				require.Len(t, out.TemplateProviderDetails, 1)
				d := out.TemplateProviderDetails[0].CfnTemplateProviderDetail
				require.NotNil(t, d)
				assert.Equal(t, "tpl", aws.ToString(d.TemplateName))
				assert.Equal(t, "https://example.com/new.yaml", aws.ToString(d.TemplateURL))
			},
		},
		{
			name: "unknown_template",
			update: &sagemakersdk.UpdateProjectInput{
				TemplateProvidersToUpdate: []smtypes.UpdateTemplateProvider{{
					CfnTemplateProvider: &smtypes.CfnUpdateTemplateProvider{
						TemplateName: aws.String("nope"),
						TemplateURL:  aws.String("https://example.com/x.yaml"),
					},
				}},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newCallerIdentityClient(t, callerIdentityBob)

			_, err := c.CreateProject(t.Context(), &sagemakersdk.CreateProjectInput{
				ProjectName: aws.String("p"),
				ServiceCatalogProvisioningDetails: &smtypes.ServiceCatalogProvisioningDetails{
					ProductId: aws.String("prod-1"),
				},
				TemplateProviders: []smtypes.CreateTemplateProvider{{
					CfnTemplateProvider: &smtypes.CfnCreateTemplateProvider{
						TemplateName: aws.String("tpl"),
						TemplateURL:  aws.String("https://example.com/old.yaml"),
						RoleARN:      aws.String("arn:aws:iam::123456789012:role/r"),
					},
				}},
			})
			require.NoError(t, err)

			tt.update.ProjectName = aws.String("p")
			_, err = c.UpdateProject(t.Context(), tt.update)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			out, err := c.DescribeProject(t.Context(), &sagemakersdk.DescribeProjectInput{ProjectName: aws.String("p")})
			require.NoError(t, err)
			tt.verify(t, out)
		})
	}
}
