package main

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/appconfig"
	"github.com/aws/aws-sdk-go-v2/service/appconfigdata"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	ssmtypes "github.com/aws/aws-sdk-go-v2/service/ssm/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAppConfigNonHostedProfiles(t *testing.T) {
	t.Parallel()

	const role = "arn:aws:iam::000000000000:role/retrieval"

	tests := []struct {
		setup func(t *testing.T, fx *sfnFixture) string
		name  string
		uri   string
		want  string
	}{
		{
			name: "ssm_parameter", uri: "ssm-parameter://app/config", want: "param-value",
			setup: func(t *testing.T, fx *sfnFixture) string {
				t.Helper()

				_, err := ssm.NewFromConfig(fx.cfg).PutParameter(t.Context(), &ssm.PutParameterInput{
					Name: aws.String(
						"app/config",
					),
					Value: aws.String("param-value"),
					Type:  ssmtypes.ParameterTypeString,
				})
				require.NoError(t, err)

				return "1"
			},
		},
		{
			name: "s3_object", uri: "s3://cfg-bucket/app/config.json", want: `{"a":1}`,
			setup: func(t *testing.T, fx *sfnFixture) string {
				t.Helper()

				c := s3.NewFromConfig(fx.cfg, func(o *s3.Options) { o.UsePathStyle = true })
				_, err := c.CreateBucket(t.Context(), &s3.CreateBucketInput{Bucket: aws.String("cfg-bucket")})
				require.NoError(t, err)

				_, err = c.PutBucketVersioning(t.Context(), &s3.PutBucketVersioningInput{
					Bucket: aws.String("cfg-bucket"),
					VersioningConfiguration: &s3types.VersioningConfiguration{
						Status: s3types.BucketVersioningStatusEnabled,
					},
				})
				require.NoError(t, err)

				put, err := c.PutObject(t.Context(), &s3.PutObjectInput{
					Bucket: aws.String("cfg-bucket"), Key: aws.String("app/config.json"),
					Body: strings.NewReader(`{"a":1}`), ContentType: aws.String("application/json"),
				})
				require.NoError(t, err)

				return aws.ToString(put.VersionId)
			},
		},
		{
			name: "secretsmanager", uri: "secretsmanager://app-secret", want: "s3cret",
			setup: func(t *testing.T, fx *sfnFixture) string {
				t.Helper()

				sec, err := secretsmanager.NewFromConfig(fx.cfg).CreateSecret(
					t.Context(), &secretsmanager.CreateSecretInput{
						Name: aws.String("app-secret"), SecretString: aws.String("s3cret"),
					},
				)
				require.NoError(t, err)

				return aws.ToString(sec.VersionId)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fx := newSFNFixture(t)
			version := tt.setup(t, fx)

			ac := appconfig.NewFromConfig(fx.cfg)

			app, err := ac.CreateApplication(t.Context(), &appconfig.CreateApplicationInput{Name: aws.String("a")})
			require.NoError(t, err)

			env, err := ac.CreateEnvironment(t.Context(), &appconfig.CreateEnvironmentInput{
				ApplicationId: app.Id, Name: aws.String("e"),
			})
			require.NoError(t, err)

			prof, err := ac.CreateConfigurationProfile(t.Context(), &appconfig.CreateConfigurationProfileInput{
				ApplicationId: app.Id, Name: aws.String("p"), LocationUri: aws.String(tt.uri),
				RetrievalRoleArn: aws.String(role),
			})
			require.NoError(t, err)

			strat, err := ac.CreateDeploymentStrategy(t.Context(), &appconfig.CreateDeploymentStrategyInput{
				Name: aws.String("now"), DeploymentDurationInMinutes: aws.Int32(0), GrowthFactor: aws.Float32(100),
				ReplicateTo: "NONE",
			})
			require.NoError(t, err)

			_, err = ac.StartDeployment(t.Context(), &appconfig.StartDeploymentInput{
				ApplicationId: app.Id, EnvironmentId: env.Id, ConfigurationProfileId: prof.Id,
				DeploymentStrategyId: strat.Id, ConfigurationVersion: aws.String(version),
			})
			require.NoError(t, err)

			acd := appconfigdata.NewFromConfig(fx.cfg)

			sess, err := acd.StartConfigurationSession(t.Context(), &appconfigdata.StartConfigurationSessionInput{
				ApplicationIdentifier: app.Id, EnvironmentIdentifier: env.Id, ConfigurationProfileIdentifier: prof.Id,
			})
			require.NoError(t, err)

			out, err := acd.GetLatestConfiguration(t.Context(), &appconfigdata.GetLatestConfigurationInput{
				ConfigurationToken: sess.InitialConfigurationToken,
			})
			require.NoError(t, err)
			assert.Equal(t, tt.want, string(out.Configuration))
		})
	}
}
