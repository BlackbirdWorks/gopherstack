package secretsmanager_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	secretsmanagersdk "github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	smtypes "github.com/aws/aws-sdk-go-v2/service/secretsmanager/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

func newRegionalSMClient(t *testing.T, baseURL, region string) *secretsmanagersdk.Client {
	t.Helper()

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(region),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	return secretsmanagersdk.NewFromConfig(cfg, func(o *secretsmanagersdk.Options) {
		o.BaseEndpoint = aws.String(baseURL)
	})
}

func newSMServerURL(t *testing.T) string {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(newSMHandler(t)))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	return srv.URL
}

func TestListSecrets_FilterCaseAndPrefixSemantics(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		want   []string
		filter smtypes.Filter
	}{
		{
			name:   "description is case-insensitive",
			filter: smtypes.Filter{Key: smtypes.FilterNameStringTypeDescription, Values: []string{"PROD"}},
			want:   []string{"a"},
		},
		{
			name:   "name is case-sensitive",
			filter: smtypes.Filter{Key: smtypes.FilterNameStringTypeName, Values: []string{"A"}},
			want:   nil,
		},
		{
			name:   "all is case-insensitive",
			filter: smtypes.Filter{Key: smtypes.FilterNameStringTypeAll, Values: []string{"ENVIRON"}},
			want:   []string{"a"},
		},
		{
			name:   "tag-key is prefix",
			filter: smtypes.Filter{Key: smtypes.FilterNameStringTypeTagKey, Values: []string{"env"}},
			want:   []string{"a"},
		},
		{
			name:   "tag-key is case-sensitive",
			filter: smtypes.Filter{Key: smtypes.FilterNameStringTypeTagKey, Values: []string{"ENV"}},
			want:   nil,
		},
		{
			name:   "tag-value is prefix",
			filter: smtypes.Filter{Key: smtypes.FilterNameStringTypeTagValue, Values: []string{"stag"}},
			want:   []string{"a"},
		},
		{
			name:   "tag-key negation",
			filter: smtypes.Filter{Key: smtypes.FilterNameStringTypeTagKey, Values: []string{"!env"}},
			want:   []string{"b"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newRegionalSMClient(t, newSMServerURL(t), "us-east-1")
			_, err := client.CreateSecret(t.Context(), &secretsmanagersdk.CreateSecretInput{
				Name: aws.String("a"), SecretString: aws.String("v"), Description: aws.String("prod db"),
				Tags: []smtypes.Tag{{Key: aws.String("environment"), Value: aws.String("staging")}},
			})
			require.NoError(t, err)
			_, err = client.CreateSecret(t.Context(), &secretsmanagersdk.CreateSecretInput{
				Name: aws.String("b"), SecretString: aws.String("v"),
			})
			require.NoError(t, err)

			out, err := client.ListSecrets(t.Context(), &secretsmanagersdk.ListSecretsInput{
				Filters: []smtypes.Filter{tc.filter},
			})
			require.NoError(t, err)

			var got []string
			for _, s := range out.SecretList {
				got = append(got, aws.ToString(s.Name))
			}

			assert.ElementsMatch(t, tc.want, got)
		})
	}
}

func TestCreateSecret_ForceOverwriteReplicaSecret(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		wantStatus smtypes.StatusType
		wantValue  string
		force      bool
	}{
		{name: "collision fails without force", wantStatus: smtypes.StatusTypeFailed, wantValue: "standalone"},
		{
			name: "force overwrites the collision", force: true,
			wantStatus: smtypes.StatusTypeInSync, wantValue: "primary",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			url := newSMServerURL(t)
			primary := newRegionalSMClient(t, url, "us-east-1")
			other := newRegionalSMClient(t, url, "us-west-2")

			_, err := other.CreateSecret(t.Context(), &secretsmanagersdk.CreateSecretInput{
				Name: aws.String("shared"), SecretString: aws.String("standalone"),
			})
			require.NoError(t, err)

			out, err := primary.CreateSecret(t.Context(), &secretsmanagersdk.CreateSecretInput{
				Name:                        aws.String("shared"),
				SecretString:                aws.String("primary"),
				AddReplicaRegions:           []smtypes.ReplicaRegionType{{Region: aws.String("us-west-2")}},
				ForceOverwriteReplicaSecret: tc.force,
			})
			require.NoError(t, err)
			require.Len(t, out.ReplicationStatus, 1)
			assert.Equal(t, tc.wantStatus, out.ReplicationStatus[0].Status)

			got, err := other.GetSecretValue(t.Context(), &secretsmanagersdk.GetSecretValueInput{
				SecretId: aws.String("shared"),
			})
			require.NoError(t, err)
			assert.Equal(t, tc.wantValue, aws.ToString(got.SecretString))
		})
	}
}
