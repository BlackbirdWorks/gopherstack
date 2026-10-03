package appconfig_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	appconfigsdk "github.com/aws/aws-sdk-go-v2/service/appconfig"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appconfig"
)

func TestListHostedConfigurationVersions_VersionLabelFilter_RealClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		filter *string
		want   []string
	}{
		{name: "no filter", want: []string{"v1", "v2a", "v2b", "x3"}},
		{name: "exact", filter: aws.String("v1"), want: []string{"v1"}},
		{name: "exact is not prefix", filter: aws.String("v2"), want: []string{}},
		{name: "prefix wildcard", filter: aws.String("v2*"), want: []string{"v2a", "v2b"}},
		{name: "broader wildcard", filter: aws.String("v*"), want: []string{"v1", "v2a", "v2b"}},
		{name: "no match wildcard", filter: aws.String("z*"), want: []string{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := appconfig.NewInMemoryBackend("000000000000", "us-east-1")
			client := newTestAppConfigClient(t, appconfig.NewHandler(backend))

			app, err := backend.CreateApplication("hv-app", "", nil)
			require.NoError(t, err)

			profile, err := backend.CreateConfigurationProfile(
				app.ID,
				"hv-profile",
				"",
				"hosted",
				"AWS.Freeform",
				"",
				"",
				nil,
				nil,
			)
			require.NoError(t, err)

			for _, label := range []string{"v1", "v2a", "v2b", "x3"} {
				_, err = backend.CreateHostedConfigurationVersion(
					app.ID,
					profile.ID,
					"application/json",
					"",
					label,
					[]byte(`{}`),
					nil,
				)
				require.NoError(t, err)
			}

			out, err := client.ListHostedConfigurationVersions(
				t.Context(),
				&appconfigsdk.ListHostedConfigurationVersionsInput{
					ApplicationId:          aws.String(app.ID),
					ConfigurationProfileId: aws.String(profile.ID),
					VersionLabel:           tt.filter,
				},
			)
			require.NoError(t, err)

			got := make([]string, 0)
			for _, v := range out.Items {
				got = append(got, aws.ToString(v.VersionLabel))
			}

			assert.ElementsMatch(t, tt.want, got)
		})
	}
}
