package amplify_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	amplifysdk "github.com/aws/aws-sdk-go-v2/service/amplify"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/amplify"
)

func newUpdateTestClient(t *testing.T) *amplifysdk.Client {
	t.Helper()

	return newTestAmplifyClient(t, amplify.NewHandler(amplify.NewInMemoryBackend("000000000000", tagsRTRegion)))
}

func TestUpdateApp_ExplicitEmptyAndFalse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		update        *amplifysdk.UpdateAppInput
		wantDesc      string
		wantBuildSpec string
		wantBasicAuth bool
		wantAutoBuild bool
	}{
		{
			name:          "omitted_members_unchanged",
			update:        &amplifysdk.UpdateAppInput{},
			wantDesc:      "original",
			wantBuildSpec: "version: 1",
			wantBasicAuth: true,
			wantAutoBuild: true,
		},
		{
			name: "explicit_empty_and_false_applied",
			update: &amplifysdk.UpdateAppInput{
				Description: aws.String(""), BuildSpec: aws.String(""),
				EnableBasicAuth: aws.Bool(false), EnableBranchAutoBuild: aws.Bool(false),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newUpdateTestClient(t)

			created, err := client.CreateApp(t.Context(), &amplifysdk.CreateAppInput{
				Name: aws.String("app"), Description: aws.String("original"), BuildSpec: aws.String("version: 1"),
				EnableBasicAuth: aws.Bool(true), EnableBranchAutoBuild: aws.Bool(true),
				BasicAuthCredentials: aws.String("dXNlcjpwdw=="),
			})
			require.NoError(t, err)

			tt.update.AppId = created.App.AppId
			_, err = client.UpdateApp(t.Context(), tt.update)
			require.NoError(t, err)

			got, err := client.GetApp(t.Context(), &amplifysdk.GetAppInput{AppId: created.App.AppId})
			require.NoError(t, err)
			assert.Equal(t, tt.wantDesc, aws.ToString(got.App.Description))
			assert.Equal(t, tt.wantBuildSpec, aws.ToString(got.App.BuildSpec))
			assert.Equal(t, tt.wantBasicAuth, aws.ToBool(got.App.EnableBasicAuth))
			assert.Equal(t, tt.wantAutoBuild, aws.ToBool(got.App.EnableBranchAutoBuild))
		})
	}
}

func TestUpdateBranch_ExplicitEmptyAndFalse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		update        *amplifysdk.UpdateBranchInput
		wantDesc      string
		wantFramework string
		wantAutoBuild bool
		wantNotify    bool
	}{
		{
			name:          "omitted_members_unchanged",
			update:        &amplifysdk.UpdateBranchInput{Stage: "BETA"},
			wantDesc:      "original",
			wantFramework: "React",
			wantAutoBuild: true,
			wantNotify:    true,
		},
		{
			name: "explicit_empty_and_false_applied",
			update: &amplifysdk.UpdateBranchInput{
				Description: aws.String(""), Framework: aws.String(""),
				EnableAutoBuild: aws.Bool(false), EnableNotification: aws.Bool(false),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newUpdateTestClient(t)

			app, err := client.CreateApp(t.Context(), &amplifysdk.CreateAppInput{Name: aws.String("app")})
			require.NoError(t, err)

			_, err = client.CreateBranch(t.Context(), &amplifysdk.CreateBranchInput{
				AppId: app.App.AppId, BranchName: aws.String("main"), Description: aws.String("original"),
				Framework: aws.String("React"), EnableAutoBuild: aws.Bool(true), EnableNotification: aws.Bool(true),
			})
			require.NoError(t, err)

			tt.update.AppId = app.App.AppId
			tt.update.BranchName = aws.String("main")
			_, err = client.UpdateBranch(t.Context(), tt.update)
			require.NoError(t, err)

			got, err := client.GetBranch(t.Context(), &amplifysdk.GetBranchInput{
				AppId: app.App.AppId, BranchName: aws.String("main"),
			})
			require.NoError(t, err)
			assert.Equal(t, tt.wantDesc, aws.ToString(got.Branch.Description))
			assert.Equal(t, tt.wantFramework, aws.ToString(got.Branch.Framework))
			assert.Equal(t, tt.wantAutoBuild, aws.ToBool(got.Branch.EnableAutoBuild))
			assert.Equal(t, tt.wantNotify, aws.ToBool(got.Branch.EnableNotification))
		})
	}
}

func TestCreateDeployment_FileMap(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		fileMap  map[string]string
		wantURLs []string
	}{
		{name: "no_file_map"},
		{
			name: "url_per_file",
			fileMap: map[string]string{
				"index.html": "d41d8cd98f00b204e9800998ecf8427e",
				"app.js":     "0cc175b9c0f1b6a831c399e269772661",
			},
			wantURLs: []string{"app.js", "index.html"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newUpdateTestClient(t)

			app, err := client.CreateApp(t.Context(), &amplifysdk.CreateAppInput{Name: aws.String("app")})
			require.NoError(t, err)

			_, err = client.CreateBranch(t.Context(), &amplifysdk.CreateBranchInput{
				AppId: app.App.AppId, BranchName: aws.String("main"),
			})
			require.NoError(t, err)

			out, err := client.CreateDeployment(t.Context(), &amplifysdk.CreateDeploymentInput{
				AppId: app.App.AppId, BranchName: aws.String("main"), FileMap: tt.fileMap,
			})
			require.NoError(t, err)
			assert.NotEmpty(t, aws.ToString(out.ZipUploadUrl))

			names := make([]string, 0, len(out.FileUploadUrls))
			for name, url := range out.FileUploadUrls {
				names = append(names, name)

				assert.Contains(t, url, aws.ToString(out.JobId))
			}

			assert.ElementsMatch(t, tt.wantURLs, names)
		})
	}
}

func TestUpdateWebhook_Description(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		update *string
		want   string
	}{
		{name: "omitted_unchanged", want: "original"},
		{name: "explicit_empty_clears", update: aws.String(""), want: ""},
		{name: "replaced", update: aws.String("new"), want: "new"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newUpdateTestClient(t)

			app, err := client.CreateApp(t.Context(), &amplifysdk.CreateAppInput{Name: aws.String("app")})
			require.NoError(t, err)

			_, err = client.CreateBranch(t.Context(), &amplifysdk.CreateBranchInput{
				AppId: app.App.AppId, BranchName: aws.String("main"),
			})
			require.NoError(t, err)

			wh, err := client.CreateWebhook(t.Context(), &amplifysdk.CreateWebhookInput{
				AppId: app.App.AppId, BranchName: aws.String("main"), Description: aws.String("original"),
			})
			require.NoError(t, err)

			updated, err := client.UpdateWebhook(t.Context(), &amplifysdk.UpdateWebhookInput{
				WebhookId: wh.Webhook.WebhookId, Description: tt.update,
			})
			require.NoError(t, err)
			assert.Equal(t, tt.want, aws.ToString(updated.Webhook.Description))
		})
	}
}
