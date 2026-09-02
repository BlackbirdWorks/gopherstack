package amplify_test

// list_filter_params_test.go ratifies the gopherstack-6flj wrapper-key
// sweep's constrained-parameter fix for amplify: ListBackendEnvironments
// declares an EnvironmentName filter (amplify@v1.41.4
// api_op_ListBackendEnvironmentsInput.go: "The name of the backend
// environment") that neither the handler nor the backend ever read --
// every call returned every backend environment for the app regardless of
// what the client asked for.

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	amplifysdk "github.com/aws/aws-sdk-go-v2/service/amplify"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListBackendEnvironments_EnvironmentNameFilter(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()
	client := newTestAmplifyClient(t, h)

	appOut, err := client.CreateApp(t.Context(), &amplifysdk.CreateAppInput{Name: aws.String("filter-app")})
	require.NoError(t, err)

	appID := aws.ToString(appOut.App.AppId)

	for _, name := range []string{"dev", "staging", "prod"} {
		_, createErr := client.CreateBackendEnvironment(t.Context(), &amplifysdk.CreateBackendEnvironmentInput{
			AppId:           aws.String(appID),
			EnvironmentName: aws.String(name),
		})
		require.NoError(t, createErr)
	}

	out, err := client.ListBackendEnvironments(t.Context(), &amplifysdk.ListBackendEnvironmentsInput{
		AppId:           aws.String(appID),
		EnvironmentName: aws.String("staging"),
	})
	require.NoError(t, err)
	require.Len(t, out.BackendEnvironments, 1, "EnvironmentName filter must narrow to the single matching environment")
	assert.Equal(t, "staging", aws.ToString(out.BackendEnvironments[0].EnvironmentName))

	all, err := client.ListBackendEnvironments(t.Context(), &amplifysdk.ListBackendEnvironmentsInput{
		AppId: aws.String(appID),
	})
	require.NoError(t, err)
	assert.Len(t, all.BackendEnvironments, 3, "no filter given: every backend environment for the app")
}
