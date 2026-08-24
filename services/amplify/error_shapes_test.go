package amplify_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	amplifysdk "github.com/aws/aws-sdk-go-v2/service/amplify"
	amplifytypes "github.com/aws/aws-sdk-go-v2/service/amplify/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/amplify"
)

// TestTagOpsNotFound_ResourceNotFoundException proves that TagResource,
// UntagResource and ListTagsForResource report an unrecognized resource ARN
// as ResourceNotFoundException, not the plain NotFoundException every other
// Amplify operation uses. Their own deserializeOpError switches
// (aws-sdk-go-v2/service/amplify@v1.41.4 deserializers.go) type
// ResourceNotFoundException specifically -- a client calling
// errors.As(err, &types.NotFoundException{}) would never match.
func TestTagOpsNotFound_ResourceNotFoundException(t *testing.T) {
	t.Parallel()

	backend := amplify.NewInMemoryBackend("000000000000", tagsRTRegion)
	client := newTestAmplifyClient(t, amplify.NewHandler(backend))

	noSuchApp := "arn:aws:amplify:us-east-1:000000000000:apps/no-such-app"

	t.Run("TagResource unknown app", func(t *testing.T) {
		t.Parallel()

		_, err := client.TagResource(t.Context(), &amplifysdk.TagResourceInput{
			ResourceArn: aws.String(noSuchApp),
			Tags:        map[string]string{"k": "v"},
		})

		var rnfe *amplifytypes.ResourceNotFoundException
		require.ErrorAs(t, err, &rnfe)

		var nfe *amplifytypes.NotFoundException
		require.NotErrorAs(t, err, &nfe)
	})

	t.Run("UntagResource unknown app", func(t *testing.T) {
		t.Parallel()

		_, err := client.UntagResource(t.Context(), &amplifysdk.UntagResourceInput{
			ResourceArn: aws.String(noSuchApp),
			TagKeys:     []string{"k"},
		})

		var rnfe *amplifytypes.ResourceNotFoundException
		require.ErrorAs(t, err, &rnfe)

		var nfe *amplifytypes.NotFoundException
		require.NotErrorAs(t, err, &nfe)
	})

	t.Run("ListTagsForResource unknown app", func(t *testing.T) {
		t.Parallel()

		_, err := client.ListTagsForResource(t.Context(), &amplifysdk.ListTagsForResourceInput{
			ResourceArn: aws.String(noSuchApp),
		})

		var rnfe *amplifytypes.ResourceNotFoundException
		require.ErrorAs(t, err, &rnfe)

		var nfe *amplifytypes.NotFoundException
		require.NotErrorAs(t, err, &nfe)
	})
}

// TestListOpsUnknownParent_BadRequestException proves that the six List*
// operations taking a parent identifier report an unrecognized appId as
// BadRequestException, not NotFoundException. Their own deserializeOpError
// switches (aws-sdk-go-v2/service/amplify@v1.41.4 deserializers.go) do not
// type NotFoundException at all -- only BadRequestException,
// InternalFailureException, UnauthorizedException (+LimitExceededException
// for some).
func TestListOpsUnknownParent_BadRequestException(t *testing.T) {
	t.Parallel()

	backend := amplify.NewInMemoryBackend("000000000000", tagsRTRegion)
	client := newTestAmplifyClient(t, amplify.NewHandler(backend))

	t.Run("ListBranches unknown app", func(t *testing.T) {
		t.Parallel()

		_, err := client.ListBranches(t.Context(), &amplifysdk.ListBranchesInput{
			AppId: aws.String("no-such-app"),
		})

		var bre *amplifytypes.BadRequestException
		require.ErrorAs(t, err, &bre)

		var nfe *amplifytypes.NotFoundException
		require.NotErrorAs(t, err, &nfe)
	})

	t.Run("ListBackendEnvironments unknown app", func(t *testing.T) {
		t.Parallel()

		_, err := client.ListBackendEnvironments(t.Context(), &amplifysdk.ListBackendEnvironmentsInput{
			AppId: aws.String("no-such-app"),
		})

		var bre *amplifytypes.BadRequestException
		require.ErrorAs(t, err, &bre)

		var nfe *amplifytypes.NotFoundException
		require.NotErrorAs(t, err, &nfe)
	})

	t.Run("ListWebhooks unknown app", func(t *testing.T) {
		t.Parallel()

		_, err := client.ListWebhooks(t.Context(), &amplifysdk.ListWebhooksInput{
			AppId: aws.String("no-such-app"),
		})

		var bre *amplifytypes.BadRequestException
		require.ErrorAs(t, err, &bre)

		var nfe *amplifytypes.NotFoundException
		require.NotErrorAs(t, err, &nfe)
	})

	t.Run("ListDomainAssociations unknown app", func(t *testing.T) {
		t.Parallel()

		_, err := client.ListDomainAssociations(t.Context(), &amplifysdk.ListDomainAssociationsInput{
			AppId: aws.String("no-such-app"),
		})

		var bre *amplifytypes.BadRequestException
		require.ErrorAs(t, err, &bre)

		var nfe *amplifytypes.NotFoundException
		require.NotErrorAs(t, err, &nfe)
	})

	t.Run("ListJobs unknown app", func(t *testing.T) {
		t.Parallel()

		_, err := client.ListJobs(t.Context(), &amplifysdk.ListJobsInput{
			AppId:      aws.String("no-such-app"),
			BranchName: aws.String("main"),
		})

		var bre *amplifytypes.BadRequestException
		require.ErrorAs(t, err, &bre)

		var nfe *amplifytypes.NotFoundException
		require.NotErrorAs(t, err, &nfe)
	})

	t.Run("ListArtifacts unknown branch", func(t *testing.T) {
		t.Parallel()

		created, err := client.CreateApp(t.Context(), &amplifysdk.CreateAppInput{
			Name: aws.String("artifacts-parent-app"),
		})
		require.NoError(t, err)

		_, err = client.ListArtifacts(t.Context(), &amplifysdk.ListArtifactsInput{
			AppId:      created.App.AppId,
			BranchName: aws.String("no-such-branch"),
			JobId:      aws.String("no-such-job"),
		})

		var bre *amplifytypes.BadRequestException
		require.ErrorAs(t, err, &bre)

		var nfe *amplifytypes.NotFoundException
		require.NotErrorAs(t, err, &nfe)
	})
}
