package rekognition_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	rekognitionsdk "github.com/aws/aws-sdk-go-v2/service/rekognition"
	"github.com/aws/aws-sdk-go-v2/service/rekognition/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSearchUsers_FaceIDOptionalUserID covers gopherstack-2wvq. SearchUsersInput
// marks only CollectionId required (rekognition@v1.54.4 api_op_SearchUsers.go):
// "The request must be provided with either FaceId or UserId. ... If a FaceId
// is provided, UserId isn't required to be present in the Collection." The
// handler previously rejected any request without UserId with a 400.
func TestSearchUsers_FaceIDOptionalUserID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestRekognitionClient(t, h)

	_, err := client.CreateCollection(t.Context(), &rekognitionsdk.CreateCollectionInput{
		CollectionId: aws.String("2wvq-search-users"),
	})
	require.NoError(t, err)

	_, err = client.CreateUser(t.Context(), &rekognitionsdk.CreateUserInput{
		CollectionId: aws.String("2wvq-search-users"),
		UserId:       aws.String("2wvq-user-1"),
	})
	require.NoError(t, err)

	indexed, err := client.IndexFaces(t.Context(), &rekognitionsdk.IndexFacesInput{
		CollectionId: aws.String("2wvq-search-users"),
		Image:        &types.Image{Bytes: []byte("fake-image-bytes")},
	})
	require.NoError(t, err)
	require.NotEmpty(t, indexed.FaceRecords)
	faceID := aws.ToString(indexed.FaceRecords[0].Face.FaceId)
	require.NotEmpty(t, faceID)

	t.Run("face id alone resolves", func(t *testing.T) {
		t.Parallel()

		out, searchErr := client.SearchUsers(t.Context(), &rekognitionsdk.SearchUsersInput{
			CollectionId: aws.String("2wvq-search-users"),
			FaceId:       aws.String(faceID),
			MaxUsers:     aws.Int32(10),
		})
		require.NoError(t, searchErr)
		require.NotNil(t, out.SearchedFace)
		assert.Equal(t, faceID, aws.ToString(out.SearchedFace.FaceId))
		assert.Nil(t, out.SearchedUser)
		assert.Len(t, out.UserMatches, 1)
		assert.Equal(t, "2wvq-user-1", aws.ToString(out.UserMatches[0].User.UserId))
	})

	t.Run("user id still works", func(t *testing.T) {
		t.Parallel()

		out, searchErr := client.SearchUsers(t.Context(), &rekognitionsdk.SearchUsersInput{
			CollectionId: aws.String("2wvq-search-users"),
			UserId:       aws.String("2wvq-user-1"),
			MaxUsers:     aws.Int32(10),
		})
		require.NoError(t, searchErr)
		require.NotNil(t, out.SearchedUser)
		assert.Equal(t, "2wvq-user-1", aws.ToString(out.SearchedUser.UserId))
		assert.Nil(t, out.SearchedFace)
	})

	t.Run("unknown face id errors", func(t *testing.T) {
		t.Parallel()

		_, searchErr := client.SearchUsers(t.Context(), &rekognitionsdk.SearchUsersInput{
			CollectionId: aws.String("2wvq-search-users"),
			FaceId:       aws.String("no-such-face"),
		})
		require.Error(t, searchErr)
	})

	t.Run("neither user id nor face id errors", func(t *testing.T) {
		t.Parallel()

		_, searchErr := client.SearchUsers(t.Context(), &rekognitionsdk.SearchUsersInput{
			CollectionId: aws.String("2wvq-search-users"),
		})
		require.Error(t, searchErr)
	})
}
