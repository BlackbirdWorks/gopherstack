package rekognition_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	rekognitionsdk "github.com/aws/aws-sdk-go-v2/service/rekognition"
	"github.com/aws/aws-sdk-go-v2/service/rekognition/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSlice7_SearchFaces_FaceMatchThreshold covers gopherstack-xhu2t slice
// 7: SearchFacesInput/SearchFacesByImageInput.FaceMatchThreshold were
// undeclared. faceSimilarity's exact-identity path returns precisely 100.0
// for a shared ExternalImageId and strictly less than 100.0 otherwise
// (faces.go), so FaceMatchThreshold=100 deterministically distinguishes
// them regardless of the underlying hash.
func TestSlice7_SearchFaces_FaceMatchThreshold(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestRekognitionClient(t, h)
	ctx := t.Context()

	_, err := client.CreateCollection(
		ctx, &rekognitionsdk.CreateCollectionInput{CollectionId: aws.String("slice7-sf")},
	)
	require.NoError(t, err)

	query, err := client.IndexFaces(ctx, &rekognitionsdk.IndexFacesInput{
		CollectionId:    aws.String("slice7-sf"),
		ExternalImageId: aws.String("shared"),
		Image:           &types.Image{Bytes: []byte("some-bytes")},
	})
	require.NoError(t, err)
	require.Len(t, query.FaceRecords, 1)
	queryID := query.FaceRecords[0].Face.FaceId

	_, err = client.IndexFaces(ctx, &rekognitionsdk.IndexFacesInput{
		CollectionId:    aws.String("slice7-sf"),
		ExternalImageId: aws.String("shared"),
		Image:           &types.Image{Bytes: []byte("some-bytes")},
	})
	require.NoError(t, err)

	_, err = client.IndexFaces(ctx, &rekognitionsdk.IndexFacesInput{
		CollectionId:    aws.String("slice7-sf"),
		ExternalImageId: aws.String("different"),
		Image:           &types.Image{Bytes: []byte("other-bytes")},
	})
	require.NoError(t, err)

	atExactMatch, err := client.SearchFaces(ctx, &rekognitionsdk.SearchFacesInput{
		CollectionId:       aws.String("slice7-sf"),
		FaceId:             queryID,
		FaceMatchThreshold: aws.Float32(100),
	})
	require.NoError(t, err)
	assert.Len(t, atExactMatch.FaceMatches, 1, "only the shared-identity face scores exactly 100")

	atZero, err := client.SearchFaces(ctx, &rekognitionsdk.SearchFacesInput{
		CollectionId:       aws.String("slice7-sf"),
		FaceId:             queryID,
		FaceMatchThreshold: aws.Float32(0),
	})
	require.NoError(t, err)
	assert.Len(t, atZero.FaceMatches, 2, "threshold 0 keeps both other faces")
}

// TestSlice7_SearchFacesByImage_FaceMatchThreshold: SearchFacesByImage's
// per-candidate similarity is always strictly below 99 (faces.go's
// searchSimilaritySpan caps the offset at 23), so threshold=99 filters
// every candidate and threshold=0 keeps them all, independent of the hash.
func TestSlice7_SearchFacesByImage_FaceMatchThreshold(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestRekognitionClient(t, h)
	ctx := t.Context()

	_, err := client.CreateCollection(
		ctx, &rekognitionsdk.CreateCollectionInput{CollectionId: aws.String("slice7-sfbi")},
	)
	require.NoError(t, err)

	_, err = client.IndexFaces(ctx, &rekognitionsdk.IndexFacesInput{
		CollectionId: aws.String("slice7-sfbi"),
		Image:        &types.Image{Bytes: []byte("some-bytes")},
	})
	require.NoError(t, err)

	atZero, err := client.SearchFacesByImage(ctx, &rekognitionsdk.SearchFacesByImageInput{
		CollectionId:       aws.String("slice7-sfbi"),
		Image:              &types.Image{Bytes: []byte("some-bytes")},
		FaceMatchThreshold: aws.Float32(0),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, atZero.FaceMatches)

	atCeiling, err := client.SearchFacesByImage(ctx, &rekognitionsdk.SearchFacesByImageInput{
		CollectionId:       aws.String("slice7-sfbi"),
		Image:              &types.Image{Bytes: []byte("some-bytes")},
		FaceMatchThreshold: aws.Float32(99),
	})
	require.NoError(t, err)
	assert.Empty(t, atCeiling.FaceMatches, "no synthetic candidate reaches 99")
}

// TestSlice7_SearchUsers_UserMatchThreshold and
// TestSlice7_SearchUsersByImage_UserMatchThreshold cover
// SearchUsersInput/SearchUsersByImageInput.UserMatchThreshold: userSimilarity
// is always strictly below 100, so threshold=100 filters every candidate.
func TestSlice7_SearchUsers_UserMatchThreshold(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestRekognitionClient(t, h)
	ctx := t.Context()

	_, err := client.CreateCollection(
		ctx, &rekognitionsdk.CreateCollectionInput{CollectionId: aws.String("slice7-su")},
	)
	require.NoError(t, err)
	_, err = client.CreateUser(ctx, &rekognitionsdk.CreateUserInput{
		CollectionId: aws.String("slice7-su"), UserId: aws.String("query-user"),
	})
	require.NoError(t, err)
	_, err = client.CreateUser(ctx, &rekognitionsdk.CreateUserInput{
		CollectionId: aws.String("slice7-su"), UserId: aws.String("other-user"),
	})
	require.NoError(t, err)

	atZero, err := client.SearchUsers(ctx, &rekognitionsdk.SearchUsersInput{
		CollectionId:       aws.String("slice7-su"),
		UserId:             aws.String("query-user"),
		UserMatchThreshold: aws.Float32(0),
	})
	require.NoError(t, err)
	assert.Len(t, atZero.UserMatches, 1)

	atCeiling, err := client.SearchUsers(ctx, &rekognitionsdk.SearchUsersInput{
		CollectionId:       aws.String("slice7-su"),
		UserId:             aws.String("query-user"),
		UserMatchThreshold: aws.Float32(100),
	})
	require.NoError(t, err)
	assert.Empty(t, atCeiling.UserMatches, "no synthetic candidate reaches 100")
}

func TestSlice7_SearchUsersByImage_UserMatchThreshold(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestRekognitionClient(t, h)
	ctx := t.Context()

	_, err := client.CreateCollection(
		ctx, &rekognitionsdk.CreateCollectionInput{CollectionId: aws.String("slice7-subi")},
	)
	require.NoError(t, err)
	_, err = client.CreateUser(ctx, &rekognitionsdk.CreateUserInput{
		CollectionId: aws.String("slice7-subi"), UserId: aws.String("user-1"),
	})
	require.NoError(t, err)

	atZero, err := client.SearchUsersByImage(ctx, &rekognitionsdk.SearchUsersByImageInput{
		CollectionId:       aws.String("slice7-subi"),
		Image:              &types.Image{Bytes: []byte("some-bytes")},
		UserMatchThreshold: aws.Float32(0),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, atZero.UserMatches)

	atCeiling, err := client.SearchUsersByImage(ctx, &rekognitionsdk.SearchUsersByImageInput{
		CollectionId:       aws.String("slice7-subi"),
		Image:              &types.Image{Bytes: []byte("some-bytes")},
		UserMatchThreshold: aws.Float32(100),
	})
	require.NoError(t, err)
	assert.Empty(t, atCeiling.UserMatches, "no synthetic candidate reaches 100")
}

// TestSlice7_AssociateFaces_UserMatchThreshold covers
// AssociateFacesInput.UserMatchThreshold: associateFaceMatchConfidence is
// always strictly below 100, so threshold=100 rejects every face with
// LOW_MATCH_CONFIDENCE and threshold=0 associates them all.
func TestSlice7_AssociateFaces_UserMatchThreshold(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestRekognitionClient(t, h)
	ctx := t.Context()

	_, err := client.CreateCollection(
		ctx, &rekognitionsdk.CreateCollectionInput{CollectionId: aws.String("slice7-af")},
	)
	require.NoError(t, err)
	_, err = client.CreateUser(ctx, &rekognitionsdk.CreateUserInput{
		CollectionId: aws.String("slice7-af"), UserId: aws.String("assoc-user"),
	})
	require.NoError(t, err)

	indexed, err := client.IndexFaces(ctx, &rekognitionsdk.IndexFacesInput{
		CollectionId: aws.String("slice7-af"),
		Image:        &types.Image{Bytes: []byte("some-bytes")},
	})
	require.NoError(t, err)
	require.Len(t, indexed.FaceRecords, 1)
	faceID := indexed.FaceRecords[0].Face.FaceId

	rejected, err := client.AssociateFaces(ctx, &rekognitionsdk.AssociateFacesInput{
		CollectionId:       aws.String("slice7-af"),
		UserId:             aws.String("assoc-user"),
		FaceIds:            []string{*faceID},
		UserMatchThreshold: aws.Float32(100),
	})
	require.NoError(t, err)
	assert.Empty(t, rejected.AssociatedFaces)
	require.Len(t, rejected.UnsuccessfulFaceAssociations, 1)
	assert.Equal(
		t,
		types.UnsuccessfulFaceAssociationReasonLowMatchConfidence,
		rejected.UnsuccessfulFaceAssociations[0].Reasons[0],
	)

	accepted, err := client.AssociateFaces(ctx, &rekognitionsdk.AssociateFacesInput{
		CollectionId:       aws.String("slice7-af"),
		UserId:             aws.String("assoc-user"),
		FaceIds:            []string{*faceID},
		UserMatchThreshold: aws.Float32(0),
	})
	require.NoError(t, err)
	assert.Len(t, accepted.AssociatedFaces, 1)
	assert.Empty(t, accepted.UnsuccessfulFaceAssociations)
}

// TestSlice7_DetectLabels_Features covers gopherstack-xhu2t slice 7:
// DetectLabelsInput.Features was undeclared. Real AWS defaults to
// GENERAL_LABELS when Features is omitted (api_op_DetectLabels.go:149-153);
// requesting only IMAGE_PROPERTIES must omit Labels.
func TestSlice7_DetectLabels_Features(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestRekognitionClient(t, h)
	ctx := t.Context()

	byDefault, err := client.DetectLabels(ctx, &rekognitionsdk.DetectLabelsInput{
		Image: &types.Image{Bytes: []byte("some-bytes")},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, byDefault.Labels, "Features omitted must default to GENERAL_LABELS")

	explicitGeneral, err := client.DetectLabels(ctx, &rekognitionsdk.DetectLabelsInput{
		Image:    &types.Image{Bytes: []byte("some-bytes")},
		Features: []types.DetectLabelsFeatureName{types.DetectLabelsFeatureNameGeneralLabels},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, explicitGeneral.Labels)

	imagePropertiesOnly, err := client.DetectLabels(ctx, &rekognitionsdk.DetectLabelsInput{
		Image:    &types.Image{Bytes: []byte("some-bytes")},
		Features: []types.DetectLabelsFeatureName{types.DetectLabelsFeatureNameImageProperties},
	})
	require.NoError(t, err)
	assert.Empty(t, imagePropertiesOnly.Labels, "IMAGE_PROPERTIES alone must not return Labels")
}

// TestSlice7_SearchUsersByImage_QualityFilterValidation covers
// gopherstack-xhu2t slice 7: SearchUsersByImageInput.QualityFilter was
// declared on no request struct at all -- an invalid value passed through
// silently. Matches the existing CompareFaces/SearchFacesByImage validation
// precedent (gopherstack-qlqz): validated, not yet applied as a real filter.
func TestSlice7_SearchUsersByImage_QualityFilterValidation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestRekognitionClient(t, h)
	ctx := t.Context()

	_, err := client.CreateCollection(
		ctx, &rekognitionsdk.CreateCollectionInput{CollectionId: aws.String("slice7-subi-qf")},
	)
	require.NoError(t, err)

	_, err = client.SearchUsersByImage(ctx, &rekognitionsdk.SearchUsersByImageInput{
		CollectionId:  aws.String("slice7-subi-qf"),
		Image:         &types.Image{Bytes: []byte("some-bytes")},
		QualityFilter: "NOT_A_REAL_VALUE",
	})
	require.Error(t, err)

	_, err = client.SearchUsersByImage(ctx, &rekognitionsdk.SearchUsersByImageInput{
		CollectionId:  aws.String("slice7-subi-qf"),
		Image:         &types.Image{Bytes: []byte("some-bytes")},
		QualityFilter: types.QualityFilterHigh,
	})
	require.NoError(t, err)
}
