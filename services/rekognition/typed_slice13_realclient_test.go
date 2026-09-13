package rekognition_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	rekognitionsdk "github.com/aws/aws-sdk-go-v2/service/rekognition"
	"github.com/aws/aws-sdk-go-v2/service/rekognition/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rekognition"
)

// newRekognitionClient stands up a fresh backend/handler pair plus a real
// SDK client bound to it.
func newRekognitionClient(t *testing.T) *rekognitionsdk.Client {
	t.Helper()

	backend := rekognition.NewInMemoryBackend("000000000000", "us-east-1")
	h := rekognition.NewHandler(backend)

	return newTestRekognitionClient(t, h)
}

// TestTypedSlice13RekognitionRealClient drives every op the census still
// listed as uncovered before this pass (gopherstack-n3zi typed-client
// coverage slice 13). Each subtest builds its own fresh client/backend.
func TestTypedSlice13RekognitionRealClient(t *testing.T) {
	t.Parallel()

	fakeImage := types.Image{Bytes: []byte("fake-image-bytes")}

	t.Run("detect_family", func(t *testing.T) {
		t.Parallel()

		client := newRekognitionClient(t)

		faces, err := client.DetectFaces(t.Context(), &rekognitionsdk.DetectFacesInput{Image: &fakeImage})
		require.NoError(t, err)
		assert.NotNil(t, faces.FaceDetails)

		labels, err := client.DetectLabels(t.Context(), &rekognitionsdk.DetectLabelsInput{
			Image:     &fakeImage,
			MaxLabels: aws.Int32(3),
		})
		require.NoError(t, err)
		assert.NotEmpty(t, labels.Labels)

		text, err := client.DetectText(t.Context(), &rekognitionsdk.DetectTextInput{Image: &fakeImage})
		require.NoError(t, err)
		require.NotEmpty(t, text.TextDetections)
		assert.NotEmpty(t, aws.ToString(text.TextDetections[0].DetectedText))

		moderation, err := client.DetectModerationLabels(t.Context(), &rekognitionsdk.DetectModerationLabelsInput{
			Image:         &fakeImage,
			MinConfidence: aws.Float32(5),
		})
		require.NoError(t, err)
		require.NotEmpty(t, moderation.ModerationLabels)
		assert.Equal(t, "Suggestive", aws.ToString(moderation.ModerationLabels[0].Name))

		ppe, err := client.DetectProtectiveEquipment(t.Context(), &rekognitionsdk.DetectProtectiveEquipmentInput{
			Image: &fakeImage,
		})
		require.NoError(t, err)
		assert.NotNil(t, ppe.Persons)

		celebs, err := client.RecognizeCelebrities(t.Context(), &rekognitionsdk.RecognizeCelebritiesInput{
			Image: &fakeImage,
		})
		require.NoError(t, err)
		assert.NotNil(t, celebs.CelebrityFaces)

		custom, err := client.DetectCustomLabels(t.Context(), &rekognitionsdk.DetectCustomLabelsInput{
			Image:             &fakeImage,
			ProjectVersionArn: aws.String("arn:aws:rekognition:us-east-1:000000000000:project/p/version/v/123"),
		})
		require.NoError(t, err)
		assert.NotNil(t, custom.CustomLabels)
	})

	t.Run("faces_collection", func(t *testing.T) {
		t.Parallel()

		client := newRekognitionClient(t)

		collectionID := "coll-" + uuid.NewString()[:8]
		_, err := client.CreateCollection(t.Context(), &rekognitionsdk.CreateCollectionInput{
			CollectionId: aws.String(collectionID),
		})
		require.NoError(t, err)

		indexed, err := client.IndexFaces(t.Context(), &rekognitionsdk.IndexFacesInput{
			CollectionId: aws.String(collectionID),
			Image:        &fakeImage,
		})
		require.NoError(t, err)
		require.Len(t, indexed.FaceRecords, 1)
		faceID := aws.ToString(indexed.FaceRecords[0].Face.FaceId)

		searched, err := client.SearchFaces(t.Context(), &rekognitionsdk.SearchFacesInput{
			CollectionId: aws.String(collectionID),
			FaceId:       aws.String(faceID),
		})
		require.NoError(t, err)
		assert.Equal(t, faceID, aws.ToString(searched.SearchedFaceId))

		byImage, err := client.SearchFacesByImage(t.Context(), &rekognitionsdk.SearchFacesByImageInput{
			CollectionId: aws.String(collectionID),
			Image:        &fakeImage,
		})
		require.NoError(t, err)
		assert.NotNil(t, byImage.FaceMatches)

		deleted, err := client.DeleteFaces(t.Context(), &rekognitionsdk.DeleteFacesInput{
			CollectionId: aws.String(collectionID),
			FaceIds:      []string{faceID},
		})
		require.NoError(t, err)
		assert.Equal(t, []string{faceID}, deleted.DeletedFaces)
	})

	t.Run("users", func(t *testing.T) {
		t.Parallel()

		client := newRekognitionClient(t)

		collectionID := "coll-" + uuid.NewString()[:8]
		_, err := client.CreateCollection(t.Context(), &rekognitionsdk.CreateCollectionInput{
			CollectionId: aws.String(collectionID),
		})
		require.NoError(t, err)

		indexed, err := client.IndexFaces(t.Context(), &rekognitionsdk.IndexFacesInput{
			CollectionId: aws.String(collectionID),
			Image:        &fakeImage,
		})
		require.NoError(t, err)
		require.Len(t, indexed.FaceRecords, 1)
		faceID := aws.ToString(indexed.FaceRecords[0].Face.FaceId)

		const userID = "user1"

		_, err = client.CreateUser(t.Context(), &rekognitionsdk.CreateUserInput{
			CollectionId: aws.String(collectionID),
			UserId:       aws.String(userID),
		})
		require.NoError(t, err)

		assoc, err := client.AssociateFaces(t.Context(), &rekognitionsdk.AssociateFacesInput{
			CollectionId: aws.String(collectionID),
			UserId:       aws.String(userID),
			FaceIds:      []string{faceID},
		})
		require.NoError(t, err)
		require.Len(t, assoc.AssociatedFaces, 1)

		listed, err := client.ListUsers(t.Context(), &rekognitionsdk.ListUsersInput{
			CollectionId: aws.String(collectionID),
		})
		require.NoError(t, err)
		require.Len(t, listed.Users, 1)
		assert.Equal(t, userID, aws.ToString(listed.Users[0].UserId))

		byImage, err := client.SearchUsersByImage(t.Context(), &rekognitionsdk.SearchUsersByImageInput{
			CollectionId: aws.String(collectionID),
			Image:        &fakeImage,
		})
		require.NoError(t, err)
		assert.NotNil(t, byImage.UserMatches)

		disassoc, err := client.DisassociateFaces(t.Context(), &rekognitionsdk.DisassociateFacesInput{
			CollectionId: aws.String(collectionID),
			UserId:       aws.String(userID),
			FaceIds:      []string{faceID},
		})
		require.NoError(t, err)
		require.Len(t, disassoc.DisassociatedFaces, 1)
		assert.Equal(t, faceID, aws.ToString(disassoc.DisassociatedFaces[0].FaceId))

		_, err = client.DeleteUser(t.Context(), &rekognitionsdk.DeleteUserInput{
			CollectionId: aws.String(collectionID),
			UserId:       aws.String(userID),
		})
		require.NoError(t, err)

		listed, err = client.ListUsers(t.Context(), &rekognitionsdk.ListUsersInput{
			CollectionId: aws.String(collectionID),
		})
		require.NoError(t, err)
		assert.Empty(t, listed.Users)
	})

	t.Run("celebrity", func(t *testing.T) {
		t.Parallel()

		client := newRekognitionClient(t)

		info, err := client.GetCelebrityInfo(t.Context(), &rekognitionsdk.GetCelebrityInfoInput{
			Id: aws.String("celeb-1"),
		})
		require.NoError(t, err)
		assert.NotEmpty(t, aws.ToString(info.Name))

		started, err := client.StartCelebrityRecognition(
			t.Context(),
			&rekognitionsdk.StartCelebrityRecognitionInput{Video: &types.Video{}},
		)
		require.NoError(t, err)
		jobID := aws.ToString(started.JobId)
		require.NotEmpty(t, jobID)

		got, err := client.GetCelebrityRecognition(t.Context(), &rekognitionsdk.GetCelebrityRecognitionInput{
			JobId: aws.String(jobID),
		})
		require.NoError(t, err)
		assert.Equal(t, types.VideoJobStatusInProgress, got.JobStatus)
	})

	t.Run("video_jobs_faces", func(t *testing.T) {
		t.Parallel()

		client := newRekognitionClient(t)

		startedDetect, err := client.StartFaceDetection(
			t.Context(),
			&rekognitionsdk.StartFaceDetectionInput{Video: &types.Video{}},
		)
		require.NoError(t, err)
		detectJobID := aws.ToString(startedDetect.JobId)

		gotDetect, err := client.GetFaceDetection(t.Context(), &rekognitionsdk.GetFaceDetectionInput{
			JobId: aws.String(detectJobID),
		})
		require.NoError(t, err)
		assert.Equal(t, types.VideoJobStatusInProgress, gotDetect.JobStatus)
		assert.NotNil(t, gotDetect.Faces)

		collectionID := "coll-" + uuid.NewString()[:8]
		_, err = client.CreateCollection(t.Context(), &rekognitionsdk.CreateCollectionInput{
			CollectionId: aws.String(collectionID),
		})
		require.NoError(t, err)

		startedSearch, err := client.StartFaceSearch(t.Context(), &rekognitionsdk.StartFaceSearchInput{
			Video:        &types.Video{},
			CollectionId: aws.String(collectionID),
		})
		require.NoError(t, err)
		searchJobID := aws.ToString(startedSearch.JobId)

		gotSearch, err := client.GetFaceSearch(t.Context(), &rekognitionsdk.GetFaceSearchInput{
			JobId: aws.String(searchJobID),
		})
		require.NoError(t, err)
		assert.Equal(t, types.VideoJobStatusInProgress, gotSearch.JobStatus)
		assert.NotNil(t, gotSearch.Persons)
	})

	t.Run("video_jobs_labels_moderation", func(t *testing.T) {
		t.Parallel()

		client := newRekognitionClient(t)

		startedLabel, err := client.StartLabelDetection(
			t.Context(),
			&rekognitionsdk.StartLabelDetectionInput{Video: &types.Video{}},
		)
		require.NoError(t, err)
		labelJobID := aws.ToString(startedLabel.JobId)

		gotLabel, err := client.GetLabelDetection(t.Context(), &rekognitionsdk.GetLabelDetectionInput{
			JobId: aws.String(labelJobID),
		})
		require.NoError(t, err)
		assert.Equal(t, types.VideoJobStatusInProgress, gotLabel.JobStatus)
		require.NotNil(t, gotLabel.GetRequestMetadata)
		assert.Equal(t, "TIMESTAMP", string(gotLabel.GetRequestMetadata.SortBy))

		startedMod, err := client.StartContentModeration(
			t.Context(),
			&rekognitionsdk.StartContentModerationInput{Video: &types.Video{}},
		)
		require.NoError(t, err)
		modJobID := aws.ToString(startedMod.JobId)

		gotMod, err := client.GetContentModeration(t.Context(), &rekognitionsdk.GetContentModerationInput{
			JobId: aws.String(modJobID),
		})
		require.NoError(t, err)
		assert.Equal(t, types.VideoJobStatusInProgress, gotMod.JobStatus)
		require.NotNil(t, gotMod.GetRequestMetadata)
		assert.Equal(t, "TIMESTAMPS", string(gotMod.GetRequestMetadata.AggregateBy))
	})

	t.Run("video_jobs_person_segment_text", func(t *testing.T) {
		t.Parallel()

		client := newRekognitionClient(t)

		startedPerson, err := client.StartPersonTracking(
			t.Context(),
			&rekognitionsdk.StartPersonTrackingInput{Video: &types.Video{}},
		)
		require.NoError(t, err)
		personJobID := aws.ToString(startedPerson.JobId)

		gotPerson, err := client.GetPersonTracking(t.Context(), &rekognitionsdk.GetPersonTrackingInput{
			JobId: aws.String(personJobID),
		})
		require.NoError(t, err)
		assert.Equal(t, types.VideoJobStatusInProgress, gotPerson.JobStatus)
		assert.NotNil(t, gotPerson.Persons)

		startedSeg, err := client.StartSegmentDetection(t.Context(), &rekognitionsdk.StartSegmentDetectionInput{
			Video:        &types.Video{},
			SegmentTypes: []types.SegmentType{types.SegmentTypeShot, types.SegmentTypeTechnicalCue},
		})
		require.NoError(t, err)
		segJobID := aws.ToString(startedSeg.JobId)

		// The wire-shape bug this subtest exists to catch: real
		// GetSegmentDetectionOutput.VideoMetadata is []types.VideoMetadata
		// (api_op_GetSegmentDetection.go), not a single object like every
		// sibling Get<Family> response -- a real client's decode of the old
		// single-object shape would fail outright, not merely lose a field.
		gotSeg, err := client.GetSegmentDetection(t.Context(), &rekognitionsdk.GetSegmentDetectionInput{
			JobId: aws.String(segJobID),
		})
		require.NoError(t, err)
		assert.Equal(t, types.VideoJobStatusInProgress, gotSeg.JobStatus)
		require.Len(t, gotSeg.VideoMetadata, 1)
		assert.Equal(t, "H264", aws.ToString(gotSeg.VideoMetadata[0].Codec))
		require.Len(t, gotSeg.SelectedSegmentTypes, 2)

		startedText, err := client.StartTextDetection(
			t.Context(),
			&rekognitionsdk.StartTextDetectionInput{Video: &types.Video{}},
		)
		require.NoError(t, err)
		textJobID := aws.ToString(startedText.JobId)

		gotText, err := client.GetTextDetection(t.Context(), &rekognitionsdk.GetTextDetectionInput{
			JobId: aws.String(textJobID),
		})
		require.NoError(t, err)
		assert.Equal(t, types.VideoJobStatusInProgress, gotText.JobStatus)
		assert.NotNil(t, gotText.TextDetections)
	})

	t.Run("stream_processors", func(t *testing.T) {
		t.Parallel()

		client := newRekognitionClient(t)

		collectionID := "coll-" + uuid.NewString()[:8]
		_, err := client.CreateCollection(t.Context(), &rekognitionsdk.CreateCollectionInput{
			CollectionId: aws.String(collectionID),
		})
		require.NoError(t, err)

		name := "sp-" + uuid.NewString()[:8]

		created, err := client.CreateStreamProcessor(t.Context(), &rekognitionsdk.CreateStreamProcessorInput{
			Name:    aws.String(name),
			RoleArn: aws.String("arn:aws:iam::000000000000:role/rekognition-sp"),
			Input: &types.StreamProcessorInput{
				KinesisVideoStream: &types.KinesisVideoStream{
					Arn: aws.String("arn:aws:kinesisvideo:us-east-1:000000000000:stream/mystream/123"),
				},
			},
			Output: &types.StreamProcessorOutput{
				S3Destination: &types.S3Destination{Bucket: aws.String("sp-output-bucket")},
			},
			Settings: &types.StreamProcessorSettings{
				FaceSearch: &types.FaceSearchSettings{CollectionId: aws.String(collectionID)},
			},
		})
		require.NoError(t, err)
		assert.NotEmpty(t, aws.ToString(created.StreamProcessorArn))

		listed, err := client.ListStreamProcessors(t.Context(), &rekognitionsdk.ListStreamProcessorsInput{})
		require.NoError(t, err)
		require.Len(t, listed.StreamProcessors, 1)
		assert.Equal(t, name, aws.ToString(listed.StreamProcessors[0].Name))

		described, err := client.DescribeStreamProcessor(t.Context(), &rekognitionsdk.DescribeStreamProcessorInput{
			Name: aws.String(name),
		})
		require.NoError(t, err)
		require.NotNil(t, described.Settings)
		require.NotNil(t, described.Settings.FaceSearch)
		assert.Equal(t, collectionID, aws.ToString(described.Settings.FaceSearch.CollectionId))

		_, err = client.StartStreamProcessor(t.Context(), &rekognitionsdk.StartStreamProcessorInput{
			Name: aws.String(name),
		})
		require.NoError(t, err)

		described, err = client.DescribeStreamProcessor(t.Context(), &rekognitionsdk.DescribeStreamProcessorInput{
			Name: aws.String(name),
		})
		require.NoError(t, err)
		assert.Equal(t, types.StreamProcessorStatusRunning, described.Status)

		_, err = client.UpdateStreamProcessor(t.Context(), &rekognitionsdk.UpdateStreamProcessorInput{
			Name: aws.String(name),
			SettingsForUpdate: &types.StreamProcessorSettingsForUpdate{
				ConnectedHomeForUpdate: &types.ConnectedHomeSettingsForUpdate{
					Labels: []string{"PERSON"},
				},
			},
		})
		require.NoError(t, err)

		_, err = client.StopStreamProcessor(t.Context(), &rekognitionsdk.StopStreamProcessorInput{
			Name: aws.String(name),
		})
		require.NoError(t, err)

		described, err = client.DescribeStreamProcessor(t.Context(), &rekognitionsdk.DescribeStreamProcessorInput{
			Name: aws.String(name),
		})
		require.NoError(t, err)
		assert.Equal(t, types.StreamProcessorStatusStopped, described.Status)

		_, err = client.DeleteStreamProcessor(t.Context(), &rekognitionsdk.DeleteStreamProcessorInput{
			Name: aws.String(name),
		})
		require.NoError(t, err)

		listed, err = client.ListStreamProcessors(t.Context(), &rekognitionsdk.ListStreamProcessorsInput{})
		require.NoError(t, err)
		assert.Empty(t, listed.StreamProcessors)
	})

	t.Run("project_versions", func(t *testing.T) {
		t.Parallel()

		client := newRekognitionClient(t)

		projectName := "proj-" + uuid.NewString()[:8]
		project, err := client.CreateProject(t.Context(), &rekognitionsdk.CreateProjectInput{
			ProjectName: aws.String(projectName),
		})
		require.NoError(t, err)
		projectARN := aws.ToString(project.ProjectArn)

		created, err := client.CreateProjectVersion(t.Context(), &rekognitionsdk.CreateProjectVersionInput{
			ProjectArn:  aws.String(projectARN),
			VersionName: aws.String("v1"),
			OutputConfig: &types.OutputConfig{
				S3Bucket: aws.String("pv-output-bucket"),
			},
		})
		require.NoError(t, err)
		versionARN := aws.ToString(created.ProjectVersionArn)

		described, err := client.DescribeProjectVersions(t.Context(), &rekognitionsdk.DescribeProjectVersionsInput{
			ProjectArn: aws.String(projectARN),
		})
		require.NoError(t, err)
		require.Len(t, described.ProjectVersionDescriptions, 1)
		assert.Equal(t, versionARN, aws.ToString(described.ProjectVersionDescriptions[0].ProjectVersionArn))

		_, err = client.StartProjectVersion(t.Context(), &rekognitionsdk.StartProjectVersionInput{
			ProjectVersionArn: aws.String(versionARN),
			MinInferenceUnits: aws.Int32(1),
		})
		require.NoError(t, err)

		_, err = client.StopProjectVersion(t.Context(), &rekognitionsdk.StopProjectVersionInput{
			ProjectVersionArn: aws.String(versionARN),
		})
		require.NoError(t, err)

		copied, err := client.CopyProjectVersion(t.Context(), &rekognitionsdk.CopyProjectVersionInput{
			SourceProjectArn:        aws.String(projectARN),
			SourceProjectVersionArn: aws.String(versionARN),
			DestinationProjectArn:   aws.String(projectARN),
			VersionName:             aws.String("v1-copy"),
			OutputConfig: &types.OutputConfig{
				S3Bucket: aws.String("pv-output-bucket"),
			},
		})
		require.NoError(t, err)
		copiedARN := aws.ToString(copied.ProjectVersionArn)
		require.NotEmpty(t, copiedARN)

		_, err = client.DeleteProjectVersion(t.Context(), &rekognitionsdk.DeleteProjectVersionInput{
			ProjectVersionArn: aws.String(copiedARN),
		})
		require.NoError(t, err)

		_, err = client.DeleteProjectVersion(t.Context(), &rekognitionsdk.DeleteProjectVersionInput{
			ProjectVersionArn: aws.String(versionARN),
		})
		require.NoError(t, err)

		described, err = client.DescribeProjectVersions(t.Context(), &rekognitionsdk.DescribeProjectVersionsInput{
			ProjectArn: aws.String(projectARN),
		})
		require.NoError(t, err)
		assert.Empty(t, described.ProjectVersionDescriptions)
	})

	t.Run("project_policies", func(t *testing.T) {
		t.Parallel()

		client := newRekognitionClient(t)

		projectName := "proj-" + uuid.NewString()[:8]
		project, err := client.CreateProject(t.Context(), &rekognitionsdk.CreateProjectInput{
			ProjectName: aws.String(projectName),
		})
		require.NoError(t, err)
		projectARN := aws.ToString(project.ProjectArn)

		put, err := client.PutProjectPolicy(t.Context(), &rekognitionsdk.PutProjectPolicyInput{
			ProjectArn:     aws.String(projectARN),
			PolicyName:     aws.String("policy1"),
			PolicyDocument: aws.String(`{"Version":"2012-10-17","Statement":[]}`),
		})
		require.NoError(t, err)
		revisionID := aws.ToString(put.PolicyRevisionId)
		require.NotEmpty(t, revisionID)

		listed, err := client.ListProjectPolicies(t.Context(), &rekognitionsdk.ListProjectPoliciesInput{
			ProjectArn: aws.String(projectARN),
		})
		require.NoError(t, err)
		require.Len(t, listed.ProjectPolicies, 1)
		assert.Equal(t, "policy1", aws.ToString(listed.ProjectPolicies[0].PolicyName))

		_, err = client.DeleteProjectPolicy(t.Context(), &rekognitionsdk.DeleteProjectPolicyInput{
			ProjectArn: aws.String(projectARN),
			PolicyName: aws.String("policy1"),
		})
		require.NoError(t, err)

		listed, err = client.ListProjectPolicies(t.Context(), &rekognitionsdk.ListProjectPoliciesInput{
			ProjectArn: aws.String(projectARN),
		})
		require.NoError(t, err)
		assert.Empty(t, listed.ProjectPolicies)

		_, err = client.DeleteProject(t.Context(), &rekognitionsdk.DeleteProjectInput{
			ProjectArn: aws.String(projectARN),
		})
		require.NoError(t, err)
	})

	t.Run("datasets", func(t *testing.T) {
		t.Parallel()

		client := newRekognitionClient(t)

		projectName := "proj-" + uuid.NewString()[:8]
		project, err := client.CreateProject(t.Context(), &rekognitionsdk.CreateProjectInput{
			ProjectName: aws.String(projectName),
		})
		require.NoError(t, err)
		projectARN := aws.ToString(project.ProjectArn)

		trainDS, err := client.CreateDataset(t.Context(), &rekognitionsdk.CreateDatasetInput{
			ProjectArn:  aws.String(projectARN),
			DatasetType: types.DatasetTypeTrain,
		})
		require.NoError(t, err)
		trainARN := aws.ToString(trainDS.DatasetArn)

		testDS, err := client.CreateDataset(t.Context(), &rekognitionsdk.CreateDatasetInput{
			ProjectArn:  aws.String(projectARN),
			DatasetType: types.DatasetTypeTest,
		})
		require.NoError(t, err)
		testARN := aws.ToString(testDS.DatasetArn)

		_, err = client.DistributeDatasetEntries(t.Context(), &rekognitionsdk.DistributeDatasetEntriesInput{
			Datasets: []types.DistributeDataset{
				{Arn: aws.String(trainARN)},
				{Arn: aws.String(testARN)},
			},
		})
		require.NoError(t, err)

		described, err := client.DescribeDataset(t.Context(), &rekognitionsdk.DescribeDatasetInput{
			DatasetArn: aws.String(trainARN),
		})
		require.NoError(t, err)
		assert.Equal(t, "UPDATE_IN_PROGRESS", string(described.DatasetDescription.Status))

		// DeleteDataset on a distinct, never-distributed dataset (trainARN
		// above is mid-UPDATE_IN_PROGRESS from DistributeDatasetEntries and
		// deliberately not used here -- that state blocks delete, matching
		// real AWS's documented precondition).
		secondProject, err := client.CreateProject(t.Context(), &rekognitionsdk.CreateProjectInput{
			ProjectName: aws.String("proj2-" + uuid.NewString()[:8]),
		})
		require.NoError(t, err)

		freshDS, err := client.CreateDataset(t.Context(), &rekognitionsdk.CreateDatasetInput{
			ProjectArn:  secondProject.ProjectArn,
			DatasetType: types.DatasetTypeTrain,
		})
		require.NoError(t, err)

		_, err = client.DeleteDataset(t.Context(), &rekognitionsdk.DeleteDatasetInput{
			DatasetArn: freshDS.DatasetArn,
		})
		require.NoError(t, err)

		_, err = client.DescribeDataset(t.Context(), &rekognitionsdk.DescribeDatasetInput{
			DatasetArn: freshDS.DatasetArn,
		})
		require.Error(t, err, "deleted dataset should no longer be describable")
	})

	t.Run("media_analysis", func(t *testing.T) {
		t.Parallel()

		client := newRekognitionClient(t)

		started, err := client.StartMediaAnalysisJob(t.Context(), &rekognitionsdk.StartMediaAnalysisJobInput{
			JobName: aws.String("job1"),
			Input: &types.MediaAnalysisInput{
				S3Object: &types.S3Object{Bucket: aws.String("in-bucket"), Name: aws.String("in-key")},
			},
			OutputConfig: &types.MediaAnalysisOutputConfig{
				S3Bucket: aws.String("out-bucket"),
			},
			OperationsConfig: &types.MediaAnalysisOperationsConfig{
				DetectModerationLabels: &types.MediaAnalysisDetectModerationLabelsConfig{
					MinConfidence: aws.Float32(50),
				},
			},
		})
		require.NoError(t, err)
		jobID := aws.ToString(started.JobId)
		require.NotEmpty(t, jobID)

		got, err := client.GetMediaAnalysisJob(t.Context(), &rekognitionsdk.GetMediaAnalysisJobInput{
			JobId: aws.String(jobID),
		})
		require.NoError(t, err)
		assert.Equal(t, "job1", aws.ToString(got.JobName))
		assert.Equal(t, types.MediaAnalysisJobStatus("SUCCEEDED"), got.Status)
		require.NotNil(t, got.OperationsConfig)
		require.NotNil(t, got.OperationsConfig.DetectModerationLabels)
		assert.InDelta(t, float32(50), aws.ToFloat32(got.OperationsConfig.DetectModerationLabels.MinConfidence), 0.01)

		listed, err := client.ListMediaAnalysisJobs(t.Context(), &rekognitionsdk.ListMediaAnalysisJobsInput{})
		require.NoError(t, err)
		require.Len(t, listed.MediaAnalysisJobs, 1)
		assert.Equal(t, jobID, aws.ToString(listed.MediaAnalysisJobs[0].JobId))
	})

	t.Run("face_liveness", func(t *testing.T) {
		t.Parallel()

		client := newRekognitionClient(t)

		created, err := client.CreateFaceLivenessSession(
			t.Context(),
			&rekognitionsdk.CreateFaceLivenessSessionInput{},
		)
		require.NoError(t, err)
		sessionID := aws.ToString(created.SessionId)
		require.NotEmpty(t, sessionID)

		results, err := client.GetFaceLivenessSessionResults(
			t.Context(),
			&rekognitionsdk.GetFaceLivenessSessionResultsInput{SessionId: aws.String(sessionID)},
		)
		require.NoError(t, err)
		assert.Equal(t, sessionID, aws.ToString(results.SessionId))
		assert.Equal(t, types.LivenessSessionStatus("SUCCEEDED"), results.Status)
	})

	t.Run("tags", func(t *testing.T) {
		t.Parallel()

		client := newRekognitionClient(t)

		collectionID := "coll-" + uuid.NewString()[:8]
		created, err := client.CreateCollection(t.Context(), &rekognitionsdk.CreateCollectionInput{
			CollectionId: aws.String(collectionID),
		})
		require.NoError(t, err)
		resourceARN := created.CollectionArn

		_, err = client.TagResource(t.Context(), &rekognitionsdk.TagResourceInput{
			ResourceArn: resourceARN,
			Tags:        map[string]string{"env": "prod"},
		})
		require.NoError(t, err)

		listed, err := client.ListTagsForResource(t.Context(), &rekognitionsdk.ListTagsForResourceInput{
			ResourceArn: resourceARN,
		})
		require.NoError(t, err)
		assert.Equal(t, map[string]string{"env": "prod"}, listed.Tags)

		_, err = client.UntagResource(t.Context(), &rekognitionsdk.UntagResourceInput{
			ResourceArn: resourceARN,
			TagKeys:     []string{"env"},
		})
		require.NoError(t, err)

		listed, err = client.ListTagsForResource(t.Context(), &rekognitionsdk.ListTagsForResourceInput{
			ResourceArn: resourceARN,
		})
		require.NoError(t, err)
		assert.Empty(t, listed.Tags)
	})
}
