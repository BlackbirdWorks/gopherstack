package rekognition_test

import (
	"testing"

	rekognitionsdk "github.com/aws/aws-sdk-go-v2/service/rekognition"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/rekognition"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// rekognition client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := rekognition.NewInMemoryBackend("000000000000", "us-east-1")
	h := rekognition.NewHandler(backend)

	// Operations not yet implemented — image/video analysis, celebrity recognition,
	// custom labels/projects/datasets, media analysis jobs, face liveness sessions,
	// user management, and async job retrieval.
	notImplemented := []string{
		"AssociateFaces",
		"CompareFaces",
		"CopyProjectVersion",
		"CreateDataset",
		"CreateFaceLivenessSession",
		"CreateProject",
		"CreateProjectVersion",
		"CreateUser",
		"DeleteDataset",
		"DeleteProject",
		"DeleteProjectPolicy",
		"DeleteProjectVersion",
		"DeleteUser",
		"DescribeDataset",
		"DescribeProjectVersions",
		"DescribeProjects",
		"DetectCustomLabels",
		"DetectFaces",
		"DetectLabels",
		"DetectModerationLabels",
		"DetectProtectiveEquipment",
		"DetectText",
		"DisassociateFaces",
		"DistributeDatasetEntries",
		"GetCelebrityInfo",
		"GetCelebrityRecognition",
		"GetContentModeration",
		"GetFaceDetection",
		"GetFaceLivenessSessionResults",
		"GetFaceSearch",
		"GetLabelDetection",
		"GetMediaAnalysisJob",
		"GetPersonTracking",
		"GetSegmentDetection",
		"GetTextDetection",
		"ListDatasetEntries",
		"ListDatasetLabels",
		"ListMediaAnalysisJobs",
		"ListProjectPolicies",
		"ListUsers",
		"PutProjectPolicy",
		"RecognizeCelebrities",
		"SearchUsers",
		"SearchUsersByImage",
		"StartCelebrityRecognition",
		"StartContentModeration",
		"StartFaceDetection",
		"StartFaceSearch",
		"StartLabelDetection",
		"StartMediaAnalysisJob",
		"StartPersonTracking",
		"StartProjectVersion",
		"StartSegmentDetection",
		"StartTextDetection",
		"StopProjectVersion",
		"UpdateDatasetEntries",
	}

	sdkcheck.CheckCompleteness(t, &rekognitionsdk.Client{}, h.GetSupportedOperations(), notImplemented)
}
