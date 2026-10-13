package rekognition_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rekognition"
)

func decodeMap(t *testing.T, body []byte) map[string]any {
	t.Helper()

	var m map[string]any
	require.NoError(t, json.Unmarshal(body, &m))

	return m
}

func TestRealism_RequestValidation(t *testing.T) {
	t.Parallel()

	s3img := map[string]any{"S3Object": map[string]any{"Bucket": "bkt", "Name": "k.jpg"}}
	imgBytes := map[string]any{"Bytes": "aGVsbG8="}

	tests := []struct {
		body     map[string]any
		name     string
		action   string
		wantType string
		wantMsg  string
	}{
		{
			name: "collection_id_pattern", action: "CreateCollection", wantType: "InvalidParameterException",
			body: map[string]any{"CollectionId": "bad id!"}, wantMsg: "CollectionId must match",
		},
		{
			name: "project_name_pattern", action: "CreateProject", wantType: "InvalidParameterException",
			body: map[string]any{"ProjectName": "bad name!"}, wantMsg: "ProjectName must match",
		},
		{
			name:     "stream_processor_name_pattern",
			action:   "CreateStreamProcessor",
			wantType: "InvalidParameterException",
			body:     map[string]any{"Name": "bad name!"},
			wantMsg:  "Name must match",
		},
		{
			name: "user_id_pattern", action: "CreateUser", wantType: "InvalidParameterException",
			body: map[string]any{"CollectionId": "c", "UserId": "bad id!"}, wantMsg: "UserId must match",
		},
		{
			name: "external_image_id_pattern", action: "IndexFaces", wantType: "InvalidParameterException",
			body: map[string]any{
				"CollectionId": "c", "Image": imgBytes, "ExternalImageId": "bad id!",
			},
			wantMsg: "ExternalImageId must match",
		},
		{
			name:     "detect_labels_min_confidence",
			action:   "DetectLabels",
			wantType: "InvalidParameterException",
			body: map[string]any{
				"Image":         s3img,
				"MinConfidence": 101,
			},
			wantMsg: "MinConfidence must be between 0 and 100",
		},
		{
			name: "detect_labels_negative_max_labels", action: "DetectLabels", wantType: "InvalidParameterException",
			body: map[string]any{"Image": s3img, "MaxLabels": -1}, wantMsg: "MaxLabels must be",
		},
		{
			name: "search_faces_threshold", action: "SearchFaces", wantType: "InvalidParameterException",
			body:    map[string]any{"CollectionId": "c", "FaceId": "f", "FaceMatchThreshold": 150},
			wantMsg: "FaceMatchThreshold must be between 0 and 100",
		},
		{
			name: "video_client_token", action: "StartLabelDetection", wantType: "InvalidParameterException",
			body: map[string]any{
				"Video":              map[string]any{"S3Object": map[string]any{"Bucket": "bkt", "Name": "k.mp4"}},
				"ClientRequestToken": "bad token!",
			},
			wantMsg: "ClientRequestToken must match",
		},
		{
			name: "face_attributes_enum", action: "StartFaceDetection", wantType: "InvalidParameterException",
			body: map[string]any{
				"Video":          map[string]any{"S3Object": map[string]any{"Bucket": "bkt", "Name": "k.mp4"}},
				"FaceAttributes": "SOME",
			},
			wantMsg: "FaceAttributes",
		},
		{
			name:     "list_collections_garbage_token",
			action:   "ListCollections",
			wantType: "InvalidPaginationTokenException",
			body:     map[string]any{"NextToken": "garbage"},
			wantMsg:  "pagination token is invalid",
		},
		{
			name: "collection_not_found_wording", action: "DescribeCollection", wantType: "ResourceNotFoundException",
			body: map[string]any{"CollectionId": "nope"}, wantMsg: "collection does not exist",
		},
		{
			name: "validation_has_no_code_prefix", action: "CreateCollection", wantType: "InvalidParameterException",
			body: map[string]any{}, wantMsg: "CollectionId is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, newTestHandler(t), tt.action, tt.body)
			require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())

			m := decodeMap(t, rec.Body.Bytes())
			assert.Equal(t, tt.wantType, m["__type"])
			assert.Contains(t, m["message"], tt.wantMsg)
			assert.NotContains(t, m["message"], "Exception: ")
		})
	}
}

func TestRealism_StreamProcessorLifecycle(t *testing.T) {
	t.Parallel()

	const delay = 30 * time.Second

	synctest.Test(t, func(t *testing.T) {
		backend := rekognition.NewInMemoryBackend("000000000000", "us-east-1")
		backend.SetLifecycleDelay(delay)
		h := rekognition.NewHandler(backend)

		require.Equal(t, http.StatusOK, doRequest(t, h, "CreateStreamProcessor", map[string]any{"Name": "sp"}).Code)

		status := func() string {
			rec := doRequest(t, h, "DescribeStreamProcessor", map[string]any{"Name": "sp"})

			return decodeMap(t, rec.Body.Bytes())["Status"].(string)
		}
		body := map[string]any{"Name": "sp"}

		assert.Equal(t, "STOPPED", status())
		require.Equal(t, http.StatusOK, doRequest(t, h, "StartStreamProcessor", body).Code)
		assert.Equal(t, "STARTING", status())

		dup := doRequest(t, h, "StartStreamProcessor", body)
		assert.Equal(t, http.StatusBadRequest, dup.Code)
		assert.Equal(t, "ResourceInUseException", decodeMap(t, dup.Body.Bytes())["__type"])

		time.Sleep(delay)
		assert.Equal(t, "RUNNING", status())

		require.Equal(t, http.StatusOK, doRequest(t, h, "StopStreamProcessor", body).Code)
		assert.Equal(t, "STOPPING", status())

		time.Sleep(delay)
		assert.Equal(t, "STOPPED", status())
	})
}

func TestRealism_DefaultStreamProcessorSettlesImmediately(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	body := map[string]any{"Name": "sp"}
	require.Equal(t, http.StatusOK, doRequest(t, h, "CreateStreamProcessor", body).Code)
	require.Equal(t, http.StatusOK, doRequest(t, h, "StartStreamProcessor", body).Code)

	got := decodeMap(t, doRequest(t, h, "DescribeStreamProcessor", body).Body.Bytes())
	assert.Equal(t, "RUNNING", got["Status"])
}

func TestRealism_DatasetLifecycle(t *testing.T) {
	t.Parallel()

	const delay = time.Minute

	synctest.Test(t, func(t *testing.T) {
		backend := rekognition.NewInMemoryBackend("000000000000", "us-east-1")
		backend.SetLifecycleDelay(delay)
		h := rekognition.NewHandler(backend)

		proj := decodeMap(t, doRequest(t, h, "CreateProject", map[string]any{"ProjectName": "p"}).Body.Bytes())
		ds := decodeMap(t, doRequest(t, h, "CreateDataset", map[string]any{
			"ProjectArn": proj["ProjectArn"], "DatasetType": "TRAIN",
		}).Body.Bytes())
		require.NotEmpty(t, ds["DatasetArn"])

		status := func() string {
			out := decodeMap(
				t,
				doRequest(t, h, "DescribeDataset", map[string]any{"DatasetArn": ds["DatasetArn"]}).Body.Bytes(),
			)

			return out["DatasetDescription"].(map[string]any)["Status"].(string)
		}

		assert.Equal(t, "CREATE_IN_PROGRESS", status())
		assert.Equal(
			t,
			http.StatusBadRequest,
			doRequest(t, h, "DeleteDataset", map[string]any{"DatasetArn": ds["DatasetArn"]}).Code,
		)

		time.Sleep(delay)
		assert.Equal(t, "CREATE_COMPLETE", status())
		assert.Equal(
			t,
			http.StatusOK,
			doRequest(t, h, "DeleteDataset", map[string]any{"DatasetArn": ds["DatasetArn"]}).Code,
		)
	})
}
