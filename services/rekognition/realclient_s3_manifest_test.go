package rekognition_test

import (
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	rekognitionsdk "github.com/aws/aws-sdk-go-v2/service/rekognition"
	"github.com/aws/aws-sdk-go-v2/service/rekognition/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRealClient_IndexFacesAndCreateDatasetS3Validation checks missing S3 refs are rejected.
func TestRealClient_IndexFacesAndCreateDatasetS3Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		bucket  string
		wantErr bool
	}{
		{name: "missing_object", bucket: "missing-bucket", wantErr: true},
		{name: "existing_object", bucket: "good-bucket", wantErr: false},
	}

	for _, tt := range tests {
		t.Run("index_faces_"+tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRoundTripClient(t, newWiredHandler(t, map[string]bool{"good-bucket/a.jpg": true}))
			_, err := client.CreateCollection(t.Context(), &rekognitionsdk.CreateCollectionInput{
				CollectionId: aws.String("c1"),
			})
			require.NoError(t, err)

			_, err = client.IndexFaces(t.Context(), &rekognitionsdk.IndexFacesInput{
				CollectionId: aws.String("c1"),
				Image: &types.Image{S3Object: &types.S3Object{
					Bucket: aws.String(tt.bucket), Name: aws.String("a.jpg"),
				}},
			})

			var invalid *types.InvalidS3ObjectException

			assert.Equal(t, tt.wantErr, errors.As(err, &invalid))

			if !tt.wantErr {
				require.NoError(t, err)
			}
		})

		t.Run("create_dataset_"+tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRoundTripClient(t, newWiredHandler(t, map[string]bool{"good-bucket/a.jsonl": true}))
			proj, err := client.CreateProject(t.Context(), &rekognitionsdk.CreateProjectInput{
				ProjectName: aws.String("p1"),
			})
			require.NoError(t, err)

			_, err = client.CreateDataset(t.Context(), &rekognitionsdk.CreateDatasetInput{
				ProjectArn:  proj.ProjectArn,
				DatasetType: types.DatasetTypeTrain,
				DatasetSource: &types.DatasetSource{GroundTruthManifest: &types.GroundTruthManifest{
					S3Object: &types.S3Object{Bucket: aws.String(tt.bucket), Name: aws.String("a.jsonl")},
				}},
			})

			var invalid *types.InvalidS3ObjectException

			assert.Equal(t, tt.wantErr, errors.As(err, &invalid))

			if !tt.wantErr {
				require.NoError(t, err)
			}
		})
	}
}
