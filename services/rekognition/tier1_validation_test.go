package rekognition_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	rekognitionsdk "github.com/aws/aws-sdk-go-v2/service/rekognition"
	rektypes "github.com/aws/aws-sdk-go-v2/service/rekognition/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIndexFaces_OptionValidation(t *testing.T) {
	t.Parallel()

	img := &rektypes.Image{Bytes: []byte("img")}

	tests := []struct {
		name    string
		in      rekognitionsdk.IndexFacesInput
		wantErr bool
	}{
		{name: "defaults", in: rekognitionsdk.IndexFacesInput{Image: img}},
		{
			name: "valid-options",
			in: rekognitionsdk.IndexFacesInput{
				Image: img, QualityFilter: rektypes.QualityFilterHigh, MaxFaces: aws.Int32(3),
				DetectionAttributes: []rektypes.Attribute{rektypes.AttributeDefault, rektypes.AttributeFaceOccluded},
			},
		},
		{name: "bad-quality", wantErr: true, in: rekognitionsdk.IndexFacesInput{Image: img, QualityFilter: "BEST"}},
		{
			name: "bad-attribute", wantErr: true,
			in: rekognitionsdk.IndexFacesInput{Image: img, DetectionAttributes: []rektypes.Attribute{"NOPE"}},
		},
		{name: "zero-maxfaces", wantErr: true, in: rekognitionsdk.IndexFacesInput{Image: img, MaxFaces: aws.Int32(0)}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestHandlerAndClient(t)
			_, err := client.CreateCollection(t.Context(), &rekognitionsdk.CreateCollectionInput{
				CollectionId: aws.String("c"),
			})
			require.NoError(t, err)

			in := tt.in
			in.CollectionId = aws.String("c")

			_, err = client.IndexFaces(t.Context(), &in)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "InvalidParameterException")

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestGetPersonTracking_SortByValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		sortBy  rektypes.PersonTrackingSortBy
		wantErr bool
	}{
		{name: "default"},
		{name: "index", sortBy: rektypes.PersonTrackingSortByIndex},
		{name: "timestamp", sortBy: rektypes.PersonTrackingSortByTimestamp},
		{name: "bogus", sortBy: "NAME", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestHandlerAndClient(t)
			start, err := client.StartPersonTracking(t.Context(), &rekognitionsdk.StartPersonTrackingInput{
				Video: &rektypes.Video{
					S3Object: &rektypes.S3Object{Bucket: aws.String("b"), Name: aws.String("v.mp4")},
				},
			})
			require.NoError(t, err)

			_, err = client.GetPersonTracking(t.Context(), &rekognitionsdk.GetPersonTrackingInput{
				JobId: start.JobId, SortBy: tt.sortBy,
			})
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "InvalidParameterException")

				return
			}

			require.NoError(t, err)
		})
	}
}
